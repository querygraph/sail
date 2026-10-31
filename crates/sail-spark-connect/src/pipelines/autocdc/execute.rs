//! Admission of AutoCDC graphs into `execution::execute`: bounded streaming
//! graphs whose every output is an SCD1 AutoCDC table over a registered
//! bounded source (decision 2 of `CDC-ACTIVATION-DESIGN.md`). Anything else
//! stays refused before its first write.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::extension::{SessionExtension, SessionExtensionAccessor};
use sail_common_datafusion::task_resources::TaskResourceScope;
use sail_plan::error::PlanError;
use sail_sdp_control::control::Control;
use sail_sdp_control::store::SystemClock;
use sail_sdp_control::turso_store::TursoStore;
use tokio::runtime::Handle;

use super::ScdType;
use super::durable::{DurableOutcome, DurableRun, run_durable_flow};
use super::run::{FlowRun, checkpoint_dir};
use super::schema::{Case, METADATA_COLUMN};
use super::write::{IN_COMMIT_TIMESTAMPS_PROPERTY, Scd1Write};
use crate::error::{SparkError, SparkResult};
use crate::pipelines::analysis::AnalyzedGraph;
use crate::pipelines::graph::{Flow, GraphDefinition, Name, Output};
use crate::pipelines::selection::Selection;
use crate::pipelines::streaming::source::BoundedSource;
use crate::session::SparkSession;
use crate::spark::connect::{OutputType, pipeline_command as pc};

/// A lease long enough to outlive one microbatch; each batch renews it.
const LEASE_MS: i64 = 60_000;
/// The microbatch bound per partition.
const MAX_ROWS_PER_PARTITION: usize = 10_000;

/// The session's bounded sources, by the qualified name a flow reads.
#[derive(Clone, Default)]
pub(super) struct BoundedSources(pub BTreeMap<Name, Arc<dyn BoundedSource + Send + Sync>>);

impl SessionExtension for BoundedSources {
    fn name() -> &'static str {
        "BoundedSources"
    }
}

/// The qualified name the flow's top-level streaming read names.
fn source_of(graph: &GraphDefinition, flow: &Flow) -> SparkResult<Option<Name>> {
    let spec::QueryNode::Read {
        read_type: spec::ReadType::NamedTable(read),
        is_streaming: true,
    } = &flow.query.node
    else {
        return Ok(None);
    };
    let name: Vec<String> = read.name.clone().into();
    graph.qualify(&name).map(Some)
}

/// Whether this graph is one the durable run can execute: only AutoCDC SCD1
/// tables, each flow over a registered bounded source.
pub(in crate::pipelines) fn admitted(
    ctx: &SessionContext,
    graph: &GraphDefinition,
) -> SparkResult<bool> {
    let Ok(sources) = ctx.extension::<BoundedSources>() else {
        return Ok(false);
    };
    if graph.outputs.iter().any(|x| x.kind != OutputType::Table) {
        return Ok(false);
    }
    for flow in &graph.flows {
        let Some(args) = &flow.auto_cdc else {
            return Ok(false);
        };
        if !matches!(args.scd_type, ScdType::Type1) {
            return Ok(false);
        }
        match source_of(graph, flow)? {
            Some(source) if sources.0.contains_key(&source) => {}
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// The local warehouse directory. The authority is a file owned by one
/// process, so a remote warehouse is refused rather than given a local one.
fn local_warehouse(directory: &str) -> SparkResult<PathBuf> {
    let path = directory.strip_prefix("file://").unwrap_or(directory);
    if path.contains("://") {
        return Err(SparkError::unsupported(
            "a durable AutoCDC run needs a local warehouse directory",
        ));
    }
    Ok(PathBuf::from(path))
}

/// The sequencing type the analyzed target schema carries in its metadata
/// column (`AutoCdcMergeFlow.sequencingType`).
fn sequencing_type(analyzed: &AnalyzedGraph, target: &Name) -> SparkResult<spec::DataType> {
    let schema = analyzed
        .schemas
        .get(target)
        .ok_or_else(|| SparkError::internal("analyzed output schema missing"))?;
    let (_, metadata) = schema.fields().find(METADATA_COLUMN).ok_or_else(|| {
        SparkError::internal("AutoCDC output has no metadata column in its schema")
    })?;
    let datafusion::arrow::datatypes::DataType::Struct(members) = metadata.data_type() else {
        return Err(SparkError::internal(
            "AutoCDC metadata column is not a struct",
        ));
    };
    let member = members
        .first()
        .ok_or_else(|| SparkError::internal("AutoCDC metadata column has no members"))?;
    crate::spark::connect::DataType::try_from(member.data_type().clone())?.try_into()
}

/// The graph with every output committing in-commit timestamps, which
/// ratified staging needs; tables are enrolled the way they were created.
fn with_commit_timestamps(graph: &GraphDefinition) -> GraphDefinition {
    let mut graph = graph.clone();
    for output in &mut graph.outputs {
        if let Some(pc::define_output::Details::TableDetails(details)) =
            &mut output.definition.details
        {
            details.table_properties.insert(
                IN_COMMIT_TIMESTAMPS_PROPERTY.to_string(),
                "true".to_string(),
            );
        }
    }
    graph
}

fn output_format(output: &Output) -> Option<&str> {
    match &output.definition.details {
        Some(pc::define_output::Details::TableDetails(details)) => details.format.as_deref(),
        _ => None,
    }
}

/// Execute an admitted graph: materialize its tables, then run each flow to
/// the frontier it captures. One failure stops the run, and the next run
/// recovers what it left unfinished.
pub(in crate::pipelines) async fn execute_graph(
    ctx: &SessionContext,
    graph: &GraphDefinition,
    analyzed: &AnalyzedGraph,
    selection: &Selection,
) -> SparkResult<Vec<DurableOutcome>> {
    // A reset must run through the authority; the plain truncate would write
    // around the fence.
    if !selection.reset.is_empty() {
        return Err(SparkError::unsupported(
            "full refresh of a durable AutoCDC table",
        ));
    }
    let graph = with_commit_timestamps(graph);
    // Materializing an existing table commits outside the authority, so only
    // tables that do not exist yet are created. An enrolled table's schema
    // does not change here.
    let manager = ctx.extension::<CatalogManager>()?;
    let mut missing = BTreeSet::new();
    for name in selection.refresh.iter() {
        match manager.get_table_or_view(name).await {
            Ok(_) => {}
            Err(CatalogError::NotFound(..)) => {
                missing.insert(name.clone());
            }
            Err(error) => return Err(PlanError::from(error).into()),
        }
    }
    let creation = Selection {
        refresh: missing,
        reset: BTreeSet::new(),
    };
    crate::pipelines::materialize::tables(ctx, &graph, &analyzed.schemas, &creation).await?;
    let spark = ctx.extension::<SparkSession>()?;
    let sources = ctx.extension::<BoundedSources>()?;
    let warehouse = local_warehouse(&spark.plan_config()?.default_warehouse_directory)?;
    let directory = warehouse.join("_control");
    std::fs::create_dir_all(&directory)
        .map_err(|e| SparkError::internal(format!("create {}: {e}", directory.display())))?;
    let path = directory.join("authority.db");
    let store = TursoStore::open(&path.to_string_lossy(), &uuid::Uuid::new_v4().to_string())
        .await
        .map_err(|e| SparkError::internal(format!("control authority: {e}")))?;
    let control = Arc::new(Control::new(store, Arc::new(SystemClock)));
    let owner = uuid::Uuid::new_v4().to_string();
    let scope = TaskResourceScope::new(Handle::current());
    let relations = BTreeMap::new();
    let mut outcomes = Vec::new();
    for name in &analyzed.order {
        let Some(flow) = graph.flows.iter().find(|flow| &flow.target == name) else {
            continue;
        };
        if !selection.runs(name) {
            continue;
        }
        let output = graph
            .outputs
            .iter()
            .find(|output| &output.name == name)
            .ok_or_else(|| SparkError::internal("analyzed output missing"))?;
        let args = flow
            .auto_cdc
            .as_ref()
            .ok_or_else(|| SparkError::internal("admitted flow is not AutoCDC"))?;
        let source_name = source_of(&graph, flow)?
            .ok_or_else(|| SparkError::internal("admitted flow has no streaming source"))?;
        let source = sources
            .0
            .get(&source_name)
            .ok_or_else(|| SparkError::internal("admitted source is not registered"))?;
        let accepted = analyzed
            .accepted_confs
            .get(&flow.name)
            .ok_or_else(|| SparkError::internal("accepted flow configuration missing"))?;
        let config = spark.plan_config_with_overrides(accepted.clone())?;
        let sequencing = sequencing_type(analyzed, name)?;
        let flow_name = flow.name.join(".");
        let write = Scd1Write {
            flow_name: &flow_name,
            destination: name,
            destination_format: output_format(output),
            args,
            sequencing_type: &sequencing,
            case: if config.case_sensitive {
                Case::Sensitive
            } else {
                Case::Insensitive
            },
        };
        let checkpoint = checkpoint_dir(&warehouse, name, &flow.name, false)?;
        let run = FlowRun {
            graph: &graph,
            flow,
            relations: &relations,
            source_name: &source_name,
            source: source.as_ref(),
            write: &write,
            checkpoint: &checkpoint,
            max_rows_per_partition: MAX_ROWS_PER_PARTITION,
        };
        outcomes.push(
            run_durable_flow(
                ctx,
                config,
                &scope,
                &DurableRun {
                    run: &run,
                    control: control.clone(),
                    owner: &owner,
                    lease_ms: LEASE_MS,
                    stage_hook: None,
                },
            )
            .await?,
        );
    }
    Ok(outcomes)
}
