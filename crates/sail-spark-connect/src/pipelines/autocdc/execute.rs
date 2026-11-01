//! Admission of AutoCDC graphs into `execution::execute`: bounded streaming
//! graphs whose every output is an SCD1 AutoCDC table over a registered
//! bounded source (decision 2 of `CDC-ACTIVATION-DESIGN.md`). Anything else
//! stays refused before its first write.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use sail_catalog::error::CatalogError;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::extension::{SessionExtension, SessionExtensionAccessor};
use sail_common_datafusion::task_resources::TaskResourceScope;
use sail_plan::config::PlanConfig;
use sail_plan::error::PlanError;
use sail_sdp_control::control::Control;
use sail_sdp_control::store::SystemClock;
use sail_sdp_control::turso_store::TursoStore;
use tokio::runtime::Handle;

use super::ScdType;
use super::durable::{DurableOutcome, DurableRun, run_durable_flow};
use super::flow_source::FlowSource;
use super::run::{FlowRun, checkpoint_dir};
use super::schema::{Case, METADATA_COLUMN};
use super::write::{IN_COMMIT_TIMESTAMPS_PROPERTY, Scd1Write, flow_output_schema, read_table};
use crate::error::{SparkError, SparkResult, StructuredSparkError};
use crate::pipelines::analysis::{AnalyzedGraph, GraphScope};
use crate::pipelines::append::spec_schema;
use crate::pipelines::graph::{Flow, GraphDefinition, Name, Output};
use crate::pipelines::reanalysis::resolve_inputs;
use crate::pipelines::schema_merge::merge_fields;
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
    let name: Name = name.iter().map(|x| x.to_ascii_lowercase()).collect();
    graph.qualify(&name).map(Some)
}

/// Whether this graph is one the durable run can execute: only AutoCDC SCD1
/// tables, each flow over a registered bounded source.
pub(in crate::pipelines) fn admitted(
    _ctx: &SessionContext,
    graph: &GraphDefinition,
) -> SparkResult<bool> {
    for flow in graph.flows.iter().filter(|flow| flow.auto_cdc.is_some()) {
        let args = flow
            .auto_cdc
            .as_ref()
            .ok_or_else(|| SparkError::internal("AutoCDC flow has no arguments"))?;
        if !matches!(args.scd_type, ScdType::Type1) {
            return Ok(false);
        }
        let target = graph.outputs.iter().find(|x| x.name == flow.target);
        if target.is_none_or(|target| target.kind != OutputType::Table) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The graph inputs of a flow, resolved now.
async fn flow_relations(
    ctx: &SessionContext,
    graph: &GraphDefinition,
    analyzed: &AnalyzedGraph,
    flow: &Flow,
    config: Arc<PlanConfig>,
) -> SparkResult<BTreeMap<Name, LogicalPlan>> {
    let accepted = analyzed
        .accepted_confs
        .get(&flow.name)
        .ok_or_else(|| SparkError::internal("accepted flow configuration missing"))?;
    resolve_inputs(ctx, graph, analyzed, &flow.target, config, accepted).await
}

/// Every output as an empty plan of its analyzed schema. A run is prepared
/// before the outputs a flow reads exist, and only needs their shape.
fn placeholder_relations(analyzed: &AnalyzedGraph) -> SparkResult<BTreeMap<Name, LogicalPlan>> {
    analyzed
        .schemas
        .iter()
        .map(|(name, schema)| {
            let table = MemTable::try_new(schema.clone(), vec![vec![]])?;
            let plan = LogicalPlanBuilder::scan(
                name.join("."),
                provider_as_source(Arc::new(table)),
                None,
            )?
            .build()?;
            Ok((name.clone(), plan))
        })
        .collect()
}

/// A flow's directory under the warehouse, which outlives its resets.
fn flow_root(warehouse: &std::path::Path, flow: &Flow) -> PathBuf {
    warehouse
        .join("_checkpoints")
        .join(flow.target.join("."))
        .join(flow.name.join("."))
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

/// Analysis gives an AutoCDC target the schema of the query that feeds it;
/// what the flow writes is `AutoCdcMergeFlow.schema`.
async fn with_output_schemas(
    ctx: &SessionContext,
    spark: &SparkSession,
    graph: &GraphDefinition,
    sources: &FlowSources,
    analyzed: &AnalyzedGraph,
) -> SparkResult<BTreeMap<Name, SchemaRef>> {
    let mut schemas = analyzed.schemas.clone();
    for flow in graph.flows.iter().filter(|flow| flow.auto_cdc.is_some()) {
        let args = flow
            .auto_cdc
            .as_ref()
            .ok_or_else(|| SparkError::internal("admitted flow is not AutoCDC"))?;
        let source = sources.of(flow)?;
        let config = flow_config(spark, analyzed, flow)?;
        let case = case_of(&config);
        let schema = flow_output_schema(ctx, config, args, case, &source.schema()).await?;
        schemas.insert(flow.target.clone(), schema);
    }
    Ok(schemas)
}

/// What the flows of `target` write, in flow order: an AutoCDC flow writes
/// `AutoCdcMergeFlow.schema`, not its query's schema, and deriving that runs
/// the schema validators, so a dry run reports what a run would.
pub(in crate::pipelines) async fn flow_schemas(
    ctx: &SessionContext,
    graph: &GraphDefinition,
    target: &Name,
    plans: &[LogicalPlan],
) -> SparkResult<Vec<SchemaRef>> {
    let flows = graph.flows.iter().filter(|flow| &flow.target == target);
    let mut schemas = Vec::with_capacity(plans.len());
    for (flow, plan) in flows.zip(plans) {
        let schema: SchemaRef = Arc::new(plan.schema().as_arrow().clone());
        schemas.push(match &flow.auto_cdc {
            Some(args) => {
                let config = ctx.extension::<SparkSession>()?.plan_config()?;
                let case = case_of(&config);
                flow_output_schema(ctx, config, args, case, &schema).await?
            }
            None => schema,
        });
    }
    Ok(schemas)
}

/// What each AutoCDC flow reads, by flow name.
#[derive(Default)]
struct FlowSources(
    BTreeMap<Name, Arc<dyn BoundedSource + Send + Sync>>,
    BTreeMap<Name, Arc<FlowSource>>,
);

impl FlowSources {
    fn of(&self, flow: &Flow) -> SparkResult<Arc<dyn BoundedSource + Send + Sync>> {
        if let Some(source) = self.1.get(&flow.name) {
            return Ok(source.clone());
        }
        self.0
            .get(&flow.name)
            .cloned()
            .ok_or_else(|| SparkError::internal("admitted flow has no source"))
    }
}

/// `StructType.merge` matches names exactly, so a column that differs from
/// the target only by case becomes a second column, and Spark's write then
/// finds the name ambiguous under case-insensitive resolution.
fn check_case_only_names(
    table: &Name,
    current: &datafusion::arrow::datatypes::Schema,
    output: &SchemaRef,
) -> SparkResult<()> {
    for field in output.fields() {
        let matches = current
            .fields()
            .iter()
            .filter(|existing| existing.name().eq_ignore_ascii_case(field.name()));
        let names: Vec<_> = matches.map(|existing| existing.name().clone()).collect();
        if !names.is_empty() && !names.contains(field.name()) {
            let qualified = |column: &str| {
                let mut parts = table.clone();
                parts.push(column.to_string());
                parts
                    .iter()
                    .map(|part| format!("`{part}`"))
                    .collect::<Vec<_>>()
                    .join(".")
            };
            let name = qualified(field.name());
            let references = format!("[{}, {}]", qualified(&names[0]), name);
            return Err(Box::new(StructuredSparkError {
                class: "org.apache.spark.sql.AnalysisException".into(),
                condition: "AMBIGUOUS_REFERENCE".into(),
                sql_state: "42704".into(),
                message: format!("Reference {name} is ambiguous, could be: {references}."),
                parameters: HashMap::from([
                    ("name".into(), name),
                    ("referenceNames".into(), references),
                ]),
            })
            .into());
        }
    }
    Ok(())
}

/// A top-level column the flow stopped writing is left alone, but Spark's
/// writer cannot fill a nested field in, so it refuses one that is gone.
fn check_nested_fields(
    current: &datafusion::arrow::datatypes::Schema,
    output: &SchemaRef,
) -> SparkResult<()> {
    fn missing(current: &DataType, output: &DataType, path: String) -> Option<String> {
        match (current, output) {
            (DataType::Struct(current), DataType::Struct(output)) => {
                current.iter().find_map(|field| {
                    let path = format!("{path}.`{}`", field.name());
                    match output.iter().find(|other| other.name() == field.name()) {
                        Some(other) => missing(field.data_type(), other.data_type(), path),
                        None => Some(path),
                    }
                })
            }
            (DataType::List(current), DataType::List(output)) => missing(
                current.data_type(),
                output.data_type(),
                format!("{path}.`element`"),
            ),
            _ => None,
        }
    }
    for field in current.fields() {
        let Ok(other) = output.field_with_name(field.name()) else {
            continue;
        };
        let path = format!("`{}`", field.name());
        if let Some(column) = missing(field.data_type(), other.data_type(), path) {
            return Err(Box::new(StructuredSparkError {
                class: "org.apache.spark.sql.AnalysisException".into(),
                condition: "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA".into(),
                sql_state: "KD000".into(),
                message: format!(
                    "Cannot write incompatible data for the table ``: Cannot find data for the \
                     output column {column}."
                ),
                parameters: HashMap::from([
                    ("tableName".into(), "``".into()),
                    ("colName".into(), column),
                ]),
            })
            .into());
        }
    }
    Ok(())
}

fn flow_config(
    spark: &SparkSession,
    analyzed: &AnalyzedGraph,
    flow: &Flow,
) -> SparkResult<Arc<PlanConfig>> {
    let accepted = analyzed
        .accepted_confs
        .get(&flow.name)
        .ok_or_else(|| SparkError::internal("accepted flow configuration missing"))?;
    spark.plan_config_with_overrides(accepted.clone())
}

fn case_of(config: &PlanConfig) -> Case {
    if config.case_sensitive {
        Case::Sensitive
    } else {
        Case::Insensitive
    }
}

/// The sequencing type the analyzed target schema carries in its metadata
/// column (`AutoCdcMergeFlow.sequencingType`).
fn sequencing_type(
    schemas: &BTreeMap<Name, SchemaRef>,
    target: &Name,
) -> SparkResult<spec::DataType> {
    let schema = schemas
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

/// What one pipeline run needs for its AutoCDC flows: their tables exist, and
/// the authority that owns them is open.
pub(in crate::pipelines) struct CdcRun {
    pub graph: GraphDefinition,
    /// The outputs AutoCDC flows write. The batch path leaves these alone.
    pub targets: BTreeSet<Name>,
    sources: FlowSources,
    schemas: BTreeMap<Name, SchemaRef>,
    warehouse: PathBuf,
    control: Arc<Control<TursoStore>>,
    owner: String,
    scope: TaskResourceScope,
}

impl CdcRun {
    /// Materialize the AutoCDC targets and open the authority.
    pub(in crate::pipelines) async fn prepare(
        ctx: &SessionContext,
        graph: &GraphDefinition,
        analyzed: &AnalyzedGraph,
        selection: &Selection,
    ) -> SparkResult<Self> {
        let spark = ctx.extension::<SparkSession>()?;
        let warehouse = local_warehouse(&spark.plan_config()?.default_warehouse_directory)?;
        let registered = ctx
            .extension::<BoundedSources>()
            .map(|x| x.as_ref().clone())
            .unwrap_or_default();
        let placeholders = placeholder_relations(analyzed)?;
        let mut sources = FlowSources::default();
        for flow in graph.flows.iter().filter(|flow| flow.auto_cdc.is_some()) {
            let bare = source_of(graph, flow)?.and_then(|name| registered.0.get(&name).cloned());
            if let Some(source) = bare {
                sources.0.insert(flow.name.clone(), source);
                continue;
            }
            let config = flow_config(&spark, analyzed, flow)?;
            let scope = GraphScope::new(graph, &placeholders, flow);
            let root = flow_root(&warehouse, flow);
            let source = FlowSource::discover(ctx, &config, &scope, graph, flow, &root).await?;
            sources.1.insert(flow.name.clone(), Arc::new(source));
        }
        let schemas = with_output_schemas(ctx, &spark, graph, &sources, analyzed).await?;
        let graph = with_commit_timestamps(graph);
        let targets: BTreeSet<Name> = graph
            .flows
            .iter()
            .filter(|flow| flow.auto_cdc.is_some())
            .map(|flow| flow.target.clone())
            .collect();
        // Materializing an existing table commits outside the authority, so
        // only tables that do not exist yet are created.
        let manager = ctx.extension::<CatalogManager>()?;
        let selected: Vec<&Name> = targets.iter().filter(|name| selection.runs(name)).collect();
        let mut missing = BTreeSet::new();
        for name in selected.iter().copied() {
            match manager.get_table_or_view(name).await {
                Ok(_) => {}
                Err(CatalogError::NotFound(..)) => {
                    missing.insert(name.clone());
                }
                Err(error) => return Err(PlanError::from(error).into()),
            }
        }
        // `DatasetManager` merges the existing schema with the flow's and
        // fails here, before any write, when they cannot merge.
        // A full refresh takes the flow's schema, so nothing has to merge.
        for name in selected
            .iter()
            .copied()
            .filter(|name| !missing.contains(*name) && !selection.reset.contains(*name))
        {
            let Some(output_schema) = schemas.get(name) else {
                continue;
            };
            let config = spark.plan_config()?;
            let current = read_table(ctx, config.clone(), name).await?;
            if !config.case_sensitive {
                check_case_only_names(name, current.schema().as_arrow(), output_schema)?;
            }
            check_nested_fields(current.schema().as_arrow(), output_schema)?;
            merge_fields(
                &spec_schema(current.schema().as_arrow())?.fields,
                &spec_schema(output_schema)?.fields,
            )?;
        }
        // A reset runs through the authority inside the flow; the plain
        // truncate would write around the fence.
        let creation = Selection {
            refresh: missing,
            reset: BTreeSet::new(),
        };
        let deferred =
            crate::pipelines::materialize::tables(ctx, &graph, &schemas, &creation).await?;
        if let Some(failure) = deferred.into_values().next() {
            return Err(Box::new(failure).into());
        }
        let directory = warehouse.join("_control");
        std::fs::create_dir_all(&directory)
            .map_err(|e| SparkError::internal(format!("create {}: {e}", directory.display())))?;
        let path = directory.join("authority.db");
        let store = TursoStore::open(&path.to_string_lossy(), &uuid::Uuid::new_v4().to_string())
            .await
            .map_err(|e| SparkError::internal(format!("control authority: {e}")))?;
        Ok(Self {
            graph,
            targets,
            sources,
            schemas,
            warehouse,
            control: Arc::new(Control::new(store, Arc::new(SystemClock))),
            owner: uuid::Uuid::new_v4().to_string(),
            scope: TaskResourceScope::new(Handle::current()),
        })
    }

    /// Run one AutoCDC flow to the frontier it captures. A failure leaves the
    /// next attempt or run to recover what is unfinished.
    pub(in crate::pipelines) async fn run_flow(
        &self,
        ctx: &SessionContext,
        analyzed: &AnalyzedGraph,
        selection: &Selection,
        flow: &Flow,
    ) -> SparkResult<DurableOutcome> {
        let spark = ctx.extension::<SparkSession>()?;
        let graph = &self.graph;
        let name = &flow.target;
        let output = graph
            .outputs
            .iter()
            .find(|output| &output.name == name)
            .ok_or_else(|| SparkError::internal("analyzed output missing"))?;
        let args = flow
            .auto_cdc
            .as_ref()
            .ok_or_else(|| SparkError::internal("admitted flow is not AutoCDC"))?;
        let source_name = source_of(graph, flow)?.unwrap_or_default();
        let source = self.sources.of(flow)?;
        let config = flow_config(&spark, analyzed, flow)?;
        let sequencing = sequencing_type(&self.schemas, name)?;
        let flow_name = flow.name.join(".");
        let write = Scd1Write {
            flow_name: &flow_name,
            destination: name,
            destination_format: output_format(output),
            args,
            sequencing_type: &sequencing,
            case: case_of(&config),
        };
        let checkpoint = checkpoint_dir(&self.warehouse, name, &flow.name, false)?;
        let relations = flow_relations(ctx, graph, analyzed, flow, config.clone()).await?;
        let run = FlowRun {
            graph,
            flow,
            relations: &relations,
            source_name: &source_name,
            source: source.as_ref(),
            write: &write,
            checkpoint: &checkpoint,
            max_rows_per_partition: MAX_ROWS_PER_PARTITION,
            flow_source: self.sources.1.get(&flow.name).map(Arc::as_ref),
        };
        run_durable_flow(
            ctx,
            config,
            &self.scope,
            &DurableRun {
                run: &run,
                control: self.control.clone(),
                owner: &self.owner,
                lease_ms: LEASE_MS,
                reset: crate::pipelines::materialize::resets(output, selection),
                stage_hook: None,
            },
        )
        .await
    }
}

/// Run every selected AutoCDC flow of a graph in order.
#[cfg(test)]
pub(in crate::pipelines) async fn execute_graph(
    ctx: &SessionContext,
    graph: &GraphDefinition,
    analyzed: &AnalyzedGraph,
    selection: &Selection,
) -> SparkResult<Vec<DurableOutcome>> {
    let run = CdcRun::prepare(ctx, graph, analyzed, selection).await?;
    let mut outcomes = Vec::new();
    for name in &analyzed.order {
        let Some(flow) = run.graph.flows.iter().find(|flow| &flow.target == name) else {
            continue;
        };
        if selection.runs(name) {
            outcomes.push(run.run_flow(ctx, analyzed, selection, flow).await?);
        }
    }
    Ok(outcomes)
}
