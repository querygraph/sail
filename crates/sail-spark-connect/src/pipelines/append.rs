//! Triggered append flows: a streaming table fed by streaming reads.
//!
//! A streaming read of a file-backed table sees the files that this flow has
//! not read yet, as Spark's file stream source does. Files that were replaced
//! or deleted are not revisited, so `INSERT OVERWRITE` on a source adds only
//! its new rows. Batch reads in the same flow, such as the static side of a
//! join, see the latest data.
//!
//! Progress lives under `<storage>/_checkpoints/<catalog>/<db>/<table>/<flow>/<n>`,
//! with a full refresh starting `n + 1`. The files of a batch are recorded
//! before it is written and marked as read after, so a crash between the two
//! replays exactly that batch. That is at-least-once: the replay can duplicate
//! rows, because a plain file table has no idempotent commit.
//!
//! Flows run one at a time and each reads to the end of what it listed, as
//! one batch. Stateful operators and sources that are not plain files are
//! refused, not read wrongly.
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::{JoinType, LogicalPlan, LogicalPlanBuilder, cast, col, lit};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use percent_encoding::percent_decode_str;
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::datasource::is_lakehouse_format;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_plan::config::PlanConfig;
use sail_plan::error::{PlanError, PlanResult};
use sail_plan::resolver::PlanResolver;
use sail_plan::resolver::scope::RelationScope;
use serde::{Deserialize, Serialize};

use super::analysis::GraphScope;
use super::execution::run_command;
use super::graph::{Flow, GraphDefinition, Name};
use crate::error::{SparkError, SparkResult, StructuredSparkError};
use crate::spark::connect::OutputType;

const SOURCE_ALIAS: &str = "__sail_stream_source";
const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";
const STATE_FILE: &str = "state.json";
const MAX_VIEW_DEPTH: usize = 32;

/// One flow's progress. `pending` is the batch being written.
#[derive(Debug, Default, Serialize, Deserialize)]
struct FlowState {
    batches: u64,
    seen: BTreeMap<String, BTreeSet<String>>,
    pending: Option<BTreeMap<String, Vec<String>>>,
}

struct Checkpoint {
    store: Arc<dyn ObjectStore>,
    file: Path,
}

impl Checkpoint {
    async fn open(
        ctx: &SessionContext,
        storage: &str,
        destination: &Name,
        flow: &Name,
        reset: bool,
    ) -> SparkResult<Self> {
        let url = ListingTableUrl::parse(storage)?;
        let store = ctx.runtime_env().object_store(url.object_store())?;
        let flow_name = flow
            .last()
            .ok_or_else(|| SparkError::internal("flow without a name"))?;
        let directory = std::iter::once("_checkpoints")
            .chain(destination.iter().map(String::as_str))
            .chain(std::iter::once(flow_name.as_str()))
            .fold(url.prefix().clone(), |path, part| path.join(part));
        let listed = store
            .list_with_delimiter(Some(&directory))
            .await
            .map_err(storage_error)?;
        let latest = listed
            .common_prefixes
            .iter()
            .filter_map(|path| path.filename()?.parse::<u64>().ok())
            .max();
        let version = match (latest, reset) {
            (None, _) => 0,
            (Some(version), true) => version + 1,
            (Some(version), false) => version,
        };
        let file = directory.join(version.to_string()).join(STATE_FILE);
        Ok(Self { store, file })
    }

    async fn load(&self) -> SparkResult<FlowState> {
        let bytes = match self.store.get(&self.file).await {
            Ok(result) => result.bytes().await.map_err(storage_error)?,
            Err(object_store::Error::NotFound { .. }) => return Ok(FlowState::default()),
            Err(error) => return Err(storage_error(error)),
        };
        serde_json::from_slice(&bytes)
            .map_err(|e| SparkError::internal(format!("read {}: {e}", self.file)))
    }

    async fn store(&self, state: &FlowState) -> SparkResult<()> {
        let json = serde_json::to_vec(state)
            .map_err(|e| SparkError::internal(format!("write {}: {e}", self.file)))?;
        self.store
            .put(&self.file, PutPayload::from(json))
            .await
            .map_err(storage_error)?;
        Ok(())
    }
}

fn storage_error(error: object_store::Error) -> SparkError {
    SparkError::internal(format!("pipeline checkpoint: {error}"))
}

/// Normalize a read name the way the graph does.
fn qualified(scope: &GraphScope<'_>, name: &[String]) -> PlanResult<Name> {
    let raw: Name = name.iter().map(|x| x.to_ascii_lowercase()).collect();
    if scope.graph.outputs.iter().any(|x| x.name == raw) {
        Ok(raw)
    } else {
        scope.qualify(&raw)
    }
}

/// Forwards to the graph while noting what is read as a stream.
struct RecordingScope<'a> {
    graph: &'a GraphScope<'a>,
    streaming: Mutex<BTreeSet<Name>>,
}

impl RelationScope for RecordingScope<'_> {
    fn resolve(&self, name: &[String]) -> PlanResult<Option<LogicalPlan>> {
        self.graph.resolve(name)
    }

    fn resolve_read(&self, name: &[String], is_streaming: bool) -> PlanResult<Option<LogicalPlan>> {
        if is_streaming {
            self.streaming
                .lock()
                .map_err(|e| PlanError::internal(e.to_string()))?
                .insert(qualified(self.graph, name)?);
        }
        self.graph.resolve_read(name, is_streaming)
    }

    fn resolve_late_bound_read(
        &self,
        name: &[String],
        is_streaming: bool,
    ) -> PlanResult<Option<LogicalPlan>> {
        if is_streaming {
            self.streaming
                .lock()
                .map_err(|e| PlanError::internal(e.to_string()))?
                .insert(qualified(self.graph, name)?);
        }
        self.graph.resolve_late_bound_read(name, is_streaming)
    }

    fn qualify(&self, name: &[String]) -> PlanResult<Vec<String>> {
        self.graph.qualify(name)
    }

    fn default_namespace(&self) -> Option<Vec<String>> {
        self.graph.default_namespace()
    }
}

/// Answers each streaming read with the plan of its unread rows.
struct BatchScope<'a> {
    graph: &'a GraphScope<'a>,
    streams: &'a BTreeMap<Name, LogicalPlan>,
}

impl BatchScope<'_> {
    fn stream(&self, name: &[String]) -> PlanResult<Option<LogicalPlan>> {
        let name = qualified(self.graph, name)?;
        self.streams
            .get(&name)
            .cloned()
            .map(Some)
            .ok_or_else(|| PlanError::internal(format!("unbound stream {}", name.join("."))))
    }
}

impl RelationScope for BatchScope<'_> {
    fn resolve(&self, name: &[String]) -> PlanResult<Option<LogicalPlan>> {
        self.graph.resolve(name)
    }

    fn resolve_read(&self, name: &[String], is_streaming: bool) -> PlanResult<Option<LogicalPlan>> {
        if is_streaming {
            return self.stream(name);
        }
        self.graph.resolve_read(name, false)
    }

    fn resolve_late_bound_read(
        &self,
        name: &[String],
        is_streaming: bool,
    ) -> PlanResult<Option<LogicalPlan>> {
        if is_streaming {
            return self.stream(name);
        }
        self.graph.resolve_late_bound_read(name, false)
    }

    fn qualify(&self, name: &[String]) -> PlanResult<Vec<String>> {
        self.graph.qualify(name)
    }

    fn default_namespace(&self) -> Option<Vec<String>> {
        self.graph.default_namespace()
    }
}

/// The names a query reads as streams, in the order a batch must resolve them.
async fn streamed_names(
    ctx: &SessionContext,
    config: &Arc<PlanConfig>,
    scope: &GraphScope<'_>,
    query: &spec::QueryPlan,
) -> SparkResult<BTreeSet<Name>> {
    let recording = RecordingScope {
        graph: scope,
        streaming: Mutex::default(),
    };
    PlanResolver::new(ctx, config.clone())
        .with_relation_scope(&recording)
        .resolve_named_plan(spec::Plan::Query(query.clone()))
        .await?;
    recording
        .streaming
        .into_inner()
        .map_err(|e| SparkError::internal(e.to_string()))
}

fn view_flow<'a>(graph: &'a GraphDefinition, name: &Name) -> Option<&'a Flow> {
    let output = graph.outputs.iter().find(|output| &output.name == name)?;
    if output.view.is_none() && output.kind != OutputType::TemporaryView {
        return None;
    }
    graph.flows.iter().find(|flow| &flow.target == name)
}

/// A streaming table's source: one file-backed table.
struct TableSource {
    format: String,
    location: String,
    /// The columns stored in the files.
    schema: spec::Schema,
    /// Every column, partition columns included.
    empty: datafusion::arrow::datatypes::SchemaRef,
    partitions: BTreeSet<String>,
}

async fn table_source(ctx: &SessionContext, name: &Name) -> SparkResult<TableSource> {
    let manager = ctx.extension::<CatalogManager>()?;
    let status = manager
        .get_table_or_view(name)
        .await
        .map_err(PlanError::from)?;
    let TableKind::Table {
        columns,
        location,
        format,
        partition_by,
        ..
    } = status.kind
    else {
        return Err(SparkError::unsupported(
            "a streaming read of a catalog view",
        ));
    };
    if is_lakehouse_format(&format) {
        return Err(SparkError::unsupported(format!(
            "a streaming read of a {format} table"
        )));
    }
    let location = location
        .ok_or_else(|| SparkError::internal(format!("table {} has no location", name.join("."))))?;
    let fields = columns
        .iter()
        .map(|column| column.field())
        .collect::<Vec<_>>();
    let partitions = partition_by
        .iter()
        .map(|field| field.column.clone())
        .collect::<BTreeSet<_>>();
    let stored = fields
        .iter()
        .filter(|field| !partitions.contains(field.name()))
        .cloned()
        .collect::<Vec<_>>();
    let schema = spec_schema(&datafusion::arrow::datatypes::Schema::new(stored))?;
    let empty = Arc::new(datafusion::arrow::datatypes::Schema::new(fields));
    Ok(TableSource {
        format,
        location,
        schema,
        empty,
        partitions,
    })
}

fn spec_schema(schema: &datafusion::arrow::datatypes::Schema) -> SparkResult<spec::Schema> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let data_type: spec::DataType =
                crate::spark::connect::DataType::try_from(field.data_type().clone())?.try_into()?;
            Ok(spec::Field {
                name: field.name().clone(),
                data_type,
                nullable: field.is_nullable(),
                metadata: vec![],
            })
        })
        .collect::<SparkResult<Vec<_>>>()?;
    Ok(spec::Schema {
        fields: spec::Fields::from(fields),
    })
}

/// The table's data files, as full URLs. Hidden files are not data.
async fn data_files(ctx: &SessionContext, location: &str) -> SparkResult<BTreeSet<String>> {
    let url = ListingTableUrl::parse(location)?;
    let store = ctx.runtime_env().object_store(url.object_store())?;
    let base = url.as_str().trim_end_matches('/').to_string();
    let prefix = url.prefix().clone();
    let listed: Vec<_> = store
        .list(Some(&prefix))
        .try_collect()
        .await
        .map_err(storage_error)?;
    let prefix_parts = prefix.parts().count();
    Ok(listed
        .into_iter()
        .filter(|meta| {
            !meta
                .location
                .parts()
                .skip(prefix_parts)
                .any(|part| part.as_ref().starts_with(['_', '.']))
        })
        .map(|meta| {
            let relative = meta.location.parts().skip(prefix_parts);
            let relative = relative.map(|x| x.as_ref().to_string()).collect::<Vec<_>>();
            format!("{base}/{}", relative.join("/"))
        })
        .collect())
}

/// Partition values from `key=value` directories, in Hive's escaped form.
fn partition_values(file: &str, source: &TableSource) -> BTreeMap<String, Option<String>> {
    let relative = file
        .strip_prefix(source.location.trim_end_matches('/'))
        .unwrap_or(file);
    relative
        .split('/')
        .filter_map(|part| part.split_once('='))
        .filter(|(key, _)| source.partitions.contains(*key))
        .map(|(key, value)| {
            let value = (value != HIVE_DEFAULT_PARTITION)
                .then(|| percent_decode_str(value).decode_utf8_lossy().into_owned());
            (key.to_string(), value)
        })
        .collect()
}

/// `files` of one partition, with every column in the table's order.
async fn partition_plan(
    ctx: &SessionContext,
    config: &Arc<PlanConfig>,
    source: &TableSource,
    files: &[String],
    values: &BTreeMap<String, Option<String>>,
) -> SparkResult<LogicalPlan> {
    let read = spec::QueryPlan::new(spec::QueryNode::Read {
        read_type: spec::ReadType::DataSource(Box::new(spec::ReadDataSource {
            format: Some(source.format.clone()),
            schema: Some(source.schema.clone()),
            options: vec![],
            paths: files.to_vec(),
            predicates: vec![],
        })),
        is_streaming: false,
    });
    let resolved = PlanResolver::new(ctx, config.clone())
        .resolve_named_plan(spec::Plan::Query(read))
        .await?;
    let plan = match resolved.fields {
        Some(fields) => rename_logical_plan(resolved.plan, &fields)?,
        None => resolved.plan,
    };
    let columns = source
        .empty
        .fields()
        .iter()
        .map(|field| {
            if !source.partitions.contains(field.name()) {
                return col(Column::from_name(field.name()));
            }
            let value = values.get(field.name()).cloned().flatten();
            cast(lit(ScalarValue::Utf8(value)), field.data_type().clone()).alias(field.name())
        })
        .collect::<Vec<_>>();
    Ok(LogicalPlanBuilder::from(plan).project(columns)?.build()?)
}

/// The rows of `files`, with user-facing names, marked as a stream source.
async fn source_plan(
    ctx: &SessionContext,
    config: &Arc<PlanConfig>,
    source: &TableSource,
    files: &[String],
) -> SparkResult<LogicalPlan> {
    let mut groups: BTreeMap<Vec<(String, Option<String>)>, Vec<String>> = BTreeMap::new();
    for file in files {
        let values = partition_values(file, source);
        groups
            .entry(values.into_iter().collect())
            .or_default()
            .push(file.clone());
    }
    let mut plans = vec![];
    for (values, files) in groups {
        let values = values.into_iter().collect();
        plans.push(partition_plan(ctx, config, source, &files, &values).await?);
    }
    let mut plans = plans.into_iter();
    let builder = match plans.next() {
        None => {
            let table = MemTable::try_new(source.empty.clone(), vec![vec![]])?;
            LogicalPlanBuilder::scan(SOURCE_ALIAS, provider_as_source(Arc::new(table)), None)?
        }
        Some(first) => {
            let mut builder = LogicalPlanBuilder::from(first);
            for plan in plans {
                builder = builder.union(plan)?;
            }
            builder
        }
    };
    Ok(builder.alias(SOURCE_ALIAS)?.build()?)
}

fn reads_a_stream(plan: &LogicalPlan) -> datafusion::common::Result<bool> {
    plan.exists(|node| {
        Ok(matches!(node, LogicalPlan::SubqueryAlias(alias)
            if alias.alias.table() == SOURCE_ALIAS))
    })
}

fn append_mode_error() -> SparkError {
    Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION".into(),
        sql_state: "42KDE".into(),
        message: "Invalid streaming output mode: append. This output mode is not supported for \
                  streaming aggregations without watermark on streaming DataFrames/DataSets."
            .into(),
        parameters: HashMap::from([
            ("outputMode".into(), "append".into()),
            (
                "operation".into(),
                "streaming aggregations without watermark".into(),
            ),
        ]),
    })
    .into()
}

/// Refuse what needs state between batches, which this run does not keep.
fn check_stateless(plan: &LogicalPlan) -> SparkResult<()> {
    let mut failure = None;
    plan.apply(|node| {
        let refused = match node {
            LogicalPlan::Aggregate(_) if reads_a_stream(node)? => Some(append_mode_error()),
            LogicalPlan::Distinct(_) if reads_a_stream(node)? => {
                Some(SparkError::unsupported("streaming deduplication"))
            }
            LogicalPlan::Window(_) if reads_a_stream(node)? => {
                Some(SparkError::unsupported("a streaming window function"))
            }
            LogicalPlan::Sort(_) if reads_a_stream(node)? => {
                Some(SparkError::unsupported("sorting a streaming read"))
            }
            LogicalPlan::Limit(_) if reads_a_stream(node)? => {
                Some(SparkError::unsupported("limiting a streaming read"))
            }
            LogicalPlan::Join(join) => {
                let left = reads_a_stream(&join.left)?;
                let right = reads_a_stream(&join.right)?;
                let allowed = match join.join_type {
                    JoinType::Inner => !(left && right),
                    JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => !right,
                    JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => !left,
                    JoinType::Full | JoinType::LeftMark | JoinType::RightMark => !left && !right,
                };
                (!allowed).then(|| {
                    SparkError::unsupported(format!(
                        "a {} join of this streaming read",
                        join.join_type
                    ))
                })
            }
            _ => None,
        };
        if let Some(error) = refused {
            failure = Some(error);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    failure.map_or(Ok(()), Err)
}

/// Resolve the streams under a set of names: tables to their unread files, and
/// streaming views to their own query, over the same unread files.
#[expect(clippy::too_many_arguments)]
fn bind_streams<'a>(
    ctx: &'a SessionContext,
    config: &'a Arc<PlanConfig>,
    graph: &'a GraphDefinition,
    scope: &'a GraphScope<'a>,
    names: BTreeSet<Name>,
    state: &'a FlowState,
    batch: &'a mut BTreeMap<String, Vec<String>>,
    streams: &'a mut BTreeMap<Name, LogicalPlan>,
    depth: usize,
) -> futures::future::BoxFuture<'a, SparkResult<()>> {
    use futures::FutureExt;
    async move {
        if depth > MAX_VIEW_DEPTH {
            return Err(SparkError::internal("streaming views nest too deeply"));
        }
        for name in names {
            if streams.contains_key(&name) {
                continue;
            }
            if let Some(view) = view_flow(graph, &name) {
                let inner = streamed_names(ctx, config, scope, &view.query).await?;
                bind_streams(
                    ctx,
                    config,
                    graph,
                    scope,
                    inner,
                    state,
                    batch,
                    streams,
                    depth + 1,
                )
                .await?;
                let bound = BatchScope {
                    graph: scope,
                    streams,
                };
                let resolved = PlanResolver::new(ctx, config.clone())
                    .with_relation_scope(&bound)
                    .resolve_named_plan(spec::Plan::Query(view.query.clone()))
                    .await?;
                let plan = match resolved.fields {
                    Some(fields) => rename_logical_plan(resolved.plan, &fields)?,
                    None => resolved.plan,
                };
                streams.insert(name, plan);
                continue;
            }
            let source = table_source(ctx, &name).await?;
            let key = name.join(".");
            let files = match batch.get(&key) {
                Some(files) => files.clone(),
                None => {
                    let seen = state.seen.get(&key);
                    let files: Vec<String> = data_files(ctx, &source.location)
                        .await?
                        .into_iter()
                        .filter(|file| seen.is_none_or(|seen| !seen.contains(file)))
                        .collect();
                    batch.insert(key, files.clone());
                    files
                }
            };
            let plan = source_plan(ctx, config, &source, &files).await?;
            streams.insert(name, plan);
        }
        Ok(())
    }
    .boxed()
}

pub(super) struct AppendFlow<'a> {
    pub graph: &'a GraphDefinition,
    pub flow: &'a Flow,
    pub destination: &'a Name,
    pub inputs: &'a BTreeMap<Name, LogicalPlan>,
    pub write: spec::CommandNode,
    pub storage: &'a str,
    pub reset: bool,
}

/// Run one flow to the end of its unread input.
pub(super) async fn run(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    run: AppendFlow<'_>,
) -> SparkResult<()> {
    let scope = GraphScope::new(run.graph, run.inputs, run.flow);
    let names = streamed_names(ctx, &config, &scope, &run.flow.query).await?;
    if names.is_empty() {
        return Err(SparkError::internal(
            "a streaming table flow has no streaming read",
        ));
    }
    let checkpoint =
        Checkpoint::open(ctx, run.storage, run.destination, &run.flow.name, run.reset).await?;
    let mut state = checkpoint.load().await?;
    let replay = state.pending.is_some();
    let mut batch = state.pending.take().unwrap_or_default();
    let mut streams = BTreeMap::new();
    bind_streams(
        ctx,
        &config,
        run.graph,
        &scope,
        names,
        &state,
        &mut batch,
        &mut streams,
        0,
    )
    .await?;
    let bound = BatchScope {
        graph: &scope,
        streams: &streams,
    };
    let planned = PlanResolver::new(ctx, config.clone())
        .with_relation_scope(&bound)
        .resolve_named_plan(spec::Plan::Query(run.flow.query.clone()))
        .await?;
    check_stateless(&planned.plan)?;
    if !replay && batch.values().all(Vec::is_empty) {
        return Ok(());
    }
    state.pending = Some(batch);
    checkpoint.store(&state).await?;
    run_command(ctx, config, run.write, Some(&bound)).await?;
    for (source, files) in state.pending.take().unwrap_or_default() {
        state.seen.entry(source).or_default().extend(files);
    }
    state.batches += 1;
    checkpoint.store(&state).await
}
