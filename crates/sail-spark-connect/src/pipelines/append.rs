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
//! A streaming read of a data source lists its path the same way. Flows run
//! one at a time and each reads to the end of what it listed, as one batch.
//! A Delta source, as a table or a path, is read from its log instead, and its
//! progress is a version: see [`sail_delta_lake::stream`]. Other sources that
//! are not plain files are refused, not read wrongly.
//! The only stateful query is an aggregate over a tumbling window with a
//! watermark: a window's rows are held under the checkpoint until the
//! watermark passes its end, and then aggregated once. Rows behind the
//! watermark are dropped, as Spark drops late rows.
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{DataType, Int64Type, TimeUnit};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder, cast, col, lit};
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
use sail_common_datafusion::session::job::JobService;
use sail_delta_lake::stream::{
    DeltaStream, DeltaStreamError, DeltaStreamFailure, DeltaStreamOffset, DeltaStreamRange,
};
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

pub(super) const SOURCE_ALIAS: &str = "__sail_stream_source";
const STREAM_SOURCE_PART: &str = "__sail_data_source";
const FILE_FORMATS: [&str; 6] = ["parquet", "orc", "json", "csv", "text", "avro"];
const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";
const STATE_FILE: &str = "state.json";
const MAX_VIEW_DEPTH: usize = 32;

/// One flow's progress. `pending` is the batch being written.
#[derive(Debug, Default, Serialize, Deserialize)]
struct FlowState {
    batches: u64,
    seen: BTreeMap<String, BTreeSet<String>>,
    pending: Option<BTreeMap<String, Vec<String>>>,
    /// The sources this flow reads; changing them needs a full refresh.
    #[serde(default)]
    sources: BTreeSet<String>,
    /// The event time below which rows are late, in microseconds.
    #[serde(default)]
    watermark: Option<i64>,
    /// Rows of windows still open, as files under the checkpoint.
    #[serde(default)]
    retained: Vec<String>,
    /// How far each Delta source has been read.
    #[serde(default)]
    delta: BTreeMap<String, DeltaStreamOffset>,
    /// The ranges of the batch being written. `None` is a source with nothing
    /// to read yet, which still counts as one of this flow's sources.
    #[serde(default)]
    pending_delta: Option<BTreeMap<String, Option<DeltaStreamRange>>>,
}

/// What one batch reads from each source.
#[derive(Default)]
struct Batch {
    files: BTreeMap<String, Vec<String>>,
    delta: BTreeMap<String, Option<DeltaStreamRange>>,
    delta_rows: bool,
}

impl Batch {
    fn has_rows(&self) -> bool {
        self.delta_rows || self.files.values().any(|files| !files.is_empty())
    }

    fn sources(&self) -> BTreeSet<String> {
        self.files
            .keys()
            .chain(self.delta.keys())
            .cloned()
            .collect()
    }
}

struct Checkpoint {
    store: Arc<dyn ObjectStore>,
    file: Path,
    /// This checkpoint version's directory, as a URL.
    root: String,
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
        let version_directory = directory.join(version.to_string());
        let file = version_directory.clone().join(STATE_FILE);
        let relative = version_directory
            .parts()
            .skip(url.prefix().parts().count())
            .map(|part| part.as_ref().to_string())
            .collect::<Vec<_>>()
            .join("/");
        let root = format!("{}/{relative}", url.as_str().trim_end_matches('/'));
        Ok(Self { store, file, root })
    }

    /// Where the open windows of a batch are written.
    fn state_url(&self, batch: u64) -> SparkResult<String> {
        Ok(format!("{}/state/{batch}", self.root))
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
pub(super) fn qualified(scope: &GraphScope<'_>, name: &[String]) -> PlanResult<Name> {
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
    sources: Mutex<Vec<spec::ReadDataSource>>,
    options: Mutex<BTreeMap<Name, Vec<(String, String)>>>,
}

impl RelationScope for RecordingScope<'_> {
    fn resolve(&self, name: &[String]) -> PlanResult<Option<LogicalPlan>> {
        self.graph.resolve(name)
    }

    fn observe_read_options(&self, name: &[String], options: &[(String, String)]) {
        if let (Ok(name), Ok(mut recorded)) = (qualified(self.graph, name), self.options.lock()) {
            recorded.insert(name, options.to_vec());
        }
    }

    fn resolve_source_stream(
        &self,
        source: &spec::ReadDataSource,
    ) -> PlanResult<Option<LogicalPlan>> {
        self.sources
            .lock()
            .map_err(|e| PlanError::internal(e.to_string()))?
            .push(source.clone());
        Ok(None)
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
        // A data source's key holds a path, which normalizing would change.
        if let Some(plan) = self.streams.get(name) {
            return Ok(Some(plan.clone()));
        }
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

    fn observe_read_options(&self, name: &[String], options: &[(String, String)]) {
        self.graph.observe_read_options(name, options);
    }

    fn resolve_source_stream(
        &self,
        source: &spec::ReadDataSource,
    ) -> PlanResult<Option<LogicalPlan>> {
        let name = source_name(source);
        self.streams
            .get(&name)
            .cloned()
            .map(Some)
            .ok_or_else(|| PlanError::internal(format!("unbound stream {}", name.join("."))))
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
) -> SparkResult<Streamed> {
    let recording = RecordingScope {
        graph: scope,
        streaming: Mutex::default(),
        sources: Mutex::default(),
        options: Mutex::default(),
    };
    PlanResolver::new(ctx, config.clone())
        .with_relation_scope(&recording)
        .resolve_named_plan(spec::Plan::Query(query.clone()))
        .await?;
    Ok(Streamed {
        names: recording
            .streaming
            .into_inner()
            .map_err(|e| SparkError::internal(e.to_string()))?,
        sources: recording
            .sources
            .into_inner()
            .map_err(|e| SparkError::internal(e.to_string()))?,
        options: recording
            .options
            .into_inner()
            .map_err(|e| SparkError::internal(e.to_string()))?,
    })
}

/// What a query reads as streams: named tables and views, and data sources.
struct Streamed {
    names: BTreeSet<Name>,
    sources: Vec<spec::ReadDataSource>,
    options: BTreeMap<Name, Vec<(String, String)>>,
}

/// A streaming data source's name in the stream map, which is also its key in
/// the checkpoint. It cannot collide with a table, which has no such part.
fn source_name(source: &spec::ReadDataSource) -> Name {
    let format = source.format.clone().unwrap_or_default();
    vec![
        STREAM_SOURCE_PART.to_string(),
        format,
        source.paths.join(","),
    ]
}

pub(super) fn view_flow<'a>(graph: &'a GraphDefinition, name: &Name) -> Option<&'a Flow> {
    let output = graph.outputs.iter().find(|output| &output.name == name)?;
    if output.view.is_none() && output.kind != OutputType::TemporaryView {
        return None;
    }
    graph.flows.iter().find(|flow| &flow.target == name)
}

/// A streaming table's source: one file-backed table.
pub(super) struct TableSource {
    pub format: String,
    pub location: String,
    /// The columns stored in the files.
    pub schema: spec::Schema,
    /// Every column, partition columns included.
    pub empty: datafusion::arrow::datatypes::SchemaRef,
    pub partitions: BTreeSet<String>,
    pub options: Vec<(String, String)>,
}

pub(super) async fn table_source(ctx: &SessionContext, name: &Name) -> SparkResult<TableSource> {
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
    if is_lakehouse_format(&format) && !is_delta(&format) {
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
        options: vec![],
    })
}

pub(super) fn is_delta(format: &str) -> bool {
    format.eq_ignore_ascii_case("delta")
}

pub(super) fn spec_schema(
    schema: &datafusion::arrow::datatypes::Schema,
) -> SparkResult<spec::Schema> {
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
pub(super) async fn data_files(
    ctx: &SessionContext,
    location: &str,
) -> SparkResult<BTreeSet<String>> {
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
pub(super) fn partition_values(
    file: &str,
    source: &TableSource,
) -> BTreeMap<String, Option<String>> {
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
pub(super) async fn partition_plan(
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
            options: source.options.clone(),
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

pub(super) fn reads_a_stream(plan: &LogicalPlan) -> datafusion::common::Result<bool> {
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
pub(super) fn check_stateless(plan: &LogicalPlan, windowed: bool) -> SparkResult<()> {
    let mut failure = None;
    plan.apply(|node| {
        let refused = match node {
            LogicalPlan::Aggregate(_) if !windowed && reads_a_stream(node)? => {
                Some(append_mode_error())
            }
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

/// Only Spark's rate limit options apply to a whole stream, and one batch is
/// always within them, but a value that cannot be parsed still fails the flow.
fn check_stream_options(options: &[(String, String)]) -> SparkResult<()> {
    for (key, value) in options {
        if key.eq_ignore_ascii_case("maxFilesPerTrigger") && value.parse::<u64>().is_err() {
            return Err(SparkError::invalid(format!(
                "Invalid value '{value}' for option '{key}', must be a positive integer"
            )));
        }
    }
    Ok(())
}

/// The rows of a source that this flow has not read, or of the replayed batch.
async fn unread_plan(
    ctx: &SessionContext,
    config: &Arc<PlanConfig>,
    source: &TableSource,
    name: &Name,
    state: &FlowState,
    batch: &mut Batch,
) -> SparkResult<LogicalPlan> {
    let key = name.join(".");
    let files = match batch.files.get(&key) {
        Some(files) => files.clone(),
        None => {
            let seen = state.seen.get(&key);
            let mut files: Vec<String> = vec![];
            for location in source.location.split(',') {
                let listed = data_files(ctx, location).await?;
                files.extend(
                    listed
                        .into_iter()
                        .filter(|file| seen.is_none_or(|seen| !seen.contains(file))),
                );
            }
            batch.files.insert(key, files.clone());
            files
        }
    };
    source_plan(ctx, config, source, &files).await
}

/// A failure of a Delta source, which Spark reports as the stream's failure.
pub(super) fn delta_error(flow: &Flow, error: DeltaStreamError) -> SparkError {
    let DeltaStreamFailure {
        condition,
        sql_state,
        message,
        analysis,
    } = match error {
        DeltaStreamError::Failure(failure) => failure,
        DeltaStreamError::Other(error) => return error.into(),
    };
    if analysis {
        return Box::new(StructuredSparkError {
            class: "org.apache.spark.sql.AnalysisException".into(),
            condition: condition.into(),
            sql_state: sql_state.into(),
            message,
            parameters: HashMap::new(),
        })
        .into();
    }
    let cause = format!("[{condition}] {message} SQLSTATE: {sql_state}");
    Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.streaming.StreamingQueryException".into(),
        condition: "STREAM_FAILED".into(),
        sql_state: "XXKST".into(),
        message: format!(
            "Query [id = {flow}] terminated with exception: {cause}",
            flow = flow.name.join(".")
        ),
        parameters: HashMap::from([("message".into(), cause)]),
    })
    .into()
}

/// The rows of a Delta source that this flow has not read, or of the replayed
/// batch.
async fn delta_plan(
    ctx: &SessionContext,
    flow: &Flow,
    paths: Vec<String>,
    options: &[(String, String)],
    name: &Name,
    state: &FlowState,
    batch: &mut Batch,
) -> SparkResult<LogicalPlan> {
    let key = name.join(".");
    let session = ctx.state();
    let fail = |error| delta_error(flow, error);
    let stream = DeltaStream::open(&session, paths, options)
        .await
        .map_err(fail)?;
    let range = match batch.delta.get(&key) {
        Some(range) => range.clone(),
        None => stream
            .next_range(state.delta.get(&key))
            .await
            .map_err(fail)?,
    };
    let read = stream.read(range.as_ref()).await.map_err(fail)?;
    batch.delta.insert(key, range);
    batch.delta_rows |= read.has_rows;
    Ok(LogicalPlanBuilder::scan(SOURCE_ALIAS, read.source, None)?
        .alias(SOURCE_ALIAS)?
        .build()?)
}

/// `readStream.format(...).load(path)` over a directory of files. Other
/// sources have no files to list and stay refused: without this refusal a
/// flow over one would complete having read nothing.
async fn file_source(
    ctx: &SessionContext,
    config: &Arc<PlanConfig>,
    source: spec::ReadDataSource,
) -> SparkResult<TableSource> {
    let format = source
        .format
        .clone()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !FILE_FORMATS.contains(&format.as_str()) || source.paths.is_empty() {
        return Err(SparkError::unsupported(format!(
            "incremental pipeline execution of a streaming read of the {format} data source"
        )));
    }
    if source.paths.iter().any(|path| path.contains(',')) {
        return Err(SparkError::unsupported("a stream path containing a comma"));
    }
    let read = spec::QueryPlan::new(spec::QueryNode::Read {
        read_type: spec::ReadType::DataSource(Box::new(source.clone())),
        is_streaming: false,
    });
    let resolved = PlanResolver::new(ctx, config.clone())
        .resolve_named_plan(spec::Plan::Query(read))
        .await?;
    let plan = match resolved.fields {
        Some(fields) => rename_logical_plan(resolved.plan, &fields)?,
        None => resolved.plan,
    };
    let empty = Arc::new(plan.schema().as_arrow().clone());
    let schema = source
        .schema
        .clone()
        .map_or_else(|| spec_schema(&empty), Ok)?;
    Ok(TableSource {
        format,
        location: source.paths.join(","),
        schema,
        empty,
        partitions: BTreeSet::new(),
        options: source.options,
    })
}

/// What a watermarked aggregate needs from its query.
pub(super) struct Windowed {
    source: Name,
    pub event_time: String,
    pub delay_micros: i64,
    window_micros: i64,
}

fn duration_micros(text: &str) -> Option<i64> {
    let text = text.trim();
    let text = text
        .strip_prefix("INTERVAL ")
        .or_else(|| text.strip_prefix("interval "))
        .unwrap_or(text)
        .trim()
        .trim_matches('\'');
    let (value, unit) = text.split_once(char::is_whitespace)?;
    let value: i64 = value.parse().ok()?;
    let scale = match unit.trim().to_ascii_lowercase().trim_end_matches('s') {
        "millisecond" => 1_000,
        "second" => 1_000_000,
        "minute" => 60_000_000,
        "hour" => 3_600_000_000,
        "day" => 86_400_000_000,
        _ => return None,
    };
    value.checked_mul(scale)
}

fn string_literal(expression: &spec::Expr) -> Option<&str> {
    match expression {
        spec::Expr::Literal(spec::Literal::Utf8 { value: Some(value) })
        | spec::Expr::Literal(spec::Literal::LargeUtf8 { value: Some(value) })
        | spec::Expr::Literal(spec::Literal::Utf8View { value: Some(value) }) => Some(value),
        _ => None,
    }
}

/// The `window(eventTime, '5 seconds')` duration among the grouping keys.
fn tumbling_window(grouping: &[spec::Expr], event_time: &str) -> SparkResult<i64> {
    for expression in grouping {
        let spec::Expr::UnresolvedFunction(function) = expression else {
            continue;
        };
        let name: Vec<String> = function.function_name.clone().into();
        if !name
            .last()
            .is_some_and(|x| x.eq_ignore_ascii_case("window"))
        {
            continue;
        }
        let [spec::Expr::UnresolvedAttribute { name, .. }, duration] =
            function.arguments.as_slice()
        else {
            return Err(SparkError::unsupported("a sliding streaming window"));
        };
        let column: Vec<String> = name.clone().into();
        if !column
            .last()
            .is_some_and(|x| x.eq_ignore_ascii_case(event_time))
        {
            return Err(SparkError::unsupported(
                "a streaming window over a column other than the watermark",
            ));
        }
        return string_literal(duration)
            .and_then(duration_micros)
            .filter(|micros| *micros > 0)
            .ok_or_else(|| SparkError::unsupported("this streaming window duration"));
    }
    Err(SparkError::unsupported(
        "a watermarked aggregate without a tumbling window",
    ))
}

/// Find the watermarked aggregate of a query, through the nodes around it.
pub(super) fn windowed_aggregate(
    scope: &GraphScope<'_>,
    query: &spec::QueryPlan,
) -> SparkResult<Option<Windowed>> {
    let aggregate = match &query.node {
        spec::QueryNode::Aggregate(aggregate) => aggregate,
        spec::QueryNode::Project {
            input: Some(input), ..
        }
        | spec::QueryNode::Filter { input, .. }
        | spec::QueryNode::Sort { input, .. } => return windowed_aggregate(scope, input),
        _ => return Ok(None),
    };
    let mut input = aggregate.input.as_ref();
    loop {
        match &input.node {
            spec::QueryNode::WithWatermark(watermark) => {
                let source = match &watermark.input.node {
                    spec::QueryNode::Read {
                        read_type: spec::ReadType::NamedTable(read),
                        is_streaming: true,
                    } => {
                        let name: Vec<String> = read.name.clone().into();
                        qualified(scope, &name)?
                    }
                    spec::QueryNode::Read {
                        read_type: spec::ReadType::DataSource(source),
                        is_streaming: true,
                    } => source_name(source),
                    _ => {
                        return Err(SparkError::unsupported(
                            "a watermark on this streaming read",
                        ));
                    }
                };
                let delay_micros =
                    duration_micros(&watermark.delay_threshold).ok_or_else(|| {
                        SparkError::unsupported(format!(
                            "the watermark delay '{}'",
                            watermark.delay_threshold
                        ))
                    })?;
                return Ok(Some(Windowed {
                    source,
                    event_time: watermark.event_time.clone(),
                    delay_micros,
                    window_micros: tumbling_window(&aggregate.grouping, &watermark.event_time)?,
                }));
            }
            spec::QueryNode::Project {
                input: Some(next), ..
            }
            | spec::QueryNode::Filter { input: next, .. } => input = next,
            spec::QueryNode::TableAlias { input: next, .. } => input = next,
            _ => return Ok(None),
        }
    }
}

/// The event time as whole microseconds, for arithmetic.
fn event_micros(plan: &LogicalPlan, column: &str) -> SparkResult<Expr> {
    let (_, field) = plan
        .schema()
        .qualified_field_with_unqualified_name(column)
        .map_err(|e| SparkError::invalid(e.to_string()))?;
    let DataType::Timestamp(unit, timezone) = field.data_type() else {
        return Err(SparkError::invalid(format!(
            "the watermark column {column} is not a timestamp"
        )));
    };
    let raw = col(Column::from_name(column));
    let micros = DataType::Timestamp(TimeUnit::Microsecond, timezone.clone());
    let _ = unit;
    Ok(cast(cast(raw, micros), DataType::Int64))
}

pub(super) async fn max_event(
    ctx: &SessionContext,
    plan: &LogicalPlan,
    column: &str,
) -> SparkResult<Option<i64>> {
    let micros = event_micros(plan, column)?;
    let plan = LogicalPlanBuilder::from(plan.clone())
        .aggregate(Vec::<Expr>::new(), vec![max(micros).alias("m")])?
        .build()?;
    let physical = ctx.state().create_physical_plan(&plan).await?;
    let service = ctx.extension::<JobService>()?;
    let mut stream = service.runner().execute(ctx, physical).await?;
    let mut value = None;
    while let Some(batch) = stream.try_next().await? {
        let column = batch.column(0).as_primitive::<Int64Type>();
        if !column.is_empty() && !column.is_null(0) {
            value = Some(column.value(0));
        }
    }
    Ok(value)
}

/// Resolve the streams under a set of names: tables to their unread files, and
/// streaming views to their own query, over the same unread files.
#[expect(clippy::too_many_arguments)]
fn bind_streams<'a>(
    ctx: &'a SessionContext,
    config: &'a Arc<PlanConfig>,
    flow: &'a Flow,
    graph: &'a GraphDefinition,
    scope: &'a GraphScope<'a>,
    read: Streamed,
    state: &'a FlowState,
    batch: &'a mut Batch,
    streams: &'a mut BTreeMap<Name, LogicalPlan>,
    depth: usize,
) -> futures::future::BoxFuture<'a, SparkResult<()>> {
    use futures::FutureExt;
    async move {
        if depth > MAX_VIEW_DEPTH {
            return Err(SparkError::internal("streaming views nest too deeply"));
        }
        for source in read.sources {
            let name = source_name(&source);
            if streams.contains_key(&name) {
                continue;
            }
            if source.format.as_deref().is_some_and(is_delta) {
                let plan = delta_plan(
                    ctx,
                    flow,
                    source.paths,
                    &source.options,
                    &name,
                    state,
                    batch,
                )
                .await?;
                streams.insert(name, plan);
                continue;
            }
            let source = file_source(ctx, config, source).await?;
            check_stream_options(&source.options)?;
            let plan = unread_plan(ctx, config, &source, &name, state, batch).await?;
            streams.insert(name, plan);
        }
        for name in read.names {
            if streams.contains_key(&name) {
                continue;
            }
            if let Some(view) = view_flow(graph, &name) {
                let inner = streamed_names(ctx, config, scope, &view.query).await?;
                bind_streams(
                    ctx,
                    config,
                    flow,
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
            let mut source = table_source(ctx, &name).await?;
            source.options = read.options.get(&name).cloned().unwrap_or_default();
            if is_delta(&source.format) {
                let paths = vec![source.location.clone()];
                let plan =
                    delta_plan(ctx, flow, paths, &source.options, &name, state, batch).await?;
                streams.insert(name, plan);
                continue;
            }
            check_stream_options(&source.options)?;
            let plan = unread_plan(ctx, config, &source, &name, state, batch).await?;
            streams.insert(name, plan);
        }
        Ok(())
    }
    .boxed()
}

const SOURCES_CHANGED: &str = "sourcesChanged";
const TIME_TRAVEL: &str = "DELTA_UNSUPPORTED_TIME_TRAVEL_VIEWS";

/// Spark's offset log refuses the changed query, as the stream's failure.
fn sources_changed_error(flow: &Flow, before: usize, after: usize) -> SparkError {
    let cause = format!(
        "There are [{before}] sources in the checkpoint offsets and now there are [{after}] \
         sources requested by the query. Cannot continue."
    );
    Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.streaming.StreamingQueryException".into(),
        condition: "STREAM_FAILED".into(),
        sql_state: "XXKST".into(),
        message: format!(
            "Query [id = {}] terminated with exception: {cause}",
            flow.name.join(".")
        ),
        parameters: HashMap::from([(SOURCES_CHANGED.into(), cause)]),
    })
    .into()
}

/// Whether this failure needs a full refresh, which no retry can stand in for.
pub(super) fn sources_changed(error: &SparkError) -> bool {
    matches!(error, SparkError::Structured(error)
        if error.parameters.contains_key(SOURCES_CHANGED) || error.condition == TIME_TRAVEL)
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
    if names.names.is_empty() && names.sources.is_empty() {
        return Err(SparkError::internal(
            "a streaming table flow has no streaming read",
        ));
    }
    let checkpoint =
        Checkpoint::open(ctx, run.storage, run.destination, &run.flow.name, run.reset).await?;
    let mut state = checkpoint.load().await?;
    let replay = state.pending.is_some();
    let mut batch = Batch {
        files: state.pending.take().unwrap_or_default(),
        delta: state.pending_delta.take().unwrap_or_default(),
        delta_rows: false,
    };
    let mut streams = BTreeMap::new();
    bind_streams(
        ctx,
        &config,
        run.flow,
        run.graph,
        &scope,
        names,
        &state,
        &mut batch,
        &mut streams,
        0,
    )
    .await?;
    let sources = batch.sources();
    if !state.sources.is_empty() && state.sources != sources {
        return Err(sources_changed_error(
            run.flow,
            state.sources.len(),
            sources.len(),
        ));
    }
    state.sources = sources;
    let windowed = windowed_aggregate(&scope, &run.flow.query)?;
    let outputs = {
        let bound = BatchScope {
            graph: &scope,
            streams: &streams,
        };
        let planned = PlanResolver::new(ctx, config.clone())
            .with_relation_scope(&bound)
            .resolve_named_plan(spec::Plan::Query(run.flow.query.clone()))
            .await?;
        check_stateless(&planned.plan, windowed.is_some())?;
        planned.fields.unwrap_or_else(|| {
            let fields = planned.plan.schema().fields();
            fields.iter().map(|field| field.name().clone()).collect()
        })
    };
    if !replay && !batch.has_rows() {
        // Commits without rows still count as read, and a first run still
        // records where its source starts.
        if batch.delta.values().any(Option::is_some) {
            advance_delta(&mut state, batch.delta);
            checkpoint.store(&state).await?;
        }
        return Ok(());
    }
    let write = pad_dropped_columns(ctx, run.destination, run.write, &outputs).await?;
    state.pending = Some(batch.files);
    state.pending_delta = Some(batch.delta);
    checkpoint.store(&state).await?;
    match windowed {
        Some(windowed) => {
            run_windowed(
                ctx,
                config,
                write,
                &scope,
                &checkpoint,
                &mut state,
                streams,
                &windowed,
            )
            .await?;
        }
        None => {
            let bound = BatchScope {
                graph: &scope,
                streams: &streams,
            };
            run_command(ctx, config, write, Some(&bound)).await?;
        }
    }
    for (source, files) in state.pending.take().unwrap_or_default() {
        state.seen.entry(source).or_default().extend(files);
    }
    let ranges = state.pending_delta.take().unwrap_or_default();
    advance_delta(&mut state, ranges);
    state.batches += 1;
    checkpoint.store(&state).await
}

fn advance_delta(state: &mut FlowState, ranges: BTreeMap<String, Option<DeltaStreamRange>>) {
    for (source, range) in ranges {
        if let Some(range) = range {
            state.delta.insert(source, range.next_offset());
        }
    }
}

/// A streaming table keeps a column its query no longer produces, so new rows
/// write null there. A sink has no columns to keep.
async fn pad_dropped_columns(
    ctx: &SessionContext,
    destination: &Name,
    write: spec::CommandNode,
    outputs: &[String],
) -> SparkResult<spec::CommandNode> {
    let spec::CommandNode::WriteTo(mut target) = write else {
        return Ok(write);
    };
    let manager = ctx.extension::<CatalogManager>()?;
    let status = manager
        .get_table_or_view(destination)
        .await
        .map_err(PlanError::from)?;
    let columns = status.kind.columns();
    let produced = |name: &str| outputs.iter().any(|x| x.eq_ignore_ascii_case(name));
    if columns.iter().all(|column| produced(&column.name)) {
        return Ok(spec::CommandNode::WriteTo(target));
    }
    let expressions = columns
        .iter()
        .map(|column| {
            if produced(&column.name) {
                return Ok(spec::Expr::UnresolvedAttribute {
                    name: spec::ObjectName::bare(column.name.clone()),
                    plan_id: None,
                    is_metadata_column: false,
                });
            }
            let cast_to_type: spec::DataType =
                crate::spark::connect::DataType::try_from(column.data_type.clone())?.try_into()?;
            Ok(spec::Expr::Alias {
                expr: Box::new(spec::Expr::Cast {
                    expr: Box::new(spec::Expr::Literal(spec::Literal::Null)),
                    cast_to_type,
                    rename: false,
                    is_try: false,
                }),
                name: vec![column.name.clone().into()],
                metadata: None,
            })
        })
        .collect::<SparkResult<Vec<_>>>()?;
    target.input = Box::new(spec::QueryPlan::new(spec::QueryNode::Project {
        input: Some(target.input),
        expressions,
    }));
    Ok(spec::CommandNode::WriteTo(target))
}

/// One batch of a watermarked aggregate. The closed windows are written to the
/// target, and the open ones are written back to the checkpoint.
async fn run_windowed(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    write: spec::CommandNode,
    scope: &GraphScope<'_>,
    checkpoint: &Checkpoint,
    state: &mut FlowState,
    mut streams: BTreeMap<Name, LogicalPlan>,
    windowed: &Windowed,
) -> SparkResult<()> {
    let new_rows = streams
        .remove(&windowed.source)
        .ok_or_else(|| SparkError::internal("watermarked source is not bound"))?;
    let column = windowed.event_time.as_str();
    // Rows behind the watermark that this batch started with are late.
    let micros = event_micros(&new_rows, column)?;
    let on_time = match state.watermark {
        Some(watermark) => LogicalPlanBuilder::from(new_rows.clone())
            .filter(micros.clone().gt_eq(lit(watermark)))?
            .build()?,
        None => new_rows.clone(),
    };
    let mut rows = LogicalPlanBuilder::from(on_time);
    if !state.retained.is_empty() {
        let empty = Arc::new(new_rows.schema().as_arrow().clone());
        let retained = source_plan(
            ctx,
            &config,
            &TableSource {
                format: "parquet".into(),
                location: String::new(),
                partitions: BTreeSet::new(),
                options: vec![],
                schema: spec_schema(&empty)?,
                empty,
            },
            &state.retained,
        )
        .await?;
        rows = rows.union(retained)?;
    }
    let rows = rows.build()?;
    let latest = max_event(ctx, &new_rows, column).await?;
    let watermark = latest
        .map(|latest| latest - windowed.delay_micros)
        .into_iter()
        .chain(state.watermark)
        .max();
    let micros = event_micros(&rows, column)?;
    let size = lit(windowed.window_micros);
    let window_end = (micros / size.clone()) * size.clone() + size;
    let limit = lit(watermark.unwrap_or(i64::MIN));
    let closed = LogicalPlanBuilder::from(rows.clone())
        .filter(window_end.clone().lt_eq(limit.clone()))?
        .build()?;
    let open = LogicalPlanBuilder::from(rows)
        .filter(window_end.gt(limit))?
        .build()?;
    streams.insert(windowed.source.clone(), closed);
    {
        let bound = BatchScope {
            graph: scope,
            streams: &streams,
        };
        run_command(ctx, config.clone(), write, Some(&bound)).await?;
    }
    // Written under the next batch's name, so a replay starts from the last
    // committed files.
    streams.insert(windowed.source.clone(), open);
    let directory = checkpoint.state_url(state.batches)?;
    let bound = BatchScope {
        graph: scope,
        streams: &streams,
    };
    let read = spec::QueryPlan::new(spec::QueryNode::Read {
        read_type: spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
            name: windowed.source.clone().into(),
            temporal: None,
            sample: None,
            options: vec![],
        })),
        is_streaming: true,
    });
    let write = spec::CommandNode::Write(spec::Write {
        input: Box::new(read),
        source: Some("parquet".into()),
        save_type: spec::SaveType::Path(directory.clone()),
        mode: Some(spec::SaveMode::Overwrite),
        sort_columns: vec![],
        partitioning_columns: vec![],
        clustering_columns: vec![],
        bucket_by: None,
        options: vec![],
    });
    run_command(ctx, config, write, Some(&bound)).await?;
    state.retained = data_files(ctx, &directory).await?.into_iter().collect();
    state.watermark = watermark;
    Ok(())
}
