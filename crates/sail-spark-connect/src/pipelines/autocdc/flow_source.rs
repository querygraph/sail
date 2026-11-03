//! What an AutoCDC flow reads, as one bounded source. Spark runs the flow's
//! query as a stream, so a flow may transform its input, join it and read it
//! through views. Each streaming read under the query is a leaf, and the flow's
//! progress is the partitions of its leaves. Rows are archived before a
//! batch's first effect, so a replay never re-evaluates the query.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::{MemTable, TableProvider, provider_as_source};
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, cast as cast_expr};
use datafusion::prelude::{SessionContext, col, lit};
use sail_common::spec;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_delta_lake::stream::{DeltaStream, DeltaStreamRange};
use sail_iggy_bounded::{FORMAT as IGGY_FORMAT, IggyBoundedConfig, IggyBoundedSource};
use sail_plan::config::PlanConfig;
use sail_plan::error::{PlanError, PlanResult};
use sail_plan::resolver::PlanResolver;
use sail_plan::resolver::scope::RelationScope;
use sha2::{Digest, Sha256};

use super::carry::{carried, occurrences};
use super::state::{Snapshot, StateKeys, collect};
use super::stateful::{Within, with_state};
use crate::error::{SparkError, SparkResult};
use crate::pipelines::analysis::GraphScope;
use crate::pipelines::append::{
    SOURCE_ALIAS, TableSource, check_stateless, data_files, delta_error, is_delta, partition_plan,
    partition_values, qualified, reads_a_stream, spec_schema, table_source, view_flow,
};
use crate::pipelines::graph::{Flow, GraphDefinition, Name};
use crate::pipelines::streaming::intent::{BatchIntent, Progress};
use crate::pipelines::streaming::source::{
    BoundedSource, Cursor, Incarnation, Partition, PartitionId, Range, ScanError, SourceDescriptor,
    SourceError,
};
use crate::pipelines::watermark::{
    Windowed, WithinWatermark, event_micros, max_event, windowed_aggregate, within_watermark,
};

/// Where a watermarked aggregate keeps each stream's open rows.
const WINDOW_STATE: &str = "window";

/// Files read in one batch at most.
const MAX_FILES_PER_BATCH: usize = 100;
const FILES_INCARNATION: &str = "files";

fn lock_error(error: impl std::fmt::Display) -> PlanError {
    PlanError::internal(error.to_string())
}

/// Notes the streaming reads of a query while resolving it.
struct Recording<'a> {
    graph: &'a GraphScope<'a>,
    tables: Mutex<BTreeSet<Name>>,
    sources: Mutex<Vec<spec::ReadDataSource>>,
}

impl RelationScope for Recording<'_> {
    fn resolve(&self, name: &[String]) -> PlanResult<Option<LogicalPlan>> {
        self.graph.resolve(name)
    }

    fn resolve_read(&self, name: &[String], is_streaming: bool) -> PlanResult<Option<LogicalPlan>> {
        if is_streaming {
            let name = qualified(self.graph, name)?;
            self.tables.lock().map_err(lock_error)?.insert(name);
        }
        self.graph.resolve_read(name, is_streaming)
    }

    fn resolve_late_bound_read(
        &self,
        name: &[String],
        is_streaming: bool,
    ) -> PlanResult<Option<LogicalPlan>> {
        if is_streaming {
            let name = qualified(self.graph, name)?;
            self.tables.lock().map_err(lock_error)?.insert(name);
        }
        self.graph.resolve_late_bound_read(name, is_streaming)
    }

    fn resolve_source_stream(
        &self,
        source: &spec::ReadDataSource,
    ) -> PlanResult<Option<LogicalPlan>> {
        self.sources
            .lock()
            .map_err(lock_error)?
            .push(source.clone());
        Ok(None)
    }

    fn qualify(&self, name: &[String]) -> PlanResult<Vec<String>> {
        self.graph.qualify(name)
    }

    fn default_namespace(&self) -> Option<Vec<String>> {
        self.graph.default_namespace()
    }
}

fn hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn source_key(source: &spec::ReadDataSource) -> SparkResult<String> {
    // Options arrive from a protobuf map, in no stable order.
    let mut options = source.options.clone();
    options.sort();
    let text = serde_json::to_string(&(&source.format, &source.paths, &options))
        .map_err(|e| SparkError::internal(format!("encode source read: {e}")))?;
    Ok(format!("source:{}", hex(text.as_bytes())))
}

/// Answers each streaming read with the rows of the batch.
struct BatchScope<'a> {
    graph: &'a GraphScope<'a>,
    streams: &'a BTreeMap<String, LogicalPlan>,
}

impl BatchScope<'_> {
    fn stream(&self, name: &[String]) -> PlanResult<Option<LogicalPlan>> {
        let key = qualified(self.graph, name)?.join(".");
        let plan = self.streams.get(&key).cloned();
        plan.map(Some)
            .ok_or_else(|| PlanError::internal(format!("unbound stream {key}")))
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

    fn resolve_source_stream(
        &self,
        source: &spec::ReadDataSource,
    ) -> PlanResult<Option<LogicalPlan>> {
        let key = source_key(source).map_err(|e| PlanError::internal(e.to_string()))?;
        Ok(self.streams.get(&key).cloned())
    }

    fn qualify(&self, name: &[String]) -> PlanResult<Vec<String>> {
        self.graph.qualify(name)
    }

    fn default_namespace(&self) -> Option<Vec<String>> {
        self.graph.default_namespace()
    }
}

/// Files in the order they were first seen. The list is kept beside the flow's
/// checkpoints and only grows, so a position in it never changes meaning.
struct FileLeaf {
    flow: Flow,
    /// A table the pipeline itself writes may not exist until its flow runs.
    table: Option<Name>,
    schema: SchemaRef,
    source: OnceLock<TableSource>,
    manifest: PathBuf,
    files: Mutex<Vec<String>>,
    present: Mutex<BTreeSet<String>>,
}

impl FileLeaf {
    fn source(&self) -> SparkResult<&TableSource> {
        self.source
            .get()
            .ok_or_else(|| SparkError::internal("file source read before it was refreshed"))
    }

    async fn refresh(&self, ctx: &SessionContext) -> SparkResult<()> {
        if let (None, Some(table)) = (self.source.get(), &self.table) {
            let source = table_source(ctx, table).await.map_err(|error| {
                SparkError::invalid(format!(
                    "AutoCDC cannot read {} as a stream: {error}",
                    table.join(".")
                ))
            })?;
            let _ = self.source.set(source);
        }
        let mut known: Vec<String> = match std::fs::read(&self.manifest) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| SparkError::internal(format!("read file manifest: {e}")))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => vec![],
            Err(error) => return Err(SparkError::internal(format!("read manifest: {error}"))),
        };
        let source = self.source()?;
        let (listed, new) = if is_delta(&source.format) {
            let new = self.delta_ranges(ctx, source, &known).await?;
            // A vacuum can take the first range's files, and a reset needs them.
            let origin = known.first().or(new.first());
            let readable = match origin {
                Some(entry) => {
                    let stream = self.delta_stream(ctx, source).await?;
                    stream.read(Some(&decode_range(entry)?)).await.is_ok()
                }
                None => true,
            };
            let listed: BTreeSet<String> = if readable {
                known.iter().chain(&new).cloned().collect()
            } else {
                BTreeSet::new()
            };
            (listed, new)
        } else {
            let listed = data_files(ctx, &source.location).await?;
            let seen: BTreeSet<&String> = known.iter().collect();
            let new: Vec<String> = listed
                .iter()
                .filter(|file| !seen.contains(file))
                .cloned()
                .collect();
            (listed, new)
        };
        if !new.is_empty() {
            known.extend(new);
            write_manifest(&self.manifest, &known)?;
        }
        *self
            .files
            .lock()
            .map_err(|e| SparkError::internal(e.to_string()))? = known;
        *self
            .present
            .lock()
            .map_err(|e| SparkError::internal(e.to_string()))? = listed;
        Ok(())
    }

    /// The ranges of the table's log past those already listed, each encoded
    /// as one entry. A range reads the same files whenever it is read.
    async fn delta_ranges(
        &self,
        ctx: &SessionContext,
        source: &TableSource,
        known: &[String],
    ) -> SparkResult<Vec<String>> {
        let stream = self.delta_stream(ctx, source).await?;
        let mut offset = match known.last() {
            Some(last) => Some(decode_range(last)?.next_offset()),
            None => None,
        };
        let mut new = vec![];
        while let Some(range) = stream
            .next_range(offset.as_ref())
            .await
            .map_err(|error| delta_error(&self.flow, error))?
        {
            offset = Some(range.next_offset());
            new.push(
                serde_json::to_string(&range)
                    .map_err(|e| SparkError::internal(format!("encode Delta range: {e}")))?,
            );
        }
        Ok(new)
    }

    async fn delta_stream(
        &self,
        ctx: &SessionContext,
        source: &TableSource,
    ) -> SparkResult<DeltaStream> {
        DeltaStream::open(&ctx.state(), vec![source.location.clone()], &source.options)
            .await
            .map_err(|error| delta_error(&self.flow, error))
    }

    fn len(&self) -> Result<usize, SourceError> {
        Ok(self.files.lock().map_err(internal)?.len())
    }

    async fn plan(
        &self,
        ctx: &SessionContext,
        config: &Arc<PlanConfig>,
        range: &Range,
    ) -> SparkResult<LogicalPlan> {
        let source = self.source()?;
        let (start, end) = (position(&range.start)?, position(&range.end)?);
        let selected: Vec<String> = {
            let files = self
                .files
                .lock()
                .map_err(|e| SparkError::internal(e.to_string()))?;
            files
                .get(start..end)
                .ok_or_else(|| SparkError::internal("file range is past the manifest"))?
                .to_vec()
        };
        if is_delta(&source.format) {
            let stream = self.delta_stream(ctx, source).await?;
            let mut plans = Vec::with_capacity(selected.len());
            for entry in &selected {
                let read = stream
                    .read(Some(&decode_range(entry)?))
                    .await
                    .map_err(|error| delta_error(&self.flow, error))?;
                plans.push(LogicalPlanBuilder::scan(SOURCE_ALIAS, read.source, None)?.build()?);
            }
            return union(plans, &source.empty);
        }
        let mut groups: BTreeMap<Vec<(String, Option<String>)>, Vec<String>> = BTreeMap::new();
        for file in selected {
            let values = partition_values(&file, source);
            groups
                .entry(values.into_iter().collect())
                .or_default()
                .push(file);
        }
        let mut plans = Vec::with_capacity(groups.len());
        for (values, files) in groups {
            let values = values.into_iter().collect();
            plans.push(partition_plan(ctx, config, source, &files, &values).await?);
        }
        union(plans, &source.empty)
    }
}

fn decode_range(entry: &str) -> SparkResult<DeltaStreamRange> {
    serde_json::from_str(entry).map_err(|e| SparkError::internal(format!("read Delta range: {e}")))
}

fn write_manifest(path: &Path, files: &[String]) -> SparkResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| SparkError::internal("manifest path has no parent"))?;
    std::fs::create_dir_all(parent)
        .map_err(|e| SparkError::internal(format!("create {}: {e}", parent.display())))?;
    let bytes = serde_json::to_vec(files)
        .map_err(|e| SparkError::internal(format!("encode manifest: {e}")))?;
    let staged = path.with_extension("next");
    std::fs::write(&staged, bytes)
        .map_err(|e| SparkError::internal(format!("write {}: {e}", staged.display())))?;
    std::fs::rename(&staged, path)
        .map_err(|e| SparkError::internal(format!("rename {}: {e}", staged.display())))
}

fn internal(error: impl std::fmt::Display) -> SourceError {
    SourceError::Internal {
        cause: Box::new(std::io::Error::other(error.to_string())),
    }
}

fn position(cursor: &Cursor) -> SparkResult<usize> {
    cursor
        .0
        .parse()
        .map_err(|e| SparkError::internal(format!("file cursor {}: {e}", cursor.0)))
}

fn union(plans: Vec<LogicalPlan>, empty: &SchemaRef) -> SparkResult<LogicalPlan> {
    let mut plans = plans.into_iter();
    let Some(first) = plans.next() else {
        let table = MemTable::try_new(empty.clone(), vec![vec![]])?;
        let scan =
            LogicalPlanBuilder::scan(SOURCE_ALIAS, provider_as_source(Arc::new(table)), None)?;
        return Ok(scan.alias(SOURCE_ALIAS)?.build()?);
    };
    let mut builder = LogicalPlanBuilder::from(first);
    for plan in plans {
        builder = builder.union(plan)?;
    }
    Ok(builder.alias(SOURCE_ALIAS)?.build()?)
}

fn scan_plan(provider: Arc<dyn TableProvider>) -> SparkResult<LogicalPlan> {
    Ok(LogicalPlanBuilder::scan("scan", provider_as_source(provider), None)?.build()?)
}

/// Spark's rate source: `value` counts rows and `timestamp` is when each was
/// due. The start is kept beside the flow's checkpoints, so positions are
/// stable across runs and a replay yields the same rows.
struct RateLeaf {
    schema: SchemaRef,
    rows_per_second: u64,
    manifest: PathBuf,
    start_ms: Mutex<u64>,
}

impl RateLeaf {
    fn refresh(&self) -> SparkResult<()> {
        let start = match std::fs::read(&self.manifest) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| SparkError::internal(format!("read rate start: {e}")))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let start = now_ms()?;
                write_manifest(&self.manifest, &[start.to_string()])?;
                vec![start.to_string()]
            }
            Err(error) => return Err(SparkError::internal(format!("read rate start: {error}"))),
        };
        let start: Vec<String> = start;
        let start = start
            .first()
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| SparkError::internal("rate start is malformed"))?;
        *self
            .start_ms
            .lock()
            .map_err(|e| SparkError::internal(e.to_string()))? = start;
        Ok(())
    }

    fn frontier(&self) -> Result<u64, SourceError> {
        let start = *self.start_ms.lock().map_err(internal)?;
        let elapsed = now_ms().map_err(internal)?.saturating_sub(start);
        Ok(elapsed / 1000 * self.rows_per_second)
    }

    fn plan(&self, range: &Range) -> SparkResult<LogicalPlan> {
        let (first, end) = (position(&range.start)? as u64, position(&range.end)? as u64);
        let start_ms = *self
            .start_ms
            .lock()
            .map_err(|e| SparkError::internal(e.to_string()))?;
        let values: Vec<i64> = (first..end).map(|x| x as i64).collect();
        let micros: Vec<i64> = (first..end)
            .map(|x| ((start_ms + x * 1000 / self.rows_per_second) * 1000) as i64)
            .collect();
        let mut columns: Vec<ArrayRef> = vec![];
        for field in self.schema.fields() {
            let column: ArrayRef = if field.name() == "value" {
                Arc::new(Int64Array::from(values.clone()))
            } else {
                Arc::new(TimestampMicrosecondArray::from(micros.clone()))
            };
            columns.push(cast(&column, field.data_type())?);
        }
        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        let table = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        scan_plan(Arc::new(table))
    }
}

fn now_ms() -> SparkResult<u64> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| SparkError::internal(e.to_string()))?;
    u64::try_from(now.as_millis()).map_err(|e| SparkError::internal(e.to_string()))
}

enum Leaf {
    Bounded(Arc<dyn BoundedSource + Send + Sync>),
    Files(Box<FileLeaf>),
    Rate(RateLeaf),
}

impl Leaf {
    fn schema(&self) -> SchemaRef {
        match self {
            Self::Bounded(source) => source.schema(),
            Self::Files(files) => files.schema.clone(),
            Self::Rate(rate) => rate.schema.clone(),
        }
    }
}

/// One flow's streaming reads as one source.
pub(in crate::pipelines) struct FlowSource {
    schema: SchemaRef,
    leaves: BTreeMap<String, Leaf>,
    /// Each watermark's value in this run. Spark keeps only the query's
    /// watermark across a restart, so each watermark starts again unset.
    watermarks: Mutex<BTreeMap<usize, i64>>,
}

impl std::fmt::Debug for FlowSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowSource")
            .field("leaves", &self.leaves.keys().collect::<Vec<_>>())
            .finish()
    }
}

fn prefix(key: &str) -> String {
    hex(key.as_bytes())[..12].to_string()
}

impl FlowSource {
    /// Find the flow's leaves through its views. `root` holds file manifests.
    pub(in crate::pipelines) async fn discover(
        ctx: &SessionContext,
        config: &Arc<PlanConfig>,
        scope: &GraphScope<'_>,
        graph: &GraphDefinition,
        flow: &Flow,
        root: &Path,
    ) -> SparkResult<Self> {
        let recording = Recording {
            graph: scope,
            tables: Mutex::default(),
            sources: Mutex::default(),
        };
        let resolved = PlanResolver::new(ctx, config.clone())
            .with_relation_scope(&recording)
            .resolve_named_plan(spec::Plan::Query(flow.query.clone()))
            .await?;
        let plan = match resolved.fields {
            Some(fields) => rename_logical_plan(resolved.plan, &fields)?,
            None => resolved.plan,
        };
        let schema: SchemaRef = Arc::new(plan.schema().as_arrow().clone());
        // Views add their own reads to the recording as they are expanded.
        let mut pending: Vec<Name> = recording
            .tables
            .lock()
            .map_err(lock_error)?
            .iter()
            .cloned()
            .collect();
        let mut tables = BTreeSet::new();
        let mut expanded = BTreeSet::new();
        while let Some(name) = pending.pop() {
            let Some(view) = view_flow(graph, &name) else {
                tables.insert(name);
                continue;
            };
            if !expanded.insert(name) {
                continue;
            }
            let before: BTreeSet<Name> =
                std::mem::take(&mut *recording.tables.lock().map_err(lock_error)?);
            PlanResolver::new(ctx, config.clone())
                .with_relation_scope(&recording)
                .resolve_named_plan(spec::Plan::Query(view.query.clone()))
                .await?;
            let inner =
                std::mem::replace(&mut *recording.tables.lock().map_err(lock_error)?, before);
            pending.extend(inner);
        }
        let mut leaves = BTreeMap::new();
        for name in tables {
            let key = name.join(".");
            let output = graph.outputs.iter().find(|output| output.name == name);
            let leaf = match (output, scope.resolve_read(&name, false)?) {
                (Some(_), Some(plan)) => {
                    let schema = Arc::new(plan.schema().as_arrow().clone());
                    file_leaf(flow, None, Some(name), schema, root, &key)
                }
                _ => {
                    let source = table_source(ctx, &name).await.map_err(|error| {
                        SparkError::invalid(format!(
                            "AutoCDC cannot read {} as a stream: {error}",
                            name.join(".")
                        ))
                    })?;
                    file_leaf(flow, Some(source), None, schema.clone(), root, &key)
                }
            };
            leaves.insert(key, leaf);
        }
        let sources = std::mem::take(&mut *recording.sources.lock().map_err(lock_error)?);
        for source in sources {
            let key = source_key(&source)?;
            let leaf = source_leaf(ctx, config, flow, &source, root, &key).await?;
            leaves.insert(key, leaf);
        }
        if leaves.is_empty() {
            return Err(SparkError::internal(
                "an AutoCDC flow has no streaming read",
            ));
        }
        Ok(Self {
            schema,
            leaves,
            watermarks: Mutex::new(BTreeMap::new()),
        })
    }

    /// See what the file leaves hold now. A run calls this before capturing
    /// its frontier, because listing is asynchronous and the frontier is not.
    pub(in crate::pipelines) async fn refresh(&self, ctx: &SessionContext) -> SparkResult<()> {
        for leaf in self.leaves.values() {
            match leaf {
                Leaf::Files(files) => files.refresh(ctx).await?,
                Leaf::Rate(rate) => rate.refresh()?,
                Leaf::Bounded(_) => {}
            }
        }
        Ok(())
    }

    fn route<'a>(
        &'a self,
        partition: &PartitionId,
    ) -> Result<(&'a Leaf, PartitionId), SourceError> {
        let (head, tail) = partition
            .0
            .split_once('/')
            .ok_or_else(|| internal(format!("partition {partition} has no leaf")))?;
        let leaf = self
            .leaves
            .iter()
            .find(|(key, _)| prefix(key) == head)
            .map(|(_, leaf)| leaf)
            .ok_or_else(|| internal(format!("no leaf for partition {partition}")))?;
        Ok((leaf, PartitionId(tail.to_string())))
    }

    /// What `batch` adds to the flow's output. A stateless query reads just
    /// the batch. A watermarked aggregate, a deduplication and an inner join
    /// of streams keep state between batches, as Spark's state store does, so
    /// a batch reads only its own rows from the source.
    #[expect(clippy::too_many_arguments)]
    pub(in crate::pipelines) async fn batch_plan(
        &self,
        ctx: &SessionContext,
        config: &Arc<PlanConfig>,
        scope: &GraphScope<'_>,
        graph: &GraphDefinition,
        flow: &Flow,
        batch: &BatchIntent,
        end: &Progress,
        state: &StateKeys,
    ) -> SparkResult<LogicalPlan> {
        let origin = self.cumulative_ranges(batch, end)?.1.is_empty();
        if let Some(windowed) = windowed_query(scope, graph, flow)? {
            return self
                .windowed_batch(
                    ctx, config, scope, graph, flow, batch, &windowed, state, origin,
                )
                .await;
        }
        let streams = self.leaf_streams(ctx, config, &batch.ranges).await?;
        let (plan, _) = self
            .resolve_query(ctx, config, scope, graph, &flow.query, streams.clone())
            .await?;
        // A deduplication within a watermark evicts by the query's watermark,
        // which follows the rows of this batch that reach it.
        let within = match within_query(scope, graph, flow)? {
            Some(within) => {
                let (marked, _) = self
                    .resolve_query(ctx, config, scope, graph, &within.watermark.input, streams)
                    .await?;
                let delay_ms = within.watermark.delay_micros / 1000;
                let next = max_event(ctx, &marked, &within.watermark.event_time)
                    .await?
                    .map(|latest| (latest.div_euclid(1000) - delay_ms) * 1000);
                let mut watermarks = self.watermarks.lock().map_err(lock_error)?;
                let candidate = match (watermarks.get(&0).copied(), next) {
                    (Some(previous), Some(next)) => Some(previous.max(next)),
                    (previous, next) => previous.or(next),
                };
                if let Some(candidate) = candidate {
                    watermarks.insert(0, candidate);
                }
                Some(Within {
                    keys: within.keys,
                    event_time: within.watermark.event_time,
                    delay_micros: within.watermark.delay_micros,
                    candidate,
                })
            }
            None => None,
        };
        let plan = with_state(ctx, plan, state, origin, within.as_ref()).await?;
        check_stateless(&plan, false)?;
        Ok(plan)
    }

    /// A watermarked aggregate, as Spark runs it in append mode. Each
    /// watermark is the latest event time in milliseconds that reaches it,
    /// less its delay. The query's watermark is the least of them, a stream
    /// with no rows yet counting as 0, as Spark's default `min` policy has it,
    /// and it never moves back. A window is emitted in the batch whose
    /// watermark passes its end, and a row whose window the watermark had
    /// already passed is late and changes nothing. The state is each
    /// stream's rows of windows still open and the query's watermark.
    #[expect(clippy::too_many_arguments)]
    async fn windowed_batch(
        &self,
        ctx: &SessionContext,
        config: &Arc<PlanConfig>,
        scope: &GraphScope<'_>,
        graph: &GraphDefinition,
        flow: &Flow,
        batch: &BatchIntent,
        windowed: &Windowed,
        state: &StateKeys,
        origin: bool,
    ) -> SparkResult<LogicalPlan> {
        let new_streams = self.leaf_streams(ctx, config, &batch.ranges).await?;
        let mut streams = new_streams.clone();
        let mut leaves = vec![];
        let mut watermark_before = 0;
        for (key, new_rows) in &new_streams {
            let leaf = prefix(key);
            let schema = Arc::new(new_rows.schema().as_arrow().clone());
            let before = match state.load(&format!("{WINDOW_STATE}-{leaf}"))? {
                Some(before) => same_types(before, &schema, flow)?,
                None if origin => Snapshot::empty(schema),
                None => return Err(missing_state(flow)),
            };
            watermark_before = watermark_before.max(before.watermark);
            // The rows kept from earlier batches are part of the stream.
            let kept = LogicalPlanBuilder::from(before.plan("state")?)
                .alias(SOURCE_ALIAS)?
                .build()?;
            let rows = LogicalPlanBuilder::from(kept)
                .union(new_rows.clone())?
                .alias(SOURCE_ALIAS)?
                .build()?;
            streams.insert(key.clone(), rows.clone());
            leaves.push((leaf, rows, new_rows.clone()));
        }
        // Each watermark follows the rows of this batch that reach it, after
        // any transformation beneath it.
        let mut operators = vec![];
        for (index, mark) in windowed.watermarks.iter().enumerate() {
            let previous = self
                .watermarks
                .lock()
                .map_err(lock_error)?
                .get(&index)
                .copied();
            let (marked, _) = self
                .resolve_query(ctx, config, scope, graph, &mark.input, new_streams.clone())
                .await?;
            let delay_ms = mark.delay_micros / 1000;
            let next = max_event(ctx, &marked, &mark.event_time)
                .await?
                .map(|latest| (latest.div_euclid(1000) - delay_ms) * 1000);
            let value = match (previous, next) {
                (Some(previous), Some(next)) => Some(previous.max(next)),
                (previous, next) => previous.or(next),
            };
            operators.push((index, value));
        }
        let watermark = operators
            .iter()
            .map(|(_, value)| value.unwrap_or(0))
            .min()
            .unwrap_or(0)
            .max(watermark_before);
        let (plan, _) = self
            .resolve_query(ctx, config, scope, graph, &flow.query, streams.clone())
            .await?;
        check_stateless(&plan, true)?;
        // A row can belong only to a window ending within a window's length
        // of it, by its event time at the watermark.
        let floor = watermark.saturating_sub(windowed.window_micros);
        for (leaf, rows, new_rows) in leaves {
            let open = self
                .open_rows(
                    ctx, config, scope, graph, windowed, &streams, &plan, rows, floor,
                )
                .await?;
            let (schema, batches) = collect(ctx, open, "c").await?;
            let schema = Arc::new(Schema::new(
                new_rows
                    .schema()
                    .fields()
                    .iter()
                    .zip(schema.fields())
                    .map(|(named, typed)| Field::new(named.name(), typed.data_type().clone(), true))
                    .collect::<Vec<_>>(),
            ));
            let batches = batches
                .into_iter()
                .map(|batch| {
                    Ok(RecordBatch::try_new(
                        schema.clone(),
                        batch.columns().to_vec(),
                    )?)
                })
                .collect::<SparkResult<Vec<_>>>()?;
            state.store(
                &format!("{WINDOW_STATE}-{leaf}"),
                &Snapshot {
                    schema,
                    batches,
                    watermark,
                },
            )?;
        }
        let mut watermarks = self.watermarks.lock().map_err(lock_error)?;
        for (index, value) in operators {
            if let Some(value) = value {
                watermarks.insert(index, value);
            }
        }
        drop(watermarks);
        windows_between(plan, watermark_before, watermark)
    }

    /// The rows of a stream that a window still open may need: those whose
    /// event time at the watermark that reads them is after `floor`. A stream
    /// read other than beneath exactly one watermark, or beneath a
    /// transformation other than projections, filters and aliases, keeps
    /// every row.
    #[expect(clippy::too_many_arguments)]
    async fn open_rows(
        &self,
        ctx: &SessionContext,
        config: &Arc<PlanConfig>,
        scope: &GraphScope<'_>,
        graph: &GraphDefinition,
        windowed: &Windowed,
        streams: &BTreeMap<String, LogicalPlan>,
        plan: &LogicalPlan,
        rows: LogicalPlan,
        floor: i64,
    ) -> SparkResult<LogicalPlan> {
        let mut readers = vec![];
        for mark in &windowed.watermarks {
            let (marked, _) = self
                .resolve_query(ctx, config, scope, graph, &mark.input, streams.clone())
                .await?;
            if occurrences(&marked, &rows)? > 0 {
                readers.push((mark, marked));
            }
        }
        let [(mark, marked)] = &readers[..] else {
            return Ok(rows);
        };
        if occurrences(plan, &rows)? != occurrences(marked, &rows)? {
            return Ok(rows);
        }
        if mark.direct {
            return Ok(match event_micros(&rows, &mark.event_time) {
                Ok(micros) => LogicalPlanBuilder::from(rows)
                    .filter(micros.gt(lit(floor)))?
                    .build()?,
                Err(_) => rows,
            });
        }
        let Some((carried, names)) = carried(marked, &rows)? else {
            return Ok(rows);
        };
        let micros = event_micros(&carried, &mark.event_time)?;
        let fields = rows.schema().fields().clone();
        Ok(LogicalPlanBuilder::from(carried)
            .filter(micros.gt(lit(floor)))?
            .project(
                names
                    .iter()
                    .zip(fields.iter())
                    .map(|(name, field)| col(Column::from_name(name)).alias(field.name())),
            )?
            .build()?)
    }

    /// The ranges from each partition's origin to the end of the batch, and to
    /// its start.
    fn cumulative_ranges(
        &self,
        batch: &BatchIntent,
        end: &Progress,
    ) -> SparkResult<(Vec<Range>, Vec<Range>)> {
        let (mut through, mut before) = (vec![], vec![]);
        for (partition, (incarnation, position)) in &end.positions {
            let start = self.earliest(partition)?;
            let range = |end: &Cursor| Range {
                partition: partition.clone(),
                incarnation: incarnation.clone(),
                start: start.clone(),
                end: end.clone(),
            };
            through.push(range(position));
            let from = batch
                .ranges
                .iter()
                .find(|range| &range.partition == partition)
                .map_or(position, |range| &range.start);
            if from != &start {
                before.push(range(from));
            }
        }
        Ok((through, before))
    }

    /// Each leaf's rows in `ranges`.
    async fn leaf_streams(
        &self,
        ctx: &SessionContext,
        config: &Arc<PlanConfig>,
        ranges: &[Range],
    ) -> SparkResult<BTreeMap<String, LogicalPlan>> {
        let mut streams = BTreeMap::new();
        for (key, leaf) in &self.leaves {
            let head = prefix(key);
            let mut plans = vec![];
            for range in ranges {
                let Some((owner, tail)) = range.partition.0.split_once('/') else {
                    return Err(SparkError::internal("a range has no leaf"));
                };
                if owner != head {
                    continue;
                }
                let range = Range {
                    partition: PartitionId(tail.to_string()),
                    ..range.clone()
                };
                plans.push(match leaf {
                    Leaf::Bounded(source) => scan_plan(source.scan(&range)?)?,
                    Leaf::Files(files) => files.plan(ctx, config, &range).await?,
                    Leaf::Rate(rate) => rate.plan(&range)?,
                });
            }
            streams.insert(key.clone(), union(plans, &leaf.schema())?);
        }
        Ok(streams)
    }

    /// The flow's query over the given leaf streams, with what each stream
    /// and view resolved to.
    async fn resolve_query(
        &self,
        ctx: &SessionContext,
        config: &Arc<PlanConfig>,
        scope: &GraphScope<'_>,
        graph: &GraphDefinition,
        query: &spec::QueryPlan,
        mut streams: BTreeMap<String, LogicalPlan>,
    ) -> SparkResult<(LogicalPlan, BTreeMap<String, LogicalPlan>)> {
        // Inner views resolve first, so an outer view finds them bound.
        let mut remaining: Vec<&crate::pipelines::graph::Output> = graph
            .outputs
            .iter()
            .filter(|output| view_flow(graph, &output.name).is_some())
            .collect();
        while !remaining.is_empty() {
            let before = remaining.len();
            let mut unresolved = vec![];
            for output in remaining {
                let Some(view) = view_flow(graph, &output.name) else {
                    continue;
                };
                let bound = BatchScope {
                    graph: scope,
                    streams: &streams,
                };
                let resolved = PlanResolver::new(ctx, config.clone())
                    .with_relation_scope(&bound)
                    .resolve_named_plan(spec::Plan::Query(view.query.clone()))
                    .await;
                match resolved {
                    Ok(resolved) => {
                        let plan = match resolved.fields {
                            Some(fields) => rename_logical_plan(resolved.plan, &fields)?,
                            None => resolved.plan,
                        };
                        let key = qualified(scope, &output.name)?.join(".");
                        streams.insert(key, plan);
                    }
                    Err(_) => unresolved.push(output),
                }
            }
            if unresolved.len() == before {
                break;
            }
            remaining = unresolved;
        }
        let bound = BatchScope {
            graph: scope,
            streams: &streams,
        };
        let resolved = PlanResolver::new(ctx, config.clone())
            .with_relation_scope(&bound)
            .resolve_named_plan(spec::Plan::Query(query.clone()))
            .await?;
        let plan = match resolved.fields {
            Some(fields) => rename_logical_plan(resolved.plan, &fields)?,
            None => resolved.plan,
        };
        Ok((plan, streams))
    }
}

/// The deduplication within a watermark of the flow's query, or of a view it
/// reads.
fn within_query(
    scope: &GraphScope<'_>,
    graph: &GraphDefinition,
    flow: &Flow,
) -> SparkResult<Option<WithinWatermark>> {
    let _ = scope;
    if let Some(within) = within_watermark(&flow.query)? {
        return Ok(Some(within));
    }
    for output in &graph.outputs {
        if let Some(view) = view_flow(graph, &output.name)
            && let Some(within) = within_watermark(&view.query)?
        {
            return Ok(Some(within));
        }
    }
    Ok(None)
}

/// The watermarked aggregate of the flow's query, or of a view it reads.
fn windowed_query(
    scope: &GraphScope<'_>,
    graph: &GraphDefinition,
    flow: &Flow,
) -> SparkResult<Option<Windowed>> {
    if let Some(windowed) = windowed_aggregate(scope, &flow.query)? {
        return Ok(Some(windowed));
    }
    for output in &graph.outputs {
        if let Some(view) = view_flow(graph, &output.name)
            && let Some(windowed) = windowed_aggregate(scope, &view.query)?
        {
            return Ok(Some(windowed));
        }
    }
    Ok(None)
}

/// The rows of windows that end after `after` and by `through`, watermarks in
/// microseconds. The filter sits on the aggregate itself, whose window is the
/// grouping key with a `start` and `end`, so a query may drop the window after.
fn windows_between(plan: LogicalPlan, after: i64, through: i64) -> SparkResult<LogicalPlan> {
    let bound = |micros| lit(ScalarValue::TimestampMicrosecond(Some(micros), None));
    let mut found = false;
    let plan = plan
        .transform_up(|node| {
            let LogicalPlan::Aggregate(aggregate) = &node else {
                return Ok(Transformed::no(node));
            };
            if found || !reads_a_stream(&node)? {
                return Ok(Transformed::no(node));
            }
            let schema = node.schema().clone();
            let window = (0..aggregate.group_expr.len()).find(|index| {
                matches!(schema.field(*index).data_type(), DataType::Struct(fields)
                    if fields.iter().any(|field| field.name() == "start")
                        && fields.iter().any(|field| field.name() == "end"))
            });
            let Some(window) = window else {
                return Ok(Transformed::no(node));
            };
            found = true;
            let (qualifier, field) = schema.qualified_field(window);
            let end = get_field(col(Column::new(qualifier.cloned(), field.name())), "end");
            let end = cast_expr(end, DataType::Timestamp(TimeUnit::Microsecond, None));
            Ok(Transformed::yes(
                LogicalPlanBuilder::from(node)
                    .filter(end.clone().gt(bound(after)).and(end.lt_eq(bound(through))))?
                    .build()?,
            ))
        })?
        .data;
    if !found {
        return Err(SparkError::internal(
            "a watermarked aggregate has no window",
        ));
    }
    Ok(plan)
}

/// A snapshot is read back only under the schema it was written with. The
/// query changed otherwise, and Spark refuses a changed state schema too.
fn same_types(stored: Snapshot, expected: &SchemaRef, flow: &Flow) -> SparkResult<Snapshot> {
    let same = stored.schema.fields().len() == expected.fields().len()
        && stored
            .schema
            .fields()
            .iter()
            .zip(expected.fields())
            .all(|(stored, expected)| stored.data_type() == expected.data_type());
    if !same {
        return Err(SparkError::invalid(format!(
            "the state of flow {} was written by another query; reset the flow to rebuild it",
            flow.name.join(".")
        )));
    }
    Ok(stored)
}

fn missing_state(flow: &Flow) -> SparkError {
    SparkError::invalid(format!(
        "the state of flow {} is missing; reset the flow to rebuild it",
        flow.name.join(".")
    ))
}

fn file_leaf(
    flow: &Flow,
    source: Option<TableSource>,
    table: Option<Name>,
    fallback: SchemaRef,
    root: &Path,
    key: &str,
) -> Leaf {
    let schema = source
        .as_ref()
        .map_or(fallback, |source| source.empty.clone());
    let cell = OnceLock::new();
    if let Some(source) = source {
        let _ = cell.set(source);
    }
    Leaf::Files(Box::new(FileLeaf {
        flow: flow.clone(),
        table,
        schema,
        source: cell,
        manifest: root.join("sources").join(format!("{}.json", prefix(key))),
        files: Mutex::default(),
        present: Mutex::default(),
    }))
}

/// `readStream.format(...)`: Iggy, or files under a path.
async fn source_leaf(
    ctx: &SessionContext,
    config: &Arc<PlanConfig>,
    flow: &Flow,
    source: &spec::ReadDataSource,
    root: &Path,
    key: &str,
) -> SparkResult<Leaf> {
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
    let schema: SchemaRef = Arc::new(plan.schema().as_arrow().clone());
    let format = source.format.clone().unwrap_or_default();
    if format.eq_ignore_ascii_case(IGGY_FORMAT) {
        let options = IggyBoundedConfig::from_options(source.options.iter().cloned())
            .map_err(SparkError::from)?;
        let iggy = IggyBoundedSource::try_new(options, schema)?;
        iggy.describe()?;
        return Ok(Leaf::Bounded(Arc::new(iggy)));
    }
    if format.eq_ignore_ascii_case("rate") {
        let option = |name: &str| {
            source
                .options
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
        };
        let rows_per_second = option("rowsPerSecond")
            .map_or(Ok(1), str::parse)
            .map_err(|e| SparkError::invalid(format!("invalid rowsPerSecond: {e}")))?;
        if rows_per_second == 0 {
            return Err(SparkError::invalid("rowsPerSecond must be positive"));
        }
        return Ok(Leaf::Rate(RateLeaf {
            schema,
            rows_per_second,
            manifest: root.join("sources").join(format!("{}.json", prefix(key))),
            start_ms: Mutex::new(0),
        }));
    }
    let [location] = source.paths.as_slice() else {
        return Err(SparkError::unsupported(format!(
            "incremental pipeline execution of a streaming {format} read"
        )));
    };
    if !matches!(
        format.to_ascii_lowercase().as_str(),
        "parquet" | "json" | "csv" | "orc" | "avro" | "text" | "delta"
    ) {
        return Err(SparkError::unsupported(format!(
            "incremental pipeline execution of a streaming {format} read"
        )));
    }
    let table = TableSource {
        format,
        location: location.clone(),
        schema: spec_schema(&schema)?,
        empty: schema.clone(),
        partitions: BTreeSet::new(),
        options: source.options.clone(),
    };
    Ok(file_leaf(flow, Some(table), None, schema, root, key))
}

impl BoundedSource for FlowSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn describe(&self) -> Result<SourceDescriptor, SourceError> {
        let mut partitions = vec![];
        for (key, leaf) in &self.leaves {
            let head = prefix(key);
            match leaf {
                Leaf::Bounded(source) => {
                    for partition in source.describe()?.partitions {
                        partitions.push(Partition {
                            id: PartitionId(format!("{head}/{}", partition.id)),
                            ..partition
                        });
                    }
                }
                Leaf::Files(_) | Leaf::Rate(_) => partitions.push(Partition {
                    id: PartitionId(format!("{head}/files")),
                    incarnation: Incarnation(FILES_INCARNATION.to_string()),
                }),
            }
        }
        Ok(SourceDescriptor {
            kind: "flow".to_string(),
            descriptor: serde_json::json!(self.leaves.keys().collect::<Vec<_>>()),
            partitions,
        })
    }

    fn earliest(&self, partition: &PartitionId) -> Result<Cursor, SourceError> {
        match self.route(partition)? {
            (Leaf::Bounded(source), inner) => source.earliest(&inner),
            (Leaf::Files(_) | Leaf::Rate(_), _) => Ok(Cursor("0".to_string())),
        }
    }

    fn retains_origin(&self, partition: &PartitionId) -> Result<bool, SourceError> {
        match self.route(partition)? {
            (Leaf::Bounded(source), inner) => source.retains_origin(&inner),
            (Leaf::Files(files), _) => {
                let known = files.files.lock().map_err(internal)?;
                let present = files.present.lock().map_err(internal)?;
                Ok(known.iter().all(|file| present.contains(file)))
            }
            (Leaf::Rate(_), _) => Ok(true),
        }
    }

    fn frontier(&self, partition: &PartitionId) -> Result<Cursor, SourceError> {
        match self.route(partition)? {
            (Leaf::Bounded(source), inner) => source.frontier(&inner),
            (Leaf::Files(files), _) => Ok(Cursor(files.len()?.to_string())),
            (Leaf::Rate(rate), _) => Ok(Cursor(rate.frontier()?.to_string())),
        }
    }

    fn plan_end(
        &self,
        partition: &PartitionId,
        start: &Cursor,
        frontier: &Cursor,
        max_rows: usize,
    ) -> Result<Cursor, SourceError> {
        match self.route(partition)? {
            (Leaf::Bounded(source), inner) => source.plan_end(&inner, start, frontier, max_rows),
            (leaf, _) => {
                let limit = match leaf {
                    Leaf::Rate(_) => max_rows,
                    _ => MAX_FILES_PER_BATCH,
                };
                let parse = |cursor: &Cursor| cursor.0.parse::<usize>().map_err(internal);
                let end = (parse(start)? + limit).min(parse(frontier)?);
                Ok(Cursor(end.to_string()))
            }
        }
    }

    fn scan(&self, range: &Range) -> Result<Arc<dyn TableProvider>, SourceError> {
        // `batch_plan` reads every leaf; nothing else has a whole batch.
        Err(ScanError::UnknownPartition {
            partition: range.partition.clone(),
        }
        .into())
    }
}
