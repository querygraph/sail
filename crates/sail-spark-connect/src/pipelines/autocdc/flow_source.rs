//! What an AutoCDC flow reads, as one bounded source. Spark runs the flow's
//! query as a stream, so a flow may transform its input, join it and read it
//! through views. Each streaming read under the query is a leaf, and the flow's
//! progress is the partitions of its leaves. Rows are archived before a
//! batch's first effect, so a replay never re-evaluates the query.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::{MemTable, TableProvider, provider_as_source};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use sail_common::spec;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_delta_lake::stream::{DeltaStream, DeltaStreamRange};
use sail_iggy_bounded::{FORMAT as IGGY_FORMAT, IggyBoundedConfig, IggyBoundedSource};
use sail_plan::config::PlanConfig;
use sail_plan::error::{PlanError, PlanResult};
use sail_plan::resolver::PlanResolver;
use sail_plan::resolver::scope::RelationScope;
use sha2::{Digest, Sha256};

use crate::error::{SparkError, SparkResult};
use crate::pipelines::analysis::GraphScope;
use crate::pipelines::append::{
    SOURCE_ALIAS, TableSource, check_stateless, data_files, delta_error, is_delta, partition_plan,
    partition_values, qualified, spec_schema, table_source, view_flow,
};
use crate::pipelines::graph::{Flow, GraphDefinition, Name};
use crate::pipelines::streaming::intent::BatchIntent;
use crate::pipelines::streaming::source::{
    BoundedSource, Cursor, Incarnation, Partition, PartitionId, Range, ScanError, SourceDescriptor,
    SourceError,
};

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
            (known.iter().chain(&new).cloned().collect(), new)
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

enum Leaf {
    Bounded(Arc<dyn BoundedSource + Send + Sync>),
    Files(Box<FileLeaf>),
}

impl Leaf {
    fn schema(&self) -> SchemaRef {
        match self {
            Self::Bounded(source) => source.schema(),
            Self::Files(files) => files.schema.clone(),
        }
    }
}

/// One flow's streaming reads as one source.
pub(in crate::pipelines) struct FlowSource {
    schema: SchemaRef,
    leaves: BTreeMap<String, Leaf>,
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
        Ok(Self { schema, leaves })
    }

    /// See what the file leaves hold now. A run calls this before capturing
    /// its frontier, because listing is asynchronous and the frontier is not.
    pub(in crate::pipelines) async fn refresh(&self, ctx: &SessionContext) -> SparkResult<()> {
        for leaf in self.leaves.values() {
            if let Leaf::Files(files) = leaf {
                files.refresh(ctx).await?;
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

    /// The batch's rows of the flow's query, rejecting what needs state.
    pub(in crate::pipelines) async fn batch_plan(
        &self,
        ctx: &SessionContext,
        config: &Arc<PlanConfig>,
        scope: &GraphScope<'_>,
        graph: &GraphDefinition,
        flow: &Flow,
        batch: &BatchIntent,
    ) -> SparkResult<LogicalPlan> {
        let mut streams = BTreeMap::new();
        for (key, leaf) in &self.leaves {
            let head = prefix(key);
            let mut plans = vec![];
            for range in &batch.ranges {
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
                });
            }
            streams.insert(key.clone(), union(plans, &leaf.schema())?);
        }
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
            .resolve_named_plan(spec::Plan::Query(flow.query.clone()))
            .await?;
        let plan = match resolved.fields {
            Some(fields) => rename_logical_plan(resolved.plan, &fields)?,
            None => resolved.plan,
        };
        check_stateless(&plan, false)?;
        Ok(plan)
    }
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
                Leaf::Files(_) => partitions.push(Partition {
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
            (Leaf::Files(_), _) => Ok(Cursor("0".to_string())),
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
        }
    }

    fn frontier(&self, partition: &PartitionId) -> Result<Cursor, SourceError> {
        match self.route(partition)? {
            (Leaf::Bounded(source), inner) => source.frontier(&inner),
            (Leaf::Files(files), _) => Ok(Cursor(files.len()?.to_string())),
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
            (Leaf::Files(_), _) => {
                let parse = |cursor: &Cursor| cursor.0.parse::<usize>().map_err(internal);
                let end = (parse(start)? + MAX_FILES_PER_BATCH).min(parse(frontier)?);
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
