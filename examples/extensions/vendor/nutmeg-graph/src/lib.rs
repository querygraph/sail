//! Grust's graph algorithms as DataFusion table functions over named graphs.
//!
//! The reference catalog — which
//! algorithms exist, their arguments, option names, defaults and declared
//! outputs — is Grust's procedure registry (`grust-algorithm-procedures`),
//! the same one behind `CALL grust.algorithms.pagerank(...)`. A call here
//! takes the same configuration map: `orientation`, `nodeLabels`,
//! `relationshipTypes`, `weightProperty`, `defaultWeight`, plus the
//! algorithm's own options; unknown keys are rejected by Grust's validator.
//! An algorithm registered in Grust appears here by name; serving it needs
//! one dispatch arm, and a test fails until that arm exists.
//! Nutmeg additionally exposes explicitly named experimental `pagerankDelta`
//! and `wccRandomized` / `wccRandomizedFused` kernels; the Grust reference names
//! remain unchanged.
//!
//! A graph is staged once under a name as node and edge record batches in
//! the grust-arrow layout (`node_id`, `label` / `source`, `target`, `label`,
//! `edge_id`, `property.<key>` + `present.<key>`). Rows in other layouts —
//! grust-sail's `grust_nodes`/`grust_edges` tables, or arbitrary tables —
//! are renamed into it when staged ([`ColumnMapping`]). Projections are built
//! by Grust's `GraphProjection::from_arrow_batches` and cached per
//! (graph revision, projection options), like Grust's own preparation cache.
//! Kernels run in process with Grust's work charging and memory admission,
//! and results leave through Grust's Arrow result cursors.
//!
//! One memory budget (`NUTMEG_MEMORY_BYTES`, 8 GiB by default) bounds all of
//! it: staged rows across every graph, each write's transient copies and its
//! sort, cached projections and running kernels. It is a Grust
//! `ExecutionContext`, so a write is admitted before its rows are copied and
//! refused, leaving the graph as it was, when it does not fit; see
//! [`Staging`] and [`Registry::memory`]. Each read runs on a child of that
//! context ([`Query`]): its memory counts against the one budget, and its
//! cancellation, deadline and work budget ([`QueryLimits`]) are its own.
//!
//! A read's kernel runs when the query executes, not while it is planned: the
//! scan plans an [`AlgorithmExec`], whose stream runs the kernel on a thread of
//! its own and hands its batches on as they are made, through a bounded
//! channel. Dropping the stream cancels the read, so an engine's interrupt
//! stops the kernel. `nutmeg_reads()` lists reads and how they ended. See
//! [`ReadExecution`] for the materialised alternative, and when it is used.
//!
//! This crate knows nothing about Sail or Spark: it registers into any
//! DataFusion 55 `SessionContext`. `nutmeg-sail` adapts it to a Sail session.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use arrow::array::{
    Array, ArrayData, ArrayRef, AsArray, BooleanArray, FixedSizeListArray, Float64Array,
    Int64Array, StringArray, StringBuilder, new_null_array,
};
use arrow::buffer::{BooleanBuffer, Buffer, OffsetBuffer};
use arrow::compute::{SortOptions, cast, interleave, is_not_null};
use arrow::datatypes::{DataType, Field, Float32Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunction, TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, internal_err, plan_err};
use datafusion_expr::{Expr, TableType};
use grust_algorithms::{
    ArrowResultCursor, CsrEstimate, GraphProjection, NodeProperties, ProjectionOptions,
    PropertyKind, WeightSelection,
};
use grust_core::Value;
use grust_procedures::{
    ChildLimits, ExecutionContext, ExecutionLimits, MemoryReservation, ProcedureDefinition,
    ProcedureError, ProcedureRegistry, RegistryBuilder, ResourceUsage, SnapshotIdentity,
    ValidatedArguments, ValueType,
};
use once_cell::sync::{Lazy, OnceCell};

mod admission;
mod graph_tables;
mod optimized;
mod prepared_schema;
pub use graph_tables::GraphSnapshot;
pub use prepared_schema::prepare_output_schemas;
mod session;
pub use session::{GraphStaging, SessionRegistry};

const PREFIX: &str = "grust.algorithms.";

fn err(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!("nutmeg: {e}"))
}

static PROCEDURES: Lazy<ProcedureRegistry> = Lazy::new(|| {
    let mut builder = RegistryBuilder::default();
    grust_algorithm_procedures::register_algorithms(&mut builder)
        .expect("grust algorithm procedures register");
    optimized::register(&mut builder).expect("Nutmeg optimized kernels register");
    builder.build()
});

/// Grust's definitions of every algorithm, straight from its registry.
pub fn definitions() -> Vec<&'static ProcedureDefinition> {
    let mut all: Vec<_> = PROCEDURES
        .definitions()
        .filter(|d| d.name.starts_with(PREFIX))
        .collect();
    all.sort_by(|a, b| a.name.cmp(&b.name));
    all
}

// Grust's registry keeps names case-folded. The spellings Grust registers
// them under, used for display and to derive SQL names, are its projection
// kernels' own names plus the two inspection procedures Nutmeg serves by hand
// in `run`; a name found in neither is shown as the registry has it.
const INSPECTIONS: [&str; 2] = ["estimateCsr", "projectionStats"];

fn spelled(registered: &'static str) -> &'static str {
    grust_algorithm_procedures::projection_kernel_names()
        .into_iter()
        .chain(INSPECTIONS)
        .chain(optimized::NAMES)
        .find(|s| s.eq_ignore_ascii_case(registered))
        .unwrap_or(registered)
}

/// The names a result's columns are reported under.
///
/// `Grust` (the default) is the registry's declared outputs, the names
/// `CALL grust.algorithms.<name>(...) YIELD ...` uses. `Gds` renames the
/// columns listed in [`GDS_COLUMN_ALIASES`] to the names Neo4j Graph Data
/// Science gives the same quantity, for code moving over from GDS; every
/// other column keeps its Grust name. The choice is per read, so no Grust
/// name is ever unreachable: a read without it gets Grust's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ColumnNames {
    #[default]
    Grust,
    Gds,
}

/// The read option, SQL configuration key and client keyword that choose
/// [`ColumnNames`]. It is Nutmeg's, not an algorithm option: it is taken
/// out before Grust validates the rest, and a test fails if any registered
/// kernel ever declares an option or argument of the same name.
pub const COLUMN_NAMES_OPTION: &str = "columnNames";

impl ColumnNames {
    pub fn parse(text: &str) -> Result<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "grust" => Ok(Self::Grust),
            "gds" => Ok(Self::Gds),
            other => {
                plan_err!("nutmeg: `{COLUMN_NAMES_OPTION}` is `grust` or `gds`, got `{other}`")
            }
        }
    }

    /// Remove the choice from a JSON configuration, leaving Grust's options.
    pub fn take(options: &mut serde_json::Map<String, serde_json::Value>) -> Result<Self> {
        let Some(key) = options
            .keys()
            .find(|k| k.eq_ignore_ascii_case(COLUMN_NAMES_OPTION))
            .cloned()
        else {
            return Ok(Self::Grust);
        };
        match options.remove(&key) {
            Some(serde_json::Value::String(text)) => Self::parse(&text),
            other => plan_err!("nutmeg: `{COLUMN_NAMES_OPTION}` must be a string, got {other:?}"),
        }
    }

    fn rename(self, name: &str) -> &str {
        match self {
            Self::Grust => name,
            Self::Gds => GDS_COLUMN_ALIASES
                .iter()
                .find(|(grust, _)| *grust == name)
                .map_or(name, |(_, gds)| gds),
        }
    }

    /// `schema` with each field renamed. Every column is looked up by its
    /// Grust name, so a rename is simultaneous: `triangles` → `triangleCount`
    /// and `triangleCount` → `globalTriangleCount` in one result is a
    /// relabelling, not a chain. A rename that would give two columns one
    /// name is refused rather than shadowing one of them.
    pub fn rename_schema(self, schema: &SchemaRef) -> Result<SchemaRef> {
        if self == Self::Grust {
            return Ok(schema.clone());
        }
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_name(self.rename(f.name())))
            .collect();
        let mut seen = HashSet::new();
        if let Some(twice) = fields.iter().find(|f| !seen.insert(f.name().as_str())) {
            return exec_err!(
                "nutmeg: `{COLUMN_NAMES_OPTION}: gds` would name two columns `{}`",
                twice.name()
            );
        }
        Ok(Arc::new(Schema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        )))
    }

    fn rename_batch(self, batch: RecordBatch) -> Result<RecordBatch> {
        if self == Self::Grust {
            return Ok(batch);
        }
        let schema = self.rename_schema(&batch.schema())?;
        Ok(RecordBatch::try_new(schema, batch.columns().to_vec())?)
    }
}

/// Grust's column name → the name Neo4j Graph Data Science uses for the same
/// quantity. Data, keyed by column and not by algorithm: an entry applies
/// wherever Grust yields that column, so it is listed only where the column
/// means the same thing in every kernel that yields it, and GDS's name is
/// cited from GDS's documentation (the procedure pages under
/// <https://neo4j.com/docs/graph-data-science/current/algorithms/>):
///
/// - `pathIndex` → `index`: `gds.shortestPath.yens.stream` yields `index`.
///   Grust cannot use `index`, a reserved word in its Cypher dialect.
/// - `iterations` → `ranIterations`, `converged` → `didConverge`: the stats
///   mode of `gds.pageRank`, `gds.articleRank`, `gds.eigenvector`,
///   `gds.hits`, `gds.labelPropagation`, `gds.k1coloring`, and
///   (`didConverge`) `gds.leiden`.
/// - `levels` → `ranLevels`: stats mode of `gds.louvain` and `gds.leiden`.
/// - `triangles` → `triangleCount`, `triangleCount` → `globalTriangleCount`:
///   `gds.triangleCount` streams a node's triangles as `triangleCount` and
///   reports the graph's total as `globalTriangleCount`; Grust's per-node
///   `triangles` and total `triangleCount` are those two.
/// - `coefficient` → `localClusteringCoefficient`, `averageCoefficient` →
///   `averageClusteringCoefficient`: `gds.localClusteringCoefficient` stream
///   and stats modes.
///
/// GDS reports the iteration and level counts in its stats mode, and Grust
/// repeats them on every row; the rename gives them GDS's names, not GDS's
/// row shape.
pub const GDS_COLUMN_ALIASES: &[(&str, &str)] = &[
    ("pathIndex", "index"),
    ("iterations", "ranIterations"),
    ("converged", "didConverge"),
    ("levels", "ranLevels"),
    ("triangles", "triangleCount"),
    ("triangleCount", "globalTriangleCount"),
    ("coefficient", "localClusteringCoefficient"),
    ("averageCoefficient", "averageClusteringCoefficient"),
];

fn short(definition: &'static ProcedureDefinition) -> &'static str {
    spelled(&definition.name[PREFIX.len()..])
}

/// Grust's short algorithm names (`pagerank`, `shortestPaths`, …).
pub fn algorithm_names() -> Vec<&'static str> {
    definitions().into_iter().map(short).collect()
}

/// `shortestPaths` → `shortest_paths`, for SQL function names.
pub fn snake(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_uppercase() {
            out.push('_');
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Resolve a user-supplied name in either spelling to Grust's.
pub fn resolve_algorithm(name: &str) -> Option<&'static str> {
    let wanted = name.trim().to_ascii_lowercase().replace(['_', '-'], "");
    algorithm_names()
        .into_iter()
        .find(|n| n.to_ascii_lowercase() == wanted)
}

/// The process's memory budget when neither `NUTMEG_MEMORY_BYTES` nor
/// [`set_memory_bytes`] sets one: 8 GiB.
pub const DEFAULT_MEMORY_BYTES: usize = 8 << 30;

/// The environment variable that sets the memory budget, in bytes.
pub const MEMORY_BYTES_VARIABLE: &str = "NUTMEG_MEMORY_BYTES";

static MEMORY_BYTES: AtomicUsize = AtomicUsize::new(DEFAULT_MEMORY_BYTES);

/// Set the process's memory budget (see [`Registry::memory`]). The budget is
/// fixed when the registry is first used, so this is refused afterwards: the
/// bytes already admitted were admitted against the old one. The
/// `NUTMEG_MEMORY_BYTES` environment variable, when set, wins.
pub fn set_memory_bytes(bytes: usize) -> Result<()> {
    if STORE.get().is_some() {
        return exec_err!(
            "nutmeg: the memory budget is fixed once the first graph is staged; \
             set it at startup, or with {MEMORY_BYTES_VARIABLE}"
        );
    }
    MEMORY_BYTES.store(bytes, Ordering::Relaxed);
    Ok(())
}

/// The environment variable that gives the store's own execution a worker
/// count. Projections are built on that execution, so this is how many threads
/// a projection build may use; unset, a build runs on one thread, as before.
/// A read that names no `concurrency` of its own inherits it.
pub const WORKERS_VARIABLE: &str = "NUTMEG_WORKERS";

fn workers() -> Option<usize> {
    static ENV: Lazy<Option<usize>> = Lazy::new(|| {
        std::env::var(WORKERS_VARIABLE)
            .ok()?
            .parse()
            .ok()
            .filter(|&workers| workers > 0)
    });
    *ENV
}

fn memory_bytes() -> usize {
    static ENV: Lazy<Option<usize>> =
        Lazy::new(|| std::env::var(MEMORY_BYTES_VARIABLE).ok()?.parse().ok());
    ENV.unwrap_or_else(|| MEMORY_BYTES.load(Ordering::Relaxed))
}

/// Which input columns hold the structural fields. Anything unset is found
/// by name: the grust-arrow names first, then grust-sail's table columns.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ColumnMapping {
    pub id: Option<String>,
    pub label: Option<String>,
    pub source: Option<String>,
    pub target: Option<String>,
    pub edge_type: Option<String>,
    pub edge_id: Option<String>,
    /// Keep integer node ids and edge endpoints as Int64 instead of casting
    /// them to text: `ids` = `int64`. Off by default, because it changes the
    /// canonical order of integer ids from text order to numeric order.
    pub integer_ids: bool,
}

impl ColumnMapping {
    /// Set one field from a lowercase option key; false when not a mapping key.
    pub fn set(&mut self, key: &str, value: String) -> bool {
        if key == "ids" {
            self.integer_ids = match value.as_str() {
                "int64" => true,
                "text" => false,
                _ => return false,
            };
            return true;
        }
        let slot = match key {
            "idcolumn" => &mut self.id,
            "labelcolumn" => &mut self.label,
            "sourcecolumn" => &mut self.source,
            "targetcolumn" => &mut self.target,
            "typecolumn" => &mut self.edge_type,
            "edgeidcolumn" => &mut self.edge_id,
            _ => return false,
        };
        *slot = Some(value);
        true
    }
}

fn pick<'a>(
    batch: &'a RecordBatch,
    explicit: &Option<String>,
    candidates: &[&str],
    what: &str,
    required: bool,
) -> Result<Option<(&'a ArrayRef, String)>> {
    if let Some(name) = explicit {
        return match batch.column_by_name(name) {
            Some(column) => Ok(Some((column, name.clone()))),
            None => plan_err!("nutmeg: {what} column `{name}` not found"),
        };
    }
    for name in candidates {
        if let Some(column) = batch.column_by_name(name) {
            return Ok(Some((column, name.to_string())));
        }
    }
    if required {
        let have: Vec<_> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        return plan_err!(
            "nutmeg: no {what} column; looked for {candidates:?} in {have:?}; name it with an option"
        );
    }
    Ok(None)
}

fn utf8(column: &ArrayRef) -> Result<ArrayRef> {
    Ok(cast(column, &DataType::Utf8)?)
}

/// A node id or an edge endpoint as staged. Text unless the mapping asks for
/// integer identity (`ids` = `int64`); then Int64 when the column is an integer
/// that fits one, and text otherwise.
///
/// Cast to text, the projection hashes two strings for every edge. Kept as
/// Int64 they reach Grust's integer path (`GraphProjection::from_arrow_batches`),
/// which resolves an endpoint through a direct table or a sorted lookup. A
/// node's external id is still its decimal text, so results and kernel sources
/// read as before; what changes is the canonical order of the staged rows,
/// numeric instead of text. UInt64 can exceed Int64, so it stays text.
fn id_column(column: &ArrayRef, mapping: &ColumnMapping) -> Result<ArrayRef> {
    if !mapping.integer_ids {
        return utf8(column);
    }
    Ok(match column.data_type() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => cast(column, &DataType::Int64)?,
        _ => cast(column, &DataType::Utf8)?,
    })
}

fn constant(value: &str, rows: usize) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(rows, value.len().saturating_mul(rows));
    for _ in 0..rows {
        builder.append_value(value);
    }
    Arc::new(builder.finish())
}

fn node_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("node_id", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
    ]))
}

/// Rename node rows into the grust-arrow layout: `node_id`, `label`, and for
/// every other column `c` a kernel could read as a node property, the pair
/// `property.c` and `present.c`. Integers become Int64 and other numbers
/// Float64, as on edges; strings stay Utf8, for categories; fixed-size lists
/// of floats stay as they are, for vectors. Columns already named
/// `property.*`/`present.*` pass through.
///
/// Until these were kept, every node column but the id and label was dropped
/// here, so no kernel that reads node properties could run on a staged graph.
pub fn normalize_nodes(batch: &RecordBatch, mapping: &ColumnMapping) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let (id, id_name) =
        pick(batch, &mapping.id, &["node_id", "id"], "node id", true)?.expect("required");
    let label = pick(batch, &mapping.label, &["label"], "node label", false)?;
    let id = id_column(id, mapping)?;
    let mut fields = vec![
        Field::new("node_id", id.data_type().clone(), true),
        Field::new("label", DataType::Utf8, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        id,
        match &label {
            Some((column, _)) => utf8(column)?,
            None => constant("", rows),
        },
    ];
    let mut used: HashSet<String> = [id_name].into();
    used.extend(label.map(|(_, name)| name));
    lift_properties(batch, &used, Lift::Node, &mut fields, &mut columns)?;
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// Which column kinds become properties: edges carry weights, which are
/// numbers; nodes also carry categories and vectors.
#[derive(Clone, Copy, PartialEq)]
enum Lift {
    Edge,
    Node,
}

/// Append `property.c` + `present.c` for every unused column `c` of a kind
/// `lift` admits, and pass `property.*`/`present.*` columns through.
fn lift_properties(
    batch: &RecordBatch,
    used: &HashSet<String>,
    lift: Lift,
    fields: &mut Vec<Field>,
    columns: &mut Vec<ArrayRef>,
) -> Result<()> {
    let schema = batch.schema();
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        let name = field.name();
        if used.contains(name) {
            continue;
        }
        if name.starts_with("property.") || name.starts_with("present.") {
            fields.push(field.as_ref().clone());
            columns.push(column.clone());
            continue;
        }
        let kind = field.data_type();
        let values = if kind.is_integer() {
            cast(column, &DataType::Int64)?
        } else if kind.is_numeric() {
            cast(column, &DataType::Float64)?
        } else if lift == Lift::Node && matches!(kind, DataType::Utf8 | DataType::LargeUtf8) {
            cast(column, &DataType::Utf8)?
        } else if lift == Lift::Node && matches!(kind, DataType::FixedSizeList(..)) {
            column.clone()
        } else {
            continue;
        };
        fields.push(Field::new(
            format!("property.{name}"),
            values.data_type().clone(),
            true,
        ));
        columns.push(values);
        fields.push(Field::new(
            format!("present.{name}"),
            DataType::Boolean,
            false,
        ));
        columns.push(Arc::new(is_not_null(column)?));
    }
    Ok(())
}

/// Rename edge rows into the grust-arrow layout: `source`, `target`, `label`,
/// `edge_id`, and for every other numeric column `c` the pair `property.c`
/// (Float64 or Int64) and `present.c`, so `weightProperty: "c"` selects it.
/// Columns already named `property.*`/`present.*` pass through.
pub fn normalize_edges(batch: &RecordBatch, mapping: &ColumnMapping) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let (source, source_name) = pick(
        batch,
        &mapping.source,
        &["source", "src_id", "src"],
        "edge source",
        true,
    )?
    .expect("required");
    let (target, target_name) = pick(
        batch,
        &mapping.target,
        &["target", "dst_id", "dst"],
        "edge target",
        true,
    )?
    .expect("required");
    let label = pick(
        batch,
        &mapping.edge_type,
        &["label", "edge_type", "type"],
        "edge type",
        false,
    )?;
    let edge_id = pick(
        batch,
        &mapping.edge_id,
        &["edge_id", "id"],
        "edge id",
        false,
    )?;
    // Both endpoints take one type: Int64 when both are integers, text otherwise.
    let (mut source, mut target) = (id_column(source, mapping)?, id_column(target, mapping)?);
    if source.data_type() != target.data_type() {
        (source, target) = (utf8(&source)?, utf8(&target)?);
    }
    let mut fields = vec![
        Field::new("source", source.data_type().clone(), true),
        Field::new("target", target.data_type().clone(), true),
        Field::new("label", DataType::Utf8, true),
        Field::new("edge_id", DataType::Utf8, true),
    ];
    let mut columns = vec![
        source,
        target,
        match &label {
            Some((column, _)) => utf8(column)?,
            None => constant("", rows),
        },
        match &edge_id {
            Some((column, _)) => utf8(column)?,
            None => new_null_array(&DataType::Utf8, rows),
        },
    ];
    let mut used: HashSet<String> = [source_name, target_name].into();
    used.extend(label.into_iter().chain(edge_id).map(|(_, name)| name));
    lift_properties(batch, &used, Lift::Edge, &mut fields, &mut columns)?;
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// The row order a staged part is kept in.
///
/// Grust's kernels are deterministic for a given input, but the input
/// includes its order: projection rows follow the order nodes are staged (or,
/// for an edges-only graph, the order their ids first appear among the
/// edges), adjacency follows edge order, and Leiden, Louvain and label
/// propagation visit nodes, and break ties, in that order. A DataFrame's rows
/// have no order Spark promises, and Sail's scan order changes from run to
/// run, so the same query staged twice could give two different results.
///
/// `Canonical`, the default, keeps each part sorted by a total order on its
/// rows, over the whole part, appends included, so what a kernel sees is a
/// function of the set of rows staged and never of the order they arrived in:
///
/// - nodes by `node_id`. Ids are unique in any graph a projection accepts
///   (Grust refuses a duplicate), so this is total on every graph that runs.
/// - edges by `source`, `target`, `edge_id` (null last), `label`, then every
///   other column, by column name. Two edges that tie on all of those are
///   equal in every column a kernel can read, so the sorted part is the same
///   whichever of them came first: without an `edge_id`, parallel edges
///   differing only in weight are still ordered, by weight.
///
/// Ids are Utf8 once staged, so the order is lexicographic: `"10"` sorts
/// before `"9"`. That is still a total order, which is all determinism needs;
/// no kernel reads the order as a numeric one.
///
/// `AsStaged` keeps rows in arrival order, as before: for callers that already
/// stage in an order of their own, or want to skip the sort.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StageOrder {
    #[default]
    Canonical,
    AsStaged,
}

/// The write option, and Python client keyword, that chooses [`StageOrder`].
pub const ORDER_OPTION: &str = "order";

/// What staging one part admitted, tier by tier, for the record: the sort's
/// working space as the three components its refusal names (permutation,
/// keys, sorted copy), the schema fill, what the normalized rows held before
/// the sort, what the part keeps afterwards, and the time the canonical sort
/// took. Under [`StageOrder::AsStaged`] `sorted` is false and the sort
/// fields are zero. These are the admitted bounds, not measured RSS, so a
/// successful stage reports the same figures a refused one would have.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct StageTiers {
    pub sorted: bool,
    pub fill_bytes: usize,
    pub sort_permutation_bytes: usize,
    pub sort_keys_bytes: usize,
    pub sorted_copy_bytes: usize,
    pub normalized_bytes: usize,
    pub retained_bytes: usize,
    pub sort_seconds: f64,
}

impl StageOrder {
    pub fn parse(text: &str) -> Result<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "canonical" => Ok(Self::Canonical),
            "asstaged" => Ok(Self::AsStaged),
            other => {
                plan_err!("nutmeg: `{ORDER_OPTION}` is `canonical` or `asStaged`, got `{other}`")
            }
        }
    }
}

/// Rows per batch of a sorted part.
const SORTED_BATCH_ROWS: usize = 64 * 1024;

/// Rewrite a whole part in canonical order (see [`StageOrder`]). The batches
/// are brought to one schema first ([`unify`]); the sort is a permutation of
/// (batch, row) positions compared through Arrow's row format, and the rows
/// are then gathered into batches of [`SORTED_BATCH_ROWS`], so no column is
/// ever concatenated past Arrow's 32-bit offsets.
///
/// The sort's working space is admitted from `budget` before the sort starts:
/// the permutation, and whichever is larger of the sort keys and the sorted
/// copy, since the keys are freed before the copy is made. The input stays
/// held throughout, by the caller's admissions. What is returned beside the
/// sorted batches is the admission for the copy, which the part keeps; the
/// rest is released on return.
fn canonicalize(
    part: Part,
    mut batches: Vec<RecordBatch>,
    declared: Option<SchemaRef>,
    budget: &Budget<'_>,
) -> Result<(Vec<RecordBatch>, Admitted, StageTiers)> {
    let started = Instant::now();
    let mut tiers = StageTiers {
        sorted: true,
        ..StageTiers::default()
    };
    if let Some(schema) = declared {
        batches.push(RecordBatch::new_empty(schema));
    }
    if batches.is_empty() {
        return Ok((batches, Admitted::default(), tiers));
    }
    let mut work = Admitted::default();
    let fill_bound = admission::unify_bound(&batches);
    budget.admit(&mut work, fill_bound, || {
        format!("{fill_bound} bytes to fill columns an append lacked")
    })?;
    let (batches, filled) = unify(batches)?;
    if filled > fill_bound {
        return internal_err!("nutmeg: schema unification exceeded its pre-admitted bound");
    }
    work.shrink_to(filled).map_err(err)?;
    tiers.fill_bytes = filled;
    let batches: Vec<RecordBatch> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();
    if batches.is_empty() {
        tiers.sort_seconds = started.elapsed().as_secs_f64();
        return Ok((batches, Admitted::default(), tiers));
    }
    let schema = batches[0].schema();
    let structural: &[&str] = match part {
        Part::Nodes => &["node_id"],
        Part::Edges => &["source", "target", "edge_id", "label"],
    };
    let mut key: Vec<usize> = structural
        .iter()
        .map(|name| schema.index_of(name))
        .collect::<std::result::Result<_, _>>()?;
    if part == Part::Edges {
        let mut rest: Vec<(&str, usize)> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, field)| {
                !key.contains(index)
                    && RowConverter::supports_fields(&[SortField::new(field.data_type().clone())])
            })
            .map(|(index, field)| (field.name().as_str(), index))
            .collect();
        rest.sort();
        key.extend(rest.into_iter().map(|(_, index)| index));
    }
    let converter = RowConverter::new(
        key.iter()
            .map(|&index| {
                SortField::new_with_options(
                    schema.field(index).data_type().clone(),
                    SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                )
            })
            .collect(),
    )?;

    // Admit the working space before any of it is allocated.
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    let permutation = total.saturating_mul(size_of::<(usize, usize)>());
    let copy = admission::copy_bound(&batches);
    let keys = admission::sort_keys_bound(&batches, &key);
    let beyond_copy = keys.saturating_sub(copy);
    let need = permutation.saturating_add(beyond_copy).saturating_add(copy);
    tiers.sort_permutation_bytes = permutation;
    tiers.sort_keys_bytes = keys;
    tiers.sorted_copy_bytes = copy;
    let describe = || {
        format!(
            "the sort's working space: {permutation} bytes of permutation, about {keys} of sort \
             keys and {copy} for the sorted copy of {total} rows (stage with `{ORDER_OPTION}` = \
             `asStaged` to skip the sort)"
        )
    };
    let mut sorted = Admitted::default();
    budget.admit(&mut work, permutation.saturating_add(beyond_copy), || {
        format!("{need} bytes of {}", describe())
    })?;
    budget.admit(&mut sorted, copy, || {
        format!("{need} bytes of {}", describe())
    })?;

    let rows: Vec<Rows> = batches
        .iter()
        .map(|batch| {
            let columns: Vec<ArrayRef> = key.iter().map(|&i| batch.column(i).clone()).collect();
            converter.convert_columns(&columns)
        })
        .collect::<std::result::Result<_, _>>()?;
    let encoded: usize = rows.iter().map(Rows::size).sum();
    let room = keys.max(copy);
    if encoded > room {
        return internal_err!("nutmeg: sort keys exceeded their pre-admitted bound");
    }
    let mut order: Vec<(usize, usize)> = Vec::with_capacity(total);
    for (b, batch) in batches.iter().enumerate() {
        order.extend((0..batch.num_rows()).map(|r| (b, r)));
    }
    // Unstable is enough: rows that compare equal are equal in every column
    // the key covers, and those are all the columns a kernel reads.
    order.sort_unstable_by(|x, y| rows[x.0].row(x.1).cmp(&rows[y.0].row(y.1)));
    drop(rows);
    let mut out = Vec::with_capacity(order.len().div_ceil(SORTED_BATCH_ROWS));
    for chunk in order.chunks(SORTED_BATCH_ROWS) {
        let columns = (0..schema.fields().len())
            .map(|c| {
                let arrays: Vec<&dyn Array> =
                    batches.iter().map(|b| b.column(c).as_ref()).collect();
                interleave(&arrays, chunk)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        out.push(RecordBatch::try_new(schema.clone(), columns)?);
    }
    // All copied buffers were admitted before interleave; no post-allocation
    // top-up can hide an underestimated bound. Return the conservative excess.
    let held = held_bytes(&out);
    if held > sorted.bytes() {
        return internal_err!("nutmeg: sorted copy exceeded its pre-admitted bound");
    } else {
        sorted.shrink_to(held).map_err(err)?;
    }
    tiers.retained_bytes = sorted.bytes();
    tiers.sort_seconds = started.elapsed().as_secs_f64();
    Ok((out, sorted, tiers))
}

/// An upper bound on the bytes Arrow's row format takes to encode the `key`
/// columns of `batch`, offsets included, or `None` for a key type covered by
/// the conservative nested-layout bound in `admission`. Per value: one byte of null
/// sentinel plus the width for fixed-width types, and for bytes and strings
/// at most `37 + 9/8 × len` (mini-blocks of 8 bytes plus a continuation byte
/// up to 32 bytes, then blocks of 32 plus one).
fn key_bytes_bound(batch: &RecordBatch, key: &[usize]) -> Option<usize> {
    let rows = batch.num_rows();
    let mut bytes = size_of::<Rows>().saturating_add((rows + 1).saturating_mul(size_of::<usize>()));
    for &index in key {
        let column = batch.column(index);
        let values = match column.data_type() {
            DataType::Utf8 => {
                let offsets = column.as_string::<i32>().offsets();
                Some((offsets[rows] - offsets[0]) as usize)
            }
            DataType::LargeUtf8 => {
                let offsets = column.as_string::<i64>().offsets();
                Some((offsets[rows] - offsets[0]) as usize)
            }
            DataType::Binary => {
                let offsets = column.as_binary::<i32>().offsets();
                Some((offsets[rows] - offsets[0]) as usize)
            }
            DataType::LargeBinary => {
                let offsets = column.as_binary::<i64>().offsets();
                Some((offsets[rows] - offsets[0]) as usize)
            }
            _ => None,
        };
        let encoded = match (values, column.data_type()) {
            (Some(values), _) => rows
                .saturating_mul(37)
                .saturating_add(values.saturating_mul(9).div_ceil(8)),
            (None, DataType::Boolean) => rows.saturating_mul(2),
            (None, DataType::Null) => rows,
            (None, kind) => rows.saturating_mul(1 + kind.primitive_width()?),
        };
        bytes = bytes.saturating_add(encoded);
    }
    Some(bytes)
}

/// An upper bound on the bytes the sorted copy of `batches` takes: every
/// value once, as [`ArrayData::get_slice_memory_size`] counts it, plus per
/// output batch and column an offset, a null bitmap that the input may not
/// have had, and 64-byte rounding of each buffer.
fn sorted_copy_bound(batches: &[RecordBatch]) -> usize {
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    let columns = batches.first().map_or(0, |b| b.num_columns());
    let values: usize = batches
        .iter()
        .flat_map(|b| b.columns())
        .map(|column| slice_bytes(column.as_ref()))
        .fold(0, usize::saturating_add);
    let chunks = rows.div_ceil(SORTED_BATCH_ROWS);
    let per_column = rows
        .div_ceil(8)
        .saturating_add(chunks.saturating_mul(4 * 64 + 16));
    values.saturating_add(columns.saturating_mul(per_column))
}

/// The bytes `array`'s own rows occupy, not counting whatever else the buffers
/// it slices hold: the size a compact copy of it would have.
fn slice_bytes(array: &dyn Array) -> usize {
    let data = array.to_data();
    data.get_slice_memory_size()
        .unwrap_or_else(|_| data.get_buffer_memory_size())
}

/// The bytes `batches` keep alive: every distinct allocation their arrays
/// reference, at its full capacity, counted once. A slice keeps its whole
/// buffer alive, so a batch sliced from a larger one is charged for all of
/// it; two columns or batches that share an allocation are charged for it
/// once.
pub fn held_bytes(batches: &[RecordBatch]) -> usize {
    fn visit(data: &ArrayData, seen: &mut HashMap<usize, usize>) {
        for buffer in data.buffers() {
            seen.insert(buffer.data_ptr().as_ptr() as usize, buffer.capacity());
        }
        if let Some(nulls) = data.nulls() {
            let buffer = nulls.buffer();
            seen.insert(buffer.data_ptr().as_ptr() as usize, buffer.capacity());
        }
        for child in data.child_data() {
            visit(child, seen);
        }
    }
    let mut seen = HashMap::new();
    for batch in batches {
        for column in batch.columns() {
            visit(&column.to_data(), &mut seen);
        }
    }
    seen.values()
        .fold(0, |sum, bytes| sum.saturating_add(*bytes))
}

/// Bring a part's batches to one schema, so they can be sorted together.
/// Staged batches normally share one already; appends of differently shaped
/// DataFrames need not. A column missing from a batch is added as Grust would
/// read its absence: a `present.*` column as all false, anything else (a
/// `property.*` column) as all null. A column name that means two data types,
/// or appears twice in one batch, has no single sorted form and is refused.
/// Also returns the bytes of the columns it had to make.
fn unify(batches: Vec<RecordBatch>) -> Result<(Vec<RecordBatch>, usize)> {
    let first = batches[0].schema();
    if batches
        .iter()
        .all(|b| b.schema().fields() == first.fields())
    {
        return Ok((batches, 0));
    }
    let mut fields: Vec<Field> = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        let mut names = HashSet::new();
        for field in schema.fields() {
            if !names.insert(field.name().as_str()) {
                return plan_err!(
                    "nutmeg: column `{}` appears twice; canonical order cannot sort it \
                     (stage with `{ORDER_OPTION}` = `asStaged`)",
                    field.name()
                );
            }
            match fields.iter_mut().find(|f| f.name() == field.name()) {
                Some(seen) if seen.data_type() != field.data_type() => {
                    return plan_err!(
                        "nutmeg: column `{}` is {} in some staged rows and {} in others; \
                         canonical order sorts the whole part as one table, so cast one of \
                         them, or stage with `{ORDER_OPTION}` = `asStaged`",
                        field.name(),
                        seen.data_type(),
                        field.data_type()
                    );
                }
                Some(seen) => {
                    let nullable = seen.is_nullable() || field.is_nullable();
                    seen.set_nullable(nullable);
                }
                None => fields.push(field.as_ref().clone()),
            }
        }
    }
    for field in &mut fields {
        let everywhere = batches
            .iter()
            .all(|b| b.schema().column_with_name(field.name()).is_some());
        if !everywhere && !field.name().starts_with("present.") {
            field.set_nullable(true);
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let mut filled = 0usize;
    let batches = batches
        .into_iter()
        .map(|batch| {
            let rows = batch.num_rows();
            let columns = schema
                .fields()
                .iter()
                .map(|field| {
                    let made = match batch.column_by_name(field.name()) {
                        Some(column) => return column.clone(),
                        None if field.name().starts_with("present.") => {
                            Arc::new(BooleanArray::new(BooleanBuffer::new_unset(rows), None))
                                as ArrayRef
                        }
                        None => new_null_array(field.data_type(), rows),
                    };
                    filled = filled.saturating_add(made.get_buffer_memory_size());
                    made
                })
                .collect();
            Ok(RecordBatch::try_new(schema.clone(), columns)?)
        })
        .collect::<Result<_>>()?;
    Ok((batches, filled))
}

/// Bytes admitted from the memory budget, held as Grust reservation tokens
/// and released when the last token drops. A part keeps the admissions for
/// its rows beside them, so dropping the rows' owner — the part replaced, the
/// graph dropped — returns their bytes. Cloning shares the tokens, and with
/// them the charge: a byte is released once, when no clone holds it.
#[derive(Clone, Debug, Default)]
struct Admitted {
    tokens: Vec<MemoryReservation>,
    bytes: usize,
}

impl Admitted {
    fn bytes(&self) -> usize {
        self.bytes
    }

    fn absorb(&mut self, other: Admitted) {
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.tokens.extend(other.tokens);
    }

    /// Lower the charge to `bytes`, returning the rest to the budget now, for
    /// an admission of an upper bound whose real size is known. The latest
    /// tokens give up their bytes first. Clones share the tokens, so this
    /// lowers the charge for every clone; call it before any is made.
    fn shrink_to(&mut self, bytes: usize) -> std::result::Result<(), ProcedureError> {
        let mut excess = self.bytes.saturating_sub(bytes);
        for token in self.tokens.iter().rev() {
            if excess == 0 {
                break;
            }
            let held = token.bytes();
            let given = held.min(excess);
            token.shrink(held - given)?;
            excess -= given;
            self.bytes -= given;
        }
        self.tokens.retain(|token| token.bytes() > 0);
        Ok(())
    }
}

/// Admission from the memory budget on behalf of one graph, so a refusal can
/// say which graph, what for, and how much. `part` is the part being staged,
/// or `None` for the nodes a read derives from the edges.
///
/// `charge` is the execution the bytes are admitted through: the pool for
/// what outlives a query (staged rows, their sort, a projection's build), a
/// query's own child of the pool for what the query alone uses. Either way
/// every byte counts against the pool, which is what a refusal reports.
struct Budget<'a> {
    pool: &'a ExecutionContext,
    charge: &'a ExecutionContext,
    graph: &'a str,
    part: Option<Part>,
}

impl Budget<'_> {
    /// Admit `bytes` into `into` before they are allocated; refused when the
    /// budget has no room, with `what` describing the need.
    fn admit(
        &self,
        into: &mut Admitted,
        bytes: usize,
        what: impl FnOnce() -> String,
    ) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        match self.charge.reserve(bytes) {
            Ok(token) => {
                into.bytes = into.bytes.saturating_add(bytes);
                into.tokens.push(token);
                Ok(())
            }
            Err(ProcedureError::BudgetExceeded { limit, .. }) => Err(self.refused(&what(), limit)),
            Err(other) => Err(err(other)),
        }
    }

    /// The refusal of an admission that the execution with memory limit
    /// `limit` refused: a read's own limit when that is the one, else the
    /// process budget.
    fn refused(&self, what: &str, limit: usize) -> DataFusionError {
        let own = self.charge.limits().memory_bytes;
        if limit == own && own < self.pool.limits().memory_bytes {
            let used = self.charge.usage().map_or(0, |u| u.live_bytes);
            return DataFusionError::ResourcesExhausted(format!(
                "nutmeg: graph `{}`: reading it needs {what}, but {used} of this read's \
                 {own}-byte memory limit (`{QUERY_MEMORY_OPTION}`) are in use",
                self.graph,
            ));
        }
        let used = self.pool.usage().map_or(0, |u| u.live_bytes);
        let (doing, outcome) = match self.part {
            Some(part) => (
                format!("staging its {}", part.name()),
                "the write was refused and the graph is as it was",
            ),
            None => (
                "reading it".to_string(),
                "drop or restage a graph to release its rows and projections",
            ),
        };
        DataFusionError::ResourcesExhausted(format!(
            "nutmeg: graph `{}`: {doing} needs {what}, but {used} of the {}-byte memory budget \
             are in use ({MEMORY_BYTES_VARIABLE}); {outcome}",
            self.graph,
            self.pool.limits().memory_bytes,
        ))
    }
}

/// One named graph: staged rows, and projections built from them.
#[derive(Clone, Default)]
struct Entry {
    nodes: Vec<RecordBatch>,
    edges: Vec<RecordBatch>,
    node_schema: Option<SchemaRef>,
    edge_schema: Option<SchemaRef>,
    /// The memory admitted for each part's rows, held as long as they are.
    node_bytes: Admitted,
    edge_bytes: Admitted,
    /// Whether the whole edges part is in canonical order, so the nodes
    /// derived from it (when no nodes are staged) are put in id order too.
    edges_canonical: bool,
    revision: u64,
    projections: HashMap<String, GraphProjection>,
    /// Every projection built for this revision, in build order, with what
    /// it cost: kept for the record, beside the projections themselves.
    projection_builds: Vec<ProjectionBuild>,
}

/// One projection build: its cache key, the wall time the build took, and
/// the pool's live bytes before it and admitted by it. The bytes are the
/// pool's live delta over the build, so a concurrent read's own admissions
/// would be counted too; a benchmark cell builds alone.
#[derive(Clone, Debug, PartialEq)]
pub struct ProjectionBuild {
    pub key: String,
    pub seconds: f64,
    pub live_bytes_before: usize,
    pub admitted_bytes: usize,
}

/// What [`Registry::list`] reports for one graph.
#[derive(Clone, Debug)]
pub struct GraphInfo {
    pub name: String,
    pub staged_nodes: usize,
    pub staged_edges: usize,
    /// Bytes of the memory budget the staged rows hold: at least what they
    /// keep alive ([`held_bytes`]), and exactly that for a sorted part, whose
    /// sort's bound on the copy is shrunk to the copy once it is made.
    /// Projections are not included; see [`MemoryInfo`].
    pub staged_bytes: usize,
    pub revision: u64,
    pub projections: usize,
    /// The builds behind `projections`, in order; see [`ProjectionBuild`].
    pub projection_builds: Vec<ProjectionBuild>,
}

/// What [`Registry::memory`] reports: the budget and what holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryInfo {
    /// The budget, from `NUTMEG_MEMORY_BYTES` or [`set_memory_bytes`].
    pub limit_bytes: usize,
    /// Everything admitted now: staged rows, writes in progress, cached
    /// projections and running kernels.
    pub used_bytes: usize,
    /// The part of `used_bytes` held by staged rows, summed over graphs.
    pub staged_bytes: usize,
    /// The most ever admitted at once.
    pub peak_bytes: usize,
}

fn poisoned() -> DataFusionError {
    DataFusionError::Execution("nutmeg: registry lock poisoned".into())
}

/// Which rows a staging call carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Part {
    Nodes,
    Edges,
}

impl Part {
    fn name(self) -> &'static str {
        match self {
            Part::Nodes => "nodes",
            Part::Edges => "edges",
        }
    }
}

/// Named graphs and the one memory budget everything built for them is
/// admitted from.
///
/// The budget is a Grust [`ExecutionContext`], so it is Grust's admission —
/// exact under concurrency, released when a reservation token drops — that
/// bounds staging too. Staged rows, each write's transient copies and its
/// sort, every cached projection and every read draw on it: projections are
/// built with it as their context and owned by it, and each read runs its
/// kernel on a child of it ([`Query`]), through a view of the projection, so
/// reads share the budget but are cancelled, budgeted and deadlined apart.
struct Store {
    graphs: RwLock<HashMap<String, Arc<RwLock<Entry>>>>,
    pool: ExecutionContext,
    /// A host-issued admission lease, retained independently of Python bindings.
    owner: Option<Arc<graph_tables::HostOwner>>,
}

static STORE: OnceCell<Store> = OnceCell::new();

fn store() -> &'static Store {
    STORE.get_or_init(|| Store::new(memory_bytes()))
}

impl Store {
    fn new(memory_bytes: usize) -> Self {
        let pool = ExecutionContext::new(ExecutionLimits {
            memory_bytes,
            work_units: usize::MAX,
            batch_rows: 8192,
            deadline: None,
        })
        .expect("a positive batch size is a valid execution");
        // Set before the execution is shared, which is the only time it can be.
        let pool = match workers() {
            Some(workers) => pool
                .with_concurrency(workers)
                .expect("a new execution takes a positive worker count"),
            None => pool,
        };
        Self {
            graphs: Default::default(),
            pool,
            owner: None,
        }
    }

    fn entry(&self, name: &str) -> Result<Option<Arc<RwLock<Entry>>>> {
        Ok(self
            .graphs
            .read()
            .map_err(|_| poisoned())?
            .get(name)
            .cloned())
    }

    fn existing(&self, name: &str) -> Result<Arc<RwLock<Entry>>> {
        match self.entry(name)? {
            Some(entry) => Ok(entry),
            None => exec_err!("nutmeg: no graph named `{name}`; stage its rows first"),
        }
    }

    fn staging(
        &self,
        name: &str,
        part: Part,
        mapping: &ColumnMapping,
        replace: bool,
        order: StageOrder,
    ) -> Staging<'_> {
        Staging {
            store: self,
            graph: name.to_string(),
            part,
            mapping: mapping.clone(),
            replace,
            order,
            normalized: Vec::new(),
            fresh: Admitted::default(),
            schema: None,
        }
    }

    fn stage(
        &self,
        name: &str,
        part: Part,
        batches: &[RecordBatch],
        mapping: &ColumnMapping,
        replace: bool,
        order: StageOrder,
    ) -> Result<usize> {
        let mut staging = self.staging(name, part, mapping, replace, order);
        for batch in batches {
            staging.push(batch)?;
        }
        staging.finish()
    }

    fn drop(&self, name: &str) -> Result<bool> {
        Ok(self
            .graphs
            .write()
            .map_err(|_| poisoned())?
            .remove(name)
            .is_some())
    }

    fn list(&self) -> Result<Vec<GraphInfo>> {
        let map = self.graphs.read().map_err(|_| poisoned())?;
        let mut out = Vec::new();
        for (name, entry) in map.iter() {
            let e = entry.read().map_err(|_| poisoned())?;
            out.push(GraphInfo {
                name: name.clone(),
                staged_nodes: e.nodes.iter().map(|b| b.num_rows()).sum(),
                staged_edges: e.edges.iter().map(|b| b.num_rows()).sum(),
                staged_bytes: e.node_bytes.bytes() + e.edge_bytes.bytes(),
                revision: e.revision,
                projections: e.projections.len(),
                projection_builds: e.projection_builds.clone(),
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    fn memory(&self) -> Result<MemoryInfo> {
        let staged_bytes = self.list()?.iter().map(|g| g.staged_bytes).sum();
        let usage = self.pool.usage().map_err(err)?;
        Ok(MemoryInfo {
            limit_bytes: self.pool.limits().memory_bytes,
            used_bytes: usage.live_bytes,
            staged_bytes,
            peak_bytes: usage.peak_bytes,
        })
    }

    /// The nodes of `e` when none were staged, derived from its edges, with
    /// the memory they take admitted through `charge` before they are made.
    fn derived_nodes(
        &self,
        name: &str,
        e: &Entry,
        charge: &ExecutionContext,
    ) -> Result<(RecordBatch, Admitted)> {
        let budget = Budget {
            pool: &self.pool,
            charge,
            graph: name,
            part: None,
        };
        let bound = derive_nodes_bound(&e.edges);
        let mut admitted = Admitted::default();
        budget.admit(&mut admitted, bound, || {
            format!("{bound} bytes to derive its nodes from its edges")
        })?;
        Ok((derive_nodes(&e.edges, e.edges_canonical)?, admitted))
    }

    fn staged_counts(&self, name: &str, charge: &ExecutionContext) -> Result<(usize, usize)> {
        let entry = self.existing(name)?;
        let e = entry.read().map_err(|_| poisoned())?;
        let edges: usize = e.edges.iter().map(|b| b.num_rows()).sum();
        let nodes = if e.nodes.is_empty() {
            self.derived_nodes(name, &e, charge)?.0.num_rows()
        } else {
            e.nodes.iter().map(|b| b.num_rows()).sum()
        };
        Ok((nodes, edges))
    }

    /// The node batches a projection of `name` is built from: those staged,
    /// or, when only edges were staged, the endpoints derived from them, with
    /// their admission through `charge`; hold it as long as they are used.
    fn node_batches(
        &self,
        name: &str,
        charge: &ExecutionContext,
    ) -> Result<(Vec<RecordBatch>, Admitted)> {
        let entry = self.existing(name)?;
        let e = entry.read().map_err(|_| poisoned())?;
        if e.nodes.is_empty() {
            let (nodes, admitted) = self.derived_nodes(name, &e, charge)?;
            Ok((vec![nodes], admitted))
        } else {
            Ok((e.nodes.clone(), Admitted::default()))
        }
    }

    fn projection(&self, name: &str, args: &ValidatedArguments) -> Result<GraphProjection> {
        let entry = self.existing(name)?;
        let key = projection_key(args);
        if let Some(found) = entry.read().map_err(|_| poisoned())?.projections.get(&key) {
            return Ok(found.clone());
        }
        let mut e = entry.write().map_err(|_| poisoned())?;
        if let Some(found) = e.projections.get(&key) {
            return Ok(found.clone());
        }
        let derived;
        let nodes: &[RecordBatch] = if e.nodes.is_empty() {
            // Transient, but taken on the pool like the projection it builds:
            // a projection is shared by every query, so none of them owns it.
            derived = self.derived_nodes(name, &e, &self.pool)?;
            std::slice::from_ref(&derived.0)
        } else {
            &e.nodes
        };
        let identity = SnapshotIdentity::new(
            name.to_string(),
            format!("r{}", e.revision),
            "nutmeg".into(),
        )
        .map_err(err)?;
        let build_started = Instant::now();
        let live_bytes_before = self.pool.usage().map_or(0, |u| u.live_bytes);
        // Built in the pool's own context, which owns it: its reservations,
        // and the transpose an in-arc kernel builds on first use, stay on the
        // pool for as long as it is cached. Kernels do not run here; each
        // query runs them on a view for its own child of the pool (`Query`).
        //
        // The transpose is not prepared here (`prepare_incoming`). A
        // projection is built by the first read that needs it, not at
        // staging, so that read would pay for it either way; and preparing it
        // would charge a second adjacency to the shared budget for every
        // directed projection, including those only out-arc kernels (BFS,
        // Dijkstra, degree, ...) ever read. Built on first use it is still
        // built once, charged to the pool, and kept with the projection; the
        // read that builds it is charged the work and can be stopped during
        // it, which keeps nothing.
        let (nodes, edges) = one_identity(nodes, &e.edges)?;
        let graph = GraphProjection::from_arrow_batches(
            identity,
            &nodes,
            &edges,
            projection_options(args)?,
            &self.pool,
        )
        .map_err(|error| match error {
            ProcedureError::BudgetExceeded {
                resource: "memory",
                limit,
            } => {
                let used = self.pool.usage().map_or(0, |u| u.live_bytes);
                DataFusionError::ResourcesExhausted(format!(
                    "nutmeg: graph `{name}`: its projection does not fit: {used} of the \
                     {limit}-byte memory budget are in use ({MEMORY_BYTES_VARIABLE}); drop or \
                     restage a graph to release its rows and projections"
                ))
            }
            other => err(other),
        })?;
        let live_bytes_after = self.pool.usage().map_or(0, |u| u.live_bytes);
        e.projection_builds.push(ProjectionBuild {
            key: key.clone(),
            seconds: build_started.elapsed().as_secs_f64(),
            live_bytes_before,
            admitted_bytes: live_bytes_after.saturating_sub(live_bytes_before),
        });
        e.projections.insert(key, graph.clone());
        Ok(graph)
    }

    /// A read on this store's budget: a child of the pool (see [`Query`]).
    fn query(&self, limits: QueryLimits) -> Result<Query> {
        // A timeout too far away to represent is no deadline.
        let deadline = limits
            .timeout
            .and_then(|timeout| Instant::now().checked_add(timeout));
        let context = self
            .pool
            .child(ChildLimits {
                memory_bytes: limits.memory_bytes,
                work_units: limits.work_units.unwrap_or(usize::MAX),
                deadline,
                // A child is shared with its parent from birth, so its worker
                // count is set here rather than with `with_concurrency`. The
                // pool asks for threads only when `NUTMEG_WORKERS` says so, and
                // a read that names none inherits whatever the pool has.
                concurrency: limits.concurrency,
                ..ChildLimits::default()
            })
            .map_err(|e| DataFusionError::Plan(format!("nutmeg: read limits: {e}")))?;
        Ok(Query {
            context,
            diagnostics: Default::default(),
        })
    }

    /// Run `algorithm` on `graph_name` for `query`: the projection is
    /// fetched or built on the pool, which owns it, and everything the read
    /// does runs through a view of it on the read's own execution.
    fn run(
        &self,
        query: &Query,
        algorithm: &str,
        graph_name: &str,
        args: &ValidatedArguments,
    ) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        self.run_each(query, algorithm, graph_name, args, &mut |batch| {
            out.push(batch);
            Ok(true)
        })?;
        Ok(out)
    }

    /// [`Store::run`], handing each batch to `emit` as the kernel's cursor
    /// produces it instead of collecting them. `emit` returns `false` to stop
    /// early: the cursor, and with it the kernel's working storage, is dropped
    /// at once. Returns whether the result was read to its end.
    fn run_each(
        &self,
        query: &Query,
        algorithm: &str,
        graph_name: &str,
        args: &ValidatedArguments,
        emit: &mut dyn FnMut(RecordBatch) -> Result<bool>,
    ) -> Result<bool> {
        let definition = definition_of(algorithm)?;
        self.run_kernel(query, algorithm, graph_name, args, &mut |batch| {
            emit(conform(definition, batch)?)
        })
    }

    fn run_kernel(
        &self,
        query: &Query,
        algorithm: &str,
        graph_name: &str,
        args: &ValidatedArguments,
        emit: &mut dyn FnMut(RecordBatch) -> Result<bool>,
    ) -> Result<bool> {
        let context = &query.context;
        // A kernel's memory refusal says which budget refused it, as the
        // read's own admissions do.
        let failed = |error: ProcedureError| match error {
            ProcedureError::BudgetExceeded {
                resource: "memory",
                limit,
            } => Budget {
                pool: &self.pool,
                charge: context,
                graph: graph_name,
                part: None,
            }
            .refused(&format!("more memory for `{algorithm}`"), limit),
            other => err(other),
        };
        // A read cancelled, or out of time, before it starts builds nothing.
        context.checkpoint().map_err(err)?;
        if algorithm == "estimateCsr" {
            // Sizing before building: counts only, as in Grust's procedure.
            let (nodes, edges) = self.staged_counts(graph_name, context)?;
            let options = projection_options(args)?;
            let weighted = matches!(options.weight, WeightSelection::Property { .. });
            let e = CsrEstimate::upper_bound(nodes, edges, options.orientation, weighted)
                .map_err(err)?;
            return emit_all(
                int_row(
                    &output_names(algorithm)?,
                    &[
                        nodes,
                        edges,
                        e.max_arcs,
                        e.outgoing_bytes,
                        e.reverse_bytes,
                        e.positions_bytes,
                    ],
                )?,
                emit,
            );
        }
        if algorithm == "ssspDeltaStar" {
            optimized::check_options(algorithm, args)?;
            let (cached, weights, _reservation) =
                self.stepping_projection(graph_name, args, context)?;
            let view = cached.with_execution(context).map_err(err)?;
            return optimized::stepping::run(&view, &weights, args, query, emit);
        }
        let cached = self.projection(graph_name, args)?;
        // The projection is the pool's and may be shared by any number of
        // reads; the view runs this read's kernel on this read's execution.
        // Grust refuses a view on any execution not within the owner's.
        let view = cached.with_execution(context).map_err(err)?;
        let g = &view;
        if optimized::NAMES.contains(&algorithm) {
            return optimized::run(algorithm, g, args, query, emit);
        }
        let cursor = match algorithm {
            "projectionStats" => {
                let s = g.statistics().map_err(failed)?;
                return emit_all(
                    int_row(
                        &output_names(algorithm)?,
                        &[s.nodes, s.edges, s.arcs, s.self_loops, s.csr_bytes],
                    )?,
                    emit,
                );
            }
            // Every projection kernel Grust registers, by name: Grust finds the
            // kernel, reads its options and returns its typed Arrow results, so a
            // new registration is served here with no code of Nutmeg's own. A
            // kernel that reads node properties names them through its options;
            // they are read from the staged node rows and handed over with the
            // projection. One that reads none takes the projection alone, so the
            // common path builds nothing extra.
            _ => {
                let wanted = grust_algorithm_procedures::node_property_requests(algorithm, args)
                    .map_err(err)?;
                if wanted.is_empty() {
                    grust_algorithm_procedures::run_on_projection(algorithm, g, args)
                        .map_err(failed)?
                } else {
                    let (nodes, _derived) = self.node_batches(graph_name, context)?;
                    let properties =
                        NodeProperties::from_arrow_batches(&nodes, g, &wanted).map_err(failed)?;
                    grust_algorithm_procedures::run_with_properties(algorithm, &properties, args)
                        .map_err(failed)?
                }
            }
        };
        drain(cursor, failed, emit)
    }
}

/// One write to one part of one graph, fed a batch at a time.
///
/// Each batch is renamed into the grust-arrow layout as it arrives, and the
/// memory the renamed rows keep is admitted as they are made, so a write
/// larger than the budget is refused at the batch that would cross it rather
/// than after all of it has been collected. [`Staging::finish`] then admits
/// the sort's working space, builds the new part aside and swaps it in.
/// Until then the graph is untouched; a write refused or abandoned at any
/// point leaves it exactly as it was, and releases what it had admitted.
pub struct Staging<'a> {
    store: &'a Store,
    graph: String,
    part: Part,
    mapping: ColumnMapping,
    replace: bool,
    order: StageOrder,
    normalized: Vec<RecordBatch>,
    /// Admitted for `normalized`.
    fresh: Admitted,
    schema: Option<SchemaRef>,
}

impl Staging<'_> {
    fn remember_schema(&mut self, schema: SchemaRef) -> Result<()> {
        if self.order == StageOrder::AsStaged {
            // The legacy unsorted path permits batches with different property
            // types. Keep that behavior; a relational scan requires one schema.
            self.schema.get_or_insert(schema);
            return Ok(());
        }
        self.schema = Some(match &self.schema {
            Some(previous) => Arc::new(Schema::try_merge([
                previous.as_ref().clone(),
                schema.as_ref().clone(),
            ])?),
            None => schema,
        });
        Ok(())
    }

    fn budget(&self) -> Budget<'_> {
        Budget {
            pool: &self.store.pool,
            charge: &self.store.pool,
            graph: &self.graph,
            part: Some(self.part),
        }
    }

    /// Rename one batch and admit the memory it keeps.
    pub fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        let rows = batch.num_rows();
        let budget = Budget {
            pool: &self.store.pool,
            charge: &self.store.pool,
            graph: &self.graph,
            part: Some(self.part),
        };
        let bound = admission::normalization_bound(batch, self.part, &self.mapping)?;
        let mut admitted = Admitted::default();
        budget.admit(&mut admitted, bound, || {
            format!("{bound} bytes to normalize {rows} more rows before allocating")
        })?;
        let normalized = match self.part {
            Part::Nodes => normalize_nodes(batch, &self.mapping)?,
            Part::Edges => normalize_edges(batch, &self.mapping)?,
        };
        // What the renamed rows keep alive: columns copied by a cast, and the
        // caller's buffers where a column is shared unchanged.
        let held = held_bytes(std::slice::from_ref(&normalized));
        if held > bound {
            return internal_err!("nutmeg: normalization exceeded its pre-admitted bound");
        }
        admitted.shrink_to(held).map_err(err)?;
        self.remember_schema(normalized.schema())?;
        if rows == 0 {
            // Empty casts can allocate builder buffers too. They were admitted
            // above, but only their schema survives this call.
            return Ok(());
        }
        self.fresh.absorb(admitted);
        self.normalized.push(normalized);
        Ok(())
    }

    /// Swap the write into the graph, creating the graph if it is new.
    /// Returns the rows now staged for that part.
    ///
    /// Under [`StageOrder::Canonical`] the whole part — earlier rows kept by
    /// an append, and these — is sorted afterwards, so the part is the same
    /// whatever order its rows and its appends arrived in. Under
    /// [`StageOrder::AsStaged`] these rows are added after the earlier ones in
    /// the order given, and the part is no longer canonical until a canonical
    /// write sorts it again.
    pub fn finish(self) -> Result<usize> {
        self.finish_reporting().map(|(rows, _)| rows)
    }

    /// [`Staging::finish`], also returning what the write admitted, tier by
    /// tier ([`StageTiers`]).
    pub fn finish_reporting(self) -> Result<(usize, StageTiers)> {
        let store = self.store;
        let (entry, created) = {
            let mut map = store.graphs.write().map_err(|_| poisoned())?;
            match map.get(&self.graph) {
                Some(entry) => (entry.clone(), false),
                None => {
                    let entry = Arc::new(RwLock::new(Entry::default()));
                    map.insert(self.graph.clone(), entry.clone());
                    (entry, true)
                }
            }
        };
        let result = self.swap_into(&entry);
        if result.is_err() && created {
            // A refused write to a new graph leaves no graph behind, unless
            // another write has staged into it meanwhile.
            let mut map = store.graphs.write().map_err(|_| poisoned())?;
            let untouched = map.get(&self.graph).is_some_and(|found| {
                Arc::ptr_eq(found, &entry) && found.read().is_ok_and(|e| e.revision == 0)
            });
            if untouched {
                map.remove(&self.graph);
            }
        }
        result
    }

    fn swap_into(&self, entry: &RwLock<Entry>) -> Result<(usize, StageTiers)> {
        let mut e = entry.write().map_err(|_| poisoned())?;
        let (rows, admitted) = match self.part {
            Part::Nodes => (&e.nodes, &e.node_bytes),
            Part::Edges => (&e.edges, &e.edge_bytes),
        };
        // Built aside and swapped in, so a write the budget or the sort
        // refuses leaves the staged rows as they were. Cloning batches clones
        // only their handles, and cloning admissions shares their tokens.
        let mut staged = if self.replace {
            Vec::new()
        } else {
            rows.clone()
        };
        staged.extend(self.normalized.iter().cloned());
        let canonical = self.order == StageOrder::Canonical;
        let normalized_bytes = self.fresh.bytes();
        let (staged, held, mut tiers) = if canonical {
            canonicalize(self.part, staged, self.schema.clone(), &self.budget())?
        } else {
            let mut held = if self.replace {
                Admitted::default()
            } else {
                admitted.clone()
            };
            held.absorb(self.fresh.clone());
            let tiers = StageTiers {
                sorted: false,
                retained_bytes: held.bytes(),
                ..StageTiers::default()
            };
            (staged, held, tiers)
        };
        tiers.normalized_bytes = normalized_bytes;
        let total = staged.iter().map(|b| b.num_rows()).sum();
        match self.part {
            Part::Nodes => {
                e.nodes = staged;
                e.node_bytes = held;
                e.node_schema = e
                    .nodes
                    .first()
                    .map(RecordBatch::schema)
                    .or_else(|| self.schema.clone());
            }
            Part::Edges => {
                e.edges = staged;
                e.edge_bytes = held;
                e.edge_schema = e
                    .edges
                    .first()
                    .map(RecordBatch::schema)
                    .or_else(|| self.schema.clone());
                e.edges_canonical = canonical;
            }
        }
        e.revision += 1;
        e.projections.clear();
        e.projection_builds.clear();
        Ok((total, tiers))
    }
}

/// The process-wide registry of named graphs.
pub struct Registry;

impl Registry {
    #[cfg(test)]
    fn entry(name: &str) -> Result<Option<Arc<RwLock<Entry>>>> {
        store().entry(name)
    }

    /// Stage rows under `name`, renaming them into the grust-arrow layout.
    /// `replace` discards that part's earlier rows. Nodes are optional: a
    /// graph staged from edges alone takes its nodes from their endpoints.
    /// Returns the rows now staged for that part. See [`Staging`] for how the
    /// write is admitted and [`Staging::finish`] for `order`.
    pub fn stage(
        name: &str,
        part: Part,
        batches: &[RecordBatch],
        mapping: &ColumnMapping,
        replace: bool,
        order: StageOrder,
    ) -> Result<usize> {
        store().stage(name, part, batches, mapping, replace, order)
    }

    /// Begin a write that is fed a batch at a time, so the caller need not
    /// collect a whole DataFrame before the budget can refuse it.
    pub fn staging(
        name: &str,
        part: Part,
        mapping: &ColumnMapping,
        replace: bool,
        order: StageOrder,
    ) -> Staging<'static> {
        store().staging(name, part, mapping, replace, order)
    }

    /// Forget `name` and everything built from it, releasing its memory once
    /// no read still running on it holds its rows or a projection.
    pub fn drop(name: &str) -> Result<bool> {
        store().drop(name)
    }

    pub fn list() -> Result<Vec<GraphInfo>> {
        store().list()
    }

    /// The memory budget and what is using it.
    pub fn memory() -> Result<MemoryInfo> {
        store().memory()
    }

    /// Table reads ([`AlgorithmTable`]): the most recently ended ones, oldest
    /// first, each with the usage it ended with, then those running now, with
    /// their live usage. Reads by [`run`] and [`Query::run`] are not listed.
    pub fn reads() -> Result<Vec<ReadInfo>> {
        let log = READS.lock().map_err(|_| poisoned())?;
        let running = log
            .running
            .values()
            .map(|(info, query)| with_usage(info.clone(), query));
        Ok(log.ended.iter().cloned().chain(running).collect())
    }

    /// The node batches a projection of `name` is built from (see
    /// [`Store::node_batches`]), with derived nodes admitted on the pool.
    #[cfg(test)]
    fn node_batches(name: &str) -> Result<(Vec<RecordBatch>, Admitted)> {
        let store = store();
        store.node_batches(name, &store.pool)
    }

    /// The projection of `name` under the projection options in `args`,
    /// built on first use and kept until the graph is staged again. It is
    /// owned by the pool ([`GraphProjection::owner`]); a kernel run on it
    /// directly runs on the pool's execution, so cancelling it would cancel
    /// every query. Run reads through a [`Query`] instead.
    pub fn projection(name: &str, args: &ValidatedArguments) -> Result<GraphProjection> {
        store().projection(name, args)
    }
}

/// The read option (and SQL configuration key) that sets a read's deadline,
/// in milliseconds from when it starts running. See [`QueryLimits`].
pub const TIMEOUT_OPTION: &str = "timeoutMs";

/// The read option that sets a read's work budget, in Grust work units.
pub const WORK_LIMIT_OPTION: &str = "workLimit";

/// The read option that sets a memory ceiling of a read's own, in bytes,
/// within the process budget.
pub const QUERY_MEMORY_OPTION: &str = "memoryLimitBytes";

/// The read option that says how many threads a read's kernel may use.
pub const CONCURRENCY_OPTION: &str = "concurrency";

/// The options [`QueryLimits::take`] removes, which no kernel may declare.
pub const QUERY_OPTIONS: [&str; 4] = [
    TIMEOUT_OPTION,
    WORK_LIMIT_OPTION,
    QUERY_MEMORY_OPTION,
    CONCURRENCY_OPTION,
];

/// Limits of one read's own, on top of the process budget.
///
/// None is set by default: a read runs until it finishes, fails, or is
/// cancelled ([`Query::cancel`]), bounded only by `NUTMEG_MEMORY_BYTES`, and
/// its kernel runs single-threaded. A read opts in with the read options
/// `timeoutMs`, `workLimit`, `memoryLimitBytes` and `concurrency`, which
/// Nutmeg takes out before Grust validates the rest, like `columnNames`. Each
/// limit stops that read alone; the process budget, the cached projection and
/// every other read are untouched.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueryLimits {
    /// Wall time from the start of the read's run to its deadline.
    pub timeout: Option<Duration>,
    /// Grust work units the read may charge, counted on its own counter.
    pub work_units: Option<usize>,
    /// Bytes the read may hold at once, in addition to (never above) the
    /// process budget. Memory kept past the read — a projection it causes to
    /// be built, a transpose a kernel caches — is the process's, not the
    /// read's, and does not count against this.
    pub memory_bytes: Option<usize>,
    /// Threads the read's kernel may use. `None` runs the code that predates
    /// threads, which is what a server with its own scheduler should get
    /// unless it asks otherwise. It is the read's, not the projection's: the
    /// kernel runs on a view of the cached projection bound to this read's
    /// execution, so one read asking for threads gives none to another.
    pub concurrency: Option<usize>,
}

impl QueryLimits {
    /// Remove the limits from a JSON configuration, leaving Grust's options.
    /// Keys match in any case, as data source options arrive lowercased;
    /// values are non-negative integers, as numbers or as their text.
    pub fn take(options: &mut serde_json::Map<String, serde_json::Value>) -> Result<Self> {
        let mut take = |name: &str| -> Result<Option<usize>> {
            let Some(key) = options
                .keys()
                .find(|k| k.eq_ignore_ascii_case(name))
                .cloned()
            else {
                return Ok(None);
            };
            let value = options.remove(&key).expect("found");
            let parsed = match &value {
                serde_json::Value::Number(n) => n.as_u64(),
                serde_json::Value::String(text) => text.trim().parse::<u64>().ok(),
                _ => None,
            };
            match parsed.map(usize::try_from) {
                Some(Ok(n)) => Ok(Some(n)),
                _ => plan_err!("nutmeg: `{name}` must be a non-negative integer, got {value}"),
            }
        };
        let limits = Self {
            timeout: take(TIMEOUT_OPTION)?.map(|ms| Duration::from_millis(ms as u64)),
            work_units: take(WORK_LIMIT_OPTION)?,
            memory_bytes: take(QUERY_MEMORY_OPTION)?,
            concurrency: take(CONCURRENCY_OPTION)?,
        };
        // Grust's own rule: an execution runs on at least one thread, so zero
        // is a mistake rather than a way to ask for none.
        if limits.concurrency == Some(0) {
            return plan_err!("nutmeg: `{CONCURRENCY_OPTION}` must be a positive integer, got 0");
        }
        Ok(limits)
    }
}

/// One read's execution: a Grust child of the process's memory budget.
///
/// Every byte a read admits counts against the one budget
/// (`NUTMEG_MEMORY_BYTES`), so concurrent reads cannot jointly exceed it. Its
/// work counter, work budget, cancellation and deadline are its own:
/// [`Query::cancel`], a [`QueryLimits`] deadline or work budget running out,
/// stop this read and reach neither the budget, the cached projection nor any
/// other read. Cancelling the budget itself would stop every read.
///
/// Long-lived memory stays on the budget: staged rows, their sort and cached
/// projections (with the transpose a kernel caches in one) outlive any read.
/// What a read alone uses — the nodes it derives, the node properties it
/// reads, kernel scratch and result batches — is admitted through the read,
/// and returned when the read's reservations drop.
///
/// Cloning shares the execution, so a clone handed to another thread cancels
/// the same read.
#[derive(Clone, Debug)]
pub struct Query {
    context: ExecutionContext,
    diagnostics: Arc<std::sync::Mutex<serde_json::Value>>,
}

impl Query {
    /// A read on the process budget, not yet started. A `timeout` counts from
    /// now.
    pub fn new(limits: QueryLimits) -> Result<Self> {
        store().query(limits)
    }

    /// Stop the read. A kernel running for it stops at its next check and
    /// fails with `cancelled`; a read not yet run fails when it starts.
    /// Cancelling is permanent and reaches nothing but this read.
    pub fn cancel(&self) -> Result<()> {
        self.context.cancel().map_err(err)
    }

    /// The read's own figures: its live and peak bytes (part of the budget's)
    /// and the work it has charged.
    pub fn usage(&self) -> Result<ResourceUsage> {
        self.context.usage().map_err(err)
    }

    /// Run one Grust algorithm on the named graph for this read. See [`run`].
    pub fn run(
        &self,
        algorithm: &str,
        graph_name: &str,
        args: &ValidatedArguments,
    ) -> Result<Vec<RecordBatch>> {
        store().run(self, algorithm, graph_name, args)
    }

    /// Whether [`Query::cancel`] has been called on this read (or on the
    /// budget it belongs to).
    pub fn is_cancelled(&self) -> bool {
        matches!(self.context.checkpoint(), Err(ProcedureError::Cancelled))
    }
}

/// What a read is doing, or how it ended. See [`Registry::reads`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadState {
    /// Its kernel is running, or its result is being read.
    Running,
    /// Its result was read to the end.
    Finished,
    /// It was cancelled, or its consumer stopped reading (a dropped stream,
    /// a `LIMIT` reached), and its kernel has stopped.
    Cancelled,
    /// It failed, for a reason of its own: a limit, a refusal, an error.
    Failed,
}

impl ReadState {
    pub fn name(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Finished => "finished",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

/// One table read, running or recently ended, as [`Registry::reads`] and
/// `nutmeg_reads()` report it. A read leaves `Running` only when the thread
/// running its kernel has returned from it, so a read listed as ended has no
/// kernel running.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadInfo {
    /// Numbered from 1 in the order reads started, in this process.
    pub id: u64,
    pub algorithm: &'static str,
    pub graph: String,
    pub state: ReadState,
    /// Why it ended, if not by finishing.
    pub message: Option<String>,
    /// Batches and rows the read has handed on so far.
    pub batches: usize,
    pub rows: usize,
    /// The read's own admitted bytes (part of the budget's): what it holds
    /// now, and the most it has held at once. An ended read's live bytes are
    /// those still held when it ended, by result batches not yet released
    /// downstream.
    pub live_bytes: usize,
    pub peak_bytes: usize,
    /// Grust work units the read has charged.
    pub work_units: usize,
    /// Bounded per-round diagnostics for explicitly named Nutmeg kernels.
    /// Like read-log strings, this is bookkeeping, outside buffer accounting.
    pub diagnostics: serde_json::Value,
}

/// Ended reads kept for [`Registry::reads`], newest last.
const ENDED_READS_KEPT: usize = 256;

#[derive(Default)]
struct ReadLog {
    started: u64,
    running: BTreeMap<u64, (ReadInfo, Query)>,
    ended: std::collections::VecDeque<ReadInfo>,
}

static READS: Lazy<Arc<std::sync::Mutex<ReadLog>>> = Lazy::new(Default::default);

fn with_usage(mut info: ReadInfo, query: &Query) -> ReadInfo {
    if let Ok(usage) = query.usage() {
        info.live_bytes = usage.live_bytes;
        info.peak_bytes = usage.peak_bytes;
        info.work_units = usage.counted_work().unwrap_or(0);
    }
    if let Ok(diagnostics) = query.diagnostics.lock() {
        info.diagnostics = diagnostics.clone();
    }
    info
}

/// A read's entry in the log while it runs. Ending it moves the entry to the
/// ended reads; one dropped without ending (its thread panicked) ends as
/// failed.
struct ReadRecord {
    log: Arc<std::sync::Mutex<ReadLog>>,
    id: u64,
    ended: bool,
}

impl ReadRecord {
    fn start(
        algorithm: &'static str,
        graph: &str,
        query: &Query,
        reads: Arc<std::sync::Mutex<ReadLog>>,
    ) -> Result<Self> {
        let mut log = reads.lock().map_err(|_| poisoned())?;
        log.started += 1;
        let id = log.started;
        let info = ReadInfo {
            id,
            algorithm,
            graph: graph.to_string(),
            state: ReadState::Running,
            message: None,
            batches: 0,
            rows: 0,
            live_bytes: 0,
            peak_bytes: 0,
            work_units: 0,
            diagnostics: serde_json::Value::Null,
        };
        log.running.insert(id, (info, query.clone()));
        drop(log);
        Ok(Self {
            log: reads,
            id,
            ended: false,
        })
    }

    fn batch(&self, rows: usize) {
        if let Ok(mut log) = self.log.lock()
            && let Some((info, _)) = log.running.get_mut(&self.id)
        {
            info.batches += 1;
            info.rows += rows;
        }
    }

    fn end(mut self, state: ReadState, message: Option<String>) {
        self.ended = true;
        Self::end_id(&self.log, self.id, state, message);
    }

    fn end_id(
        reads: &std::sync::Mutex<ReadLog>,
        id: u64,
        state: ReadState,
        message: Option<String>,
    ) {
        let Ok(mut log) = reads.lock() else {
            return;
        };
        if let Some((info, query)) = log.running.remove(&id) {
            let mut info = with_usage(info, &query);
            info.state = state;
            info.message = message;
            log.ended.push_back(info);
            while log.ended.len() > ENDED_READS_KEPT {
                log.ended.pop_front();
            }
        }
    }
}

impl Drop for ReadRecord {
    fn drop(&mut self) {
        if !self.ended {
            Self::end_id(
                &self.log,
                self.id,
                ReadState::Failed,
                Some("the read's thread ended without an outcome".into()),
            );
        }
    }
}

/// An upper bound on the memory [`derive_nodes`] takes for `edges`: per
/// endpoint an entry of its sort and at most an id offset and a label offset;
/// the id text at most once; and the buffers' rounding.
fn derive_nodes_bound(edges: &[RecordBatch]) -> usize {
    let per_endpoint = size_of::<(&str, usize)>() + 2 * size_of::<i32>();
    let mut bound = 1024usize;
    for batch in edges {
        bound = bound.saturating_add(batch.num_rows().saturating_mul(2 * per_endpoint));
        for name in ["source", "target"] {
            if let Some(column) = batch.column_by_name(name) {
                bound = bound.saturating_add(slice_bytes(column.as_ref()));
            }
        }
    }
    bound
}

/// Distinct edge endpoints as a node batch: in id order when the edges are
/// canonical, so a graph staged from edges alone projects its nodes in the
/// order the same nodes staged explicitly would take; otherwise in the order
/// they first appear among the edges.
///
/// Found by sorting the endpoints, not with a hash set, so the memory it takes
/// is known before it starts ([`derive_nodes_bound`]): one vector sized to the
/// endpoints, and the id column built to its exact size.
fn derive_nodes(edges: &[RecordBatch], sorted: bool) -> Result<RecordBatch> {
    let integer = edges.first().is_some_and(|batch| {
        batch
            .column_by_name("source")
            .is_some_and(|column| column.data_type() == &DataType::Int64)
    });
    if integer {
        return derive_integer_nodes(edges, sorted);
    }
    let endpoints: usize = edges.iter().map(|b| 2 * b.num_rows()).sum();
    // (id, position of its first appearance)
    let mut seen: Vec<(&str, usize)> = Vec::with_capacity(endpoints);
    for batch in edges {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| err(format!("staged edges lack `{name}`")))
        };
        let (sources, targets) = (column("source")?, column("target")?);
        for row in 0..batch.num_rows() {
            for endpoint in [sources, targets] {
                if endpoint.is_null(row) {
                    return exec_err!("nutmeg: null edge endpoint at row {row}");
                }
                seen.push((endpoint.value(row), seen.len()));
            }
        }
    }
    // By id, then position, so the first of each id is where it first appears.
    seen.sort_unstable();
    seen.dedup_by(|later, first| later.0 == first.0);
    if !sorted {
        seen.sort_unstable_by_key(|(_, position)| *position);
    }
    let rows = seen.len();
    let text = seen.iter().map(|(id, _)| id.len()).sum();
    let mut ids = StringBuilder::with_capacity(rows, text);
    for (id, _) in &seen {
        ids.append_value(id);
    }
    drop(seen);
    Ok(RecordBatch::try_new(
        node_schema(),
        vec![Arc::new(ids.finish()), empty_labels(rows)],
    )?)
}

/// [`derive_nodes`] for Int64 endpoints: the same order rules, ids compared
/// as integers. Its entries are smaller than the text form's, so
/// [`derive_nodes_bound`] covers it.
fn derive_integer_nodes(edges: &[RecordBatch], sorted: bool) -> Result<RecordBatch> {
    let endpoints: usize = edges.iter().map(|b| 2 * b.num_rows()).sum();
    let mut seen: Vec<(i64, usize)> = Vec::with_capacity(endpoints);
    for batch in edges {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| err(format!("staged edges lack an Int64 `{name}`")))
        };
        let (sources, targets) = (column("source")?, column("target")?);
        for row in 0..batch.num_rows() {
            for endpoint in [sources, targets] {
                if endpoint.is_null(row) {
                    return exec_err!("nutmeg: null edge endpoint at row {row}");
                }
                seen.push((endpoint.value(row), seen.len()));
            }
        }
    }
    seen.sort_unstable();
    seen.dedup_by(|later, first| later.0 == first.0);
    if !sorted {
        seen.sort_unstable_by_key(|(_, position)| *position);
    }
    let rows = seen.len();
    let ids = Int64Array::from_iter_values(seen.iter().map(|(id, _)| *id));
    drop(seen);
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("node_id", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
        ])),
        vec![Arc::new(ids), empty_labels(rows)],
    )?)
}

/// `rows` empty labels, without a pass over them.
fn empty_labels(rows: usize) -> ArrayRef {
    Arc::new(StringArray::new(
        OffsetBuffer::new_zeroed(rows),
        Buffer::from(Vec::<u8>::new()),
        None,
    ))
}

/// Staged batches as they are, or a copy with an id column recast.
type Batches<'a> = std::borrow::Cow<'a, [RecordBatch]>;

/// Node and edge batches with one identity type for the projection build.
///
/// Grust takes `node_id`, `source` and `target` all as Utf8 or all as Int64. A
/// graph staged with integer ids on one side and text on the other used to
/// work, because everything was cast to text; here the integer side is cast to
/// text for the build, which is what it would have been.
fn one_identity<'a>(
    nodes: &'a [RecordBatch],
    edges: &'a [RecordBatch],
) -> Result<(Batches<'a>, Batches<'a>)> {
    use std::borrow::Cow;
    let kind = |batches: &[RecordBatch], name: &str| {
        batches
            .first()
            .and_then(|batch| batch.column_by_name(name))
            .map(|column| column.data_type().clone())
    };
    let text = |batches: &[RecordBatch], names: &[&str]| -> Result<Vec<RecordBatch>> {
        batches
            .iter()
            .map(|batch| {
                let schema = batch.schema();
                let mut fields: Vec<Field> =
                    schema.fields().iter().map(|f| f.as_ref().clone()).collect();
                let mut columns = batch.columns().to_vec();
                for name in names {
                    let index = schema.index_of(name)?;
                    columns[index] = utf8(&columns[index])?;
                    fields[index] = Field::new(*name, DataType::Utf8, true);
                }
                Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
            })
            .collect()
    };
    match (kind(nodes, "node_id"), kind(edges, "source")) {
        (Some(DataType::Int64), Some(DataType::Utf8)) => {
            Ok((Cow::Owned(text(nodes, &["node_id"])?), Cow::Borrowed(edges)))
        }
        (Some(DataType::Utf8), Some(DataType::Int64)) => Ok((
            Cow::Borrowed(nodes),
            Cow::Owned(text(edges, &["source", "target"])?),
        )),
        _ => Ok((Cow::Borrowed(nodes), Cow::Borrowed(edges))),
    }
}

const PROJECTION_KEYS: [&str; 5] = [
    "orientation",
    "nodeLabels",
    "relationshipTypes",
    "weightProperty",
    "defaultWeight",
];

fn projection_key(args: &ValidatedArguments) -> String {
    let picked: BTreeMap<&str, String> = PROJECTION_KEYS
        .iter()
        .map(|k| (*k, format!("{:?}", args.options().get(*k))))
        .collect();
    format!("{picked:?}")
}

// Grust's own reading of the projection options, so a projection built here
// means what the registered procedure's would.
fn projection_options(args: &ValidatedArguments) -> Result<ProjectionOptions<'_>> {
    grust_algorithm_procedures::projection_options(args).map_err(err)
}

fn json_text(value: serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    }
}

/// Turn a call's JSON options into Grust's validated arguments. Positional
/// arguments (`source`, `sources`) are given by name in the same object.
pub fn validate(
    algorithm: &str,
    options: &serde_json::Map<String, serde_json::Value>,
) -> Result<ValidatedArguments> {
    let resolved = PROCEDURES
        .resolve(&format!("{PREFIX}{algorithm}"))
        .map_err(err)?;
    let definition = resolved.definition();
    let mut configuration = options.clone();
    optimized::normalize_seed(algorithm, &mut configuration);
    let mut args = Vec::new();
    for (index, argument) in definition.arguments.iter().enumerate() {
        if Some(index) == definition.options_argument {
            continue;
        }
        let name = &argument.field.name;
        let Some(given) = configuration.remove(name) else {
            return plan_err!("nutmeg: `{algorithm}` needs `{name}`");
        };
        args.push(match (argument.field.value_type, given) {
            (ValueType::Strings, serde_json::Value::Array(items)) => {
                Value::StringArray(items.into_iter().map(json_text).collect())
            }
            (ValueType::Strings, other) => Value::StringArray(
                json_text(other)
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect(),
            ),
            (_, other) => Value::String(json_text(other)),
        });
    }
    args.push(Value::Json(serde_json::Value::Object(configuration)));
    let args = resolved
        .validate_arguments(args)
        .map_err(|e| DataFusionError::Plan(format!("nutmeg: {algorithm}: {e}")))?;
    optimized::check_options(algorithm, &args)?;
    Ok(args)
}

/// Data source options arrive as lowercase-keyed strings; restore Grust's
/// spelling of each key and give each value its JSON type, so `"0.9"`
/// validates as a number and `["a","b"]` as an array.
pub fn options_from_strings(
    algorithm: &str,
    pairs: impl IntoIterator<Item = (String, String)>,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let resolved = PROCEDURES
        .resolve(&format!("{PREFIX}{algorithm}"))
        .map_err(err)?;
    let definition = resolved.definition();
    let mut spelled: HashMap<String, (&str, ValueType)> = HashMap::new();
    for option in &definition.options {
        spelled.insert(
            option.field.name.to_ascii_lowercase(),
            (&option.field.name, option.field.value_type),
        );
    }
    for argument in &definition.arguments {
        spelled.insert(
            argument.field.name.to_ascii_lowercase(),
            (&argument.field.name, argument.field.value_type),
        );
    }
    let mut out = serde_json::Map::new();
    for (key, value) in pairs {
        let (name, kind) = match spelled.get(&key.to_ascii_lowercase()) {
            Some((name, kind)) => (name.to_string(), Some(*kind)),
            None => (key, None),
        };
        let parsed = match kind {
            Some(ValueType::String) | None => serde_json::Value::String(value),
            Some(_) => serde_json::from_str(&value).unwrap_or(serde_json::Value::String(value)),
        };
        out.insert(name, parsed);
    }
    Ok(out)
}

/// Pull the cursor a batch at a time into `emit`, until it ends or `emit`
/// declines more. Each batch is computed when it is pulled for a cursor that
/// streams (all-pairs shortest paths), so stopping early stops the kernel.
fn drain(
    mut cursor: ArrowResultCursor,
    failed: impl Fn(ProcedureError) -> DataFusionError,
    emit: &mut dyn FnMut(RecordBatch) -> Result<bool>,
) -> Result<bool> {
    while let Some(batch) = cursor.next_batch().map_err(&failed)? {
        if !emit(batch.record_batch().clone())? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn emit_all(
    batches: Vec<RecordBatch>,
    emit: &mut dyn FnMut(RecordBatch) -> Result<bool>,
) -> Result<bool> {
    for batch in batches {
        if !emit(batch)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn int_row(names: &[&str], values: &[usize]) -> Result<Vec<RecordBatch>> {
    if names.len() != values.len() {
        return exec_err!(
            "nutmeg: Grust declares {names:?}; this build produces {} values",
            values.len()
        );
    }
    let fields: Vec<Field> = names
        .iter()
        .map(|n| Field::new(*n, DataType::Int64, false))
        .collect();
    let columns: Vec<ArrayRef> = values
        .iter()
        .map(|v| Arc::new(Int64Array::from(vec![*v as i64])) as ArrayRef)
        .collect();
    Ok(vec![RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?])
}

fn definition_of(algorithm: &str) -> Result<&'static ProcedureDefinition> {
    definitions()
        .into_iter()
        .find(|d| short(d).eq_ignore_ascii_case(algorithm))
        .ok_or_else(|| err(format!("unknown algorithm `{algorithm}`")))
}

fn output_names(algorithm: &str) -> Result<Vec<&'static str>> {
    Ok(definition_of(algorithm)?
        .outputs
        .iter()
        .map(|f| f.name.as_str())
        .collect())
}

/// Run one Grust algorithm on the named graph, as a read of its own with no
/// limits beyond the process budget ([`Query`]). Every batch carries the
/// nullability Grust declares for each output column, whatever the rows in it
/// happen to hold (see [`conform`]).
pub fn run(
    algorithm: &str,
    graph_name: &str,
    args: &ValidatedArguments,
) -> Result<Vec<RecordBatch>> {
    Query::new(QueryLimits::default())?.run(algorithm, graph_name, args)
}

/// Restate a result batch's schema with each column's declared nullability.
///
/// Grust's `run_on_projection` and `run_with_properties` now give every batch
/// the nullability each kernel's registration declares, so on a current Grust
/// this changes nothing. It stays as the boundary check: Grust releases before
/// that fix built batches with `RecordBatch::try_from_iter`, which marks a
/// column nullable exactly when that batch holds a null, and a column name or
/// count that differs from the declaration is refused here either way. The
/// declaration is the contract; the data types are kept as produced. A
/// declared non-nullable column that holds a null is refused, by Arrow's own
/// check, rather than passed on.
fn conform(definition: &ProcedureDefinition, batch: RecordBatch) -> Result<RecordBatch> {
    let observed = batch.schema();
    if observed.fields().len() != definition.outputs.len() {
        return exec_err!(
            "nutmeg: `{}` produced {} columns, Grust declares {}",
            definition.name,
            observed.fields().len(),
            definition.outputs.len()
        );
    }
    let fields: Vec<Field> = observed
        .fields()
        .iter()
        .zip(&definition.outputs)
        .map(|(field, declared)| {
            if *field.name() != declared.name {
                return exec_err!(
                    "nutmeg: `{}` produced column `{}` where Grust declares `{}`",
                    definition.name,
                    field.name(),
                    declared.name
                );
            }
            Ok(field.as_ref().clone().with_nullable(declared.nullable))
        })
        .collect::<Result<_>>()?;
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        observed.metadata().clone(),
    ));
    RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(|e| {
        err(format!(
            "`{}` broke its declared output schema: {e}",
            definition.name
        ))
    })
}

/// The arguments a schema probe runs with: the given options, plus a probe
/// node for every positional argument and a probe column for every node
/// property the kernel declares.
fn probe_args_with(
    algorithm: &str,
    mut options: serde_json::Map<String, serde_json::Value>,
) -> Result<ValidatedArguments> {
    let definition = definition_of(algorithm)?;
    // Successive node arguments name different probe nodes: a kernel that takes
    // a source and a target, such as max flow, refuses the same node twice.
    let mut ids = ["a", "b", "c"].into_iter().cycle();
    for (index, argument) in definition.arguments.iter().enumerate() {
        if Some(index) != definition.options_argument {
            let id = ids.next().unwrap_or("a");
            let value = match argument.field.value_type {
                ValueType::Strings => serde_json::json!([id]),
                _ => serde_json::json!(id),
            };
            options.insert(argument.field.name.clone(), value);
        }
    }
    // A kernel that reads node properties would otherwise name its defaults,
    // which the probe graph does not have; point each at the probe's column.
    // Only projection kernels declare properties: `estimateCsr` and
    // `projectionStats` are served here by hand and have none to declare.
    if grust_algorithm_procedures::projection_kernel_names().contains(&algorithm) {
        for declared in grust_algorithm_procedures::node_property_options(algorithm).map_err(err)? {
            options.insert(
                declared.option.to_string(),
                serde_json::json!(probe_key(declared.option, declared.kind)),
            );
        }
    }
    validate(algorithm, &options)
}

/// The probe column standing in for one declared property option. Keyed by
/// option and kind together, so two kernels that happen to share an option name
/// with different kinds do not collide.
fn probe_key(option: &str, kind: PropertyKind) -> String {
    let kind = match kind {
        PropertyKind::Number => "number",
        PropertyKind::Integer => "integer",
        PropertyKind::Vector => "vector",
        PropertyKind::Category => "category",
    };
    format!("probe.{option}.{kind}")
}

/// The probe's three nodes, carrying a column for every property option any
/// registered kernel declares. Values are chosen to be valid for every reader:
/// numbers lie within ±90, so they serve as latitudes and longitudes; integers
/// split the nodes into two communities; vectors are distinct and nonzero.
fn probe_nodes() -> Result<RecordBatch> {
    let ids: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let mut fields = vec![
        Field::new("node_id", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, true),
    ];
    let mut columns: Vec<ArrayRef> = vec![ids, Arc::new(StringArray::from(vec!["", "", ""]))];
    let mut seen = HashSet::new();
    for name in grust_algorithm_procedures::projection_kernel_names() {
        for declared in grust_algorithm_procedures::node_property_options(name).map_err(err)? {
            let key = probe_key(declared.option, declared.kind);
            if !seen.insert(key.clone()) {
                continue;
            }
            let values: ArrayRef = match declared.kind {
                PropertyKind::Number => Arc::new(Float64Array::from(vec![0.0, 0.5, 1.0])),
                PropertyKind::Integer => Arc::new(Int64Array::from(vec![0, 1, 0])),
                PropertyKind::Category => Arc::new(StringArray::from(vec!["x", "y", "x"])),
                PropertyKind::Vector => {
                    Arc::new(
                        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                            vec![
                                Some(vec![Some(1.0), Some(0.0)]),
                                Some(vec![Some(0.0), Some(1.0)]),
                                Some(vec![Some(1.0), Some(1.0)]),
                            ],
                            2,
                        ),
                    )
                }
            };
            fields.push(Field::new(
                format!("property.{key}"),
                values.data_type().clone(),
                true,
            ));
            columns.push(values);
            fields.push(Field::new(
                format!("present.{key}"),
                DataType::Boolean,
                false,
            ));
            columns.push(Arc::new(BooleanArray::from(vec![true, true, true])));
        }
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

const PROBE: &str = "nutmeg.schema-probe";
static SCHEMA_STORE: Lazy<Arc<Store>> = Lazy::new(|| Arc::new(Store::new(16 << 20)));

static SCHEMAS: Lazy<RwLock<HashMap<String, SchemaRef>>> = Lazy::new(Default::default);

/// Run `algorithm` once on the probe graph, which must be staged, on top of
/// `options`, returning the arguments it ran with and its batches. A kernel
/// defined on undirected graphs refuses the default directed projection, and
/// says so; it is probed on an undirected one instead.
fn probe(
    algorithm: &str,
    options: serde_json::Map<String, serde_json::Value>,
) -> Result<(ValidatedArguments, Vec<RecordBatch>)> {
    let args = probe_args_with(algorithm, options.clone())?;
    match SCHEMA_STORE.run(
        &SCHEMA_STORE.query(QueryLimits::default())?,
        algorithm,
        PROBE,
        &args,
    ) {
        Err(error) if error.to_string().contains("undirected") => {
            let mut options = options;
            options.insert("orientation".into(), serde_json::json!("undirected"));
            let args = probe_args_with(algorithm, options)?;
            let batches = SCHEMA_STORE.run(
                &SCHEMA_STORE.query(QueryLimits::default())?,
                algorithm,
                PROBE,
                &args,
            )?;
            Ok((args, batches))
        }
        other => Ok((args, other?)),
    }
}

/// The option through which a kernel is asked for the precision it keeps its
/// scores in. It is Grust's own option, not one of Nutmeg's: `pagerank` and
/// `articleRank` declare it in the registry, with `f64` as the default, so it
/// reaches Grust's validator like `damping` and is refused there and in the
/// kernel like any other bad option value. Nutmeg only has to read it back,
/// because the `score` column's Arrow type follows it (`Float64` at `f64`,
/// `Float32` at `f32`) and so must the schema a read reports.
pub const PRECISION_OPTION: &str = "precision";

/// The value of [`PRECISION_OPTION`] a call runs at, or `None` when the
/// algorithm does not declare the option. Read from the validated arguments,
/// so a call that does not name it gets Grust's declared default rather than
/// one written down here.
fn precision_of(algorithm: &str, args: &ValidatedArguments) -> Result<Option<String>> {
    let resolved = PROCEDURES
        .resolve(&format!("{PREFIX}{algorithm}"))
        .map_err(err)?;
    if !resolved
        .definition()
        .options
        .iter()
        .any(|option| option.field.name == PRECISION_OPTION)
    {
        return Ok(None);
    }
    Ok(match args.options().get(PRECISION_OPTION) {
        Some(Value::String(text)) => Some(text.clone()),
        _ => None,
    })
}

/// The Arrow schema an algorithm's result has: the kernel's own column names
/// and types, observed by running it once on a three-node graph, not a
/// transcription of them. Nullability is Grust's declaration, not what the
/// probe's rows held: on three nodes most nullable columns are full. These
/// are Grust's names; [`output_schema_named`] reports them as a read with a
/// [`ColumnNames`] choice returns them.
pub fn output_schema_named(algorithm: &str, names: ColumnNames) -> Result<SchemaRef> {
    names.rename_schema(&output_schema(algorithm)?)
}

/// The schema a read with these validated arguments returns. An option can
/// decide a column's Arrow type — [`PRECISION_OPTION`] decides `score`'s — so
/// the schema is probed and cached per value of it, not per algorithm. Every
/// other option leaves the result's types alone, so they do not enter the key.
pub fn output_schema_for(
    algorithm: &str,
    args: &ValidatedArguments,
    names: ColumnNames,
) -> Result<SchemaRef> {
    let precision = precision_of(algorithm, args)?;
    names.rename_schema(&output_schema_at(algorithm, precision.as_deref())?)
}

/// See [`output_schema_named`]; Grust's names, at the declared default of
/// every option.
pub fn output_schema(algorithm: &str) -> Result<SchemaRef> {
    output_schema_at(algorithm, None)
}

fn output_schema_at(algorithm: &str, precision: Option<&str>) -> Result<SchemaRef> {
    // `#` cannot occur in a registered algorithm name, so no key collides.
    let key = match precision {
        Some(value) => format!("{algorithm}#{PRECISION_OPTION}={value}"),
        None => algorithm.to_string(),
    };
    if let Some(found) = SCHEMAS.read().map_err(|_| poisoned())?.get(&key) {
        return Ok(found.clone());
    }
    let mut schemas = SCHEMAS.write().map_err(|_| poisoned())?;
    if let Some(schema) = optimized::schema(algorithm) {
        schemas.insert(key, schema.clone());
        return Ok(schema);
    }
    if SCHEMA_STORE.entry(PROBE)?.is_none() {
        let edges = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("source", DataType::Utf8, false),
                Field::new("target", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(StringArray::from(vec!["b", "c", "c"])),
            ],
        )?;
        SCHEMA_STORE.stage(
            PROBE,
            Part::Edges,
            &[edges],
            &ColumnMapping::default(),
            true,
            StageOrder::Canonical,
        )?;
        SCHEMA_STORE.stage(
            PROBE,
            Part::Nodes,
            &[probe_nodes()?],
            &ColumnMapping::default(),
            true,
            StageOrder::Canonical,
        )?;
    }
    let mut options = serde_json::Map::new();
    if let Some(value) = precision {
        options.insert(PRECISION_OPTION.into(), serde_json::json!(value));
    }
    let (_, batches) = probe(algorithm, options)?;
    let Some(first) = batches.first() else {
        return exec_err!("nutmeg: probing `{algorithm}` produced no batch");
    };
    schemas.insert(key, first.schema());
    Ok(first.schema())
}

/// A table whose scan runs one algorithm on one named graph.
#[derive(Clone, Debug)]
pub struct AlgorithmTable {
    session: Option<SessionRegistry>,
    algorithm: &'static str,
    graph: String,
    args: Arc<ValidatedArguments>,
    names: ColumnNames,
    limits: QueryLimits,
    schema: SchemaRef,
}

impl AlgorithmTable {
    /// A table over `graph` with Grust's column names.
    pub fn new(
        algorithm: &str,
        graph: String,
        options: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Self> {
        Self::named(algorithm, graph, options, ColumnNames::Grust)
    }

    /// A table over `graph` whose columns are reported under `names`. The
    /// schema reported and the batches a scan returns are renamed by the same
    /// function, and `batches` re-checks one against the other. The read's own
    /// limits ([`QueryLimits`]) are taken out of `options` before Grust
    /// validates the rest.
    pub fn named(
        algorithm: &str,
        graph: String,
        options: &serde_json::Map<String, serde_json::Value>,
        names: ColumnNames,
    ) -> Result<Self> {
        Self::build(algorithm, graph, options, names, true)
    }

    fn build(
        algorithm: &str,
        graph: String,
        options: &serde_json::Map<String, serde_json::Value>,
        names: ColumnNames,
        allow_schema_probe: bool,
    ) -> Result<Self> {
        let Some(algorithm) = resolve_algorithm(algorithm) else {
            return plan_err!(
                "nutmeg: unknown algorithm `{algorithm}`; Grust registers {:?}",
                algorithm_names()
            );
        };
        let mut options = options.clone();
        let limits = QueryLimits::take(&mut options)?;
        let args = validate(algorithm, &options)?;
        // The schema follows the call's options where one decides a column's
        // Arrow type: `precision` decides `score`'s. See `output_schema_for`.
        let schema = if allow_schema_probe {
            output_schema_for(algorithm, &args, names)?
        } else {
            prepared_schema::cached_output_schema(algorithm, &args, names)?
        };
        Ok(Self {
            session: None,
            algorithm,
            graph,
            args: Arc::new(args),
            names,
            limits,
            schema,
        })
    }

    /// The limits each scan of this table runs under.
    pub fn limits(&self) -> QueryLimits {
        self.limits
    }

    /// A read for one scan of this table, under its limits; its timeout
    /// counts from now. Hand a clone to whatever may cancel it, and run it
    /// with [`AlgorithmTable::batches_for`].
    pub fn query(&self) -> Result<Query> {
        match &self.session {
            Some(session) => session.store.query(self.limits),
            None => Query::new(self.limits),
        }
    }

    /// Run the table's algorithm as a read of its own, under its limits.
    pub fn batches(&self) -> Result<Vec<RecordBatch>> {
        self.batches_for(&self.query()?)
    }

    /// Run the table's algorithm for `query`, collecting its result.
    pub fn batches_for(&self, query: &Query) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        self.read_each(query, &mut |batch| {
            out.push(batch);
            Ok(true)
        })?;
        Ok(out)
    }

    /// Run the table's algorithm for `query`, handing each batch, renamed and
    /// checked against the table's schema, to `emit` as it is produced;
    /// `emit` returns `false` to stop the read. The read is listed by
    /// [`Registry::reads`] from start to end.
    pub fn read_each(
        &self,
        query: &Query,
        emit: &mut dyn FnMut(RecordBatch) -> Result<bool>,
    ) -> Result<()> {
        let reads = self
            .session
            .as_ref()
            .map(|s| s.reads.clone())
            .unwrap_or_else(|| READS.clone());
        let record = ReadRecord::start(self.algorithm, &self.graph, query, reads)?;
        let store = self
            .session
            .as_ref()
            .map(|s| s.store.as_ref())
            .unwrap_or_else(|| store());
        let outcome = store.run_each(
            query,
            self.algorithm,
            &self.graph,
            &self.args,
            &mut |batch| {
                let batch = match &store.owner {
                    Some(owner) => graph_tables::retain_owner(batch, owner.clone())?,
                    None => batch,
                };
                let batch = self.names.rename_batch(batch)?;
                if batch.schema().fields() != self.schema.fields() {
                    return exec_err!(
                        "nutmeg: `{}` produced {:?}, declared {:?}",
                        self.algorithm,
                        batch.schema().fields(),
                        self.schema.fields()
                    );
                }
                record.batch(batch.num_rows());
                emit(batch)
            },
        );
        match &outcome {
            Ok(true) => record.end(ReadState::Finished, None),
            Ok(false) => record.end(
                ReadState::Cancelled,
                Some("its consumer stopped reading".into()),
            ),
            Err(error) => {
                let state = if query.is_cancelled() {
                    ReadState::Cancelled
                } else {
                    ReadState::Failed
                };
                record.end(state, Some(error.to_string()));
            }
        }
        outcome.map(|_| ())
    }
}

/// Where a read's kernel runs.
///
/// `Streaming` (the default) plans an [`AlgorithmExec`]: nothing runs while
/// the query is planned or explained, and the kernel starts when the plan's
/// stream is first polled, on a thread of its own, feeding batches through a
/// bounded channel. Dropping the stream cancels the read, which is how an
/// interrupt (Sail's `interruptAll`, `interruptTag`, `interruptOperation`)
/// reaches the kernel.
///
/// `Materialized` runs the kernel inside `TableProvider::scan`, while the
/// query is planned, and plans the collected result as an in-memory table.
/// Nothing can interrupt it, and the whole result is held before its first
/// row leaves. It exists for engines that ship physical plans to other
/// processes, which can serialise an in-memory table but not an
/// [`AlgorithmExec`]: `nutmeg-sail` selects it in Sail's cluster modes.
///
/// A session selects one with a [`SessionConfig`] extension
/// (`config.with_extension(Arc::new(ReadExecution::Materialized))`);
/// without one, the `NUTMEG_READS` environment variable (`streaming` or
/// `materialized`) does, and otherwise reads stream.
///
/// [`SessionConfig`]: datafusion::prelude::SessionConfig
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadExecution {
    #[default]
    Streaming,
    Materialized,
}

/// The environment variable that selects [`ReadExecution`] when a session
/// does not.
pub const READS_VARIABLE: &str = "NUTMEG_READS";

impl ReadExecution {
    pub fn parse(text: &str) -> Result<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "streaming" => Ok(Self::Streaming),
            "materialized" | "materialised" => Ok(Self::Materialized),
            other => plan_err!(
                "nutmeg: {READS_VARIABLE} is `streaming` or `materialized`, got `{other}`"
            ),
        }
    }

    /// The choice `NUTMEG_READS` makes, if it is set.
    pub fn from_env() -> Result<Option<Self>> {
        match std::env::var(READS_VARIABLE) {
            Ok(text) => Self::parse(&text).map(Some),
            Err(_) => Ok(None),
        }
    }

    /// The session's choice, else the environment's, else streaming.
    pub fn for_session(state: &dyn Session) -> Result<Self> {
        if let Some(chosen) = state.config().get_extension::<Self>() {
            return Ok(*chosen);
        }
        Ok(Self::from_env()?.unwrap_or_default())
    }
}

/// Batches a running read may have produced and not yet handed on: the
/// kernel's thread blocks once this many wait in the channel, so a slow
/// consumer holds the read to this many batches beyond the one it is
/// reading and the one the kernel is making.
pub const READ_CHANNEL_BATCHES: usize = 2;

/// The physical plan of a streaming read (see [`ReadExecution`]): one
/// partition, whose stream starts the table's read when first polled and
/// cancels it when dropped.
#[derive(Debug)]
pub struct AlgorithmExec {
    table: Arc<AlgorithmTable>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl AlgorithmExec {
    pub fn try_new(
        table: Arc<AlgorithmTable>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let schema = match &projection {
            Some(columns) => Arc::new(table.schema.project(columns)?),
            None => table.schema.clone(),
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            table,
            projection,
            limit,
            schema,
            properties,
        })
    }
}

impl DisplayAs for AlgorithmExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "NutmegAlgorithmExec: algorithm={}, graph={}",
                    self.table.algorithm, self.table.graph
                )?;
                if let Some(columns) = &self.projection {
                    write!(f, ", projection={columns:?}")?;
                }
                if let Some(limit) = self.limit {
                    write!(f, ", limit={limit}")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => write!(
                f,
                "algorithm={}\ngraph={}",
                self.table.algorithm, self.table.graph
            ),
        }
    }
}

impl ExecutionPlan for AlgorithmExec {
    fn name(&self) -> &str {
        "NutmegAlgorithmExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("NutmegAlgorithmExec has no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("NutmegAlgorithmExec has one partition, not {partition}");
        }
        Ok(Box::pin(AlgorithmStream {
            table: self.table.clone(),
            projection: self.projection.clone(),
            limit: self.limit,
            rows: 0,
            schema: self.schema.clone(),
            state: StreamState::Unstarted,
        }))
    }
}

/// The stream of one streaming read.
///
/// On its first poll it creates the read's [`Query`] (so a `timeoutMs`
/// counts from there) and starts a thread that runs the kernel and sends each
/// batch through a channel of [`READ_CHANNEL_BATCHES`]. Kernels are
/// synchronous and CPU-bound, so they run on no thread of the async runtime.
/// The channel is the backpressure: a thread that has filled it waits for the
/// consumer, and a cursor that computes batches as they are pulled (all-pairs
/// shortest paths) waits with it.
///
/// Dropping the stream before its end cancels the query. The thread then
/// stops at whichever comes first: the kernel's next cancellation check,
/// where it fails with `cancelled`, or its next send, which finds the channel
/// closed. Either way it drops the cursor and its working memory and returns,
/// so the thread is detached rather than joined: joining would block the
/// runtime thread that dropped the stream until that check. [`Registry::reads`]
/// lists the read as running until the thread has returned from the kernel.
struct AlgorithmStream {
    table: Arc<AlgorithmTable>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    rows: usize,
    schema: SchemaRef,
    state: StreamState,
}

enum StreamState {
    Unstarted,
    Running(RunningRead),
    Ended,
}

struct RunningRead {
    batches: tokio::sync::mpsc::Receiver<Result<RecordBatch>>,
    query: Query,
    /// Whether the thread has sent its last message: then there is nothing
    /// left to cancel.
    ended: bool,
}

impl Drop for RunningRead {
    fn drop(&mut self) {
        if !self.ended {
            // Cancelling a read cannot fail: every Nutmeg read observes
            // interruption. The closed channel stops the thread regardless.
            let _ = self.query.cancel();
        }
    }
}

impl AlgorithmStream {
    fn start(&self) -> Result<RunningRead> {
        let query = self.table.query()?;
        let (sender, batches) = tokio::sync::mpsc::channel(READ_CHANNEL_BATCHES);
        let (table, projection, read) =
            (self.table.clone(), self.projection.clone(), query.clone());
        std::thread::Builder::new()
            .name("nutmeg-read".into())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    table.read_each(&read, &mut |batch| {
                        let batch = match &projection {
                            Some(columns) => batch.project(columns)?,
                            None => batch,
                        };
                        // Blocks while the channel is full; fails once the
                        // stream is dropped, which stops the read.
                        Ok(sender.blocking_send(Ok(batch)).is_ok())
                    })
                }));
                let error = match outcome {
                    Ok(Ok(())) => return,
                    Ok(Err(error)) => error,
                    Err(_) => err(format!(
                        "`{}` on `{}` panicked",
                        table.algorithm, table.graph
                    )),
                };
                let _ = sender.blocking_send(Err(error));
            })
            .map_err(|e| err(format!("could not start a thread for the read: {e}")))?;
        Ok(RunningRead {
            batches,
            query,
            ended: false,
        })
    }
}

impl futures::Stream for AlgorithmStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let this = &mut *self;
        if matches!(this.state, StreamState::Unstarted) {
            if this.limit == Some(0) {
                this.state = StreamState::Ended;
                return Poll::Ready(None);
            }
            match this.start() {
                Ok(running) => this.state = StreamState::Running(running),
                Err(error) => {
                    this.state = StreamState::Ended;
                    return Poll::Ready(Some(Err(error)));
                }
            }
        }
        let StreamState::Running(running) = &mut this.state else {
            return Poll::Ready(None);
        };
        let batch = match running.batches.poll_recv(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Ok(batch))) => batch,
            Poll::Ready(end) => {
                // The thread's last message: the end, or its error.
                running.ended = true;
                this.state = StreamState::Ended;
                return Poll::Ready(end);
            }
        };
        let Some(limit) = this.limit else {
            return Poll::Ready(Some(Ok(batch)));
        };
        let left = limit - this.rows;
        if batch.num_rows() < left {
            this.rows += batch.num_rows();
            return Poll::Ready(Some(Ok(batch)));
        }
        // The limit is reached: stop the read, which is no longer needed.
        this.rows = limit;
        this.state = StreamState::Ended;
        Poll::Ready(Some(Ok(batch.slice(0, left))))
    }
}

impl RecordBatchStream for AlgorithmStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Every table read, running or recently ended ([`Registry::reads`]), as rows
/// of `nutmeg_reads()`.
#[derive(Debug)]
pub struct ReadsTable;

impl ReadsTable {
    pub fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("readId", DataType::Int64, false),
            Field::new("algorithm", DataType::Utf8, false),
            Field::new("graph", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("message", DataType::Utf8, true),
            Field::new("batches", DataType::Int64, false),
            Field::new("rows", DataType::Int64, false),
            Field::new("liveBytes", DataType::Int64, false),
            Field::new("peakBytes", DataType::Int64, false),
            Field::new("workUnits", DataType::Int64, false),
        ]))
    }

    pub fn batches() -> Result<Vec<RecordBatch>> {
        let rows = Registry::reads()?;
        let ints = |f: fn(&ReadInfo) -> usize| -> ArrayRef {
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| i64::try_from(f(r)).unwrap_or(i64::MAX))
                    .collect::<Vec<_>>(),
            ))
        };
        let texts = |f: fn(&ReadInfo) -> Option<&str>| -> ArrayRef {
            Arc::new(StringArray::from(rows.iter().map(f).collect::<Vec<_>>()))
        };
        Ok(vec![RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                ints(|r| r.id as usize),
                texts(|r| Some(r.algorithm)),
                texts(|r| Some(r.graph.as_str())),
                texts(|r| Some(r.state.name())),
                texts(|r| r.message.as_deref()),
                ints(|r| r.batches),
                ints(|r| r.rows),
                ints(|r| r.live_bytes),
                ints(|r| r.peak_bytes),
                ints(|r| r.work_units),
            ],
        )?])
    }
}

#[async_trait]
impl TableProvider for ReadsTable {
    fn schema(&self) -> SchemaRef {
        Self::arrow_schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(Self::arrow_schema(), vec![Self::batches()?])?
            .scan(state, projection, filters, limit)
            .await
    }
}

/// The listing of staged graphs.
#[derive(Debug)]
pub struct GraphsTable;

impl GraphsTable {
    pub fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("stagedNodes", DataType::Int64, false),
            Field::new("stagedEdges", DataType::Int64, false),
            Field::new("stagedBytes", DataType::Int64, false),
            Field::new("revision", DataType::Int64, false),
            Field::new("projections", DataType::Int64, false),
        ]))
    }

    pub fn batches() -> Result<Vec<RecordBatch>> {
        let rows: Vec<GraphInfo> = Registry::list()?
            .into_iter()
            .filter(|g| g.name != PROBE)
            .collect();
        let ints = |f: fn(&GraphInfo) -> usize| -> ArrayRef {
            Arc::new(Int64Array::from(
                rows.iter().map(|g| f(g) as i64).collect::<Vec<_>>(),
            ))
        };
        Ok(vec![RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|g| g.name.as_str()).collect::<Vec<_>>(),
                )),
                ints(|g| g.staged_nodes),
                ints(|g| g.staged_edges),
                ints(|g| g.staged_bytes),
                ints(|g| g.revision as usize),
                ints(|g| g.projections),
            ],
        )?])
    }
}

/// The memory budget and what is using it, as one row: `limitBytes`,
/// `usedBytes` (staged rows, writes in progress, cached projections and
/// running kernels), `stagedBytes` (the staged rows alone, the sum of the
/// graph listing's `stagedBytes`) and `peakBytes`.
#[derive(Debug)]
pub struct MemoryTable;

impl MemoryTable {
    pub fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("limitBytes", DataType::Int64, false),
            Field::new("usedBytes", DataType::Int64, false),
            Field::new("stagedBytes", DataType::Int64, false),
            Field::new("peakBytes", DataType::Int64, false),
        ]))
    }

    pub fn batches() -> Result<Vec<RecordBatch>> {
        let m = Registry::memory()?;
        let int = |v: usize| Arc::new(Int64Array::from(vec![v as i64])) as ArrayRef;
        Ok(vec![RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                int(m.limit_bytes),
                int(m.used_bytes),
                int(m.staged_bytes),
                int(m.peak_bytes),
            ],
        )?])
    }
}

#[async_trait]
impl TableProvider for MemoryTable {
    fn schema(&self) -> SchemaRef {
        Self::arrow_schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(Self::arrow_schema(), vec![Self::batches()?])?
            .scan(state, projection, filters, limit)
            .await
    }
}

#[async_trait]
impl TableProvider for AlgorithmTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    /// Plans the read without running it, unless the session materialises
    /// reads (see [`ReadExecution`]).
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match ReadExecution::for_session(state)? {
            ReadExecution::Streaming => Ok(Arc::new(AlgorithmExec::try_new(
                Arc::new(self.clone()),
                projection.cloned(),
                limit,
            )?)),
            ReadExecution::Materialized => {
                MemTable::try_new(self.schema.clone(), vec![self.batches()?])?
                    .scan(state, projection, filters, limit)
                    .await
            }
        }
    }
}

#[async_trait]
impl TableProvider for GraphsTable {
    fn schema(&self) -> SchemaRef {
        Self::arrow_schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        MemTable::try_new(Self::arrow_schema(), vec![Self::batches()?])?
            .scan(state, projection, filters, limit)
            .await
    }
}

fn literal_string(expr: &Expr, position: usize) -> Result<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Ok(s.clone()),
        other => plan_err!("nutmeg: argument {position} must be a string literal, got {other}"),
    }
}

/// Parse a JSON object of options; empty text means none.
pub fn parse_options(json: &str) -> Result<serde_json::Map<String, serde_json::Value>> {
    if json.trim().is_empty() {
        return Ok(Default::default());
    }
    match serde_json::from_str(json) {
        Ok(serde_json::Value::Object(map)) => Ok(map),
        Ok(_) => plan_err!("nutmeg: options must be a JSON object"),
        Err(e) => plan_err!("nutmeg: options: {e}"),
    }
}

/// `nutmeg_<algorithm>('graph'[, '{json configuration}'])`. The configuration
/// is Grust's, plus Nutmeg's `columnNames` (`"grust"` or `"gds"`).
#[derive(Debug)]
struct AlgorithmFunction(&'static str);

impl TableFunctionImpl for AlgorithmFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let (graph, mut options) = match args {
            [g] => (literal_string(g, 1)?, Default::default()),
            [g, o] => (
                literal_string(g, 1)?,
                parse_options(&literal_string(o, 2)?)?,
            ),
            _ => {
                return plan_err!(
                    "nutmeg_{}(graph[, configuration]) takes one or two arguments",
                    snake(self.0)
                );
            }
        };
        let names = ColumnNames::take(&mut options)?;
        Ok(Arc::new(AlgorithmTable::named(
            self.0, graph, &options, names,
        )?))
    }
}

/// `nutmeg_graphs()`.
#[derive(Debug)]
struct GraphsFunction;

impl TableFunctionImpl for GraphsFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return plan_err!("nutmeg_graphs() takes no arguments");
        }
        Ok(Arc::new(GraphsTable))
    }
}

/// `nutmeg_reads()`.
#[derive(Debug)]
struct ReadsFunction;

impl TableFunctionImpl for ReadsFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return plan_err!("nutmeg_reads() takes no arguments");
        }
        Ok(Arc::new(ReadsTable))
    }
}

/// `nutmeg_memory()`.
#[derive(Debug)]
struct MemoryFunction;

impl TableFunctionImpl for MemoryFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if !args.is_empty() {
            return plan_err!("nutmeg_memory() takes no arguments");
        }
        Ok(Arc::new(MemoryTable))
    }
}

/// One table function per Grust algorithm, plus `nutmeg_graphs`,
/// `nutmeg_memory` and `nutmeg_reads`.
pub fn table_functions() -> Vec<(String, Arc<TableFunction>)> {
    let mut out: Vec<(String, Arc<TableFunction>)> = algorithm_names()
        .into_iter()
        .map(|name| {
            let sql = format!("nutmeg_{}", snake(name));
            (
                sql.clone(),
                Arc::new(TableFunction::new(sql, Arc::new(AlgorithmFunction(name)))),
            )
        })
        .collect();
    out.push((
        "nutmeg_graphs".into(),
        Arc::new(TableFunction::new(
            "nutmeg_graphs".into(),
            Arc::new(GraphsFunction),
        )),
    ));
    out.push((
        "nutmeg_memory".into(),
        Arc::new(TableFunction::new(
            "nutmeg_memory".into(),
            Arc::new(MemoryFunction),
        )),
    ));
    out.push((
        "nutmeg_reads".into(),
        Arc::new(TableFunction::new(
            "nutmeg_reads".into(),
            Arc::new(ReadsFunction),
        )),
    ));
    out
}

/// Register every table function in a DataFusion session.
pub fn register(ctx: &SessionContext) {
    for (name, function) in table_functions() {
        ctx.register_udtf(&name, function.function().clone());
    }
}

#[cfg(test)]
mod tests;
