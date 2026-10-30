//! Pre-resolution binding of a flow's reads for one bounded lowering
//! (BOUNDED-PLAN-ADMISSION-DESIGN §2 as narrowed in §10 after CF-0136).
//!
//! Before `PlanResolver` runs, and therefore before any resolver I/O, the
//! flow's `spec::QueryPlan` is walked and every read is classified against
//! explicit, caller-supplied bindings: a streaming read must name a bound
//! source, a batch read must name a local output or a proven static binding,
//! and the constructs whose resolution executes helper plans are refused.
//! Nothing is rewritten: there is no synthetic namespace, and a flow that
//! reads one name in both modes is refused instead.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use sail_common::spec;
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_plan::resolver::scope::RelationScope;

use super::intent::BatchIntent;
use super::source::SourceDescriptor;
use crate::pipelines::analysis::GraphScope;
use crate::pipelines::graph::{Flow, GraphDefinition, Name, quoted};

/// A recorded streaming source bound to this lowering, with its identity
/// stated by the caller: the graph-local name the flow reads it by, the
/// source schema every scan must present, the descriptor captured with the
/// frontier, the intent the batch covers, and one provider per range of
/// that intent, in range order.
pub struct SourceBinding {
    pub name: Name,
    pub schema: SchemaRef,
    pub descriptor: SourceDescriptor,
    pub intent: BatchIntent,
    pub scans: Vec<Arc<dyn TableProvider>>,
}

/// A proven static side for a batch read of a name that is not a local
/// output: `provider` pinned to `identity` (a table version, a snapshot
/// digest) by the caller. A name alone never qualifies, and the identity
/// text alone proves nothing about the provider; binding the two is the
/// caller's obligation, checked at integration.
pub struct StaticBinding {
    pub name: Name,
    pub identity: String,
    pub provider: Arc<dyn TableProvider>,
}

/// A local output the flow reads as a batch, bound to the relation that
/// was materialised for it: `provider` pinned to `identity` (the run and
/// commit it was read at) by the caller. During lowering a local output
/// is never a schema-only placeholder. One provider per name: a local
/// temporary view that expands to several leaf providers under other scan
/// names is not representable here and is rejected by admission as unbound
/// leaves; that boundary is tracked, not supported.
pub struct LocalBinding {
    pub name: Name,
    pub identity: String,
    pub provider: Arc<dyn TableProvider>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadMode {
    Batch,
    Streaming,
}

/// Why a flow is refused before resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BindingError {
    /// A streaming read of a name with no source binding.
    UnboundStreamingRead(Name),
    /// A batch read of a name that is neither a local output nor a static
    /// binding; resolving it would reach the catalog.
    UnboundCatalogRead(Name),
    /// One name read as a stream and as a batch in the same flow.
    MixedReadModes(Name),
    /// `ReadNamedTable.temporal`: time travel resolution executes a helper
    /// plan (`time_travel.rs`).
    TimeTravel(Name),
    /// `Pivot` without explicit values infers them by executing the input
    /// (`pivoting.rs`).
    PivotInference,
    /// `Tail` resolves through a count query (`limit.rs`).
    Tail,
    /// The `Stat*` nodes execute helper plans during resolution (`stat.rs`).
    StatHelper(&'static str),
    /// A read kind outside the initial subset.
    UnsupportedRead(&'static str),
    /// The name cannot be qualified for this flow.
    Unqualifiable(String),
    /// A source binding has a different number of scans than ranges.
    ScanCountMismatch {
        name: Name,
        ranges: usize,
        scans: usize,
    },
    /// A scan presents a schema other than the binding's source schema.
    ScanSchemaMismatch { name: Name, range: usize },
    /// A range names a partition or incarnation the descriptor does not hold.
    RangeOutsideDescriptor { name: Name, range: usize },
    /// A static binding without a pinned identity proves nothing.
    StaticIdentityMissing(Name),
    /// Two bindings normalise to one name; the lowerer must not pick one by
    /// insertion order.
    DuplicateBinding(Name),
    /// One name is both a source and a static binding.
    AmbiguousBinding(Name),
    /// A binding name is not in the canonical form the flow's reads are
    /// qualified to.
    NoncanonicalBinding(Name),
    /// A local binding names something that is not a pipeline output.
    LocalBindingNotAnOutput(Name),
}

impl fmt::Display for BindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnboundStreamingRead(name) => {
                write!(f, "streaming read of {} has no bound range", quoted(name))
            }
            Self::UnboundCatalogRead(name) => write!(
                f,
                "batch read of {} is neither a pipeline output nor a pinned static binding",
                quoted(name)
            ),
            Self::MixedReadModes(name) => write!(
                f,
                "{} is read both as a stream and as a batch in one flow",
                quoted(name)
            ),
            Self::TimeTravel(name) => {
                write!(f, "time travel read of {} is not admitted", quoted(name))
            }
            Self::PivotInference => write!(f, "pivot without explicit values is not admitted"),
            Self::Tail => write!(f, "tail is not admitted"),
            Self::StatHelper(kind) => write!(f, "{kind} is not admitted"),
            Self::UnsupportedRead(kind) => write!(f, "{kind} read is not admitted"),
            Self::Unqualifiable(message) => write!(f, "{message}"),
            Self::ScanCountMismatch {
                name,
                ranges,
                scans,
            } => write!(
                f,
                "binding {} has {ranges} ranges but {scans} scans",
                quoted(name)
            ),
            Self::ScanSchemaMismatch { name, range } => write!(
                f,
                "scan {range} of binding {} does not present the source schema",
                quoted(name)
            ),
            Self::RangeOutsideDescriptor { name, range } => write!(
                f,
                "range {range} of binding {} is outside the captured descriptor",
                quoted(name)
            ),
            Self::StaticIdentityMissing(name) => {
                write!(f, "static binding {} has no pinned identity", quoted(name))
            }
            Self::DuplicateBinding(name) => {
                write!(f, "binding {} is supplied more than once", quoted(name))
            }
            Self::AmbiguousBinding(name) => write!(
                f,
                "{} is supplied both as a source and as a static binding",
                quoted(name)
            ),
            Self::NoncanonicalBinding(name) => write!(
                f,
                "binding name {} is not in the flow's canonical qualified form",
                quoted(name)
            ),
            Self::LocalBindingNotAnOutput(name) => {
                write!(f, "local binding {} is not a pipeline output", quoted(name))
            }
        }
    }
}

impl std::error::Error for BindingError {}

/// The reads of a validated flow, by qualified name.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReadSet {
    pub streaming: BTreeSet<Name>,
    pub local: BTreeSet<Name>,
    pub statics: BTreeSet<Name>,
}

/// The compatibility a binding must show before it is used: one scan per
/// range, every scan presenting the source schema, every range inside the
/// captured descriptor (its partition under its incarnation). Zero ranges is
/// a recorded empty intent and is valid; it is not an absent binding.
pub fn check_source(binding: &SourceBinding) -> Result<(), BindingError> {
    let ranges = binding.intent.ranges.len();
    let scans = binding.scans.len();
    if ranges != scans {
        return Err(BindingError::ScanCountMismatch {
            name: binding.name.clone(),
            ranges,
            scans,
        });
    }
    for (index, (range, scan)) in binding.intent.ranges.iter().zip(&binding.scans).enumerate() {
        let known = binding.descriptor.partitions.iter().any(|partition| {
            partition.id == range.partition && partition.incarnation == range.incarnation
        });
        if !known {
            return Err(BindingError::RangeOutsideDescriptor {
                name: binding.name.clone(),
                range: index,
            });
        }
        if scan.schema() != binding.schema {
            return Err(BindingError::ScanSchemaMismatch {
                name: binding.name.clone(),
                range: index,
            });
        }
    }
    Ok(())
}

/// The canonical form of a name under `scope`, as `GraphScope::resolve`
/// qualifies a read: lower-cased parts, taken as is when they spell an
/// output, else qualified for the flow. Binding names must already be in
/// this form, so a bare `x` and `catalog.database.x` cannot both denote one
/// source under the flow.
fn canonical(
    graph: &GraphDefinition,
    scope: &GraphScope<'_>,
    name: &Name,
) -> Result<Name, BindingError> {
    let raw: Name = name.iter().map(|part| part.to_ascii_lowercase()).collect();
    if graph.outputs.iter().any(|output| output.name == raw) {
        return Ok(raw);
    }
    scope
        .qualify(&raw)
        .map_err(|error| BindingError::Unqualifiable(error.to_string()))
}

/// Validates every read of `plan` against the bindings, before resolution,
/// after checking each binding's own compatibility and that every binding
/// name is canonical and unique. `ctx` is consulted only for its in-memory
/// data source registry, which decides whether a two-part name is a
/// `format.path` data source read (`read.rs`), exactly as the resolver
/// decides it; no I/O happens here.
pub fn validate_reads(
    ctx: &SessionContext,
    graph: &GraphDefinition,
    flow: &Flow,
    plan: &spec::QueryPlan,
    sources: &[SourceBinding],
    statics: &[StaticBinding],
    locals: &[LocalBinding],
) -> Result<ReadSet, BindingError> {
    let empty = BTreeMap::new();
    let scope = GraphScope::new(graph, &empty, flow);
    let canonical_name = |name: &Name| -> Result<Name, BindingError> {
        let form = canonical(graph, &scope, name)?;
        if &form != name {
            return Err(BindingError::NoncanonicalBinding(name.clone()));
        }
        Ok(form)
    };
    let mut source_names: BTreeSet<Name> = BTreeSet::new();
    for binding in sources {
        check_source(binding)?;
        if !source_names.insert(canonical_name(&binding.name)?) {
            return Err(BindingError::DuplicateBinding(binding.name.clone()));
        }
    }
    let mut static_names: BTreeSet<Name> = BTreeSet::new();
    for binding in statics {
        if binding.identity.is_empty() {
            return Err(BindingError::StaticIdentityMissing(binding.name.clone()));
        }
        let name = canonical_name(&binding.name)?;
        if source_names.contains(&name) {
            return Err(BindingError::AmbiguousBinding(binding.name.clone()));
        }
        if !static_names.insert(name) {
            return Err(BindingError::DuplicateBinding(binding.name.clone()));
        }
    }
    let mut local_names: BTreeSet<Name> = BTreeSet::new();
    for binding in locals {
        if binding.identity.is_empty() {
            return Err(BindingError::StaticIdentityMissing(binding.name.clone()));
        }
        let name = canonical_name(&binding.name)?;
        if !graph.outputs.iter().any(|output| output.name == name) {
            return Err(BindingError::LocalBindingNotAnOutput(binding.name.clone()));
        }
        if source_names.contains(&name) || static_names.contains(&name) {
            return Err(BindingError::AmbiguousBinding(binding.name.clone()));
        }
        if !local_names.insert(name) {
            return Err(BindingError::DuplicateBinding(binding.name.clone()));
        }
    }
    // Absent only in bare test contexts; the Spark session always registers it.
    let registry = ctx.extension::<DataSourceRegistry>().ok();
    let mut walker = Walker {
        graph,
        scope: &scope,
        registry,
        sources: source_names,
        statics: static_names,
        modes: BTreeMap::new(),
        reads: ReadSet::default(),
    };
    walker.walk(plan, &HashSet::new())?;
    Ok(walker.reads)
}

struct Walker<'a> {
    graph: &'a GraphDefinition,
    scope: &'a GraphScope<'a>,
    /// The session's data source registry, for `format.path` reads.
    registry: Option<Arc<DataSourceRegistry>>,
    sources: BTreeSet<Name>,
    statics: BTreeSet<Name>,
    modes: BTreeMap<Name, ReadMode>,
    reads: ReadSet,
}

impl Walker<'_> {
    /// The qualified name of a read, as `GraphScope::resolve` would see it:
    /// lower-cased parts, taken as is when they spell an output, else
    /// qualified for the flow.
    fn qualify(&self, name: &spec::ObjectName) -> Result<Name, BindingError> {
        let parts: Name = name
            .parts()
            .iter()
            .map(|part| part.as_ref().to_string())
            .collect();
        canonical(self.graph, self.scope, &parts)
    }

    fn read(
        &mut self,
        read_type: &spec::ReadType,
        streaming: bool,
        cte_scope: &HashSet<String>,
    ) -> Result<(), BindingError> {
        let table = match read_type {
            spec::ReadType::NamedTable(table) => table,
            spec::ReadType::DataSource(_) => {
                return Err(BindingError::UnsupportedRead("data source"));
            }
            spec::ReadType::Udtf(_) => return Err(BindingError::UnsupportedRead("table function")),
            spec::ReadType::DynamicTable(_) => {
                return Err(BindingError::UnsupportedRead("dynamic table"));
            }
        };
        // `format.path` is a data source read when the format is registered
        // (`resolve_query_read_named_table`), decided before any table lookup.
        if let [format, _] = table.name.parts()
            && let Some(registry) = &self.registry
            && registry
                .get_data_source(&format.as_ref().to_ascii_lowercase())
                .is_ok()
        {
            return Err(BindingError::UnsupportedRead("data source"));
        }
        // A bare name defined by an enclosing CTE is substituted by the
        // resolver (`state.get_cte`, keyed by the literal bare reference, so
        // the match is case-sensitive); time travel on it is refused by the
        // resolver too, before any lookup. The streaming flag is ignored by
        // substitution, as `is_streaming_query_plan` also assumes.
        if let [name] = table.name.parts()
            && cte_scope.contains(name.as_ref())
        {
            if table.temporal.is_some() {
                return Err(BindingError::TimeTravel(vec![name.as_ref().to_string()]));
            }
            return Ok(());
        }
        let name = self.qualify(&table.name)?;
        if table.temporal.is_some() {
            return Err(BindingError::TimeTravel(name));
        }
        let mode = if streaming {
            ReadMode::Streaming
        } else {
            ReadMode::Batch
        };
        match self.modes.insert(name.clone(), mode) {
            Some(previous) if previous != mode => {
                return Err(BindingError::MixedReadModes(name));
            }
            _ => {}
        }
        match mode {
            ReadMode::Streaming => {
                if !self.sources.contains(&name) {
                    return Err(BindingError::UnboundStreamingRead(name));
                }
                self.reads.streaming.insert(name);
            }
            ReadMode::Batch => {
                if self.graph.outputs.iter().any(|output| output.name == name) {
                    self.reads.local.insert(name);
                } else if self.statics.contains(&name) {
                    self.reads.statics.insert(name);
                } else {
                    return Err(BindingError::UnboundCatalogRead(name));
                }
            }
        }
        Ok(())
    }

    /// Query/CTE scope and admission policy remain this walker's responsibility.
    fn expr(&mut self, expr: &spec::Expr, cte_scope: &HashSet<String>) -> Result<(), BindingError> {
        expr.visit_subqueries(&mut |query| self.walk(query, cte_scope))
    }

    fn walk(
        &mut self,
        plan: &spec::QueryPlan,
        cte_scope: &HashSet<String>,
    ) -> Result<(), BindingError> {
        use spec::QueryNode as N;
        // Admission policy stays here; structural traversal has no refusals.
        match &plan.node {
            N::Read {
                read_type,
                is_streaming,
            } => {
                self.read(read_type, *is_streaming, cte_scope)?;
            }
            N::CommonInlineUserDefinedTableFunction(function) => {
                if let spec::TableFunctionDefinition::PythonUdtf {
                    return_type: None, ..
                } = &function.function
                {
                    return Err(BindingError::UnsupportedRead(
                        "dynamic inline table function",
                    ));
                }
            }
            N::Tail { .. } => return Err(BindingError::Tail),
            N::Pivot(pivot) if pivot.values.is_empty() => return Err(BindingError::PivotInference),
            N::StatSummary { .. } => return Err(BindingError::StatHelper("summary")),
            N::StatDescribe { .. } => return Err(BindingError::StatHelper("describe")),
            N::StatCrosstab { .. } => return Err(BindingError::StatHelper("crosstab")),
            N::StatCov { .. } => return Err(BindingError::StatHelper("covariance")),
            N::StatCorr { .. } => return Err(BindingError::StatHelper("correlation")),
            N::StatApproxQuantile { .. } => {
                return Err(BindingError::StatHelper("approximate quantile"));
            }
            N::StatFreqItems { .. } => return Err(BindingError::StatHelper("frequent items")),
            N::StatSampleBy { .. } => return Err(BindingError::StatHelper("sample by")),
            N::WithCtes {
                input,
                ctes,
                recursive,
            } => {
                // `resolve_query_with_ctes` keeps the last definition of a name
                // (`cte.rs`); shadowed definitions are never resolved, so they
                // are not walked either. A recursive definition sees its own
                // name.
                let mut kept: Vec<&(spec::Identifier, spec::QueryPlan)> = vec![];
                let mut seen: HashSet<&str> = HashSet::new();
                for cte in ctes.iter().rev() {
                    if seen.insert(cte.0.as_ref()) {
                        kept.push(cte);
                    }
                }
                kept.reverse();
                let mut inner = cte_scope.clone();
                for (name, definition) in kept {
                    if *recursive {
                        inner.insert(name.as_ref().to_string());
                    }
                    self.walk(definition, &inner)?;
                    inner.insert(name.as_ref().to_string());
                }
                return self.walk(input, &inner);
            }
            // Each new query kind requires an explicit admission decision.
            N::Project { .. }
            | N::Filter { .. }
            | N::Join(_)
            | N::SetOperation(_)
            | N::Sort { .. }
            | N::Limit { .. }
            | N::Aggregate(_)
            | N::LocalRelation { .. }
            | N::Sample(_)
            | N::TableSample { .. }
            | N::Deduplicate(_)
            | N::Range(_)
            | N::SubqueryAlias { .. }
            | N::Repartition { .. }
            | N::ToDf { .. }
            | N::WithColumnsRenamed { .. }
            | N::Drop { .. }
            | N::WithColumns { .. }
            | N::Hint { .. }
            | N::Pivot(_)
            | N::Unpivot(_)
            | N::ToSchema { .. }
            | N::RepartitionByExpression { .. }
            | N::MapPartitions { .. }
            | N::CollectMetrics { .. }
            | N::Parse(_)
            | N::GroupMap(_)
            | N::CoGroupMap(_)
            | N::WithWatermark(_)
            | N::ApplyInPandasWithState(_)
            | N::CachedLocalRelation { .. }
            | N::CachedRemoteRelation { .. }
            | N::FillNa { .. }
            | N::DropNa { .. }
            | N::Replace { .. }
            | N::Empty { .. }
            | N::WithParameters { .. }
            | N::Values(_)
            | N::TableAlias { .. }
            | N::NamedWindows { .. }
            | N::WithRelations { .. }
            | N::LateralView { .. }
            | N::LateralJoin { .. } => {}
        }
        plan.visit_children(&mut |child| match child {
            spec::QueryChild::Expression(expr) => self.expr(expr, cte_scope),
            spec::QueryChild::Window(window) => {
                window.visit_subqueries(&mut |query| self.walk(query, cte_scope))
            }
            spec::QueryChild::Query(query) => self.walk(query, cte_scope),
        })
    }
}

#[cfg(test)]
#[path = "binding_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "binding_wildcard_tests.rs"]
mod wildcard_tests;

#[cfg(test)]
#[path = "binding_sample_tests.rs"]
mod sample_tests;
