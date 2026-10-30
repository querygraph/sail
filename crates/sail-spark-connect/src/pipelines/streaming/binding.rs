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
    #[cfg_attr(
        test,
        expect(dead_code, reason = "read by the admission integration unit")
    )]
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
    #[cfg_attr(
        test,
        expect(dead_code, reason = "read by the admission integration unit")
    )]
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

    fn exprs(
        &mut self,
        exprs: &[spec::Expr],
        cte_scope: &HashSet<String>,
    ) -> Result<(), BindingError> {
        exprs.iter().try_for_each(|expr| self.expr(expr, cte_scope))
    }

    fn sort_orders(
        &mut self,
        orders: &[spec::SortOrder],
        cte_scope: &HashSet<String>,
    ) -> Result<(), BindingError> {
        orders
            .iter()
            .try_for_each(|order| self.expr(&order.child, cte_scope))
    }

    fn window(
        &mut self,
        window: &spec::Window,
        cte_scope: &HashSet<String>,
    ) -> Result<(), BindingError> {
        match window {
            spec::Window::Named(_) => Ok(()),
            spec::Window::Unnamed {
                cluster_by,
                partition_by,
                order_by,
                frame,
            } => {
                self.exprs(cluster_by, cte_scope)?;
                self.exprs(partition_by, cte_scope)?;
                self.sort_orders(order_by, cte_scope)?;
                if let Some(frame) = frame {
                    for boundary in [&frame.lower, &frame.upper] {
                        match boundary {
                            spec::WindowFrameBoundary::Preceding(expr)
                            | spec::WindowFrameBoundary::Following(expr)
                            | spec::WindowFrameBoundary::Value(expr) => {
                                self.expr(expr, cte_scope)?;
                            }
                            _ => {}
                        }
                    }
                }
                Ok(())
            }
        }
    }

    /// Every expression variant, exhaustively: child expressions are
    /// followed and embedded queries (`InSubquery`, `ScalarSubquery`,
    /// `Exists`) are walked as plans under the enclosing CTE scope, so a
    /// read hidden in a predicate, a function argument, a case branch, a
    /// cast, a window or a join condition is classified before resolution.
    fn expr(&mut self, expr: &spec::Expr, cte_scope: &HashSet<String>) -> Result<(), BindingError> {
        use spec::Expr as E;
        match expr {
            E::Literal(_)
            | E::UnresolvedAttribute { .. }
            | E::UnresolvedStar { .. }
            | E::UnresolvedRegex { .. }
            | E::UnresolvedNamedLambdaVariable(_)
            | E::DefaultColumnValue
            | E::Placeholder(_)
            | E::UnresolvedDate { .. }
            | E::UnresolvedTime { .. }
            | E::UnresolvedTimestamp { .. } => Ok(()),
            E::UnresolvedFunction(function) => {
                self.exprs(&function.arguments, cte_scope)?;
                for (_, argument) in &function.named_arguments {
                    self.expr(argument, cte_scope)?;
                }
                if let Some(filter) = &function.filter {
                    self.expr(filter, cte_scope)?;
                }
                if let Some(order_by) = &function.order_by {
                    self.sort_orders(order_by, cte_scope)?;
                }
                Ok(())
            }
            E::CommonInlineUserDefinedFunction(function) => {
                self.exprs(&function.arguments, cte_scope)
            }
            E::Alias { expr, .. }
            | E::Cast { expr, .. }
            | E::Table { expr }
            | E::IdentifierClause { expr }
            | E::IsFalse(expr)
            | E::IsNotFalse(expr)
            | E::IsTrue(expr)
            | E::IsNotTrue(expr)
            | E::IsNull(expr)
            | E::IsNotNull(expr)
            | E::IsUnknown(expr)
            | E::IsNotUnknown(expr) => self.expr(expr, cte_scope),
            E::NamedArgument { value, .. } => self.expr(value, cte_scope),
            E::SortOrder(order) => self.expr(&order.child, cte_scope),
            E::LambdaFunction { function, .. } => self.expr(function, cte_scope),
            E::Window {
                window_function,
                window,
            } => {
                self.expr(window_function, cte_scope)?;
                self.window(window, cte_scope)
            }
            E::UnresolvedExtractValue { child, extraction } => {
                self.expr(child, cte_scope)?;
                self.expr(extraction, cte_scope)
            }
            E::UpdateFields {
                struct_expression,
                value_expression,
                ..
            } => {
                self.expr(struct_expression, cte_scope)?;
                match value_expression {
                    Some(value) => self.expr(value, cte_scope),
                    None => Ok(()),
                }
            }
            E::CallFunction { arguments, .. } | E::Rollup(arguments) | E::Cube(arguments) => {
                self.exprs(arguments, cte_scope)
            }
            E::GroupingSets(sets) => sets.iter().try_for_each(|set| self.exprs(set, cte_scope)),
            E::InSubquery { expr, subquery, .. } => {
                self.expr(expr, cte_scope)?;
                self.walk(subquery, cte_scope)
            }
            E::ScalarSubquery { subquery } | E::Exists { subquery, .. } => {
                self.walk(subquery, cte_scope)
            }
            E::Subquery {
                in_subquery_values, ..
            } => self.exprs(in_subquery_values, cte_scope),
            E::InList { expr, list, .. } => {
                self.expr(expr, cte_scope)?;
                self.exprs(list, cte_scope)
            }
            E::Between {
                expr, low, high, ..
            } => {
                self.expr(expr, cte_scope)?;
                self.expr(low, cte_scope)?;
                self.expr(high, cte_scope)
            }
            E::IsDistinctFrom { left, right } | E::IsNotDistinctFrom { left, right } => {
                self.expr(left, cte_scope)?;
                self.expr(right, cte_scope)
            }
            E::SimilarTo { expr, pattern, .. } => {
                self.expr(expr, cte_scope)?;
                self.expr(pattern, cte_scope)
            }
        }
    }

    fn walk(
        &mut self,
        plan: &spec::QueryPlan,
        cte_scope: &HashSet<String>,
    ) -> Result<(), BindingError> {
        use spec::QueryNode as N;
        match &plan.node {
            N::Read {
                read_type,
                is_streaming,
            } => self.read(read_type, *is_streaming, cte_scope),
            // Leaves without query inputs; literal rows and an inline table
            // function still carry expressions.
            N::LocalRelation { .. }
            | N::CachedLocalRelation { .. }
            | N::CachedRemoteRelation { .. }
            | N::Range(_)
            | N::Empty { .. } => Ok(()),
            N::Values(rows) => rows.iter().try_for_each(|row| self.exprs(row, cte_scope)),
            N::CommonInlineUserDefinedTableFunction(function) => {
                self.exprs(&function.arguments, cte_scope)
            }
            // Constructs whose resolution executes a helper plan.
            N::Tail { .. } => Err(BindingError::Tail),
            N::Pivot(pivot) if pivot.values.is_empty() => Err(BindingError::PivotInference),
            N::StatSummary { .. } => Err(BindingError::StatHelper("summary")),
            N::StatDescribe { .. } => Err(BindingError::StatHelper("describe")),
            N::StatCrosstab { .. } => Err(BindingError::StatHelper("crosstab")),
            N::StatCov { .. } => Err(BindingError::StatHelper("covariance")),
            N::StatCorr { .. } => Err(BindingError::StatHelper("correlation")),
            N::StatApproxQuantile { .. } => Err(BindingError::StatHelper("approximate quantile")),
            N::StatFreqItems { .. } => Err(BindingError::StatHelper("frequent items")),
            N::StatSampleBy { .. } => Err(BindingError::StatHelper("sample by")),
            // One required input, no expressions.
            N::SubqueryAlias { input, .. }
            | N::Repartition { input, .. }
            | N::ToDf { input, .. }
            | N::WithColumnsRenamed { input, .. }
            | N::ToSchema { input, .. }
            | N::DropNa { input, .. }
            | N::Replace { input, .. }
            | N::TableAlias { input, .. }
            | N::TableSample { input, .. } => self.walk(input, cte_scope),
            // One required input with expressions.
            N::Filter { input, condition } => {
                self.expr(condition, cte_scope)?;
                self.walk(input, cte_scope)
            }
            N::Sort { input, order, .. } => {
                self.sort_orders(order, cte_scope)?;
                self.walk(input, cte_scope)
            }
            N::Limit { input, skip, limit } => {
                for expr in [skip, limit].into_iter().flatten() {
                    self.expr(expr, cte_scope)?;
                }
                self.walk(input, cte_scope)
            }
            N::Drop { input, columns, .. } => {
                self.exprs(columns, cte_scope)?;
                self.walk(input, cte_scope)
            }
            N::WithColumns { input, aliases } => {
                self.exprs(aliases, cte_scope)?;
                self.walk(input, cte_scope)
            }
            N::Hint {
                input, parameters, ..
            } => {
                self.exprs(parameters, cte_scope)?;
                self.walk(input, cte_scope)
            }
            N::RepartitionByExpression {
                input,
                partition_expressions,
                ..
            } => {
                self.exprs(partition_expressions, cte_scope)?;
                self.walk(input, cte_scope)
            }
            N::MapPartitions {
                input, function, ..
            } => {
                self.exprs(&function.arguments, cte_scope)?;
                self.walk(input, cte_scope)
            }
            N::CollectMetrics { input, metrics, .. } => {
                self.exprs(metrics, cte_scope)?;
                self.walk(input, cte_scope)
            }
            N::FillNa { input, values, .. } => {
                self.exprs(values, cte_scope)?;
                self.walk(input, cte_scope)
            }
            N::WithParameters {
                input,
                positional_arguments,
                named_arguments,
            } => {
                self.exprs(positional_arguments, cte_scope)?;
                for (_, argument) in named_arguments {
                    self.expr(argument, cte_scope)?;
                }
                self.walk(input, cte_scope)
            }
            N::NamedWindows { input, windows } => {
                for (_, window) in windows {
                    self.window(window, cte_scope)?;
                }
                self.walk(input, cte_scope)
            }
            // One optional input with expressions.
            N::Project { input, expressions } => {
                self.exprs(expressions, cte_scope)?;
                match input {
                    Some(input) => self.walk(input, cte_scope),
                    None => Ok(()),
                }
            }
            N::LateralView {
                input,
                arguments,
                named_arguments,
                ..
            } => {
                self.exprs(arguments, cte_scope)?;
                for (_, argument) in named_arguments {
                    self.expr(argument, cte_scope)?;
                }
                match input {
                    Some(input) => self.walk(input, cte_scope),
                    None => Ok(()),
                }
            }
            // Nested structs with one input.
            N::Aggregate(aggregate) => {
                self.exprs(&aggregate.grouping, cte_scope)?;
                self.exprs(&aggregate.aggregate, cte_scope)?;
                if let Some(having) = &aggregate.having {
                    self.expr(having, cte_scope)?;
                }
                self.walk(&aggregate.input, cte_scope)
            }
            N::Sample(sample) => self.walk(&sample.input, cte_scope),
            N::Deduplicate(deduplicate) => self.walk(&deduplicate.input, cte_scope),
            N::Pivot(pivot) => {
                if let Some(grouping) = &pivot.grouping {
                    self.exprs(grouping, cte_scope)?;
                }
                self.exprs(&pivot.aggregate, cte_scope)?;
                self.exprs(&pivot.columns, cte_scope)?;
                for value in &pivot.values {
                    self.exprs(&value.values, cte_scope)?;
                }
                self.walk(&pivot.input, cte_scope)
            }
            N::Unpivot(unpivot) => {
                if let Some(ids) = &unpivot.ids {
                    self.exprs(ids, cte_scope)?;
                }
                for value in unpivot.values.iter().flatten() {
                    self.exprs(&value.columns, cte_scope)?;
                }
                self.walk(&unpivot.input, cte_scope)
            }
            N::Parse(parse) => self.walk(&parse.input, cte_scope),
            N::WithWatermark(watermark) => self.walk(&watermark.input, cte_scope),
            N::ApplyInPandasWithState(apply) => {
                self.exprs(&apply.grouping_expressions, cte_scope)?;
                self.exprs(&apply.function.arguments, cte_scope)?;
                self.walk(&apply.input, cte_scope)
            }
            N::GroupMap(group) => {
                self.exprs(&group.grouping_expressions, cte_scope)?;
                self.exprs(&group.sorting_expressions, cte_scope)?;
                self.exprs(&group.initial_grouping_expressions, cte_scope)?;
                self.exprs(&group.function.arguments, cte_scope)?;
                self.walk(&group.input, cte_scope)?;
                match &group.initial_input {
                    Some(initial) => self.walk(initial, cte_scope),
                    None => Ok(()),
                }
            }
            // Several inputs.
            N::Join(join) => {
                if let Some(spec::JoinCriteria::On(condition)) = &join.join_criteria {
                    self.expr(condition, cte_scope)?;
                }
                self.walk(&join.left, cte_scope)?;
                self.walk(&join.right, cte_scope)
            }
            N::LateralJoin {
                left,
                right,
                join_condition,
                ..
            } => {
                if let Some(condition) = join_condition {
                    self.expr(condition, cte_scope)?;
                }
                self.walk(left, cte_scope)?;
                self.walk(right, cte_scope)
            }
            N::SetOperation(set) => {
                self.walk(&set.left, cte_scope)?;
                self.walk(&set.right, cte_scope)
            }
            N::CoGroupMap(cogroup) => {
                self.exprs(&cogroup.input_grouping_expressions, cte_scope)?;
                self.exprs(&cogroup.other_grouping_expressions, cte_scope)?;
                self.exprs(&cogroup.input_sorting_expressions, cte_scope)?;
                self.exprs(&cogroup.other_sorting_expressions, cte_scope)?;
                self.exprs(&cogroup.function.arguments, cte_scope)?;
                self.walk(&cogroup.input, cte_scope)?;
                self.walk(&cogroup.other, cte_scope)
            }
            N::WithRelations { root, references } => {
                for reference in references {
                    self.walk(reference, cte_scope)?;
                }
                self.walk(root, cte_scope)
            }
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
                self.walk(input, &inner)
            }
        }
    }
}

#[cfg(test)]
#[path = "binding_tests.rs"]
mod tests;
