//! Ordered graph input discovery before semantic column resolution.
use std::collections::{BTreeMap, HashSet};

use sail_common::spec;
use sail_plan::error::{PlanError, PlanResult};

use crate::pipelines::analysis::GraphScope;
use crate::pipelines::graph::{Flow, GraphDefinition, Name};

/// Discover local reads, preserving occurrences and structural binding order.
/// CTE matching uses the initial call configuration, before view overwrites.
/// This performs no provider/catalog lookup, semantic analysis or helper query.
/// The caller filters tables from view configuration contributions.
pub(super) fn ordered_inputs(
    graph: &GraphDefinition,
    flow: &Flow,
    query: &spec::QueryPlan,
    case_sensitive: bool,
    is_source_format: impl Fn(&str) -> bool,
) -> PlanResult<Vec<Name>> {
    let resolved = BTreeMap::new();
    let scope = GraphScope::new(graph, &resolved, flow);
    let mut trace = Trace {
        scope,
        case_sensitive,
        is_source_format,
        reads: vec![],
    };
    trace.query(query, &HashSet::new())?;
    Ok(trace.reads)
}

struct Trace<'a, F> {
    scope: GraphScope<'a>,
    case_sensitive: bool,
    is_source_format: F,
    reads: Vec<Name>,
}
impl<F: Fn(&str) -> bool> Trace<'_, F> {
    fn key(&self, value: &str) -> String {
        if self.case_sensitive {
            value.to_string()
        } else {
            value.to_ascii_lowercase()
        }
    }
    fn expression(&mut self, expr: &spec::Expr, ctes: &HashSet<String>) -> PlanResult<()> {
        if matches!(expr, spec::Expr::Subquery { .. }) {
            return Err(PlanError::unsupported(
                "configuration trace of plan-ID subquery references",
            ));
        }
        expr.visit_subqueries(&mut |query| self.query(query, ctes))
    }
    fn query(&mut self, query: &spec::QueryPlan, ctes: &HashSet<String>) -> PlanResult<()> {
        use spec::QueryNode as N;
        match &query.node {
            N::WithCtes {
                input,
                recursive,
                ctes: definitions,
            } => {
                let mut kept = vec![];
                let mut seen = HashSet::new();
                for (name, definition) in definitions.iter().rev() {
                    if seen.insert(self.key(name.as_ref())) {
                        kept.push((name, definition));
                    }
                }
                let mut inner = ctes.clone();
                for (name, definition) in kept.into_iter().rev() {
                    let key = self.key(name.as_ref());
                    if *recursive {
                        inner.insert(key.clone());
                    }
                    self.query(definition, &inner)?;
                    inner.insert(key);
                }
                return self.query(input, &inner);
            }
            N::WithRelations { .. } => {
                return Err(PlanError::unsupported(
                    "configuration trace of WithRelations bindings",
                ));
            }
            N::Read { read_type, .. } => match read_type {
                spec::ReadType::NamedTable(table) => {
                    let parts: Name = table.name.clone().into();
                    let format_path = matches!(parts.as_slice(), [format, _] if (self.is_source_format)(&format.to_ascii_lowercase()));
                    let cte = matches!(parts.as_slice(), [name] if ctes.contains(&self.key(name)));
                    if !format_path && !cte {
                        let name = self.scope.input_name(&parts)?;
                        if self
                            .scope
                            .graph
                            .outputs
                            .iter()
                            .any(|output| output.name == name)
                        {
                            self.reads.push(name);
                        }
                    }
                }
                // The computed target is materialized after graph substitution.
                // Its expression/sample subqueries are visited below as usual.
                spec::ReadType::DynamicTable(_) => {}
                spec::ReadType::DataSource(_) | spec::ReadType::Udtf(_) => {}
            },
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
            | N::Tail { .. }
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
            | N::CommonInlineUserDefinedTableFunction(_)
            | N::FillNa { .. }
            | N::DropNa { .. }
            | N::Replace { .. }
            | N::StatSummary { .. }
            | N::StatDescribe { .. }
            | N::StatCrosstab { .. }
            | N::StatCov { .. }
            | N::StatCorr { .. }
            | N::StatApproxQuantile { .. }
            | N::StatFreqItems { .. }
            | N::StatSampleBy { .. }
            | N::Empty { .. }
            | N::WithParameters { .. }
            | N::Values(_)
            | N::TableAlias { .. }
            | N::NamedWindows { .. }
            | N::LateralView { .. }
            | N::LateralJoin { .. }
            | N::Extension { .. } => {}
        }
        query.visit_children(&mut |child| match child {
            spec::QueryChild::Expression(expr) => self.expression(expr, ctes),
            spec::QueryChild::Window(window) => {
                window.visit_subqueries(&mut |query| self.query(query, ctes))
            }
            spec::QueryChild::Query(query) => self.query(query, ctes),
        })
    }
}

#[cfg(test)]
#[path = "trace_tests.rs"]
mod tests;
