//! Admission of a resolved, bound plan to the stateless bounded subset
//! (BOUNDED-PLAN-ADMISSION-DESIGN §4 as narrowed in §10 after CF-0136).
//!
//! The visitor classifies every operator of the resolved `LogicalPlan` and
//! returns evidence, not only a verdict: which operators were admitted or
//! rejected and why, which bound sources, static sides and local outputs
//! were scanned (each with its provider identity verified against the
//! bindings, never by name alone), whether every scan was bound, and
//! whether the plan is replayable in content. At most one range-bearing
//! side may meet any operator, counted per subquery and per join input,
//! through projections, aliases, unions, nested joins and subquery
//! expressions; join kinds follow Spark's stream-static orientation rules
//! (`UnsupportedOperationChecker.scala:498–545`). A schema-only
//! `EmptyRelation` is an analysis placeholder and never stands in for rows.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::common::JoinType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::{TableProvider, source_as_provider};
use datafusion::logical_expr::{Expr, LogicalPlan, Volatility, WindowFunctionDefinition};
use serde::{Deserialize, Serialize};

use super::binding::{LocalBinding, ReadSet, SourceBinding, StaticBinding};
use crate::pipelines::graph::Name;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decision {
    Admit,
    Reject,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorEvidence {
    pub operator: String,
    pub decision: Decision,
    pub reason: String,
}

/// Serialisable admission evidence, recorded next to the intent.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionEvidence {
    pub admitted: bool,
    pub operators: Vec<OperatorEvidence>,
    /// Bound streaming sources the plan scans.
    pub sources_read: BTreeSet<Name>,
    /// Local outputs and static bindings the plan scans.
    pub static_read: BTreeSet<Name>,
    /// Scans that match no binding, or whose provider is not the bound one,
    /// rendered as the resolver named them.
    pub unbound_scans: Vec<String>,
    /// `true` when every admitted scan's provider was verified by identity
    /// against the bindings; `false` never admits.
    pub provenance_verified: bool,
    /// `false` once a volatile expression was met: the same plan can yield
    /// different rows on re-evaluation, so the batch is non-replayable in
    /// content and must be materialised once.
    pub replayable: bool,
}

/// What a subtree carries, for the join rule.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Side {
    range_bearing: bool,
}

/// The bindings the lowering was built from; admission verifies every scan
/// against the providers they hold. Local outputs are bound too: the
/// materialised relation the flow reads, with its provenance, never a
/// schema-only placeholder.
pub struct Bindings<'a> {
    pub sources: &'a [SourceBinding],
    pub statics: &'a [StaticBinding],
    pub locals: &'a [LocalBinding],
    /// Empty scans the lowering minted for sources whose recorded intent has
    /// no range, one per such source; identity-checked like any other scan.
    pub empty_scans: &'a [(Name, Arc<dyn TableProvider>)],
}

/// Classifies `plan` against the reads the binding step accepted and the
/// providers the bindings hold.
pub fn admit(plan: &LogicalPlan, reads: &ReadSet, bindings: &Bindings<'_>) -> AdmissionEvidence {
    let mut visitor = Visitor {
        reads,
        bindings,
        evidence: AdmissionEvidence {
            admitted: true,
            replayable: true,
            provenance_verified: true,
            ..Default::default()
        },
    };
    visitor.visit(plan);
    visitor.evidence
}

struct Visitor<'a> {
    reads: &'a ReadSet,
    bindings: &'a Bindings<'a>,
    evidence: AdmissionEvidence,
}

fn same_provider(a: &Arc<dyn TableProvider>, b: &Arc<dyn TableProvider>) -> bool {
    Arc::ptr_eq(a, b)
}

impl Visitor<'_> {
    fn record(&mut self, operator: &str, decision: Decision, reason: impl Into<String>) {
        if decision == Decision::Reject {
            self.evidence.admitted = false;
        }
        self.evidence.operators.push(OperatorEvidence {
            operator: operator.to_string(),
            decision,
            reason: reason.into(),
        });
    }

    fn admit(&mut self, operator: &str, reason: &str) {
        self.record(operator, Decision::Admit, reason);
    }

    fn reject(&mut self, operator: &str, reason: &str) {
        self.record(operator, Decision::Reject, reason);
    }

    /// Functions whose signature is not `Immutable` make the batch
    /// non-replayable; each subquery embedded in an expression is visited as
    /// a plan and contributes its own side to the join rule, so two
    /// range-bearing subqueries under one operator count as two.
    fn expressions(&mut self, operator: &str, exprs: &[Expr]) -> Vec<Side> {
        let mut sides = vec![];
        for expr in exprs {
            for (name, volatility) in non_immutable_functions(expr) {
                let class = match volatility {
                    Volatility::Volatile => "volatile",
                    Volatility::Stable => "stable",
                    Volatility::Immutable => continue,
                };
                self.evidence.replayable = false;
                self.record(
                    operator,
                    Decision::Admit,
                    format!(
                        "function `{name}` is {class}, not immutable: evaluated once, not replayable in content"
                    ),
                );
            }
            let mut subqueries = vec![];
            let _ = expr.apply(|node| {
                match node {
                    Expr::ScalarSubquery(subquery) => subqueries.push(subquery.subquery.clone()),
                    Expr::InSubquery(in_subquery) => {
                        subqueries.push(in_subquery.subquery.subquery.clone())
                    }
                    Expr::Exists(exists) => subqueries.push(exists.subquery.subquery.clone()),
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            });
            for subquery in subqueries {
                sides.push(self.visit(&subquery));
            }
        }
        sides
    }

    /// The join rule over any set of sides: more than one range-bearing side
    /// needs cross-batch state and is rejected.
    fn combine(&mut self, operator: &str, sides: &[Side]) -> Side {
        let range_bearing = sides.iter().filter(|side| side.range_bearing).count();
        match range_bearing {
            0 => {
                self.admit(operator, "static sides only");
                Side {
                    range_bearing: false,
                }
            }
            1 => {
                self.admit(operator, "one range-bearing side and static sides");
                Side {
                    range_bearing: true,
                }
            }
            _ => {
                self.reject(
                    operator,
                    "more than one range-bearing side needs cross-batch state",
                );
                Side {
                    range_bearing: true,
                }
            }
        }
    }

    /// Spark's stream-static orientation rules for one range-bearing side
    /// (`UnsupportedOperationChecker.scala:498–545`), applied to DataFusion's
    /// join kinds; kinds without a pinned Spark counterpart are rejected.
    fn join(&mut self, join_type: JoinType, left: Side, right: Side, condition: &[Side]) -> Side {
        let condition_ranges = condition.iter().filter(|side| side.range_bearing).count();
        let ranges =
            usize::from(left.range_bearing) + usize::from(right.range_bearing) + condition_ranges;
        if ranges == 0 {
            self.admit("Join", "static sides only");
            return Side::default();
        }
        if ranges > 1 {
            self.reject(
                "Join",
                "more than one range-bearing side needs cross-batch state",
            );
            return Side {
                range_bearing: true,
            };
        }
        if condition_ranges == 1 {
            self.reject("Join", "range-bearing subquery in a join condition");
            return Side {
                range_bearing: true,
            };
        }
        let stream_left = left.range_bearing;
        let verdict: Result<&str, &str> = match join_type {
            JoinType::Inner => Ok("inner join, one range-bearing side"),
            JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti if stream_left => {
                Ok("range-bearing side on the left, static on the right")
            }
            JoinType::Left | JoinType::LeftSemi => Err(
                "left outer or left semi join with the static side on the left and the range-bearing side on the right",
            ),
            JoinType::LeftAnti => Err("left anti join with the range-bearing side on the right"),
            JoinType::Right if !stream_left => {
                Ok("range-bearing side on the right, static on the left")
            }
            JoinType::Right => Err("right outer join with the range-bearing side on the left"),
            JoinType::Full => Err("full outer join between a range-bearing and a static side"),
            JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::LeftMark
            | JoinType::RightMark => Err("join kind without a pinned Spark streaming rule"),
        };
        match verdict {
            Ok(reason) => self.admit("Join", reason),
            Err(reason) => self.reject("Join", reason),
        }
        Side {
            range_bearing: true,
        }
    }

    fn visit(&mut self, plan: &LogicalPlan) -> Side {
        match plan {
            LogicalPlan::TableScan(scan) => {
                let name: Name = [
                    scan.table_name.catalog(),
                    scan.table_name.schema(),
                    Some(scan.table_name.table()),
                ]
                .into_iter()
                .flatten()
                .map(str::to_string)
                .collect();
                let filter_sides = self.expressions("TableScan", &scan.filters);
                if filter_sides.iter().any(|side| side.range_bearing) {
                    self.reject("TableScan", "range-bearing subquery in a pushed filter");
                }
                let provider = source_as_provider(&scan.source).ok();
                let matches = |candidate: &Arc<dyn TableProvider>| {
                    provider
                        .as_ref()
                        .is_some_and(|p| same_provider(candidate, p))
                };
                if self.reads.streaming.contains(&name)
                    && self
                        .bindings
                        .sources
                        .iter()
                        .any(|binding| binding.name == name && binding.scans.iter().any(&matches))
                {
                    self.evidence.sources_read.insert(name);
                    self.admit(
                        "TableScan",
                        "bound range of a recorded source, provider verified",
                    );
                    return Side {
                        range_bearing: true,
                    };
                }
                if self.reads.streaming.contains(&name)
                    && self
                        .bindings
                        .empty_scans
                        .iter()
                        .any(|(bound, provider)| bound == &name && matches(provider))
                {
                    self.evidence.sources_read.insert(name);
                    self.admit(
                        "TableScan",
                        "empty recorded intent of a bound source, minted scan verified",
                    );
                    return Side {
                        range_bearing: true,
                    };
                }
                if self.reads.statics.contains(&name)
                    && self
                        .bindings
                        .statics
                        .iter()
                        .any(|binding| binding.name == name && matches(&binding.provider))
                {
                    self.evidence.static_read.insert(name);
                    self.admit("TableScan", "pinned static binding, provider verified");
                    return Side::default();
                }
                if self.reads.local.contains(&name)
                    && self
                        .bindings
                        .locals
                        .iter()
                        .any(|binding| binding.name == name && matches(&binding.provider))
                {
                    self.evidence.static_read.insert(name);
                    self.admit("TableScan", "materialised local output, provider verified");
                    return Side::default();
                }
                self.evidence
                    .unbound_scans
                    .push(scan.table_name.to_string());
                self.evidence.provenance_verified = false;
                self.reject(
                    "TableScan",
                    "scan outside the binding set or with a provider that is not the bound one",
                );
                Side::default()
            }
            // Only analysis installs schema-only placeholders; rows cannot be
            // read from one, so a lowering that meets it is not over
            // materialised inputs.
            LogicalPlan::EmptyRelation(_) => {
                self.evidence.provenance_verified = false;
                self.reject(
                    "EmptyRelation",
                    "schema-only analysis placeholder cannot stand in for rows",
                );
                Side::default()
            }
            LogicalPlan::Values(values) => {
                let exprs: Vec<Expr> = values.values.iter().flatten().cloned().collect();
                let sides = self.expressions("Values", &exprs);
                self.combine("Values", &sides)
            }
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Unnest(_) => {
                let operator = operator_name(plan);
                let mut sides = self.expressions(operator, &plan.expressions());
                for input in plan.inputs() {
                    sides.push(self.visit(input));
                }
                self.combine(operator, &sides)
            }
            LogicalPlan::Union(union) => {
                let sides: Vec<Side> = union.inputs.iter().map(|input| self.visit(input)).collect();
                let range_bearing = sides.iter().any(|side| side.range_bearing);
                self.admit("Union", "row-local union of its inputs");
                Side { range_bearing }
            }
            LogicalPlan::Join(join) => {
                let condition_sides = self.expressions("Join", &plan.expressions());
                let left = self.visit(&join.left);
                let right = self.visit(&join.right);
                self.join(join.join_type, left, right, &condition_sides)
            }
            LogicalPlan::Subquery(subquery) => self.visit(&subquery.subquery),
            LogicalPlan::Aggregate(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::RecursiveQuery(_) => {
                let operator = operator_name(plan);
                self.reject(
                    operator,
                    "needs durable cross-batch state under AvailableNow semantics",
                );
                let mut side = Side::default();
                for input in plan.inputs() {
                    side.range_bearing |= self.visit(input).range_bearing;
                }
                side
            }
            LogicalPlan::Extension(extension) => {
                self.reject(
                    &format!("Extension({})", extension.node.name()),
                    "extension node without qualified semantics",
                );
                let mut side = Side::default();
                for input in plan.inputs() {
                    side.range_bearing |= self.visit(input).range_bearing;
                }
                side
            }
            LogicalPlan::Statement(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_) => {
                self.reject(operator_name(plan), "not a query over bound inputs");
                Side::default()
            }
        }
    }
}

fn operator_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Projection(_) => "Projection",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Window(_) => "Window",
        LogicalPlan::Aggregate(_) => "Aggregate",
        LogicalPlan::Sort(_) => "Sort",
        LogicalPlan::Join(_) => "Join",
        LogicalPlan::Repartition(_) => "Repartition",
        LogicalPlan::Union(_) => "Union",
        LogicalPlan::TableScan(_) => "TableScan",
        LogicalPlan::EmptyRelation(_) => "EmptyRelation",
        LogicalPlan::Subquery(_) => "Subquery",
        LogicalPlan::SubqueryAlias(_) => "SubqueryAlias",
        LogicalPlan::Limit(_) => "Limit",
        LogicalPlan::Statement(_) => "Statement",
        LogicalPlan::Values(_) => "Values",
        LogicalPlan::Explain(_) => "Explain",
        LogicalPlan::Analyze(_) => "Analyze",
        LogicalPlan::Extension(_) => "Extension",
        LogicalPlan::Distinct(_) => "Distinct",
        LogicalPlan::Dml(_) => "Dml",
        LogicalPlan::Ddl(_) => "Ddl",
        LogicalPlan::Copy(_) => "Copy",
        LogicalPlan::DescribeTable(_) => "DescribeTable",
        LogicalPlan::Unnest(_) => "Unnest",
        LogicalPlan::RecursiveQuery(_) => "RecursiveQuery",
    }
}

/// Every function an expression calls whose signature is not `Immutable`,
/// with its declared volatility. Only an `Immutable` function is guaranteed
/// to yield the same value across separately planned attempts. `Stable`
/// means stable within one query execution: clock functions such as `now`,
/// `current_date` and `current_timestamp` are `Stable` and are fixed at
/// each attempt's planning time, so once a batch is re-planned they are as
/// non-replayable in content as a `Volatile` call. Scalar, aggregate and
/// window signatures are all inspected; no function is special-cased by
/// name.
fn non_immutable_functions(expr: &Expr) -> Vec<(String, Volatility)> {
    let mut found = vec![];
    let _ = expr.apply(|node| {
        let signature = match node {
            Expr::ScalarFunction(function) => {
                Some((function.func.name(), function.func.signature()))
            }
            Expr::AggregateFunction(function) => {
                Some((function.func.name(), function.func.signature()))
            }
            Expr::WindowFunction(window) => Some(match &window.fun {
                WindowFunctionDefinition::AggregateUDF(function) => {
                    (function.name(), function.signature())
                }
                WindowFunctionDefinition::WindowUDF(function) => {
                    (function.name(), function.signature())
                }
            }),
            _ => None,
        };
        if let Some((name, signature)) =
            signature.filter(|(_, signature)| signature.volatility != Volatility::Immutable)
        {
            found.push((name.to_string(), signature.volatility));
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

#[cfg(test)]
#[path = "admission_tests.rs"]
mod tests;
