//! Pure traversal of queries embedded in expressions, independent of resolution.
use super::{Expr, QueryPlan, SortOrder, Window, WindowFrameBoundary};

impl Expr {
    /// Visit queries embedded in this expression and its expression children.
    ///
    /// Preserve field/list order and repeated occurrences, stopping at the first
    /// callback error. The callback decides how to descend into each query and
    /// how to interpret its scope; this method neither resolves nor visits the
    /// query's contents. `Subquery` plan-ID references are not expanded here.
    /// This is typed spec traversal, not a guarantee of Catalyst plan order.
    pub fn visit_subqueries<'a, E>(
        &'a self,
        visitor: &mut impl FnMut(&'a QueryPlan) -> Result<(), E>,
    ) -> Result<(), E> {
        use Expr as Node;
        match self {
            Node::Literal(_)
            | Node::UnresolvedAttribute { .. }
            | Node::UnresolvedRegex { .. }
            | Node::UnresolvedNamedLambdaVariable(_)
            | Node::DefaultColumnValue
            | Node::Placeholder(_)
            | Node::UnresolvedDate { .. }
            | Node::UnresolvedTime { .. }
            | Node::UnresolvedTimestamp { .. } => Ok(()),
            Node::UnresolvedStar {
                wildcard_options, ..
            } => {
                for replacement in wildcard_options.replace_columns.iter().flatten() {
                    replacement.expression.visit_subqueries(visitor)?;
                }
                Ok(())
            }
            Node::UnresolvedFunction(function) => {
                visit_exprs(&function.arguments, visitor)?;
                for (_, argument) in &function.named_arguments {
                    argument.visit_subqueries(visitor)?;
                }
                if let Some(filter) = &function.filter {
                    filter.visit_subqueries(visitor)?;
                }
                if let Some(order_by) = &function.order_by {
                    visit_sort_orders(order_by, visitor)?;
                }
                Ok(())
            }
            Node::CommonInlineUserDefinedFunction(function) => {
                visit_exprs(&function.arguments, visitor)
            }
            Node::Alias { expr, .. }
            | Node::Cast { expr, .. }
            | Node::Table { expr }
            | Node::IdentifierClause { expr }
            | Node::IsFalse(expr)
            | Node::IsNotFalse(expr)
            | Node::IsTrue(expr)
            | Node::IsNotTrue(expr)
            | Node::IsNull(expr)
            | Node::IsNotNull(expr)
            | Node::IsUnknown(expr)
            | Node::IsNotUnknown(expr) => expr.visit_subqueries(visitor),
            Node::NamedArgument { value, .. } => value.visit_subqueries(visitor),
            Node::SortOrder(order) => order.child.visit_subqueries(visitor),
            Node::LambdaFunction { function, .. } => function.visit_subqueries(visitor),
            Node::Window {
                window_function,
                window,
            } => {
                window_function.visit_subqueries(visitor)?;
                window.visit_subqueries(visitor)
            }
            Node::UnresolvedExtractValue { child, extraction } => {
                child.visit_subqueries(visitor)?;
                extraction.visit_subqueries(visitor)
            }
            Node::UpdateFields {
                struct_expression,
                value_expression,
                ..
            } => {
                struct_expression.visit_subqueries(visitor)?;
                match value_expression {
                    Some(value) => value.visit_subqueries(visitor),
                    None => Ok(()),
                }
            }
            Node::CallFunction { arguments, .. }
            | Node::Rollup(arguments)
            | Node::Cube(arguments) => visit_exprs(arguments, visitor),
            Node::GroupingSets(sets) => sets.iter().try_for_each(|set| visit_exprs(set, visitor)),
            Node::InSubquery { expr, subquery, .. } => {
                expr.visit_subqueries(visitor)?;
                visitor(subquery)
            }
            Node::ScalarSubquery { subquery } | Node::Exists { subquery, .. } => visitor(subquery),
            Node::Subquery {
                in_subquery_values, ..
            } => visit_exprs(in_subquery_values, visitor),
            Node::InList { expr, list, .. } => {
                expr.visit_subqueries(visitor)?;
                visit_exprs(list, visitor)
            }
            Node::Between {
                expr, low, high, ..
            } => {
                expr.visit_subqueries(visitor)?;
                low.visit_subqueries(visitor)?;
                high.visit_subqueries(visitor)
            }
            Node::IsDistinctFrom { left, right } | Node::IsNotDistinctFrom { left, right } => {
                left.visit_subqueries(visitor)?;
                right.visit_subqueries(visitor)
            }
            Node::SimilarTo { expr, pattern, .. } => {
                expr.visit_subqueries(visitor)?;
                pattern.visit_subqueries(visitor)
            }
        }
    }
}

impl Window {
    /// Visit queries in the window's expressions, including frame bounds.
    /// Uses the same callback/error contract as [`Expr::visit_subqueries`].
    pub fn visit_subqueries<'a, E>(
        &'a self,
        visitor: &mut impl FnMut(&'a QueryPlan) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            Window::Named(_) => Ok(()),
            Window::Unnamed {
                cluster_by,
                partition_by,
                order_by,
                frame,
            } => {
                visit_exprs(cluster_by, visitor)?;
                visit_exprs(partition_by, visitor)?;
                visit_sort_orders(order_by, visitor)?;
                if let Some(frame) = frame {
                    for boundary in [&frame.lower, &frame.upper] {
                        match boundary {
                            WindowFrameBoundary::Preceding(expr)
                            | WindowFrameBoundary::Following(expr)
                            | WindowFrameBoundary::Value(expr) => {
                                expr.visit_subqueries(visitor)?;
                            }
                            _ => {}
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

fn visit_exprs<'a, E>(
    exprs: &'a [Expr],
    visitor: &mut impl FnMut(&'a QueryPlan) -> Result<(), E>,
) -> Result<(), E> {
    exprs
        .iter()
        .try_for_each(|expr| expr.visit_subqueries(visitor))
}

fn visit_sort_orders<'a, E>(
    orders: &'a [SortOrder],
    visitor: &mut impl FnMut(&'a QueryPlan) -> Result<(), E>,
) -> Result<(), E> {
    orders
        .iter()
        .try_for_each(|order| order.child.visit_subqueries(visitor))
}

#[cfg(test)]
#[path = "subquery_tests.rs"]
mod tests;
