//! Borrowed query-node children for request-local inspection, without resolution.
use super::{
    Expr, QueryPlan, ReadType, SortOrder, TableSample, TableSampleMethod, TableTemporal, Window,
};

/// A query node's expression/window fields and query inputs. This is structural
/// navigation: the caller decides whether/how to recurse, interpret scopes,
/// expand plan-ID references or admit helper-executing constructs.
#[derive(Debug, Clone, Copy)]
pub enum QueryChild<'a> {
    Expression(&'a Expr),
    Window(&'a Window),
    Query(&'a QueryPlan),
}

impl QueryPlan {
    /// Visit this node's children without cloning, resolving or descending.
    ///
    /// Expression/window fields precede query inputs. Lists retain their order
    /// and duplicates. CTE definitions are unfiltered and precede the body;
    /// WithRelations references precede its root. Scope, name matching, CTE
    /// shadowing and plan-ID lookup remain caller policy, not this traversal's.
    /// The first callback error is returned unchanged. This is not a semantic
    /// resolution order or a general Catalyst traversal equivalence guarantee.
    pub fn visit_children<'a, E>(
        &'a self,
        visitor: &mut impl FnMut(QueryChild<'a>) -> Result<(), E>,
    ) -> Result<(), E> {
        use super::QueryNode as N;
        match &self.node {
            N::Read { read_type, .. } => visit_read(read_type, visitor),
            // Leaves without query inputs; literal rows and an inline table
            // function still carry expressions.
            N::LocalRelation { .. }
            | N::CachedLocalRelation { .. }
            | N::CachedRemoteRelation { .. }
            | N::Range(_)
            | N::Empty { .. } => Ok(()),
            N::Values(rows) => rows.iter().try_for_each(|row| visit_exprs(row, visitor)),
            N::CommonInlineUserDefinedTableFunction(function) => {
                visit_exprs(&function.arguments, visitor)
            }
            N::Tail { input, limit } => {
                visitor(QueryChild::Expression(limit))?;
                visitor(QueryChild::Query(input))
            }
            N::StatSummary { input, .. }
            | N::StatDescribe { input, .. }
            | N::StatCrosstab { input, .. }
            | N::StatCov { input, .. }
            | N::StatCorr { input, .. }
            | N::StatApproxQuantile { input, .. }
            | N::StatFreqItems { input, .. } => visitor(QueryChild::Query(input)),
            N::StatSampleBy { input, column, .. } => {
                visitor(QueryChild::Expression(column))?;
                visitor(QueryChild::Query(input))
            }
            // One required input, no expressions.
            N::SubqueryAlias { input, .. }
            | N::Repartition { input, .. }
            | N::ToDf { input, .. }
            | N::WithColumnsRenamed { input, .. }
            | N::ToSchema { input, .. }
            | N::DropNa { input, .. }
            | N::Replace { input, .. }
            | N::TableAlias { input, .. } => visitor(QueryChild::Query(input)),
            N::TableSample { input, sample } => {
                visit_sample(sample, visitor)?;
                visitor(QueryChild::Query(input))
            }
            // One required input with expressions.
            N::Filter { input, condition } => {
                visitor(QueryChild::Expression(condition))?;
                visitor(QueryChild::Query(input))
            }
            N::Sort { input, order, .. } => {
                visit_sort_orders(order, visitor)?;
                visitor(QueryChild::Query(input))
            }
            N::Limit { input, skip, limit } => {
                for expr in [skip, limit].into_iter().flatten() {
                    visitor(QueryChild::Expression(expr))?;
                }
                visitor(QueryChild::Query(input))
            }
            N::Drop { input, columns, .. } => {
                visit_exprs(columns, visitor)?;
                visitor(QueryChild::Query(input))
            }
            N::WithColumns { input, aliases } => {
                visit_exprs(aliases, visitor)?;
                visitor(QueryChild::Query(input))
            }
            N::Hint {
                input, parameters, ..
            } => {
                visit_exprs(parameters, visitor)?;
                visitor(QueryChild::Query(input))
            }
            N::RepartitionByExpression {
                input,
                partition_expressions,
                ..
            } => {
                visit_exprs(partition_expressions, visitor)?;
                visitor(QueryChild::Query(input))
            }
            N::MapPartitions {
                input, function, ..
            } => {
                visit_exprs(&function.arguments, visitor)?;
                visitor(QueryChild::Query(input))
            }
            N::CollectMetrics { input, metrics, .. } => {
                visit_exprs(metrics, visitor)?;
                visitor(QueryChild::Query(input))
            }
            N::FillNa { input, values, .. } => {
                visit_exprs(values, visitor)?;
                visitor(QueryChild::Query(input))
            }
            N::WithParameters {
                input,
                positional_arguments,
                named_arguments,
            } => {
                visit_exprs(positional_arguments, visitor)?;
                for (_, argument) in named_arguments {
                    visitor(QueryChild::Expression(argument))?;
                }
                visitor(QueryChild::Query(input))
            }
            N::NamedWindows { input, windows } => {
                for (_, window) in windows {
                    visitor(QueryChild::Window(window))?;
                }
                visitor(QueryChild::Query(input))
            }
            // One optional input with expressions.
            N::Project { input, expressions } => {
                visit_exprs(expressions, visitor)?;
                match input {
                    Some(input) => visitor(QueryChild::Query(input)),
                    None => Ok(()),
                }
            }
            N::LateralView {
                input,
                arguments,
                named_arguments,
                ..
            } => {
                visit_exprs(arguments, visitor)?;
                for (_, argument) in named_arguments {
                    visitor(QueryChild::Expression(argument))?;
                }
                match input {
                    Some(input) => visitor(QueryChild::Query(input)),
                    None => Ok(()),
                }
            }
            // Nested structs with one input.
            N::Aggregate(aggregate) => {
                visit_exprs(&aggregate.grouping, visitor)?;
                visit_exprs(&aggregate.aggregate, visitor)?;
                if let Some(having) = &aggregate.having {
                    visitor(QueryChild::Expression(having))?;
                }
                visitor(QueryChild::Query(&aggregate.input))
            }
            N::Sample(sample) => visitor(QueryChild::Query(&sample.input)),
            N::Deduplicate(deduplicate) => visitor(QueryChild::Query(&deduplicate.input)),
            N::Pivot(pivot) => {
                if let Some(grouping) = &pivot.grouping {
                    visit_exprs(grouping, visitor)?;
                }
                visit_exprs(&pivot.aggregate, visitor)?;
                visit_exprs(&pivot.columns, visitor)?;
                for value in &pivot.values {
                    visit_exprs(&value.values, visitor)?;
                }
                visitor(QueryChild::Query(&pivot.input))
            }
            N::Unpivot(unpivot) => {
                if let Some(ids) = &unpivot.ids {
                    visit_exprs(ids, visitor)?;
                }
                for value in unpivot.values.iter().flatten() {
                    visit_exprs(&value.columns, visitor)?;
                }
                visitor(QueryChild::Query(&unpivot.input))
            }
            N::Parse(parse) => visitor(QueryChild::Query(&parse.input)),
            N::WithWatermark(watermark) => visitor(QueryChild::Query(&watermark.input)),
            N::ApplyInPandasWithState(apply) => {
                visit_exprs(&apply.grouping_expressions, visitor)?;
                visit_exprs(&apply.function.arguments, visitor)?;
                visitor(QueryChild::Query(&apply.input))
            }
            N::GroupMap(group) => {
                visit_exprs(&group.grouping_expressions, visitor)?;
                visit_exprs(&group.sorting_expressions, visitor)?;
                visit_exprs(&group.initial_grouping_expressions, visitor)?;
                visit_exprs(&group.function.arguments, visitor)?;
                visitor(QueryChild::Query(&group.input))?;
                match &group.initial_input {
                    Some(initial) => visitor(QueryChild::Query(initial)),
                    None => Ok(()),
                }
            }
            // Several inputs.
            N::Join(join) => {
                if let Some(super::JoinCriteria::On(condition)) = &join.join_criteria {
                    visitor(QueryChild::Expression(condition))?;
                }
                visitor(QueryChild::Query(&join.left))?;
                visitor(QueryChild::Query(&join.right))
            }
            N::LateralJoin {
                left,
                right,
                join_condition,
                ..
            } => {
                if let Some(condition) = join_condition {
                    visitor(QueryChild::Expression(condition))?;
                }
                visitor(QueryChild::Query(left))?;
                visitor(QueryChild::Query(right))
            }
            N::SetOperation(set) => {
                visitor(QueryChild::Query(&set.left))?;
                visitor(QueryChild::Query(&set.right))
            }
            N::CoGroupMap(cogroup) => {
                visit_exprs(&cogroup.input_grouping_expressions, visitor)?;
                visit_exprs(&cogroup.other_grouping_expressions, visitor)?;
                visit_exprs(&cogroup.input_sorting_expressions, visitor)?;
                visit_exprs(&cogroup.other_sorting_expressions, visitor)?;
                visit_exprs(&cogroup.function.arguments, visitor)?;
                visitor(QueryChild::Query(&cogroup.input))?;
                visitor(QueryChild::Query(&cogroup.other))
            }
            N::WithRelations { root, references } => {
                for reference in references {
                    visitor(QueryChild::Query(reference))?;
                }
                visitor(QueryChild::Query(root))
            }
            N::WithCtes { input, ctes, .. } => {
                for (_, definition) in ctes {
                    visitor(QueryChild::Query(definition))?;
                }
                visitor(QueryChild::Query(input))
            }
        }
    }
}

fn visit_exprs<'a, E>(
    exprs: &'a [Expr],
    visitor: &mut impl FnMut(QueryChild<'a>) -> Result<(), E>,
) -> Result<(), E> {
    exprs
        .iter()
        .try_for_each(|expr| visitor(QueryChild::Expression(expr)))
}
fn visit_sort_orders<'a, E>(
    orders: &'a [SortOrder],
    visitor: &mut impl FnMut(QueryChild<'a>) -> Result<(), E>,
) -> Result<(), E> {
    orders
        .iter()
        .try_for_each(|order| visitor(QueryChild::Expression(&order.child)))
}
fn visit_sample<'a, E>(
    sample: &'a TableSample,
    visitor: &mut impl FnMut(QueryChild<'a>) -> Result<(), E>,
) -> Result<(), E> {
    match &sample.method {
        TableSampleMethod::Percent { value } | TableSampleMethod::Rows { value } => {
            visitor(QueryChild::Expression(value))
        }
        TableSampleMethod::Bucket { .. } => Ok(()),
    }
}
fn visit_read<'a, E>(
    read: &'a ReadType,
    visitor: &mut impl FnMut(QueryChild<'a>) -> Result<(), E>,
) -> Result<(), E> {
    match read {
        ReadType::NamedTable(table) => {
            if let Some(TableTemporal::Version { value } | TableTemporal::Timestamp { value }) =
                &table.temporal
            {
                visitor(QueryChild::Expression(value))?;
            }
            if let Some(sample) = &table.sample {
                visit_sample(sample, visitor)?;
            }
            Ok(())
        }
        ReadType::DynamicTable(table) => {
            visitor(QueryChild::Expression(&table.name))?;
            if let Some(sample) = &table.sample {
                visit_sample(sample, visitor)?;
            }
            Ok(())
        }
        ReadType::DataSource(source) => visit_exprs(&source.predicates, visitor),
        ReadType::Udtf(function) => {
            visit_exprs(&function.arguments, visitor)?;
            for (_, arg) in &function.named_arguments {
                visitor(QueryChild::Expression(arg))?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "query_child_tests.rs"]
mod tests;
