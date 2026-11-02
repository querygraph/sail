//! Carry a stream's own columns through the row-wise transformation beneath a
//! watermark, so each row read can be judged by the event time it has at the
//! watermark. A watermarked aggregate keeps a row only while a window it may
//! belong to is open, and for a watermark after a transformation that event
//! time is not a column of the read.

use std::sync::Arc;

use datafusion::common::Column;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Filter, LogicalPlan, Projection, SubqueryAlias};
use datafusion::prelude::col;

use crate::error::SparkResult;

/// How often `leaf` appears in `plan`.
pub(super) fn occurrences(plan: &LogicalPlan, leaf: &LogicalPlan) -> SparkResult<usize> {
    let mut count = 0;
    plan.apply(|node| {
        if node == leaf {
            count += 1;
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(count)
}

/// `plan`, a chain of projections, filters and aliases over `leaf`, with
/// `leaf`'s columns carried to its output under the returned names. `None`
/// when anything else stands between them.
pub(super) fn carried(
    plan: &LogicalPlan,
    leaf: &LogicalPlan,
) -> SparkResult<Option<(LogicalPlan, Vec<String>)>> {
    let names: Vec<String> = (0..leaf.schema().fields().len())
        .map(|index| format!("__autocdc_leaf_{index}"))
        .collect();
    Ok(carry(plan, leaf, &names)?.map(|(plan, _)| (plan, names)))
}

fn carry(
    plan: &LogicalPlan,
    leaf: &LogicalPlan,
    names: &[String],
) -> SparkResult<Option<(LogicalPlan, Vec<Column>)>> {
    if plan == leaf {
        let schema = leaf.schema();
        let columns = (0..schema.fields().len())
            .map(|index| {
                let (qualifier, field) = schema.qualified_field(index);
                Column::new(qualifier.cloned(), field.name())
            })
            .collect();
        return Ok(Some((plan.clone(), columns)));
    }
    Ok(match plan {
        LogicalPlan::Projection(projection) => {
            let Some((input, columns)) = carry(&projection.input, leaf, names)? else {
                return Ok(None);
            };
            let mut expr = projection.expr.clone();
            expr.extend(
                columns
                    .into_iter()
                    .zip(names)
                    .map(|(column, name)| col(column).alias(name)),
            );
            let plan = LogicalPlan::Projection(Projection::try_new(expr, Arc::new(input))?);
            let columns = names.iter().map(Column::from_name).collect();
            Some((plan, columns))
        }
        LogicalPlan::Filter(filter) => {
            let Some((input, columns)) = carry(&filter.input, leaf, names)? else {
                return Ok(None);
            };
            let plan =
                LogicalPlan::Filter(Filter::try_new(filter.predicate.clone(), Arc::new(input))?);
            Some((plan, columns))
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let Some((input, columns)) = carry(&alias.input, leaf, names)? else {
                return Ok(None);
            };
            let positions = columns
                .iter()
                .map(|column| input.schema().index_of_column(column))
                .collect::<Result<Vec<_>, _>>()?;
            let plan = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::new(input),
                alias.alias.clone(),
            )?);
            let schema = plan.schema().clone();
            let columns = positions
                .into_iter()
                .map(|position| {
                    let (qualifier, field) = schema.qualified_field(position);
                    Column::new(qualifier.cloned(), field.name())
                })
                .collect();
            Some((plan, columns))
        }
        _ => None,
    })
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::prelude::{SessionContext, lit};

    use super::*;

    #[tokio::test]
    async fn a_filtered_renamed_stream_carries_its_columns_to_the_watermark() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("ts", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let leaf = LogicalPlanBuilder::scan("t", provider_as_source(Arc::new(table)), None)
            .unwrap()
            .alias("__source")
            .unwrap()
            .build()
            .unwrap();
        // filter(id != 2).select(ts as event).alias(v), as a watermark's input.
        let marked = LogicalPlanBuilder::from(leaf.clone())
            .filter(col("id").not_eq(lit(2i64)))
            .unwrap()
            .project(vec![col("ts").alias("event")])
            .unwrap()
            .alias("v")
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(occurrences(&marked, &leaf).unwrap(), 1);
        let (carried, names) = carried(&marked, &leaf).unwrap().unwrap();
        let kept = LogicalPlanBuilder::from(carried)
            .filter(col("event").gt(lit(15i64)))
            .unwrap()
            .project(names.iter().map(|name| col(Column::from_name(name))))
            .unwrap()
            .build()
            .unwrap();
        let ctx = SessionContext::new();
        let rows = ctx
            .execute_logical_plan(kept)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let ids: Vec<i64> = rows
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        // Row 2 never reaches the watermark and row 1 is before the floor.
        assert_eq!(ids, [3]);
    }
}
