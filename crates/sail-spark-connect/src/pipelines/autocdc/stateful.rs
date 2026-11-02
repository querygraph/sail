//! The stateful operators of a flow's query that keep state between batches,
//! as Spark's state store does: a deduplication of a stream keeps the keys it
//! has seen, and an inner join of two streams keeps the rows of both sides.
//! Each runs over the batch's own rows and its state, so a batch never reads
//! the source's history. The innermost operator runs first, and its result
//! stands in for it in the plan, so an operator above it reads that result.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchemaRef, NullEquality};
use datafusion::logical_expr::{Distinct, Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionContext, col};

use super::state::{Snapshot, StateKeys, collect};
use crate::error::{SparkError, SparkResult};
use crate::pipelines::append::{SOURCE_ALIAS, reads_a_stream};

const SEEN_ALIAS: &str = "__autocdc_seen";
const ROWS_ALIAS: &str = "__autocdc_rows";

/// The plan with each stateful operator over streams run against its state.
pub(super) async fn with_state(
    ctx: &SessionContext,
    mut plan: LogicalPlan,
    state: &StateKeys,
    origin: bool,
) -> SparkResult<LogicalPlan> {
    let mut index = 0;
    while let Some(node) = innermost(&plan)? {
        let replacement = match &node {
            LogicalPlan::Distinct(_) => deduplicate(ctx, &node, state, origin, index).await?,
            LogicalPlan::Join(_) => join(ctx, &node, state, origin, index).await?,
            _ => return Err(SparkError::internal("not a stateful operator")),
        };
        plan = plan
            .transform_up(|candidate| {
                Ok(if candidate == node {
                    Transformed::yes(replacement.clone())
                } else {
                    Transformed::no(candidate)
                })
            })?
            .data;
        index += 1;
    }
    Ok(plan)
}

fn stateful(node: &LogicalPlan) -> datafusion::common::Result<bool> {
    Ok(match node {
        LogicalPlan::Distinct(_) => reads_a_stream(node)?,
        LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
            reads_a_stream(&join.left)? && reads_a_stream(&join.right)?
        }
        _ => false,
    })
}

/// A stateful operator with none beneath it.
fn innermost(plan: &LogicalPlan) -> SparkResult<Option<LogicalPlan>> {
    let mut found = None;
    plan.apply(|node| {
        if stateful(node)?
            && !node
                .inputs()
                .iter()
                .any(|input| input.exists(stateful).unwrap_or(true))
        {
            found = Some(node.clone());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

fn missing(operator: &str) -> SparkError {
    SparkError::invalid(format!(
        "the state of {operator} is missing; reset the flow to rebuild it"
    ))
}

/// The operator's state when the batch started, empty for the first batch.
fn load(
    state: &StateKeys,
    operator: &str,
    origin: bool,
    schema: &DFSchemaRef,
    columns: &[usize],
    prefix: &str,
) -> SparkResult<Snapshot> {
    let expected: Vec<&datafusion::arrow::datatypes::DataType> = columns
        .iter()
        .map(|column| schema.field(*column).data_type())
        .collect();
    match state.load_appended(operator)? {
        // Read back only under the types it was written with; otherwise the
        // query changed, and Spark refuses a changed state schema too.
        Some(snapshot)
            if snapshot.schema.fields().len() == expected.len()
                && snapshot
                    .schema
                    .fields()
                    .iter()
                    .zip(&expected)
                    .all(|(stored, expected)| stored.data_type() == *expected) =>
        {
            Ok(snapshot)
        }
        Some(_) => Err(SparkError::invalid(format!(
            "the state of {operator} was written by another query; reset the flow to rebuild it"
        ))),
        None if origin => Ok(Snapshot::empty(Arc::new(Schema::new(
            columns
                .iter()
                .enumerate()
                .map(|(position, column)| {
                    Field::new(
                        format!("{prefix}{position}"),
                        schema.field(*column).data_type().clone(),
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        )))),
        None => Err(missing(operator)),
    }
}

/// Rows named `c0`, `c1`… under `alias`, with the columns of `schema`. The
/// rows stand in for an operator that read a stream, so they are marked as a
/// stream read themselves: an operator above them is then still stateful,
/// and an aggregate above them still needs a watermark.
fn as_schema(rows: &Snapshot, alias: &str, schema: &DFSchemaRef) -> SparkResult<LogicalPlan> {
    let marked = LogicalPlanBuilder::from(rows.plan(alias)?)
        .alias(SOURCE_ALIAS)?
        .build()?;
    Ok(LogicalPlanBuilder::from(marked)
        .project((0..schema.fields().len()).map(|position| {
            let (qualifier, field) = schema.qualified_field(position);
            col(Column::new(Some(SOURCE_ALIAS), format!("c{position}")))
                .alias_qualified(qualifier.cloned(), field.name())
        }))?
        .build()?)
}

async fn rows(ctx: &SessionContext, plan: LogicalPlan) -> SparkResult<Snapshot> {
    let (schema, batches) = collect(ctx, plan, "c").await?;
    Ok(Snapshot {
        schema,
        batches,
        watermark: 0,
    })
}

/// A batch keeps one row of each key it has not seen before, as Spark's
/// `dropDuplicates` does, and adds those keys to the state.
async fn deduplicate(
    ctx: &SessionContext,
    node: &LogicalPlan,
    state: &StateKeys,
    origin: bool,
    index: usize,
) -> SparkResult<LogicalPlan> {
    let schema = node.schema().clone();
    let keys: Vec<usize> = match node {
        LogicalPlan::Distinct(Distinct::All(_)) => (0..schema.fields().len()).collect(),
        LogicalPlan::Distinct(Distinct::On(on)) => on
            .on_expr
            .iter()
            .map(|expr| match expr {
                Expr::Column(column) => Ok(schema.index_of_column(column)?),
                _ => Err(SparkError::unsupported(
                    "dropDuplicates on an expression of a stream",
                )),
            })
            .collect::<SparkResult<_>>()?,
        _ => return Err(SparkError::internal("not a deduplication")),
    };
    let operator = format!("{index}-dedup");
    let seen = load(state, &operator, origin, &schema, &keys, "k")?;
    let column = |key: usize| {
        let (qualifier, field) = schema.qualified_field(key);
        Column::new(qualifier.cloned(), field.name())
    };
    let fresh = LogicalPlanBuilder::from(node.clone())
        .join_detailed(
            seen.plan(SEEN_ALIAS)?,
            JoinType::LeftAnti,
            (
                keys.iter().map(|key| column(*key)).collect::<Vec<_>>(),
                (0..keys.len())
                    .map(|position| Column::new(Some(SEEN_ALIAS), format!("k{position}")))
                    .collect::<Vec<_>>(),
            ),
            None,
            NullEquality::NullEqualsNull,
        )?
        .build()?;
    let fresh = rows(ctx, fresh).await?;
    let added = LogicalPlanBuilder::from(fresh.plan(ROWS_ALIAS)?)
        .project(keys.iter().enumerate().map(|(position, key)| {
            col(Column::new(Some(ROWS_ALIAS), format!("c{key}"))).alias(format!("k{position}"))
        }))?
        .build()?;
    let (added_schema, added_rows) = collect(ctx, added, "k").await?;
    state.append(
        &operator,
        &Snapshot {
            schema: added_schema,
            batches: added_rows,
            watermark: 0,
        },
    )?;
    as_schema(&fresh, ROWS_ALIAS, &schema)
}

/// An inner join of two streams, as Spark runs one without a watermark: both
/// sides keep every row, and a batch adds the pairs of its new rows with
/// every row of the other side, so a pair is emitted once, when its later
/// row arrives.
async fn join(
    ctx: &SessionContext,
    node: &LogicalPlan,
    state: &StateKeys,
    origin: bool,
    index: usize,
) -> SparkResult<LogicalPlan> {
    let LogicalPlan::Join(join) = node else {
        return Err(SparkError::internal("not a join"));
    };
    let mut sides = vec![];
    for (side, input) in [("left", &join.left), ("right", &join.right)] {
        let schema = input.schema().clone();
        let operator = format!("{index}-join-{side}");
        let all: Vec<usize> = (0..schema.fields().len()).collect();
        let old = load(state, &operator, origin, &schema, &all, "c")?;
        let new = rows(ctx, input.as_ref().clone()).await?;
        sides.push((operator, schema, old, new));
    }
    let [
        (left_name, left_schema, left_old, left_new),
        (right_name, right_schema, right_old, right_new),
    ] = &sides[..]
    else {
        return Err(SparkError::internal("a join has two sides"));
    };
    // Both sides' rows so far, without rewriting what is already stored.
    let right_all = Snapshot {
        schema: right_old.schema.clone(),
        batches: right_old
            .batches
            .iter()
            .chain(&right_new.batches)
            .map(|batch| {
                Ok(RecordBatch::try_new(
                    right_old.schema.clone(),
                    batch.columns().to_vec(),
                )?)
            })
            .collect::<SparkResult<Vec<_>>>()?,
        watermark: 0,
    };
    let pair = |left: &Snapshot, right: &Snapshot| -> SparkResult<LogicalPlan> {
        Ok(node.with_new_exprs(
            node.expressions(),
            vec![
                as_schema(left, ROWS_ALIAS, left_schema)?,
                as_schema(right, ROWS_ALIAS, right_schema)?,
            ],
        )?)
    };
    let pairs = LogicalPlanBuilder::from(pair(left_new, &right_all)?)
        .union(pair(left_old, right_new)?)?
        .build()?;
    let pairs = rows(ctx, pairs).await?;
    state.append(left_name, left_new)?;
    state.append(right_name, right_new)?;
    as_schema(&pairs, ROWS_ALIAS, node.schema())
}
