//! The stateful operators of a flow's query that keep state between batches,
//! as Spark's state store does: a deduplication of a stream keeps the keys it
//! has seen, and an inner join of two streams keeps the rows of both sides.
//! Each runs over the batch's own rows and its state, so a batch never reads
//! the source's history. The innermost operator runs first, and its result
//! stands in for it in the plan, so an operator above it reads that result.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchemaRef, NullEquality, ScalarValue};
use datafusion::logical_expr::{
    Distinct, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, cast,
};
use datafusion::prelude::{SessionContext, col, lit};

use super::state::{Snapshot, StateKeys, collect};
use crate::error::{SparkError, SparkResult};
use crate::pipelines::append::{SOURCE_ALIAS, reads_a_stream};

const SEEN_ALIAS: &str = "__autocdc_seen";
const ROWS_ALIAS: &str = "__autocdc_rows";

/// A `dropDuplicatesWithinWatermark` in the query: its keys, the event time
/// and delay of its watermark, and this batch's watermark candidate, the
/// latest event time the batch brings to the watermark less the delay.
#[derive(Debug, Clone)]
pub(super) struct Within {
    pub keys: Vec<String>,
    pub event_time: String,
    pub delay_micros: i64,
    pub candidate: Option<i64>,
}

/// The watermarks of a query that joins streams: the event-time columns
/// they are on, and the query's watermark this batch computed, the least
/// over them with a stream that has seen nothing counting as 0.
#[derive(Debug, Clone)]
pub(super) struct JoinWatermarks {
    pub event_times: Vec<String>,
    pub current: i64,
}

/// The plan with each stateful operator over streams run against its state.
pub(super) async fn with_state(
    ctx: &SessionContext,
    mut plan: LogicalPlan,
    state: &StateKeys,
    origin: bool,
    within: Option<&Within>,
    joins: Option<&JoinWatermarks>,
) -> SparkResult<LogicalPlan> {
    // A resolved plan names its columns `#n` and restores the user's names
    // in an enclosing projection; the aliases there map them back.
    let names = user_names(&plan)?;
    let origins = origin_names(&plan)?;
    let mut index = 0;
    while let Some(node) = innermost(&plan)? {
        let replacement = match &node {
            LogicalPlan::Distinct(_) => {
                deduplicate(ctx, &node, state, origin, index, within, &names).await?
            }
            LogicalPlan::Join(_) => join(ctx, &node, state, origin, index, joins, &origins).await?,
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
    within: Option<&Within>,
    names: &HashMap<String, String>,
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
    let user = |position: usize| -> &str {
        let name = schema.field(position).name();
        names.get(name).map_or(name.as_str(), String::as_str)
    };
    if let Some(within) = within
        && within.keys.len() == keys.len()
        && within
            .keys
            .iter()
            .all(|key| keys.iter().any(|k| user(*k).eq_ignore_ascii_case(key)))
    {
        let event_time = (0..schema.fields().len())
            .find(|position| user(*position).eq_ignore_ascii_case(&within.event_time))
            .ok_or_else(|| {
                SparkError::invalid(format!(
                    "the watermark column {} is not among the deduplicated columns",
                    within.event_time
                ))
            })?;
        return deduplicate_within(ctx, node, state, origin, index, &keys, event_time, within)
            .await;
    }
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

/// `dropDuplicatesWithinWatermark`, as Spark runs it: a key is kept until
/// the watermark passes the first row's event time plus the delay, and a row
/// of a kept key is dropped. The state is the kept keys with their expiry
/// and the watermark, rewritten each batch because eviction shrinks it.
async fn deduplicate_within(
    ctx: &SessionContext,
    node: &LogicalPlan,
    state: &StateKeys,
    origin: bool,
    index: usize,
    keys: &[usize],
    event_time: usize,
    within: &Within,
) -> SparkResult<LogicalPlan> {
    let schema = node.schema().clone();
    let operator = format!("{index}-dedup-within");
    let mut columns: Vec<usize> = keys.to_vec();
    let expires_at = keys.len();
    // The expiry column is typed by the state schema, not by the input.
    columns.push(usize::MAX);
    let kept = match state.load_appended(&operator)? {
        Some(snapshot) => snapshot,
        None if origin => Snapshot::empty(Arc::new(Schema::new(
            keys.iter()
                .enumerate()
                .map(|(position, key)| {
                    Field::new(
                        format!("k{position}"),
                        schema.field(*key).data_type().clone(),
                        true,
                    )
                })
                .chain([Field::new(
                    format!("k{expires_at}"),
                    datafusion::arrow::datatypes::DataType::Int64,
                    true,
                )])
                .collect::<Vec<_>>(),
        ))),
        None => return Err(missing(&operator)),
    };
    drop(columns);
    // Spark evicts with the watermark as it stood before the batch; the
    // batch's own rows advance it for the next one. It starts at 0 and never
    // moves back.
    let eviction = kept.watermark;
    let watermark = within.candidate.unwrap_or(0).max(kept.watermark);
    let live = LogicalPlanBuilder::from(kept.plan(SEEN_ALIAS)?)
        .filter(col(Column::new(Some(SEEN_ALIAS), format!("k{expires_at}"))).gt(lit(eviction)))?
        .build()?;
    let live = rows_named(ctx, live, "k").await?;
    let column = |key: usize| {
        let (qualifier, field) = schema.qualified_field(key);
        Column::new(qualifier.cloned(), field.name())
    };
    let DataType::Timestamp(_, timezone) = schema.field(event_time).data_type() else {
        return Err(SparkError::invalid(format!(
            "the watermark column {} is not a timestamp",
            within.event_time
        )));
    };
    let micros = DataType::Timestamp(TimeUnit::Microsecond, timezone.clone());
    let expires =
        cast(cast(col(column(event_time)), micros), DataType::Int64) + lit(within.delay_micros);
    let fresh = LogicalPlanBuilder::from(node.clone())
        .join_detailed(
            live.plan(SEEN_ALIAS)?,
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
        .project(
            (0..schema.fields().len())
                .map(|position| col(column(position)))
                .chain([expires.alias("__autocdc_expires")]),
        )?
        .build()?;
    let fresh = rows(ctx, fresh).await?;
    let added = LogicalPlanBuilder::from(fresh.plan(ROWS_ALIAS)?)
        .project(
            keys.iter()
                .enumerate()
                .map(|(position, key)| {
                    col(Column::new(Some(ROWS_ALIAS), format!("c{key}")))
                        .alias(format!("k{position}"))
                })
                .chain([col(Column::new(
                    Some(ROWS_ALIAS),
                    format!("c{}", schema.fields().len()),
                ))
                .alias(format!("k{expires_at}"))]),
        )?
        .build()?;
    let all = LogicalPlanBuilder::from(live.plan(SEEN_ALIAS)?)
        .union(added)?
        .build()?;
    let (all_schema, all_rows) = collect(ctx, all, "k").await?;
    state.rewrite(
        &operator,
        &Snapshot {
            schema: all_schema,
            batches: all_rows,
            watermark,
        },
    )?;
    as_schema(&fresh, ROWS_ALIAS, &schema)
}

/// The user's name of each `#n` column, from the aliases of the plan's
/// projections.
fn user_names(plan: &LogicalPlan) -> SparkResult<HashMap<String, String>> {
    let mut names = HashMap::new();
    plan.apply(|node| {
        if let LogicalPlan::Projection(projection) = node {
            for expr in &projection.expr {
                if let Expr::Alias(alias) = expr
                    && let Expr::Column(column) = alias.expr.as_ref()
                {
                    names
                        .entry(column.name.clone())
                        .or_insert_with(|| alias.name.clone());
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(names)
}

/// What each aliased column was aliased from, so a column's lineage can be
/// followed down to the leaf it came from.
fn origin_names(plan: &LogicalPlan) -> SparkResult<HashMap<String, String>> {
    let mut origins = HashMap::new();
    plan.apply(|node| {
        if let LogicalPlan::Projection(projection) = node {
            for expr in &projection.expr {
                // A renamed column may carry a chain of aliases, as
                // `col("ts").alias("rts")` does once resolved (`#7 AS rts
                // AS #10`); every name in the chain originates from the
                // column.
                let mut names = Vec::new();
                let mut inner = expr;
                while let Expr::Alias(alias) = inner {
                    names.push(alias.name.clone());
                    inner = alias.expr.as_ref();
                }
                if let Expr::Column(column) = inner {
                    for name in names {
                        if name != column.name {
                            origins.entry(name).or_insert_with(|| column.name.clone());
                        }
                    }
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(origins)
}

/// `rows`, with the columns named by `prefix` and position.
async fn rows_named(
    ctx: &SessionContext,
    plan: LogicalPlan,
    prefix: &str,
) -> SparkResult<Snapshot> {
    let (schema, batches) = collect(ctx, plan, prefix).await?;
    Ok(Snapshot {
        schema,
        batches,
        watermark: 0,
    })
}

/// An inner join of two streams, as Spark runs it: both sides keep their
/// rows, and a batch adds the pairs of its new rows with every row of the
/// other side, so a pair is emitted once, when its later row arrives. With
/// watermarks, each side has a state watermark, from an event-time join key
/// or from a time-range condition, as `StreamingJoinHelper` derives it: a
/// new row behind it is late and dropped, and state behind it is evicted.
/// Without one a side keeps every row, as in Spark.
async fn join(
    ctx: &SessionContext,
    node: &LogicalPlan,
    state: &StateKeys,
    origin: bool,
    index: usize,
    joins: Option<&JoinWatermarks>,
    origins: &HashMap<String, String>,
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
    // Spark evicts and drops late rows with the watermark as it stood before
    // the batch; the batch's own rows advance it for the next one.
    let eviction = left_old.watermark.max(right_old.watermark);
    let current = joins.map_or(0, |joins| joins.current).max(eviction);
    // The watermark travels with its column through renames, as Spark's
    // attribute metadata does: a column is the event time if the alias chain
    // beneath it ends at the watermark's column.
    let event_time = |schema: &DFSchemaRef| -> Option<usize> {
        let joins = joins?;
        (0..schema.fields().len()).find(|position| {
            // Internal names are `#n`; the chain ends at the first user name,
            // and a bound guards a projection that renames both ways.
            let mut name = schema.field(*position).name().as_str();
            let mut steps = 0;
            while name.starts_with('#')
                && let Some(source) = origins.get(name)
                && steps < 64
            {
                name = source;
                steps += 1;
            }
            joins
                .event_times
                .iter()
                .any(|event| event.eq_ignore_ascii_case(name))
        })
    };
    let (left_time, right_time) = (event_time(left_schema), event_time(right_schema));
    let watermarks = state_watermarks(
        join,
        left_schema,
        right_schema,
        left_time,
        right_time,
        eviction,
    )?;
    let mut live = vec![];
    for ((schema, old, new), (time, watermark)) in [
        (left_schema, left_old, left_new),
        (right_schema, right_old, right_new),
    ]
    .into_iter()
    .zip([(left_time, watermarks.0), (right_time, watermarks.1)])
    {
        let (old, new) = match (time, watermark) {
            (Some(time), Some(watermark)) => {
                let not_late = |snapshot: &Snapshot, alias: &str| -> SparkResult<LogicalPlan> {
                    let field = schema.field(time);
                    Ok(LogicalPlanBuilder::from(snapshot.plan(alias)?)
                        .filter(
                            micros_of(field, Column::new(Some(alias), format!("c{time}")))?
                                .gt(lit(watermark)),
                        )?
                        .build()?)
                };
                (
                    rows(ctx, not_late(old, SEEN_ALIAS)?).await?,
                    rows(ctx, not_late(new, ROWS_ALIAS)?).await?,
                )
            }
            _ => (old.clone_rows()?, new.clone_rows()?),
        };
        live.push((old, new, watermark.is_some()));
    }
    let [
        (left_old, left_new, left_evicts),
        (right_old, right_new, right_evicts),
    ] = &live[..]
    else {
        return Err(SparkError::internal("a join has two sides"));
    };
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
        watermark: current,
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
    // A side with a state watermark is rewritten, because eviction shrank
    // it; a side without one only grows.
    for (name, old, new, evicts) in [
        (left_name, left_old, left_new, *left_evicts),
        (right_name, right_old, right_new, *right_evicts),
    ] {
        let stamped = |snapshot: &Snapshot| Snapshot {
            schema: snapshot.schema.clone(),
            batches: snapshot.batches.clone(),
            watermark: current,
        };
        if evicts {
            let mut all = stamped(old);
            all.batches.extend(
                new.batches
                    .iter()
                    .map(|batch| {
                        Ok(RecordBatch::try_new(
                            all.schema.clone(),
                            batch.columns().to_vec(),
                        )?)
                    })
                    .collect::<SparkResult<Vec<_>>>()?,
            );
            state.rewrite(name, &all)?;
        } else {
            state.append(name, &stamped(new))?;
        }
    }
    as_schema(&pairs, ROWS_ALIAS, node.schema())
}

/// Each side's state watermark, as Spark derives it: the watermark itself
/// when the event-time columns are a join key; otherwise the least of what
/// each range conjunct of the condition allows, with the other side's event
/// time at the watermark. `None` keeps every row.
fn state_watermarks(
    join: &datafusion::logical_expr::Join,
    left_schema: &DFSchemaRef,
    right_schema: &DFSchemaRef,
    left_time: Option<usize>,
    right_time: Option<usize>,
    watermark: i64,
) -> SparkResult<(Option<i64>, Option<i64>)> {
    let (Some(left_time), Some(right_time)) = (left_time, right_time) else {
        return Ok((None, None));
    };
    let left_column = left_schema.field(left_time).name();
    let right_column = right_schema.field(right_time).name();
    let on_key = join.on.iter().any(|(left, right)| {
        matches!((left, right), (Expr::Column(l), Expr::Column(r))
            if l.name == *left_column && r.name == *right_column)
    });
    if on_key {
        return Ok((Some(watermark), Some(watermark)));
    }
    let Some(filter) = &join.filter else {
        return Ok((None, None));
    };
    let mut conjuncts = vec![];
    split_conjunction(filter, &mut conjuncts);
    let mut bounds = (None, None);
    for conjunct in conjuncts {
        let Expr::BinaryExpr(binary) = conjunct else {
            continue;
        };
        // Normalized to `lesser < greater`, with one microsecond off for `<=`.
        let (lesser, greater, slack) = match binary.op {
            Operator::Lt => (&binary.left, &binary.right, 0),
            Operator::LtEq => (&binary.left, &binary.right, -1_000),
            Operator::Gt => (&binary.right, &binary.left, 0),
            Operator::GtEq => (&binary.right, &binary.left, -1_000),
            _ => continue,
        };
        let mut terms = vec![];
        if !linear_terms(lesser, 1, &mut terms) || !linear_terms(greater, -1, &mut terms) {
            continue;
        }
        for (side, own, other) in [
            (0, left_column, right_column),
            (1, right_column, left_column),
        ] {
            let own_terms: Vec<_> = terms
                .iter()
                .filter(|term| matches!(term, Term::Column(sign, name) if name == own && *sign < 0))
                .collect();
            if own_terms.len() != 1
                || terms.iter().any(
                    |term| matches!(term, Term::Column(sign, name) if name == own && *sign > 0),
                )
            {
                continue;
            }
            let mut value: Option<i64> = Some(slack);
            for term in &terms {
                value = match term {
                    Term::Column(_, name) if name == own => value,
                    Term::Column(sign, name) if name == other => {
                        value.map(|value| value + i64::from(*sign) * watermark)
                    }
                    Term::Column(..) => None,
                    Term::Constant(micros) => value.map(|value| value + micros),
                };
            }
            let Some(value) = value else {
                continue;
            };
            let bound = if side == 0 {
                &mut bounds.0
            } else {
                &mut bounds.1
            };
            *bound = Some(bound.map_or(value, |bound: i64| bound.min(value)));
        }
    }
    Ok(bounds)
}

enum Term {
    Column(i8, String),
    Constant(i64),
}

/// `expr` as a sum of signed columns and constants in microseconds, as far
/// as Spark's solver reads one: columns, interval literals, and sums and
/// differences of them.
fn linear_terms(expr: &Expr, sign: i8, terms: &mut Vec<Term>) -> bool {
    match expr {
        Expr::Column(column) => {
            terms.push(Term::Column(sign, column.name.clone()));
            true
        }
        Expr::Alias(alias) => linear_terms(&alias.expr, sign, terms),
        Expr::Cast(cast) => linear_terms(&cast.expr, sign, terms),
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::Plus => {
                linear_terms(&binary.left, sign, terms) && linear_terms(&binary.right, sign, terms)
            }
            Operator::Minus => {
                linear_terms(&binary.left, sign, terms) && linear_terms(&binary.right, -sign, terms)
            }
            _ => false,
        },
        Expr::Literal(value, _) => match interval_micros(value) {
            Some(micros) => {
                terms.push(Term::Constant(i64::from(sign) * micros));
                true
            }
            None => false,
        },
        _ => false,
    }
}

fn interval_micros(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::IntervalMonthDayNano(Some(interval)) if interval.months == 0 => {
            Some(i64::from(interval.days) * 86_400_000_000 + interval.nanoseconds / 1_000)
        }
        ScalarValue::IntervalDayTime(Some(interval)) => Some(
            i64::from(interval.days) * 86_400_000_000 + i64::from(interval.milliseconds) * 1_000,
        ),
        ScalarValue::DurationMicrosecond(Some(micros)) => Some(*micros),
        ScalarValue::DurationMillisecond(Some(millis)) => Some(*millis * 1_000),
        _ => None,
    }
}

fn split_conjunction<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            split_conjunction(&binary.left, out);
            split_conjunction(&binary.right, out);
        }
        other => out.push(other),
    }
}

/// A snapshot column as whole microseconds.
fn micros_of(field: &Field, column: Column) -> SparkResult<Expr> {
    let DataType::Timestamp(_, timezone) = field.data_type() else {
        return Err(SparkError::invalid(format!(
            "the watermark column {} is not a timestamp",
            field.name()
        )));
    };
    let micros = DataType::Timestamp(TimeUnit::Microsecond, timezone.clone());
    Ok(cast(cast(col(column), micros), DataType::Int64))
}
