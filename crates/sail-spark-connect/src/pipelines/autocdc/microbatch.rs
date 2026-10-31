//! Per-microbatch input validation, `autocdc/ScdBatchValidator.scala`
//! (Spark 4.2.1, 617618396): the sequencing type must be orderable, no row
//! may have a null sequencing value, and no row may have a null key column.
//! `Scd1ForeachBatchHandler.execute` runs it on the original rows before any
//! reconciliation, deduplication or merge, and `Scd1BatchProcessor` keeps it
//! outside its transforms, so an invalid row can never disappear because
//! another row with the same key wins. This is the pure component; the
//! wiring unit supplies the resolved inputs described on [`Microbatch`].

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::functions_aggregate::count::count;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionContext, lit, when};
use sail_common_datafusion::session::plan::PlanFormatter;
use sail_plan::formatter::SparkPlanFormatter;

use super::{UnqualifiedColumnName, errors};
use crate::error::{SparkError, SparkResult};
use crate::pipelines::graph::quoted;

/// The inputs `ScdBatchValidator` takes, already resolved by the caller:
/// the batch, the sequencing expression resolved against the batch schema,
/// the key columns as batch field indices (resolved by the `StructType`
/// lookup under the session's case setting) with the user's spelling for
/// messages, the target identifier parts and the batch id.
pub(super) struct Microbatch<'a> {
    pub batch: LogicalPlan,
    pub sequencing: Expr,
    pub keys: &'a [(UnqualifiedColumnName, usize)],
    pub target: &'a [String],
    pub batch_id: i64,
}

/// `OrderUtils.isOrderable` over Arrow types: everything atomic, structs and
/// arrays of orderable types, and nothing map-like or opaque.
pub(super) fn is_orderable(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null => true,
        DataType::Map(_, _) | DataType::Union(_, _) | DataType::RunEndEncoded(_, _) => false,
        // Spark's CalendarIntervalType is not an AtomicType; its year-month
        // and day-time intervals are.
        DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano) => false,
        DataType::Struct(fields) => fields.iter().all(|field| is_orderable(field.data_type())),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => is_orderable(field.data_type()),
        DataType::Dictionary(_, value) => is_orderable(value),
        _ => true,
    }
}

/// `ScdBatchValidator.validateMicrobatch`: the orderability check, then one
/// aggregation over the original rows counting null sequencing values and
/// null values per key, reported in Spark's precedence.
pub(super) async fn validate(ctx: &SessionContext, input: Microbatch<'_>) -> SparkResult<()> {
    let table_name = quoted(input.target);
    let batch_id = input.batch_id.to_string();
    let schema = input.batch.schema();
    let sequence_type = input.sequencing.get_type(schema)?;
    if !is_orderable(&sequence_type) {
        let data_type = SparkPlanFormatter.data_type_to_simple_string(&sequence_type)?;
        return Err(errors::non_orderable_sequence(
            &table_name,
            &batch_id,
            &data_type,
        ));
    }
    let null_count = |expression: Expr| -> SparkResult<Expr> {
        Ok(count(when(expression.is_null(), lit(1)).end()?))
    };
    let mut aggregates = vec![null_count(input.sequencing)?.alias("__autocdc_seq_null_count")];
    for (index, (_, field)) in input.keys.iter().enumerate() {
        let (qualifier, field) = schema.qualified_field(*field);
        let column = Expr::Column(datafusion::common::Column::from((qualifier, field)));
        aggregates.push(null_count(column)?.alias(format!("__autocdc_key_null_count_{index}")));
    }
    let plan = LogicalPlanBuilder::from(input.batch)
        .aggregate(Vec::<Expr>::new(), aggregates)?
        .build()?;
    let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
    let counts = |column: usize| -> SparkResult<i64> {
        batches
            .iter()
            .find(|batch| batch.num_rows() > 0)
            .and_then(|batch| {
                batch
                    .column(column)
                    .as_primitive_opt::<Int64Type>()
                    .map(|array| array.value(0))
            })
            .ok_or_else(|| SparkError::internal("microbatch null counts produced no row"))
    };
    let null_sequences = counts(0)?;
    if null_sequences > 0 {
        return Err(errors::null_sequence(
            &table_name,
            &batch_id,
            &null_sequences.to_string(),
        ));
    }
    let mut null_keys = Vec::new();
    for (index, (key, _)) in input.keys.iter().enumerate() {
        let rows = counts(index + 1)?;
        if rows > 0 {
            null_keys.push(format!("{}={rows}", key.quoted()));
        }
    }
    if !null_keys.is_empty() {
        return Err(errors::null_key(
            &table_name,
            &batch_id,
            &null_keys.join(", "),
        ));
    }
    Ok(())
}
