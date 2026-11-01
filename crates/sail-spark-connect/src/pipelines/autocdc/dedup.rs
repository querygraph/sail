//! `Scd1BatchProcessor.deduplicateMicrobatch` (Spark 4.2.1, 617618396): keep
//! the most recent event per key as ordered by the sequencing expression.
//! Spark packs every column of the row into a struct, takes
//! `max_by(struct, sequencing)` per key group and unpacks the struct, so the
//! result keeps the input's column names, types and order; rows with equal
//! sequence values for one key leave the survivor undefined.
//!
//! This is the pure component that follows [`super::microbatch::validate`];
//! it assumes a validated batch (no null sequence values) but, as in Spark,
//! does not depend on it: a null sequence loses to any non-null one and a key
//! whose sequences are all null yields a row of nulls.
//!
//! Reuse: Sail's `max_by` is [`MaxByFunction`], which the optimizer rewrites
//! to `last_value(struct) ORDER BY sequencing ASC NULLS FIRST FILTER
//! sequencing IS NOT NULL`, so the survivor among ties is the last row in
//! that order, within Spark's "undefined" contract, and null sequences are
//! filtered before the pick. The struct is Sail's `StructFunction`, which
//! names fields exactly, so literal-dot column names survive the round trip.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::expr::{AggregateFunction, AggregateFunctionParams, ScalarFunction};
use datafusion::logical_expr::{
    AggregateUDF, Expr, LogicalPlan, LogicalPlanBuilder, ScalarUDF, col,
};
use sail_function::aggregate::max_min_by::MaxByFunction;
use sail_function::scalar::struct_function::StructFunction;

use crate::error::{SparkResult, StructuredSparkError};

/// `Scd1BatchProcessor.winningRowColName`: the temporary column holding the
/// packed winning row. Spark reports `AMBIGUOUS_REFERENCE` when a key column
/// carries this name; DataFusion rejects the duplicate output column of the
/// aggregate instead, with its own error.
pub(super) const WINNING_ROW_COLUMN: &str = "__spark_autocdc_winning_row";

/// Groups `batch` by the key columns (batch field indices) and keeps, per
/// group, the row with the greatest `sequencing`, unpacked to the input schema.
pub(super) fn deduplicate(
    batch: LogicalPlan,
    keys: &[usize],
    sequencing: Expr,
) -> SparkResult<LogicalPlan> {
    let schema = batch.schema();
    let column = |index: usize| -> Expr {
        let (qualifier, field) = schema.qualified_field(index);
        Expr::Column(datafusion::common::Column::from((qualifier, field)))
    };
    let names: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    // A client cannot reach this: the reserved prefix is refused first.
    if names.iter().any(|name| name == WINNING_ROW_COLUMN) {
        let name = format!("`{WINNING_ROW_COLUMN}`");
        return Err(Box::new(StructuredSparkError {
            class: "org.apache.spark.sql.AnalysisException".into(),
            condition: "AMBIGUOUS_REFERENCE".into(),
            sql_state: "42704".into(),
            message: format!("Reference {name} is ambiguous, could be: [{name}, {name}]."),
            parameters: HashMap::from([
                ("name".into(), name.clone()),
                ("referenceNames".into(), format!("[{name}, {name}]")),
            ]),
        })
        .into());
    }
    let group: Vec<Expr> = keys.iter().map(|&index| column(index)).collect();
    let row = Expr::ScalarFunction(ScalarFunction {
        func: Arc::new(ScalarUDF::from(StructFunction::new(names.clone()))),
        args: (0..names.len()).map(column).collect(),
    });
    let winner = Expr::AggregateFunction(AggregateFunction {
        func: Arc::new(AggregateUDF::from(MaxByFunction::new())),
        params: AggregateFunctionParams {
            args: vec![row, sequencing],
            distinct: false,
            filter: None,
            order_by: vec![],
            null_treatment: None,
        },
    })
    .alias(WINNING_ROW_COLUMN);
    let unpacked: Vec<Expr> = names
        .iter()
        .map(|name| get_field(col(WINNING_ROW_COLUMN), name.as_str()).alias(name))
        .collect();
    Ok(LogicalPlanBuilder::from(batch)
        .aggregate(group, vec![winner])?
        .project(unpacked)?
        .build()?)
}
