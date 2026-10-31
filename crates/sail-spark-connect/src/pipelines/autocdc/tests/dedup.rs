//! `autocdc/Scd1BatchProcessorSuite.scala` `deduplicateMicrobatch` tests
//! (lines 68–479) over the component, with rows rendered through
//! `ScalarValue` for comparison and multi-row answers compared as sets.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::functions::expr_fn::greatest;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionContext, col};

use super::*;
use crate::pipelines::autocdc::dedup::{WINNING_ROW_COLUMN, deduplicate};

fn scan(columns: Vec<(&str, ArrayRef)>) -> SparkResult<LogicalPlan> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let arrays = columns.into_iter().map(|(_, array)| array).collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    Ok(LogicalPlanBuilder::scan("batch", provider_as_source(Arc::new(table)), None)?.build()?)
}

/// id INT, seq LONG, value STRING: the suite's usual schema.
fn batch(rows: &[(Option<i32>, Option<i64>, &str)]) -> SparkResult<LogicalPlan> {
    scan(vec![
        (
            "id",
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "seq",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ),
        (
            "value",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ),
    ])
}

/// Every output row as rendered scalars, sorted, with the output field names and types.
async fn answer(
    ctx: &SessionContext,
    plan: LogicalPlan,
) -> SparkResult<(Vec<Vec<String>>, Vec<(String, DataType)>)> {
    let fields = plan
        .schema()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();
    let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let mut values = Vec::new();
            for column in batch.columns() {
                values.push(ScalarValue::try_from_array(column, row)?.to_string());
            }
            rows.push(values);
        }
    }
    rows.sort();
    Ok((rows, fields))
}

fn row(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| (*v).to_string()).collect()
}

async fn dedup_rows(
    ctx: &SessionContext,
    plan: LogicalPlan,
    keys: &[usize],
) -> SparkResult<Vec<Vec<String>>> {
    Ok(answer(ctx, deduplicate(plan, keys, col("seq"))?).await?.0)
}

#[tokio::test]
async fn keeps_only_the_row_with_the_largest_sequence_value_per_key() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let plan = batch(&[
        (Some(1), Some(10), "first"),
        (Some(1), Some(30), "winner"),
        (Some(1), Some(20), "middle"),
    ])?;
    assert_eq!(
        dedup_rows(&ctx, plan, &[0]).await?,
        vec![row(&["1", "30", "winner"])]
    );
    Ok(())
}

#[tokio::test]
async fn is_a_no_op_for_a_single_event_per_key() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let plan = batch(&[(Some(1), Some(10), "only-row")])?;
    assert_eq!(
        dedup_rows(&ctx, plan, &[0]).await?,
        vec![row(&["1", "10", "only-row"])]
    );
    Ok(())
}

#[tokio::test]
async fn equal_sequencing_values_keep_exactly_one_row() -> SparkResult<()> {
    // Spark guarantees one survivor and nothing about which; Sail's max_by
    // keeps the last row in sequence order, within that contract.
    let ctx = SessionContext::new();
    let plan = batch(&[
        (Some(1), Some(10), "first-tied-row"),
        (Some(1), Some(10), "second-tied-row"),
    ])?;
    let rows = dedup_rows(&ctx, plan, &[0]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][..2], &row(&["1", "10"])[..]);
    assert!(
        ["first-tied-row", "second-tied-row"].contains(&rows[0][2].as_str()),
        "{rows:?}"
    );
    Ok(())
}

#[tokio::test]
async fn null_sequencing_rows_lose_to_a_non_null_value() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let plan = batch(&[
        (Some(1), None, "null-sequence"),
        (Some(1), Some(10), "non-null-sequence"),
    ])?;
    assert_eq!(
        dedup_rows(&ctx, plan, &[0]).await?,
        vec![row(&["1", "10", "non-null-sequence"])]
    );
    Ok(())
}

#[tokio::test]
async fn all_null_sequencing_values_for_a_key_yield_a_null_row() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let plan = batch(&[(Some(1), None, "null-sequence")])?;
    assert_eq!(
        dedup_rows(&ctx, plan, &[0]).await?,
        vec![row(&["NULL", "NULL", "NULL"])]
    );
    Ok(())
}

#[tokio::test]
async fn keys_are_processed_independently() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let plan = batch(&[
        (Some(1), Some(10), "a1"),
        (Some(2), Some(50), "b1-winner"),
        (Some(1), Some(20), "a2-winner"),
        (Some(2), Some(40), "b2-loser"),
        (Some(3), Some(1), "c1-only"),
    ])?;
    assert_eq!(
        dedup_rows(&ctx, plan, &[0]).await?,
        vec![
            row(&["1", "20", "a2-winner"]),
            row(&["2", "50", "b1-winner"]),
            row(&["3", "1", "c1-only"])
        ]
    );
    Ok(())
}

#[tokio::test]
async fn non_key_columns_come_from_the_winning_row() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let plan = scan(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 1])) as ArrayRef),
        ("seq", Arc::new(Int64Array::from(vec![10, 20]))),
        (
            "name",
            Arc::new(StringArray::from(vec!["old-name", "winning-name"])),
        ),
        ("amount", Arc::new(Int32Array::from(vec![100, 200]))),
    ])?;
    assert_eq!(
        dedup_rows(&ctx, plan, &[0]).await?,
        vec![row(&["1", "20", "winning-name", "200"])]
    );
    Ok(())
}

#[tokio::test]
async fn nested_columns_come_from_the_winning_row() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let payload_fields = Fields::from(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Int32, true),
    ]);
    let payload = StructArray::new(
        payload_fields,
        vec![
            Arc::new(StringArray::from(vec!["old", "new"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
        None,
    );
    let plan = scan(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 1])) as ArrayRef),
        ("seq", Arc::new(Int64Array::from(vec![10, 20]))),
        ("payload", Arc::new(payload)),
    ])?;
    let rows = dedup_rows(&ctx, plan, &[0]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][..2], &row(&["1", "20"])[..]);
    assert!(
        rows[0][2].contains("new") && rows[0][2].contains("200"),
        "{rows:?}"
    );
    Ok(())
}

#[tokio::test]
async fn composite_keys_group_independently() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let plan = scan(vec![
        (
            "region",
            Arc::new(StringArray::from(vec!["US", "US", "EU", "US"])) as ArrayRef,
        ),
        ("customer_id", Arc::new(Int32Array::from(vec![1, 1, 1, 2]))),
        ("seq", Arc::new(Int64Array::from(vec![10, 20, 5, 99]))),
        (
            "value",
            Arc::new(StringArray::from(vec![
                "us1-old", "us1-new", "eu1-only", "us2-only",
            ])),
        ),
    ])?;
    assert_eq!(
        dedup_rows(&ctx, plan, &[0, 1]).await?,
        vec![
            row(&["EU", "1", "5", "eu1-only"]),
            row(&["US", "1", "20", "us1-new"]),
            row(&["US", "2", "99", "us2-only"])
        ]
    );
    Ok(())
}

#[tokio::test]
async fn an_arbitrary_sequencing_expression_is_evaluated_per_row() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let plan = scan(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 1, 1])) as ArrayRef),
        ("seq", Arc::new(Int64Array::from(vec![10, 25, 15]))),
        ("alt_seq", Arc::new(Int64Array::from(vec![30, 20, 15]))),
        (
            "value",
            Arc::new(StringArray::from(vec![
                "winner",
                "would-win-on-seq-alone",
                "always-loses",
            ])),
        ),
    ])?;
    let plan = deduplicate(plan, &[0], greatest(vec![col("seq"), col("alt_seq")]))?;
    assert_eq!(
        answer(&ctx, plan).await?.0,
        vec![row(&["1", "10", "30", "winner"])]
    );
    Ok(())
}

#[tokio::test]
async fn literal_dot_column_names_survive() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let plan = scan(vec![
        (
            "user.id",
            Arc::new(Int32Array::from(vec![1, 1])) as ArrayRef,
        ),
        ("seq", Arc::new(Int64Array::from(vec![10, 20]))),
        (
            "event.value",
            Arc::new(StringArray::from(vec!["old", "new"])),
        ),
    ])?;
    let (rows, fields) = answer(&ctx, deduplicate(plan, &[0], col("seq"))?).await?;
    assert_eq!(rows, vec![row(&["1", "20", "new"])]);
    assert_eq!(
        fields.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        ["user.id", "seq", "event.value"]
    );
    Ok(())
}

#[tokio::test]
async fn a_key_column_named_like_the_winning_row_column_fails() -> SparkResult<()> {
    // Spark reports AMBIGUOUS_REFERENCE (42704) from its analyzer; DataFusion
    // rejects the aggregate's duplicate output column with its own error. The
    // failure is asserted, the Spark condition is not: recorded as partial.
    let ctx = SessionContext::new();
    let plan = scan(vec![
        (
            WINNING_ROW_COLUMN,
            Arc::new(StringArray::from(vec!["k1", "k1"])) as ArrayRef,
        ),
        ("seq", Arc::new(Int64Array::from(vec![10, 20]))),
        (
            "value",
            Arc::new(StringArray::from(vec!["loser", "winner"])),
        ),
    ])?;
    let result = match deduplicate(plan, &[0], col("seq")) {
        Ok(plan) => answer(&ctx, plan).await.map(|_| ()),
        Err(error) => Err(error),
    };
    assert!(result.is_err(), "{result:?}");
    Ok(())
}

#[tokio::test]
async fn input_column_names_types_and_order_are_preserved() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let plan = scan(vec![
        (
            "a",
            Arc::new(StringArray::from(vec!["a1", "a2"])) as ArrayRef,
        ),
        ("id", Arc::new(Int32Array::from(vec![1, 1]))),
        ("z", Arc::new(Float64Array::from(vec![1.5, 2.5]))),
        ("seq", Arc::new(Int64Array::from(vec![10, 20]))),
        ("flag", Arc::new(BooleanArray::from(vec![true, false]))),
    ])?;
    let expected: Vec<(String, DataType)> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();
    let (rows, fields) = answer(&ctx, deduplicate(plan, &[1], col("seq"))?).await?;
    assert_eq!(fields, expected);
    assert_eq!(rows, vec![row(&["a2", "1", "2.5", "20", "false"])]);
    Ok(())
}

#[tokio::test]
async fn an_empty_batch_yields_an_empty_result_with_the_schema_preserved() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let plan = batch(&[])?;
    let expected: Vec<(String, DataType)> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect();
    let (rows, fields) = answer(&ctx, deduplicate(plan, &[0], col("seq"))?).await?;
    assert!(rows.is_empty());
    assert_eq!(fields, expected);
    Ok(())
}
