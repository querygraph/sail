//! `autocdc/Scd1ForeachBatchHandlerSuite.scala` microbatch validation tests
//! (lines 84–264) over the component. The originals also assert that the
//! auxiliary and target tables are untouched after the failure; without a
//! writer that part is recorded as partial in the Grust evidence.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int32Builder, Int64Array, MapBuilder, StringArray,
    StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionContext, col};

use super::*;
use crate::pipelines::autocdc::microbatch::{Microbatch, is_orderable, validate};

/// `defaultTargetTableIdentifier` of the suite.
fn target() -> Vec<String> {
    names(&["test_catalog", "test_db", "target"])
}

fn keys(names: &[(&str, usize)]) -> Vec<(UnqualifiedColumnName, usize)> {
    names
        .iter()
        .map(|(name, index)| (column(name), *index))
        .collect()
}

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

/// `microbatchOf(sourceSchema)(rows...)` with id INT, value STRING, seq LONG, is_delete BOOLEAN.
fn batch(rows: &[(Option<i32>, &str, Option<i64>, bool)]) -> SparkResult<LogicalPlan> {
    scan(vec![
        (
            "id",
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "value",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ),
        (
            "seq",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ),
        (
            "is_delete",
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
        ),
    ])
}

#[tokio::test]
async fn null_sequencing_rows_are_invalid_before_any_merge() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let batch = batch(&[(Some(1), "invalid", None, false)])?;
    check_error(
        validate(
            &ctx,
            Microbatch {
                batch,
                sequencing: col("seq"),
                keys: &keys(&[("id", 0)]),
                target: &target(),
                batch_id: 123,
            },
        )
        .await,
        "AUTOCDC_MICROBATCH_VALIDATION.NULL_SEQUENCE",
        "22000",
        &[
            ("tableName", "`test_catalog`.`test_db`.`target`"),
            ("batchId", "123"),
            ("nullCount", "1"),
        ],
    );
    Ok(())
}

#[tokio::test]
async fn null_key_rows_are_counted_without_touching_well_formed_rows() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let batch = batch(&[
        (None, "no-id-1", Some(5), false),
        (Some(2), "ok", Some(6), false),
        (None, "no-id-2", Some(7), true),
    ])?;
    check_error(
        validate(
            &ctx,
            Microbatch {
                batch,
                sequencing: col("seq"),
                keys: &keys(&[("id", 0)]),
                target: &target(),
                batch_id: 13,
            },
        )
        .await,
        "AUTOCDC_MICROBATCH_VALIDATION.NULL_KEY",
        "22000",
        &[
            ("tableName", "`test_catalog`.`test_db`.`target`"),
            ("batchId", "13"),
            ("nullKeyCounts", "`id`=2"),
        ],
    );
    Ok(())
}

#[tokio::test]
async fn composite_key_null_counts_follow_the_configured_key_order() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let batch = scan(vec![
        (
            "country",
            Arc::new(StringArray::from(vec![None, Some("US"), None])) as ArrayRef,
        ),
        (
            "city",
            Arc::new(StringArray::from(vec![Some("Boston"), None, None])),
        ),
        ("seq", Arc::new(Int64Array::from(vec![1, 2, 3]))),
        (
            "is_delete",
            Arc::new(BooleanArray::from(vec![false, false, false])),
        ),
    ])?;
    check_error(
        validate(
            &ctx,
            Microbatch {
                batch,
                sequencing: col("seq"),
                keys: &keys(&[("country", 0), ("city", 1)]),
                target: &target(),
                batch_id: 7,
            },
        )
        .await,
        "AUTOCDC_MICROBATCH_VALIDATION.NULL_KEY",
        "22000",
        &[
            ("tableName", "`test_catalog`.`test_db`.`target`"),
            ("batchId", "7"),
            ("nullKeyCounts", "`country`=2, `city`=2"),
        ],
    );
    Ok(())
}

#[tokio::test]
async fn null_sequence_is_reported_before_null_key() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let batch = batch(&[(None, "bad", None, false)])?;
    check_error(
        validate(
            &ctx,
            Microbatch {
                batch,
                sequencing: col("seq"),
                keys: &keys(&[("id", 0)]),
                target: &target(),
                batch_id: 99,
            },
        )
        .await,
        "AUTOCDC_MICROBATCH_VALIDATION.NULL_SEQUENCE",
        "22000",
        &[
            ("tableName", "`test_catalog`.`test_db`.`target`"),
            ("batchId", "99"),
            ("nullCount", "1"),
        ],
    );
    Ok(())
}

#[tokio::test]
async fn a_map_typed_sequencing_column_is_not_orderable() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    map.keys().append_value("k");
    map.values().append_value(1);
    map.append(true)?;
    let batch = scan(vec![
        ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ("value", Arc::new(StringArray::from(vec!["invalid"]))),
        ("seq", Arc::new(map.finish())),
        ("is_delete", Arc::new(BooleanArray::from(vec![false]))),
    ])?;
    check_error(
        validate(
            &ctx,
            Microbatch {
                batch,
                sequencing: col("seq"),
                keys: &keys(&[("id", 0)]),
                target: &target(),
                batch_id: 124,
            },
        )
        .await,
        "AUTOCDC_MICROBATCH_VALIDATION.NON_ORDERABLE_SEQUENCE",
        "22000",
        &[
            ("tableName", "`test_catalog`.`test_db`.`target`"),
            ("batchId", "124"),
            ("dataType", "map<string,int>"),
        ],
    );
    Ok(())
}

#[tokio::test]
async fn a_losing_row_with_a_null_sequence_still_fails_the_batch() -> SparkResult<()> {
    // Supplemental: the newer row for key 1 would win deduplication, but
    // validation runs over the original rows first, so the older row's null
    // sequence is still an error.
    let ctx = SessionContext::new();
    let batch = batch(&[
        (Some(1), "newer", Some(10), false),
        (Some(1), "older", None, false),
    ])?;
    check_error(
        validate(
            &ctx,
            Microbatch {
                batch,
                sequencing: col("seq"),
                keys: &keys(&[("id", 0)]),
                target: &target(),
                batch_id: 5,
            },
        )
        .await,
        "AUTOCDC_MICROBATCH_VALIDATION.NULL_SEQUENCE",
        "22000",
        &[
            ("tableName", "`test_catalog`.`test_db`.`target`"),
            ("batchId", "5"),
            ("nullCount", "1"),
        ],
    );
    Ok(())
}

#[tokio::test]
async fn a_well_formed_batch_passes_and_a_sequencing_expression_is_honored() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let batch = batch(&[
        (Some(1), "a", Some(1), false),
        (Some(2), "b", Some(2), true),
    ])?;
    validate(
        &ctx,
        Microbatch {
            batch,
            sequencing: col("seq") + datafusion::prelude::lit(1_i64),
            keys: &keys(&[("id", 0)]),
            target: &target(),
            batch_id: 1,
        },
    )
    .await
}

#[test]
fn orderability_follows_order_utils() {
    let struct_of = |data_type: DataType| {
        DataType::Struct(Fields::from(vec![Field::new("f", data_type, true)]))
    };
    let list_of = |data_type: DataType| DataType::List(Field::new("item", data_type, true).into());
    let map = DataType::Map(
        Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        )
        .into(),
        false,
    );
    for data_type in [
        DataType::Null,
        DataType::Int32,
        DataType::Utf8,
        DataType::Binary,
        DataType::Boolean,
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Decimal128(10, 2),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Duration(TimeUnit::Microsecond),
        struct_of(DataType::Int64),
        list_of(DataType::Utf8),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ] {
        assert!(is_orderable(&data_type), "{data_type:?}");
    }
    for data_type in [
        map.clone(),
        struct_of(map.clone()),
        list_of(map),
        DataType::Interval(IntervalUnit::MonthDayNano),
    ] {
        assert!(!is_orderable(&data_type), "{data_type:?}");
    }
}
