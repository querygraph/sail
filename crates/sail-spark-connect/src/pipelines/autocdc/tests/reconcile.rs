//! `autocdc/Scd1BatchProcessorSuite.scala` `extendMicrobatchRowsWithCdcMetadata`
//! (lines 480–670), `projectTargetColumnsOntoMicrobatch` (672–896) and
//! `applyTombstonesToMicrobatch` (898–1197) tests over the component. Rows
//! are rendered with Arrow's formatter (nulls as `NULL`) and multi-row
//! answers compared as sorted sets.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionContext, col, lit};

use super::super::reconcile::{
    apply_tombstones, extend_with_metadata, project_selected, reconcile,
};
use super::super::schema::{Case, METADATA_COLUMN};
use super::super::{ColumnSelection, SparkResult, UnqualifiedColumnName};

type TestResult = Result<(), Box<dyn std::error::Error>>;
/// `(deleteSequence, upsertSequence)` of a CDC metadata value.
type Meta = (Option<i64>, Option<i64>);

fn scan(name: &str, columns: Vec<(&str, ArrayRef)>) -> SparkResult<LogicalPlan> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let arrays = columns.into_iter().map(|(_, array)| array).collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    Ok(LogicalPlanBuilder::scan(name, provider_as_source(Arc::new(table)), None)?.build()?)
}

fn metadata_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("deleteSequence", DataType::Int64, true),
        Field::new("upsertSequence", DataType::Int64, true),
    ]))
}

/// `cdcMetadataRow(deleteSeq, upsertSeq)` rows as a struct array.
fn metadata(rows: &[Meta]) -> ArrayRef {
    let DataType::Struct(fields) = metadata_type() else {
        unreachable!("metadata type is a struct")
    };
    Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
        None,
    ))
}

fn ints(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

fn longs(values: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn strings(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn bools(values: &[Option<bool>]) -> ArrayRef {
    Arc::new(BooleanArray::from(values.to_vec()))
}

/// Every output row rendered, sorted, plus the output field names and types.
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
    let options = FormatOptions::default().with_null("NULL");
    let mut rows = Vec::new();
    for batch in &batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            rows.push(
                formatters
                    .iter()
                    .map(|formatter| formatter.value(row).to_string())
                    .collect::<Vec<_>>(),
            );
        }
    }
    rows.sort();
    Ok((rows, fields))
}

fn row(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| (*v).to_string()).collect()
}

fn meta(delete: &str, upsert: &str) -> String {
    format!("{{deleteSequence: {delete}, upsertSequence: {upsert}}}")
}

fn key(name: &str) -> SparkResult<UnqualifiedColumnName> {
    UnqualifiedColumnName::parse(name)
}

// ---- extendMicrobatchRowsWithCdcMetadata ----

#[tokio::test]
async fn extend_classifies_each_row_by_the_delete_condition() -> TestResult {
    let ctx = SessionContext::new();
    let batch = scan(
        "batch",
        vec![
            ("id", ints(&[Some(1), Some(2), Some(3), Some(4)])),
            ("seq", longs(&[Some(10), Some(20), Some(30), Some(40)])),
            (
                "is_delete",
                bools(&[Some(false), Some(true), Some(false), Some(true)]),
            ),
        ],
    )?;
    let plan = extend_with_metadata(
        batch,
        col("seq"),
        Some(col("is_delete").eq(lit(true))),
        &DataType::Int64,
    )?;
    let (rows, _) = answer(&ctx, plan).await?;
    assert_eq!(
        rows,
        vec![
            row(&["1", "10", "false", &meta("NULL", "10")]),
            row(&["2", "20", "true", &meta("20", "NULL")]),
            row(&["3", "30", "false", &meta("NULL", "30")]),
            row(&["4", "40", "true", &meta("40", "NULL")]),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn extend_treats_a_null_delete_condition_as_an_upsert() -> TestResult {
    let ctx = SessionContext::new();
    let batch = scan(
        "batch",
        vec![
            ("id", ints(&[Some(1)])),
            ("seq", longs(&[Some(10)])),
            ("is_delete", bools(&[None])),
        ],
    )?;
    let plan = extend_with_metadata(batch, col("seq"), Some(col("is_delete")), &DataType::Int64)?;
    let (rows, _) = answer(&ctx, plan).await?;
    assert_eq!(rows, vec![row(&["1", "10", "NULL", &meta("NULL", "10")])]);
    Ok(())
}

#[tokio::test]
async fn extend_treats_every_row_as_an_upsert_without_a_delete_condition() -> TestResult {
    let ctx = SessionContext::new();
    let batch = scan(
        "batch",
        vec![
            ("id", ints(&[Some(1), Some(2)])),
            ("seq", longs(&[Some(10), Some(20)])),
            ("value", strings(&[Some("a"), Some("b")])),
        ],
    )?;
    let plan = extend_with_metadata(batch, col("seq"), None, &DataType::Int64)?;
    let (rows, fields) = answer(&ctx, plan).await?;
    assert_eq!(
        rows,
        vec![
            row(&["1", "10", "a", &meta("NULL", "10")]),
            row(&["2", "20", "b", &meta("NULL", "20")]),
        ]
    );
    // "appends CDC metadata as the last column"
    assert_eq!(
        fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "seq", "value", METADATA_COLUMN]
    );
    Ok(())
}

#[tokio::test]
async fn extend_casts_the_sequence_fields_to_the_resolved_type() -> TestResult {
    let ctx = SessionContext::new();
    let batch = scan(
        "batch",
        vec![
            ("id", ints(&[Some(1)])),
            ("seq", ints(&[Some(10)])),
            ("value", strings(&[Some("a")])),
        ],
    )?;
    let plan = extend_with_metadata(batch, col("seq"), None, &DataType::Int64)?;
    let (rows, fields) = answer(&ctx, plan).await?;
    let DataType::Struct(members) = &fields[3].1 else {
        return Err("metadata column must be a struct".into());
    };
    assert_eq!(
        members
            .iter()
            .map(|f| (f.name().as_str(), f.data_type().clone()))
            .collect::<Vec<_>>(),
        vec![
            ("deleteSequence", DataType::Int64),
            ("upsertSequence", DataType::Int64)
        ]
    );
    assert_eq!(rows, vec![row(&["1", "10", "a", &meta("NULL", "10")])]);
    Ok(())
}

#[tokio::test]
async fn extend_fails_when_the_sequencing_column_cannot_be_cast() -> TestResult {
    let batch = scan(
        "batch",
        vec![
            ("id", ints(&[Some(1)])),
            ("seq", metadata(&[(Some(1), Some(0))])),
        ],
    )?;
    let message = format!(
        "{:?}",
        extend_with_metadata(batch, col("seq"), None, &DataType::Int64)
    );
    assert!(
        message.contains("DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION"),
        "{message}"
    );
    assert!(message.contains("42K09"), "{message}");
    Ok(())
}

// ---- projectTargetColumnsOntoMicrobatch ----

fn with_metadata(rows: &[(i32, &str, i32, Meta)]) -> SparkResult<LogicalPlan> {
    scan(
        "batch",
        vec![
            (
                "id",
                ints(&rows.iter().map(|r| Some(r.0)).collect::<Vec<_>>()),
            ),
            (
                "name",
                strings(&rows.iter().map(|r| Some(r.1)).collect::<Vec<_>>()),
            ),
            (
                "age",
                ints(&rows.iter().map(|r| Some(r.2)).collect::<Vec<_>>()),
            ),
            (
                METADATA_COLUMN,
                metadata(&rows.iter().map(|r| r.3).collect::<Vec<_>>()),
            ),
        ],
    )
}

fn include(names: &[&str]) -> SparkResult<ColumnSelection> {
    Ok(ColumnSelection::Include(
        names
            .iter()
            .map(|n| key(n))
            .collect::<SparkResult<Vec<_>>>()?,
    ))
}

fn exclude(names: &[&str]) -> SparkResult<ColumnSelection> {
    Ok(ColumnSelection::Exclude(
        names
            .iter()
            .map(|n| key(n))
            .collect::<SparkResult<Vec<_>>>()?,
    ))
}

#[tokio::test]
async fn project_keeps_every_user_column_and_the_metadata_without_a_selection() -> TestResult {
    let ctx = SessionContext::new();
    let batch = with_metadata(&[
        (1, "alice", 30, (None, Some(10))),
        (2, "bob", 25, (Some(20), None)),
    ])?;
    let (rows, fields) = answer(&ctx, project_selected(batch, None, Case::Insensitive)?).await?;
    assert_eq!(
        fields.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        vec!["id", "name", "age", METADATA_COLUMN]
    );
    assert_eq!(
        rows,
        vec![
            row(&["1", "alice", "30", &meta("NULL", "10")]),
            row(&["2", "bob", "25", &meta("20", "NULL")]),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn project_retains_the_metadata_even_when_include_omits_it() -> TestResult {
    let ctx = SessionContext::new();
    let batch = with_metadata(&[(1, "alice", 30, (None, Some(10)))])?;
    let selection = include(&["id", "age"])?;
    let (rows, fields) = answer(
        &ctx,
        project_selected(batch, Some(&selection), Case::Insensitive)?,
    )
    .await?;
    assert_eq!(
        fields.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        vec!["id", "age", METADATA_COLUMN]
    );
    assert_eq!(rows, vec![row(&["1", "30", &meta("NULL", "10")])]);
    Ok(())
}

#[tokio::test]
async fn project_respects_exclude() -> TestResult {
    let ctx = SessionContext::new();
    let batch = with_metadata(&[(1, "alice", 30, (None, Some(10)))])?;
    let selection = exclude(&["age"])?;
    let (rows, fields) = answer(
        &ctx,
        project_selected(batch, Some(&selection), Case::Insensitive)?,
    )
    .await?;
    assert_eq!(
        fields.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        vec!["id", "name", METADATA_COLUMN]
    );
    assert_eq!(rows, vec![row(&["1", "alice", &meta("NULL", "10")])]);
    Ok(())
}

#[tokio::test]
async fn project_preserves_the_batch_order_not_the_selection_order() -> TestResult {
    let ctx = SessionContext::new();
    let batch = with_metadata(&[(1, "alice", 30, (None, Some(10)))])?;
    let selection = include(&["age", "id"])?;
    let (rows, fields) = answer(
        &ctx,
        project_selected(batch, Some(&selection), Case::Insensitive)?,
    )
    .await?;
    assert_eq!(
        fields.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        vec!["id", "age", METADATA_COLUMN]
    );
    assert_eq!(rows, vec![row(&["1", "30", &meta("NULL", "10")])]);
    Ok(())
}

#[tokio::test]
async fn project_handles_backticked_names_with_a_literal_dot() -> TestResult {
    let ctx = SessionContext::new();
    let batch = scan(
        "batch",
        vec![
            ("id", ints(&[Some(1)])),
            ("user.id", strings(&[Some("u-100")])),
            (METADATA_COLUMN, metadata(&[(None, Some(10))])),
        ],
    )?;
    let selection = include(&["id", "`user.id`"])?;
    let (rows, fields) = answer(
        &ctx,
        project_selected(batch, Some(&selection), Case::Insensitive)?,
    )
    .await?;
    assert_eq!(
        fields.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        vec!["id", "user.id", METADATA_COLUMN]
    );
    assert_eq!(rows, vec![row(&["1", "u-100", &meta("NULL", "10")])]);
    Ok(())
}

#[tokio::test]
async fn project_resolves_the_selection_case_insensitively() -> TestResult {
    let ctx = SessionContext::new();
    let batch = with_metadata(&[(1, "alice", 30, (None, Some(10)))])?;
    let selection = include(&["ID", "AGE"])?;
    let (rows, fields) = answer(
        &ctx,
        project_selected(batch, Some(&selection), Case::Insensitive)?,
    )
    .await?;
    assert_eq!(
        fields.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        vec!["id", "age", METADATA_COLUMN]
    );
    assert_eq!(rows, vec![row(&["1", "30", &meta("NULL", "10")])]);
    Ok(())
}

// ---- applyTombstonesToMicrobatch ----

fn microbatch(rows: &[(i32, &str, Meta)]) -> SparkResult<LogicalPlan> {
    scan(
        "batch",
        vec![
            (
                "id",
                ints(&rows.iter().map(|r| Some(r.0)).collect::<Vec<_>>()),
            ),
            (
                "value",
                strings(&rows.iter().map(|r| Some(r.1)).collect::<Vec<_>>()),
            ),
            (
                METADATA_COLUMN,
                metadata(&rows.iter().map(|r| r.2).collect::<Vec<_>>()),
            ),
        ],
    )
}

fn auxiliary(rows: &[(i32, Meta)]) -> SparkResult<LogicalPlan> {
    scan(
        "aux",
        vec![
            (
                "id",
                ints(&rows.iter().map(|r| Some(r.0)).collect::<Vec<_>>()),
            ),
            (
                METADATA_COLUMN,
                metadata(&rows.iter().map(|r| r.1).collect::<Vec<_>>()),
            ),
        ],
    )
}

async fn tombstoned(
    ctx: &SessionContext,
    batch: LogicalPlan,
    aux: LogicalPlan,
    keys: &[&str],
) -> SparkResult<Vec<Vec<String>>> {
    let keys = keys
        .iter()
        .map(|k| key(k))
        .collect::<SparkResult<Vec<_>>>()?;
    Ok(
        answer(ctx, apply_tombstones(batch, aux, &keys, Case::Insensitive)?)
            .await?
            .0,
    )
}

#[tokio::test]
async fn tombstones_drop_late_deletes_and_upserts_for_a_matching_key() -> TestResult {
    let ctx = SessionContext::new();
    let batch = microbatch(&[
        (1, "stale-upsert", (None, Some(5))),
        (1, "stale-delete", (Some(7), None)),
    ])?;
    let aux = auxiliary(&[(1, (Some(10), None))])?;
    assert!(tombstoned(&ctx, batch, aux, &["id"]).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn tombstones_keep_a_row_that_ties_the_delete_sequence() -> TestResult {
    let ctx = SessionContext::new();
    let batch = microbatch(&[(1, "tied-upsert", (None, Some(10)))])?;
    let aux = auxiliary(&[(1, (Some(10), None))])?;
    assert_eq!(
        tombstoned(&ctx, batch, aux, &["id"]).await?,
        vec![row(&["1", "tied-upsert", &meta("NULL", "10")])]
    );
    Ok(())
}

#[tokio::test]
async fn tombstones_keep_rows_whose_effective_sequence_exceeds_the_tombstone() -> TestResult {
    let ctx = SessionContext::new();
    let batch = microbatch(&[
        (1, "fresher-upsert", (None, Some(15))),
        (1, "fresher-delete", (Some(20), None)),
    ])?;
    let aux = auxiliary(&[(1, (Some(10), None))])?;
    assert_eq!(
        tombstoned(&ctx, batch, aux, &["id"]).await?,
        vec![
            row(&["1", "fresher-delete", &meta("20", "NULL")]),
            row(&["1", "fresher-upsert", &meta("NULL", "15")]),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn tombstones_for_a_different_key_leave_rows_untouched() -> TestResult {
    let ctx = SessionContext::new();
    let batch = microbatch(&[(1, "stays", (None, Some(5)))])?;
    let aux = auxiliary(&[(2, (Some(1000), None))])?;
    assert_eq!(
        tombstoned(&ctx, batch, aux, &["id"]).await?,
        vec![row(&["1", "stays", &meta("NULL", "5")])]
    );
    Ok(())
}

#[tokio::test]
async fn tombstones_with_composite_keys_need_every_key_to_match() -> TestResult {
    let ctx = SessionContext::new();
    let batch = scan(
        "batch",
        vec![
            ("region", strings(&[Some("US"), Some("US")])),
            ("customer_id", ints(&[Some(1), Some(2)])),
            (
                METADATA_COLUMN,
                metadata(&[(None, Some(5)), (None, Some(5))]),
            ),
        ],
    )?;
    let aux = scan(
        "aux",
        vec![
            ("region", strings(&[Some("US")])),
            ("customer_id", ints(&[Some(99)])),
            (METADATA_COLUMN, metadata(&[(Some(1000), None)])),
        ],
    )?;
    assert_eq!(
        tombstoned(&ctx, batch, aux, &["region", "customer_id"]).await?,
        vec![
            row(&["US", "1", &meta("NULL", "5")]),
            row(&["US", "2", &meta("NULL", "5")]),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn tombstones_support_backticked_key_names_with_a_literal_dot() -> TestResult {
    let ctx = SessionContext::new();
    let batch = scan(
        "batch",
        vec![
            ("user.id", ints(&[Some(1)])),
            (METADATA_COLUMN, metadata(&[(None, Some(5))])),
        ],
    )?;
    let aux = scan(
        "aux",
        vec![
            ("user.id", ints(&[Some(1)])),
            (METADATA_COLUMN, metadata(&[(Some(10), None)])),
        ],
    )?;
    assert!(
        tombstoned(&ctx, batch, aux, &["`user.id`"])
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn tombstones_are_a_no_op_with_an_empty_auxiliary_table() -> TestResult {
    let ctx = SessionContext::new();
    let batch = microbatch(&[
        (1, "kept-upsert", (None, Some(5))),
        (2, "kept-delete", (Some(7), None)),
    ])?;
    let aux = auxiliary(&[])?;
    assert_eq!(
        tombstoned(&ctx, batch, aux, &["id"]).await?,
        vec![
            row(&["1", "kept-upsert", &meta("NULL", "5")]),
            row(&["2", "kept-delete", &meta("7", "NULL")]),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn tombstones_with_a_null_delete_sequence_keep_the_row() -> TestResult {
    let ctx = SessionContext::new();
    let batch = microbatch(&[(1, "kept-upsert", (None, Some(5)))])?;
    let aux = auxiliary(&[(1, (None, None))])?;
    assert_eq!(
        tombstoned(&ctx, batch, aux, &["id"]).await?,
        vec![row(&["1", "kept-upsert", &meta("NULL", "5")])]
    );
    Ok(())
}

#[tokio::test]
async fn tombstones_are_unaffected_by_stale_entries() -> TestResult {
    let ctx = SessionContext::new();
    let batch = microbatch(&[(1, "stale-upsert", (None, Some(8)))])?;
    let aux = auxiliary(&[(1, (Some(5), None)), (1, (Some(10), None))])?;
    assert!(tombstoned(&ctx, batch, aux, &["id"]).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn tombstones_ignore_the_auxiliary_upsert_sequence() -> TestResult {
    let ctx = SessionContext::new();
    let batch = microbatch(&[(1, "stale-upsert", (None, Some(5)))])?;
    let aux = auxiliary(&[(1, (Some(10), Some(20)))])?;
    assert!(tombstoned(&ctx, batch, aux, &["id"]).await?.is_empty());
    Ok(())
}

// ---- reconcileMicrobatch ----

// The composition: two events per key are deduplicated by `seq`, the winner
// is classified, the selection drops `note`, and the key with a newer
// tombstone in the auxiliary table is dropped.
#[tokio::test]
async fn reconcile_composes_dedup_extend_project_and_tombstones() -> TestResult {
    let ctx = SessionContext::new();
    let batch = scan(
        "batch",
        vec![
            ("id", ints(&[Some(1), Some(1), Some(2), Some(3)])),
            ("seq", longs(&[Some(5), Some(9), Some(4), Some(7)])),
            (
                "is_delete",
                bools(&[Some(false), Some(true), Some(false), Some(false)]),
            ),
            (
                "note",
                strings(&[Some("old"), Some("new"), Some("x"), Some("y")]),
            ),
        ],
    )?;
    let aux = auxiliary(&[(2, (Some(10), None)), (3, (Some(6), None))])?;
    let selection = exclude(&["note"])?;
    let plan = reconcile(
        batch,
        aux,
        &[key("id")?],
        Some(&selection),
        col("seq"),
        Some(col("is_delete").eq(lit(true))),
        &DataType::Int64,
        Case::Insensitive,
    )?;
    let (rows, fields) = answer(&ctx, plan).await?;
    assert_eq!(
        fields.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        vec!["id", "seq", "is_delete", METADATA_COLUMN]
    );
    // id 1: the seq 9 delete wins; id 2: superseded by the tombstone at 10;
    // id 3: seq 7 beats the tombstone at 6.
    assert_eq!(
        rows,
        vec![
            row(&["1", "9", "true", &meta("9", "NULL")]),
            row(&["3", "7", "false", &meta("NULL", "7")]),
        ]
    );
    Ok(())
}
