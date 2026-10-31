//! Source-driven tests of the prepared-input manifest: a unit-3
//! DeterministicSource feeds intents, unit 2a validates the original rows,
//! unit 2b reconciles, and the manifest seals the result. Nothing is wired.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{SessionContext, col};

use super::*;
use crate::pipelines::autocdc::dedup::deduplicate;
use crate::pipelines::autocdc::microbatch::{Microbatch, validate};
use crate::pipelines::autocdc::prepared::{
    EvaluationContext, InputIdentity, PreparedBatch, SealError, ValidationOutcome,
    ValidationReceipt, seal, verify,
};
use crate::pipelines::streaming::intent::{BatchIntent, Progress, RunFrontier, next_intent};
use crate::pipelines::streaming::source::BoundedSource;
use crate::pipelines::streaming::test_source::DeterministicSource;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("seq", DataType::Int64, true),
        Field::new("value", DataType::Utf8, true),
    ]))
}

fn row(id: i64, seq: Option<i64>, value: &str) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(Some(id)),
        ScalarValue::Int64(seq),
        ScalarValue::Utf8(Some(value.into())),
    ]
}

fn evaluation() -> EvaluationContext {
    EvaluationContext {
        case: Case::Insensitive.label().into(),
        keys: vec!["id".into()],
        sequencing: "seq".into(),
        delete_condition: None,
        column_selection: None,
    }
}

fn identity(intent: &BatchIntent, batch_id: i64) -> InputIdentity {
    InputIdentity {
        flow: names(&["test_catalog", "test_db", "cdc_flow"]),
        batch_id,
        intent: intent.clone(),
        evaluation: evaluation(),
    }
}

/// The batch plan of an intent: the union of its range scans.
fn plan(source: &dyn BoundedSource, intent: &BatchIntent) -> SparkResult<LogicalPlan> {
    let mut builder: Option<LogicalPlanBuilder> = None;
    for range in &intent.ranges {
        let scan = LogicalPlanBuilder::scan(
            format!("range_{}", range.partition),
            provider_as_source(source.scan(range)?),
            None,
        )?;
        builder = Some(match builder {
            None => scan,
            Some(previous) => previous.union(scan.build()?)?,
        });
    }
    Ok(builder
        .ok_or_else(|| SparkError::invalid("empty intent"))?
        .build()?)
}

/// Validate the original rows, reconcile, and return the receipt and rows.
async fn prepare(
    ctx: &SessionContext,
    source: &dyn BoundedSource,
    intent: &BatchIntent,
    batch_id: i64,
) -> SparkResult<(InputIdentity, ValidationReceipt, Vec<RecordBatch>)> {
    let identity = identity(intent, batch_id);
    let batch = plan(source, intent)?;
    let validation = validate(
        ctx,
        Microbatch {
            batch: batch.clone(),
            sequencing: col("seq"),
            keys: &[(column("id"), 0)],
            target: &names(&["test_catalog", "test_db", "target"]),
            batch_id,
        },
    )
    .await;
    let receipt = ValidationReceipt::record(&identity, &validation);
    let rows = if validation.is_ok() {
        let reconciled = deduplicate(batch, &[0], col("seq"))?;
        ctx.execute_logical_plan(reconciled)
            .await?
            .collect()
            .await?
    } else {
        Vec::new()
    };
    Ok((identity, receipt, rows))
}

fn source_with(rows: Vec<Vec<ScalarValue>>) -> DeterministicSource {
    let source = DeterministicSource::new("events", schema());
    source.create_partition("p0");
    source.append("p0", rows);
    source
}

async fn sealed(
    ctx: &SessionContext,
    source: &DeterministicSource,
    batch_id: i64,
) -> SparkResult<(PreparedBatch, Vec<RecordBatch>)> {
    let frontier = RunFrontier::capture(source)?;
    let intent = next_intent(source, &frontier, &Progress::default(), 100)?
        .ok_or_else(|| SparkError::invalid("nothing to read"))?;
    let (identity, receipt, rows) = prepare(ctx, source, &intent, batch_id).await?;
    Ok((seal(identity, receipt, &schema(), &rows)?, rows))
}

#[tokio::test]
async fn the_same_intent_seals_to_the_same_digest_even_after_appends() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let source = source_with(vec![
        row(1, Some(10), "old"),
        row(1, Some(20), "new"),
        row(2, Some(5), "only"),
    ]);
    let (first, rows) = sealed(&ctx, &source, 1).await?;
    assert_eq!(first.row_count, 2);
    assert!(first.completion_required);
    assert_eq!(first.validation.outcome, ValidationOutcome::Validated);
    assert_eq!(verify(&first.identity, &first, &schema(), &rows), Ok(()));
    // Rows appended after capture are outside the intent, so replaying the
    // recorded intent prepares the same content.
    source.append("p0", vec![row(1, Some(30), "later")]);
    let (identity, receipt, replayed) = prepare(&ctx, &source, &first.identity.intent, 1).await?;
    let second = seal(identity, receipt, &schema(), &replayed)?;
    assert_eq!(second.identity_digest, first.identity_digest);
    assert_eq!(second.content_digest, first.content_digest);
    assert_eq!(
        verify(&first.identity, &first, &schema(), &replayed),
        Ok(())
    );
    Ok(())
}

#[tokio::test]
async fn a_batch_that_fails_validation_cannot_be_sealed() -> SparkResult<()> {
    // A null-sequence row that would lose deduplication to a valid row with
    // the same key still fails the batch, and no prepared state exists.
    let ctx = SessionContext::new();
    let source = source_with(vec![
        row(1, Some(10), "valid"),
        row(1, None, "null-sequence"),
    ]);
    let frontier = RunFrontier::capture(&source)?;
    let intent = next_intent(&source, &frontier, &Progress::default(), 100)?
        .ok_or_else(|| SparkError::invalid("nothing to read"))?;
    let (identity, receipt, rows) = prepare(&ctx, &source, &intent, 7).await?;
    assert!(rows.is_empty());
    assert_eq!(
        receipt.outcome,
        ValidationOutcome::Failed {
            condition: "AUTOCDC_MICROBATCH_VALIDATION.NULL_SEQUENCE".into()
        }
    );
    assert_eq!(
        seal(identity, receipt, &schema(), &rows).err(),
        Some(SealError::ValidationFailed {
            condition: "AUTOCDC_MICROBATCH_VALIDATION.NULL_SEQUENCE".into()
        })
    );
    Ok(())
}

#[tokio::test]
async fn duplicate_null_key_counts_survive_the_source_driven_path() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let source = DeterministicSource::new("events", schema());
    source.create_partition("p0");
    source.append(
        "p0",
        vec![
            vec![
                ScalarValue::Int64(None),
                ScalarValue::Int64(Some(5)),
                ScalarValue::Utf8(Some("no-id-1".into())),
            ],
            row(2, Some(6), "ok"),
            vec![
                ScalarValue::Int64(None),
                ScalarValue::Int64(Some(7)),
                ScalarValue::Utf8(Some("no-id-2".into())),
            ],
        ],
    );
    let frontier = RunFrontier::capture(&source)?;
    let intent = next_intent(&source, &frontier, &Progress::default(), 100)?
        .ok_or_else(|| SparkError::invalid("nothing to read"))?;
    let batch = plan(&source, &intent)?;
    check_error(
        validate(
            &ctx,
            Microbatch {
                batch,
                sequencing: col("seq"),
                keys: &[(column("id"), 0)],
                target: &names(&["test_catalog", "test_db", "target"]),
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
async fn an_empty_reconciled_result_still_seals_with_its_obligations() -> SparkResult<()> {
    // Every original row is valid, but a selection could still leave nothing
    // to merge; here the reconciled rows are simply empty.
    let ctx = SessionContext::new();
    let source = source_with(vec![row(1, Some(10), "a")]);
    let frontier = RunFrontier::capture(&source)?;
    let intent = next_intent(&source, &frontier, &Progress::default(), 100)?
        .ok_or_else(|| SparkError::invalid("nothing to read"))?;
    let (identity, receipt, _) = prepare(&ctx, &source, &intent, 2).await?;
    let empty: Vec<RecordBatch> = Vec::new();
    let prepared = seal(identity, receipt, &schema(), &empty)?;
    assert_eq!(prepared.row_count, 0);
    assert!(prepared.completion_required);
    assert_eq!(prepared.validation.outcome, ValidationOutcome::Validated);
    assert_eq!(
        verify(&prepared.identity, &prepared, &schema(), &empty),
        Ok(())
    );
    Ok(())
}

#[tokio::test]
async fn mismatched_identity_and_tampered_rows_are_refused() -> SparkResult<()> {
    let ctx = SessionContext::new();
    let source = source_with(vec![row(1, Some(10), "a"), row(2, Some(20), "b")]);
    let (prepared, rows) = sealed(&ctx, &source, 1).await?;
    // A receipt computed for another batch id does not seal this identity.
    let mut other = prepared.identity.clone();
    other.batch_id = 99;
    let foreign = ValidationReceipt::record(&other, &Ok(()));
    assert!(matches!(
        seal(prepared.identity.clone(), foreign, &schema(), &rows),
        Err(SealError::ReceiptForAnotherIdentity { .. })
    ));
    // Rows with one changed value fail verification.
    let tampered = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["a", "changed"])),
        ],
    )?;
    assert!(matches!(
        verify(&prepared.identity, &prepared, &schema(), &[tampered]),
        Err(SealError::ContentMismatch { .. })
    ));
    // A different evaluation context is a different identity.
    let mut changed = prepared.identity.clone();
    changed.evaluation.case = Case::Sensitive.label().into();
    assert_ne!(changed.digest(), prepared.identity_digest);
    assert_eq!(changed.evaluation.case(), Ok(Case::Sensitive));
    // The manifest round-trips through serde.
    let json = serde_json::to_string(&prepared)?;
    let decoded: PreparedBatch = serde_json::from_str(&json)?;
    assert_eq!(decoded, prepared);
    Ok(())
}

#[test]
fn equal_schemas_digest_equally_whatever_the_metadata_order_and_differences_count() {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::Field;

    let field_with = |meta: &[(&str, &str)]| {
        Field::new("id", DataType::Int64, true).with_metadata(
            meta.iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect::<HashMap<_, _>>(),
        )
    };
    let schema_with = |meta: &[(&str, &str)], nullable: bool, data_type: DataType| {
        Arc::new(Schema::new_with_metadata(
            vec![field_with(meta), Field::new("seq", data_type, nullable)],
            meta.iter()
                .rev()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        ))
    };
    let a = schema_with(&[("x", "1"), ("y", "2"), ("z", "3")], true, DataType::Int64);
    let b = schema_with(&[("z", "3"), ("x", "1"), ("y", "2")], true, DataType::Int64);
    assert_eq!(a, b);
    let digest = |schema: &SchemaRef| {
        let empty: Vec<RecordBatch> = Vec::new();
        let identity = InputIdentity {
            flow: names(&["f"]),
            batch_id: 1,
            intent: BatchIntent { ranges: vec![] },
            evaluation: EvaluationContext {
                keys: vec!["id".into()],
                ..evaluation()
            },
        };
        let receipt = ValidationReceipt::record(&identity, &Ok(()));
        seal(identity, receipt, schema, &empty).map(|p| p.schema_digest)
    };
    // Same digest across many constructions: HashMap iteration order varies.
    let first = digest(&a).ok();
    for _ in 0..50 {
        assert_eq!(digest(&b).ok(), first);
    }
    assert_ne!(
        digest(&schema_with(&[("x", "1")], true, DataType::Int64)).ok(),
        first
    );
    assert_ne!(
        digest(&schema_with(
            &[("x", "1"), ("y", "2"), ("z", "3")],
            false,
            DataType::Int64
        ))
        .ok(),
        first
    );
    assert_ne!(
        digest(&schema_with(
            &[("x", "1"), ("y", "2"), ("z", "3")],
            true,
            DataType::Int32
        ))
        .ok(),
        first
    );
}

#[tokio::test]
async fn a_recovered_manifest_is_validated_against_the_expected_identity_and_field_by_field()
-> SparkResult<()> {
    let ctx = SessionContext::new();
    let source = source_with(vec![row(1, Some(10), "a"), row(2, Some(20), "b")]);
    let (prepared, rows) = sealed(&ctx, &source, 1).await?;
    let expected = prepared.identity.clone();
    assert_eq!(verify(&expected, &prepared, &schema(), &rows), Ok(()));
    // External binding first: a manifest that is self-consistent but belongs
    // to another flow or batch is refused against the recorded intent.
    let mut other_flow = prepared.clone();
    other_flow.identity.flow = names(&["other"]);
    other_flow.identity_digest = other_flow.identity.digest();
    other_flow.validation.identity_digest = other_flow.identity_digest.clone();
    assert_eq!(
        verify(&other_flow.identity, &other_flow, &schema(), &rows),
        Ok(())
    );
    assert!(matches!(
        verify(&expected, &other_flow, &schema(), &rows),
        Err(SealError::IdentityNotExpected { .. })
    ));
    let mut other_batch = prepared.clone();
    other_batch.identity.batch_id = 99;
    other_batch.identity_digest = expected.digest();
    assert!(matches!(
        verify(&expected, &other_batch, &schema(), &rows),
        Err(SealError::IdentityNotExpected { .. })
    ));
    // Internal consistency, each field mutated independently with the
    // caller expecting the mutated identity where the identity changed.
    let mut m = prepared.clone();
    m.identity_digest = "0".repeat(64);
    assert!(matches!(
        verify(&expected, &m, &schema(), &rows),
        Err(SealError::IdentityNotExpected { .. })
    ));
    let mut m = prepared.clone();
    m.validation.identity_digest = "0".repeat(64);
    assert!(matches!(
        verify(&expected, &m, &schema(), &rows),
        Err(SealError::ReceiptForAnotherIdentity { .. })
    ));
    let mut m = prepared.clone();
    m.validation.outcome = ValidationOutcome::Failed {
        condition: "X".into(),
    };
    assert!(matches!(
        verify(&expected, &m, &schema(), &rows),
        Err(SealError::ValidationFailed { .. })
    ));
    let mut m = prepared.clone();
    m.completion_required = false;
    assert_eq!(
        verify(&expected, &m, &schema(), &rows),
        Err(SealError::CompletionNotRequired)
    );
    let mut m = prepared.clone();
    m.row_count = 1;
    assert!(matches!(
        verify(&expected, &m, &schema(), &rows),
        Err(SealError::RowCountMismatch {
            expected: 1,
            actual: 2
        })
    ));
    let mut m = prepared.clone();
    m.identity.evaluation.case = "mixed".into();
    m.identity_digest = m.identity.digest();
    m.validation.identity_digest = m.identity_digest.clone();
    assert!(matches!(
        verify(&m.identity.clone(), &m, &schema(), &rows),
        Err(SealError::UnknownCase { .. })
    ));
    // The serde round trip of the valid manifest still verifies.
    let json = serde_json::to_string(&prepared)?;
    let decoded: PreparedBatch = serde_json::from_str(&json)?;
    assert_eq!(verify(&expected, &decoded, &schema(), &rows), Ok(()));
    Ok(())
}

#[tokio::test]
async fn keys_bind_to_the_reconciled_schema_under_the_recorded_case_setting() -> SparkResult<()> {
    use datafusion::arrow::datatypes::Field;

    // Schema with a literal-dot field; the user spells the key in another case.
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("user.id", DataType::Int64, true),
        Field::new("seq", DataType::Int64, true),
    ]));
    let rows = |n: usize| -> Vec<RecordBatch> {
        if n == 0 {
            return Vec::new();
        }
        vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from((1..=n as i64).collect::<Vec<_>>())),
                    Arc::new(Int64Array::from(
                        (1..=n as i64).map(|i| i * 10).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap_or_else(|_| RecordBatch::new_empty(schema.clone())),
        ]
    };
    let identity = |case: Case| InputIdentity {
        flow: names(&["f"]),
        batch_id: 1,
        intent: BatchIntent { ranges: vec![] },
        evaluation: EvaluationContext {
            case: case.label().into(),
            keys: vec!["USER.ID".into()],
            sequencing: "seq".into(),
            delete_condition: None,
            column_selection: None,
        },
    };
    for n in [0, 1, 2] {
        // Case-insensitive: USER.ID resolves to user.id for every row count.
        let id = identity(Case::Insensitive);
        let receipt = ValidationReceipt::record(&id, &Ok(()));
        let prepared = seal(id, receipt, &schema, &rows(n))?;
        assert_eq!(prepared.row_count, n);
        assert_eq!(
            verify(&prepared.identity, &prepared, &schema, &rows(n)),
            Ok(())
        );
        // Case-sensitive: the key does not resolve, even with no rows.
        let id = identity(Case::Sensitive);
        let receipt = ValidationReceipt::record(&id, &Ok(()));
        assert!(matches!(
            seal(id, receipt, &schema, &rows(n)),
            Err(SealError::KeyNotResolved { .. })
        ));
    }
    // The order is by the resolved key: rows supplied out of order digest the same.
    let id = identity(Case::Insensitive);
    let receipt = ValidationReceipt::record(&id, &Ok(()));
    let forward = rows(2);
    let backward = vec![RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![2, 1])),
            Arc::new(Int64Array::from(vec![20, 10])),
        ],
    )?];
    let prepared = seal(id, receipt, &schema, &forward)?;
    assert_eq!(
        verify(&prepared.identity, &prepared, &schema, &backward),
        Ok(())
    );
    Ok(())
}
