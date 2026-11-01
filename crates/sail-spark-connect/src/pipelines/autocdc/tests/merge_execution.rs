//! `autocdc/Scd1BatchProcessorMergeSuite.scala` (lines 107–541): the two
//! merges executed against Delta tables through Sail's MERGE resolver, with a
//! MemTable reconciled microbatch published as the temporary view the write
//! component uses. The suite's `processor` has key `id` and an irrelevant
//! sequencing; the merges read only the CDC metadata column.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::CreateTemporaryViewOptions;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_plan::config::PlanConfig;

use super::super::merge::{auxiliary_merge, target_merge};
use super::super::schema::{Case, METADATA_COLUMN};
use super::super::{SparkResult, UnqualifiedColumnName};
use super::write::{
    column, context, create_target_with, finish, key, meta, metadata_column, row, rows,
};
use crate::pipelines::execution::run_command;
use crate::pipelines::graph::Name;

type TestResult = Result<(), Box<dyn std::error::Error>>;
type Meta = (Option<i64>, Option<i64>);

pub(super) const VIEW: &str = "__spark_autocdc_reconciled_test";

pub(super) fn aux_name() -> Name {
    vec![
        "test_catalog".into(),
        "test_db".into(),
        "__spark_autocdc_aux_state_target".into(),
    ]
}

pub(super) fn target_name() -> Name {
    vec!["test_catalog".into(), "test_db".into(), "target".into()]
}

fn metadata(rows: &[Meta]) -> ArrayRef {
    let fields = Fields::from(vec![
        Field::new("deleteSequence", DataType::Int64, true),
        Field::new("upsertSequence", DataType::Int64, true),
    ]);
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

/// `minimalSchema` rows: `(id, metadata)`.
pub(super) fn minimal(rows: &[(i32, Meta)]) -> SparkResult<LogicalPlan> {
    scan(vec![
        (
            "id",
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            METADATA_COLUMN,
            metadata(&rows.iter().map(|r| r.1).collect::<Vec<_>>()),
        ),
    ])
}

/// `targetSchema` rows: `(id, value, metadata)`.
pub(super) fn valued(rows: &[(i32, &str, Meta)]) -> SparkResult<LogicalPlan> {
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
            METADATA_COLUMN,
            metadata(&rows.iter().map(|r| r.2).collect::<Vec<_>>()),
        ),
    ])
}

pub(super) async fn publish(ctx: &SessionContext, plan: LogicalPlan) -> SparkResult<()> {
    ctx.extension::<CatalogManager>()?
        .create_temporary_view(
            VIEW,
            CreateTemporaryViewOptions {
                input: Arc::new(plan),
                columns: vec![],
                if_not_exists: false,
                replace: true,
                comment: None,
                properties: vec![],
                source: None,
            },
        )
        .await
        .map_err(sail_plan::error::PlanError::from)?;
    Ok(())
}

/// `createTable(..., seedRows)`: a Delta table with `columns`, seeded by an
/// insert-all merge from the seed rows (every seed key is new).
pub(super) async fn create_seeded(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    name: &Name,
    columns: Vec<spec::TableColumnDefinition>,
    seed: Option<LogicalPlan>,
) -> SparkResult<()> {
    create_seeded_with(ctx, config, name, columns, seed, vec![]).await
}

pub(super) async fn create_seeded_with(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    name: &Name,
    columns: Vec<spec::TableColumnDefinition>,
    seed: Option<LogicalPlan>,
    properties: Vec<(String, String)>,
) -> SparkResult<()> {
    create_target_with(ctx, config.clone(), name, "delta", columns, properties).await?;
    if let Some(seed) = seed {
        publish(ctx, seed).await?;
        let seed_columns: Vec<String> = ctx
            .extension::<CatalogManager>()?
            .get_temporary_view(VIEW)
            .await
            .map_err(sail_plan::error::PlanError::from)?
            .kind
            .columns()
            .iter()
            .map(|c| c.name.clone())
            .collect();
        // A seed merge with no existing rows: every row inserts.
        let mut merge = target_merge(name, VIEW, &[], &seed_columns, Case::Insensitive);
        merge.on_condition = spec::ExprWithSource {
            expr: spec::Expr::Literal(spec::Literal::Boolean { value: Some(false) }),
            source: None,
        };
        merge.clauses.truncate(3);
        if let spec::MergeClause::NotMatchedByTarget(clause) = &mut merge.clauses[2] {
            clause.condition = None;
        }
        merge.clauses.drain(0..2);
        run_command(ctx, config, spec::CommandNode::MergeInto(merge), None).await?;
    }
    Ok(())
}

pub(super) fn minimal_columns() -> Vec<spec::TableColumnDefinition> {
    vec![column("id", spec::DataType::Int32, true), metadata_column()]
}

pub(super) fn target_columns() -> Vec<spec::TableColumnDefinition> {
    vec![
        column("id", spec::DataType::Int32, true),
        column("value", spec::DataType::Utf8, true),
        metadata_column(),
    ]
}

async fn merge_aux(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    keys: &[UnqualifiedColumnName],
    microbatch: LogicalPlan,
) -> SparkResult<()> {
    publish(ctx, microbatch).await?;
    run_command(
        ctx,
        config,
        spec::CommandNode::MergeInto(auxiliary_merge(&aux_name(), VIEW, keys)),
        None,
    )
    .await
}

async fn merge_target(
    ctx: &SessionContext,
    config: Arc<PlanConfig>,
    keys: &[UnqualifiedColumnName],
    microbatch: LogicalPlan,
    case: Case,
) -> SparkResult<()> {
    let columns: Vec<String> = microbatch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    publish(ctx, microbatch).await?;
    run_command(
        ctx,
        config,
        spec::CommandNode::MergeInto(target_merge(&target_name(), VIEW, keys, &columns, case)),
        None,
    )
    .await
}

pub(super) fn id_key() -> SparkResult<Vec<UnqualifiedColumnName>> {
    Ok(vec![key("id")?])
}

// ---- mergeMicrobatchOntoAuxiliaryTable ----

#[tokio::test]
async fn aux_replaces_a_tombstone_with_a_newer_one_dropping_data_columns() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &aux_name(),
        minimal_columns(),
        Some(minimal(&[(1, (Some(10), None))])?),
    )
    .await?;
    merge_aux(
        &ctx,
        config.clone(),
        &id_key()?,
        valued(&[(1, "data-leak", (Some(20), None))])?,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &aux_name()).await?,
        vec![row(&["1", &meta("20", "NULL")])]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn aux_deletes_a_tombstone_superseded_by_a_newer_upsert() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &aux_name(),
        minimal_columns(),
        Some(minimal(&[(1, (Some(10), None))])?),
    )
    .await?;
    merge_aux(
        &ctx,
        config.clone(),
        &id_key()?,
        minimal(&[(1, (None, Some(20)))])?,
    )
    .await?;
    assert!(rows(&ctx, config.clone(), &aux_name()).await?.is_empty());
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn aux_inserts_a_tombstone_for_an_untracked_key() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(&ctx, config.clone(), &aux_name(), minimal_columns(), None).await?;
    merge_aux(
        &ctx,
        config.clone(),
        &id_key()?,
        minimal(&[(1, (Some(10), None))])?,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &aux_name()).await?,
        vec![row(&["1", &meta("10", "NULL")])]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn aux_leaves_unrelated_keys_untouched() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &aux_name(),
        minimal_columns(),
        Some(minimal(&[(1, (Some(10), None))])?),
    )
    .await?;
    merge_aux(
        &ctx,
        config.clone(),
        &id_key()?,
        minimal(&[(2, (Some(100), None))])?,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &aux_name()).await?,
        vec![
            row(&["1", &meta("10", "NULL")]),
            row(&["2", &meta("100", "NULL")])
        ]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn aux_ignores_older_deletes_and_older_upserts() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &aux_name(),
        minimal_columns(),
        Some(minimal(&[(1, (Some(10), None))])?),
    )
    .await?;
    merge_aux(
        &ctx,
        config.clone(),
        &id_key()?,
        minimal(&[(1, (Some(5), None))])?,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &aux_name()).await?,
        vec![row(&["1", &meta("10", "NULL")])]
    );
    merge_aux(
        &ctx,
        config.clone(),
        &id_key()?,
        minimal(&[(1, (None, Some(5)))])?,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &aux_name()).await?,
        vec![row(&["1", &meta("10", "NULL")])]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn aux_tied_sequences_keep_deletes_and_drop_tombstones_on_upserts() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &aux_name(),
        minimal_columns(),
        Some(minimal(&[(1, (Some(10), None)), (2, (Some(20), None))])?),
    )
    .await?;
    merge_aux(
        &ctx,
        config.clone(),
        &id_key()?,
        minimal(&[(1, (Some(10), None)), (2, (None, Some(20)))])?,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &aux_name()).await?,
        vec![row(&["1", &meta("10", "NULL")])]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn aux_upsert_for_a_different_key_keeps_the_tombstone() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &aux_name(),
        minimal_columns(),
        Some(minimal(&[(2, (Some(5), None))])?),
    )
    .await?;
    merge_aux(
        &ctx,
        config.clone(),
        &id_key()?,
        minimal(&[(1, (None, Some(10)))])?,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &aux_name()).await?,
        vec![row(&["2", &meta("5", "NULL")])]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn aux_merge_is_idempotent_across_every_clause() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &aux_name(),
        minimal_columns(),
        Some(minimal(&[
            (1, (Some(10), None)),
            (2, (Some(20), None)),
            (3, (Some(30), None)),
        ])?),
    )
    .await?;
    let batch = || {
        minimal(&[
            (1, (None, Some(15))),
            (2, (Some(25), None)),
            (4, (Some(40), None)),
        ])
    };
    let expected = vec![
        row(&["2", &meta("25", "NULL")]),
        row(&["3", &meta("30", "NULL")]),
        row(&["4", &meta("40", "NULL")]),
    ];
    merge_aux(&ctx, config.clone(), &id_key()?, batch()?).await?;
    assert_eq!(rows(&ctx, config.clone(), &aux_name()).await?, expected);
    merge_aux(&ctx, config.clone(), &id_key()?, batch()?).await?;
    assert_eq!(rows(&ctx, config.clone(), &aux_name()).await?, expected);
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn aux_inserts_tombstones_for_composite_keys() -> TestResult {
    let (ctx, config) = context()?;
    let composite = |rows: &[(&str, i32, Meta)]| -> SparkResult<LogicalPlan> {
        scan(vec![
            (
                "region",
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "customer_id",
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ),
            (
                METADATA_COLUMN,
                metadata(&rows.iter().map(|r| r.2).collect::<Vec<_>>()),
            ),
        ])
    };
    create_seeded(
        &ctx,
        config.clone(),
        &aux_name(),
        vec![
            column("region", spec::DataType::Utf8, true),
            column("customer_id", spec::DataType::Int32, true),
            metadata_column(),
        ],
        Some(composite(&[("US", 99, (Some(50), None))])?),
    )
    .await?;
    let keys = vec![key("region")?, key("customer_id")?];
    merge_aux(
        &ctx,
        config.clone(),
        &keys,
        composite(&[("US", 1, (Some(10), None))])?,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &aux_name()).await?,
        vec![
            row(&["US", "1", &meta("10", "NULL")]),
            row(&["US", "99", &meta("50", "NULL")])
        ]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn aux_merges_backticked_dotted_keys() -> TestResult {
    let (ctx, config) = context()?;
    let dotted = |rows: &[(i32, Meta)]| -> SparkResult<LogicalPlan> {
        scan(vec![
            (
                "user.id",
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                METADATA_COLUMN,
                metadata(&rows.iter().map(|r| r.1).collect::<Vec<_>>()),
            ),
        ])
    };
    create_seeded(
        &ctx,
        config.clone(),
        &aux_name(),
        vec![
            column("user.id", spec::DataType::Int32, true),
            metadata_column(),
        ],
        Some(dotted(&[(1, (Some(10), None))])?),
    )
    .await?;
    merge_aux(
        &ctx,
        config.clone(),
        &[key("`user.id`")?],
        dotted(&[(1, (Some(20), None))])?,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &aux_name()).await?,
        vec![row(&["1", &meta("20", "NULL")])]
    );
    finish(&ctx).await?;
    Ok(())
}

// ---- mergeMicrobatchOntoTarget ----

#[tokio::test]
async fn target_updates_an_existing_row_with_a_newer_upsert() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &target_name(),
        target_columns(),
        Some(valued(&[(1, "old", (None, Some(10)))])?),
    )
    .await?;
    merge_target(
        &ctx,
        config.clone(),
        &id_key()?,
        valued(&[(1, "new", (None, Some(20)))])?,
        Case::Insensitive,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &target_name()).await?,
        vec![row(&["1", "new", &meta("NULL", "20")])]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn target_deletes_an_existing_row_with_a_newer_delete() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &target_name(),
        target_columns(),
        Some(valued(&[
            (1, "delete-me", (None, Some(10))),
            (2, "keep-me", (None, Some(20))),
        ])?),
    )
    .await?;
    merge_target(
        &ctx,
        config.clone(),
        &id_key()?,
        valued(&[(1, "unused", (Some(15), None))])?,
        Case::Insensitive,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &target_name()).await?,
        vec![row(&["2", "keep-me", &meta("NULL", "20")])]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn target_inserts_new_upserts_but_not_tombstone_deletes() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &target_name(),
        target_columns(),
        Some(valued(&[(1, "existing", (None, Some(10)))])?),
    )
    .await?;
    merge_target(
        &ctx,
        config.clone(),
        &id_key()?,
        valued(&[
            (2, "insert-me", (None, Some(20))),
            (3, "do-not-insert", (Some(30), None)),
        ])?,
        Case::Insensitive,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &target_name()).await?,
        vec![
            row(&["1", "existing", &meta("NULL", "10")]),
            row(&["2", "insert-me", &meta("NULL", "20")])
        ]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn target_ignores_stale_upserts_and_tied_deletes() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &target_name(),
        target_columns(),
        Some(valued(&[
            (1, "target-delete-tie", (None, Some(10))),
            (2, "target-newer", (None, Some(20))),
        ])?),
    )
    .await?;
    merge_target(
        &ctx,
        config.clone(),
        &id_key()?,
        valued(&[
            (1, "delete-tie", (Some(10), None)),
            (2, "older-upsert", (None, Some(15))),
        ])?,
        Case::Insensitive,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &target_name()).await?,
        vec![
            row(&["1", "target-delete-tie", &meta("NULL", "10")]),
            row(&["2", "target-newer", &meta("NULL", "20")]),
        ]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn target_gives_tied_upserts_priority() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &target_name(),
        target_columns(),
        Some(valued(&[(1, "old", (None, Some(10)))])?),
    )
    .await?;
    merge_target(
        &ctx,
        config.clone(),
        &id_key()?,
        valued(&[(1, "same-sequence-upsert", (None, Some(10)))])?,
        Case::Insensitive,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &target_name()).await?,
        vec![row(&["1", "same-sequence-upsert", &meta("NULL", "10")])]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn target_matches_escaped_key_column_names() -> TestResult {
    let (ctx, config) = context()?;
    let escaped = |rows: &[(i32, &str, Meta)]| -> SparkResult<LogicalPlan> {
        scan(vec![
            (
                "a`b",
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
                METADATA_COLUMN,
                metadata(&rows.iter().map(|r| r.2).collect::<Vec<_>>()),
            ),
        ])
    };
    create_seeded(
        &ctx,
        config.clone(),
        &target_name(),
        vec![
            column("a`b", spec::DataType::Int32, true),
            column("value", spec::DataType::Utf8, true),
            metadata_column(),
        ],
        Some(escaped(&[(1, "old", (None, Some(10)))])?),
    )
    .await?;
    merge_target(
        &ctx,
        config.clone(),
        &[key("`a``b`")?],
        escaped(&[(1, "new", (None, Some(20)))])?,
        Case::Insensitive,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &target_name()).await?,
        vec![row(&["1", "new", &meta("NULL", "20")])]
    );
    finish(&ctx).await?;
    Ok(())
}

// gridTest: key comparison follows case sensitivity. Case-insensitive resolves
// `ID`; case-sensitive fails to resolve it (Spark: UNRESOLVED_COLUMN).
#[tokio::test]
async fn target_key_comparison_respects_case_sensitivity() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &target_name(),
        target_columns(),
        Some(valued(&[(1, "old", (None, Some(10)))])?),
    )
    .await?;
    merge_target(
        &ctx,
        config.clone(),
        &[key("ID")?],
        valued(&[(1, "new", (None, Some(20)))])?,
        Case::Insensitive,
    )
    .await?;
    assert_eq!(
        rows(&ctx, config.clone(), &target_name()).await?,
        vec![row(&["1", "new", &meta("NULL", "20")])]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn a_mis_cased_key_fails_a_case_sensitive_merge() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &target_name(),
        target_columns(),
        Some(valued(&[(1, "old", (None, Some(10)))])?),
    )
    .await?;
    let sensitive = Arc::new(PlanConfig {
        case_sensitive: true,
        ..config.as_ref().clone()
    });
    let refused = merge_target(
        &ctx,
        sensitive,
        &[key("ID")?],
        valued(&[(1, "new", (None, Some(20)))])?,
        Case::Sensitive,
    )
    .await;
    let message = format!("{:?}", refused.err().ok_or("the merge must fail")?);
    assert!(message.contains("UNRESOLVED_COLUMN"), "{message}");
    assert_eq!(
        rows(&ctx, config.clone(), &target_name()).await?,
        vec![row(&["1", "old", &meta("NULL", "10")])]
    );
    finish(&ctx).await?;
    Ok(())
}

#[tokio::test]
async fn target_merge_is_idempotent_across_a_microbatch() -> TestResult {
    let (ctx, config) = context()?;
    create_seeded(
        &ctx,
        config.clone(),
        &target_name(),
        target_columns(),
        Some(valued(&[
            (1, "delete-me", (None, Some(10))),
            (2, "update-me", (None, Some(20))),
            (3, "untouched", (None, Some(30))),
        ])?),
    )
    .await?;
    let batch = || {
        valued(&[
            (1, "delete-event", (Some(15), None)),
            (2, "updated", (None, Some(25))),
            (4, "inserted", (None, Some(40))),
            (5, "absent-delete", (Some(50), None)),
        ])
    };
    let expected = vec![
        row(&["2", "updated", &meta("NULL", "25")]),
        row(&["3", "untouched", &meta("NULL", "30")]),
        row(&["4", "inserted", &meta("NULL", "40")]),
    ];
    merge_target(
        &ctx,
        config.clone(),
        &id_key()?,
        batch()?,
        Case::Insensitive,
    )
    .await?;
    assert_eq!(rows(&ctx, config.clone(), &target_name()).await?, expected);
    merge_target(
        &ctx,
        config.clone(),
        &id_key()?,
        batch()?,
        Case::Insensitive,
    )
    .await?;
    assert_eq!(rows(&ctx, config.clone(), &target_name()).await?, expected);
    finish(&ctx).await?;
    Ok(())
}
