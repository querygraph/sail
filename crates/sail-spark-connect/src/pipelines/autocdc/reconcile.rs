//! `Scd1BatchProcessor.reconcileMicrobatch` (`autocdc/Scd1BatchProcessor.scala`,
//! Spark 4.2.1 at `617618396`) as pure `LogicalPlan` transformations:
//! deduplicate (see [`super::dedup`]), extend each row with the CDC metadata
//! struct (`:131–150`), project the user's column selection and re-append the
//! metadata (`:152–200`), and drop rows a tombstone in the auxiliary table
//! supersedes (`:202–248`). The caller resolves the sequencing and delete
//! expressions against the batch schema; nothing here reads a table or
//! writes one. The merges that follow are a separate unit.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, JoinType, ScalarValue};
use datafusion::functions::core::expr_fn::get_field;
use datafusion::functions::expr_fn::greatest;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, ScalarUDF};
use datafusion::prelude::{lit, when};
use sail_function::scalar::struct_function::StructFunction;

use super::schema::{Case, DELETE_SEQUENCE_FIELD, METADATA_COLUMN, UPSERT_SEQUENCE_FIELD};
use super::{ColumnSelection, UnqualifiedColumnName, dedup};
use crate::error::{SparkError, SparkResult};

/// The column at a batch field index, qualified as the plan qualifies it.
fn column_at(plan: &LogicalPlan, index: usize) -> Expr {
    let (qualifier, field) = plan.schema().qualified_field(index);
    Expr::Column(Column::from((qualifier, field)))
}

/// The index of the field `name` resolves to under the session case setting.
fn field_index(plan: &LogicalPlan, name: &str, case: Case) -> SparkResult<usize> {
    let resolver = case.resolver();
    plan.schema()
        .fields()
        .iter()
        .position(|field| resolver(field.name(), name))
        .ok_or_else(|| {
            SparkError::internal(format!(
                "AutoCDC column '{name}' is missing from the microbatch schema"
            ))
        })
}

/// `extendMicrobatchRowsWithCdcMetadata`: `deleteSequence` is the sequencing
/// value where the delete condition holds (null otherwise, and null for every
/// row without a delete condition); `upsertSequence` is the sequencing value
/// where `deleteSequence` is null; both are cast to the resolved sequencing
/// type and packed into the metadata column appended last.
pub(super) fn extend_with_metadata(
    batch: LogicalPlan,
    sequencing: Expr,
    delete_condition: Option<Expr>,
    sequencing_type: &DataType,
) -> SparkResult<LogicalPlan> {
    let null = || lit(ScalarValue::Null);
    let delete_sequence = match delete_condition {
        Some(condition) => when(condition, sequencing.clone()).otherwise(null())?,
        None => null(),
    };
    let upsert_sequence = when(delete_sequence.clone().is_null(), sequencing).otherwise(null())?;
    let schema = batch.schema();
    let members = vec![
        delete_sequence.cast_to(sequencing_type, schema)?,
        upsert_sequence.cast_to(sequencing_type, schema)?,
    ];
    let metadata = Expr::ScalarFunction(ScalarFunction {
        func: Arc::new(ScalarUDF::from(StructFunction::new(vec![
            DELETE_SEQUENCE_FIELD.to_string(),
            UPSERT_SEQUENCE_FIELD.to_string(),
        ]))),
        args: members,
    })
    .alias(METADATA_COLUMN);
    let mut exprs: Vec<Expr> = (0..schema.fields().len())
        .map(|index| column_at(&batch, index))
        .collect();
    exprs.push(metadata);
    Ok(LogicalPlanBuilder::from(batch).project(exprs)?.build()?)
}

/// `projectTargetColumnsOntoMicrobatch`: the user columns are the batch
/// without the metadata column; the flow's column selection applies to them
/// under the session case setting, in batch order; the metadata column is
/// re-appended last.
pub(super) fn project_selected(
    batch: LogicalPlan,
    selection: Option<&ColumnSelection>,
    case: Case,
) -> SparkResult<LogicalPlan> {
    let names: Vec<String> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let metadata_excluded =
        ColumnSelection::Exclude(vec![UnqualifiedColumnName::from_parts(vec![
            METADATA_COLUMN.to_string(),
        ])?]);
    let user = ColumnSelection::apply(Some(&metadata_excluded), "microbatch", &names, case)?;
    let user_names: Vec<String> = user.iter().map(|index| names[*index].clone()).collect();
    let selected = ColumnSelection::apply(selection, "microbatch", &user_names, case)?;
    let mut exprs: Vec<Expr> = selected
        .iter()
        .map(|index| column_at(&batch, user[*index]))
        .collect();
    exprs.push(column_at(
        &batch,
        field_index(&batch, METADATA_COLUMN, case)?,
    ));
    Ok(LogicalPlanBuilder::from(batch).project(exprs)?.build()?)
}

/// `applyTombstonesToMicrobatch`: a left anti join of the batch against the
/// auxiliary table on every key equal and
/// `greatest(deleteSequence, upsertSequence) < auxiliary.deleteSequence`.
/// A tie keeps the row; a null tombstone keeps the row; the auxiliary
/// `upsertSequence` is ignored.
pub(super) fn apply_tombstones(
    batch: LogicalPlan,
    auxiliary: LogicalPlan,
    keys: &[UnqualifiedColumnName],
    case: Case,
) -> SparkResult<LogicalPlan> {
    let batch = LogicalPlanBuilder::from(batch)
        .alias("microbatch")?
        .build()?;
    let auxiliary = LogicalPlanBuilder::from(auxiliary)
        .alias("auxiliaryTable")?
        .build()?;
    let batch_metadata = column_at(&batch, field_index(&batch, METADATA_COLUMN, case)?);
    let auxiliary_metadata = column_at(&auxiliary, field_index(&auxiliary, METADATA_COLUMN, case)?);
    let effective_sequence = greatest(vec![
        get_field(batch_metadata.clone(), DELETE_SEQUENCE_FIELD),
        get_field(batch_metadata, UPSERT_SEQUENCE_FIELD),
    ]);
    let tombstone_sequence = get_field(auxiliary_metadata, DELETE_SEQUENCE_FIELD);
    let mut keys_match: Option<Expr> = None;
    for key in keys {
        let left = column_at(&batch, field_index(&batch, key.name(), case)?);
        let right = column_at(&auxiliary, field_index(&auxiliary, key.name(), case)?);
        let equal = left.eq(right);
        keys_match = Some(match keys_match {
            Some(previous) => previous.and(equal),
            None => equal,
        });
    }
    let keys_match =
        keys_match.ok_or_else(|| SparkError::internal("AutoCDC flow without key columns"))?;
    let superseded = keys_match.and(effective_sequence.lt(tombstone_sequence));
    Ok(LogicalPlanBuilder::from(batch)
        .join_on(auxiliary, JoinType::LeftAnti, [superseded])?
        .build()?)
}

/// `reconcileMicrobatch`: deduplicate, extend, project, apply tombstones.
/// `sequencing` and `delete_condition` are resolved against the batch
/// schema by the caller; keys and the column selection come from the
/// flow's change arguments.
pub(super) fn reconcile(
    batch: LogicalPlan,
    auxiliary: LogicalPlan,
    keys: &[UnqualifiedColumnName],
    selection: Option<&ColumnSelection>,
    sequencing: Expr,
    delete_condition: Option<Expr>,
    sequencing_type: &DataType,
    case: Case,
) -> SparkResult<LogicalPlan> {
    let key_indices = keys
        .iter()
        .map(|key| field_index(&batch, key.name(), case))
        .collect::<SparkResult<Vec<_>>>()?;
    let deduplicated = dedup::deduplicate(batch, &key_indices, sequencing.clone())?;
    let extended =
        extend_with_metadata(deduplicated, sequencing, delete_condition, sequencing_type)?;
    let projected = project_selected(extended, selection, case)?;
    apply_tombstones(projected, auxiliary, keys, case)
}
