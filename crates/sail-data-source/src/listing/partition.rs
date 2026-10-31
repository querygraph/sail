use std::sync::Arc;

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::logical_expr::Expr;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Result, plan_err};
use sail_common::spec;

fn matches_name(left: &str, right: &str, case_sensitive: bool) -> bool {
    if case_sensitive {
        left == right
    } else {
        left.eq_ignore_ascii_case(right)
    }
}

/// Current shared partition type inference remains STRING. Keep this limitation
/// separate from choosing directory columns and binding their logical names.
pub(super) fn inferred_partition_fields(names: &[String]) -> Vec<FieldRef> {
    names
        .iter()
        .map(|name| Arc::new(Field::new(name, DataType::Utf8, true)))
        .collect()
}

fn auto_partition_field(field: &Field) -> FieldRef {
    // Spark creates a fresh nullable StructField and drops column metadata.
    // These Arrow keys encode its DataType in Sail; child fields are already
    // part of the DataType and must retain their own metadata/nullability.
    let metadata = field
        .metadata()
        .iter()
        .filter(|(key, _)| {
            matches!(
                key.as_str(),
                spec::SAIL_SPARK_TIME_METADATA_KEY
                    | spec::SAIL_SPARK_INTERVAL_METADATA_KEY
                    | spec::SAIL_SPARK_UDT_METADATA_KEY
                    | EXTENSION_TYPE_NAME_KEY
                    | EXTENSION_TYPE_METADATA_KEY
            )
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    Arc::new(Field::new(field.name(), field.data_type().clone(), true).with_metadata(metadata))
}

/// Split a supplied schema using discovered partitions and the captured resolver.
/// An explicit selector first uses a supplied field, then a discovered field.
/// Auto-discovery retains every directory column, including omitted fields.
pub(super) fn prepare_explicit_partitions(
    schema: &Schema,
    discovered: &[String],
    selectors: &[String],
    case_sensitive: bool,
) -> Result<(Schema, Vec<FieldRef>)> {
    let inferred = inferred_partition_fields(discovered);
    let partitions = if selectors.is_empty() {
        inferred
            .into_iter()
            .map(|inferred| {
                schema
                    .fields()
                    .iter()
                    .find(|field| matches_name(field.name(), inferred.name(), case_sensitive))
                    .map(|field| auto_partition_field(field))
                    .unwrap_or(inferred)
            })
            .collect::<Vec<_>>()
    } else {
        selectors
            .iter()
            .map(|name| {
                schema
                    .fields()
                    .iter()
                    .chain(inferred.iter())
                    .find(|field| matches_name(field.name(), name, case_sensitive))
                    .cloned()
                    .ok_or_else(|| {
                        datafusion_common::plan_datafusion_err!(
                            "partition column {name} is absent from the supplied schema and discovered directories"
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?
    };
    Ok((
        without_partition_fields(schema, &partitions, case_sensitive),
        partitions,
    ))
}

/// Directory partition values replace matching physical file columns. Keep this
/// preparation identical for supplied and inferred schemas.
pub(super) fn without_partition_fields(
    schema: &Schema,
    partitions: &[FieldRef],
    case_sensitive: bool,
) -> Schema {
    let file_fields = schema
        .fields()
        .iter()
        .filter(|field| {
            !partitions
                .iter()
                .any(|partition| matches_name(field.name(), partition.name(), case_sensitive))
        })
        .cloned()
        .collect::<Vec<_>>();
    Schema::new_with_metadata(file_fields, schema.metadata().clone())
}

/// Resolve the fields selected by existing schema preparation back to observed
/// directory keys. An explicit partition selector may use a different spelling
/// from both the selected field and its directory. This does not change the
/// schema preparation policy that chooses the logical fields.
pub(super) fn partition_directory_names(
    fields: &[FieldRef],
    discovered: &[String],
) -> Result<Vec<String>> {
    fields
        .iter()
        .map(|field| {
            if let Some(name) = discovered.iter().find(|name| *name == field.name()) {
                return Ok(name.clone());
            }
            let mut matches = discovered
                .iter()
                .filter(|name| name.eq_ignore_ascii_case(field.name()));
            let name = matches.next();
            if matches.next().is_some() {
                return plan_err!("ambiguous partition directory key for {}", field.name());
            }
            // Retain the selected field name when discovery has no match so
            // existing layout validation still reports the mismatch.
            Ok(name.unwrap_or_else(|| field.name()).clone())
        })
        .collect()
}

/// DataFusion's directory parser and pruning expressions must use the same
/// physical keys. Logical names remain in the table and scan schemas.
pub(super) fn rebind_partition_filters(
    filters: &[Expr],
    fields: &[FieldRef],
    directory_names: &[String],
) -> Result<Vec<Expr>> {
    filters
        .iter()
        .map(|filter| {
            filter
                .clone()
                .transform_up(|expr| {
                    let Expr::Column(mut column) = expr else {
                        return Ok(Transformed::no(expr));
                    };
                    let Some((_, name)) = fields
                        .iter()
                        .zip(directory_names)
                        .find(|(field, _)| field.name() == &column.name)
                    else {
                        return Ok(Transformed::no(Expr::Column(column)));
                    };
                    if name == &column.name {
                        return Ok(Transformed::no(Expr::Column(column)));
                    }
                    column.name = name.clone();
                    Ok(Transformed::yes(Expr::Column(column)))
                })
                .map(|result| result.data)
        })
        .collect()
}

#[cfg(test)]
#[path = "partition_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "partition_preparation_tests.rs"]
mod preparation_tests;
