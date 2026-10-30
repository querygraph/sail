//! Pure persisted-view metadata conversion. Resolution and authority stay with
//! the caller; field names are requested lazily only without an explicit list.

use datafusion_common::DFSchema;
use sail_catalog::provider::CreateViewColumnOptions;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::scope::{VIEW_NAMESPACE_PART, VIEW_NAMESPACE_SIZE};

pub(super) fn columns(
    schema: &DFSchema,
    explicit: Option<Vec<spec::ViewColumnDefinition>>,
    names: impl FnOnce() -> PlanResult<Vec<String>>,
) -> PlanResult<Vec<CreateViewColumnOptions>> {
    if let Some(columns) = explicit {
        let field_count = schema.fields().len();
        if columns.len() != field_count {
            return Err(PlanError::AnalysisError(format!(
                "CREATE VIEW column list has {} columns, but the query produces {} columns",
                columns.len(),
                field_count
            )));
        }
        Ok(columns
            .into_iter()
            .zip(schema.fields().iter())
            .map(|(column, field)| {
                let spec::ViewColumnDefinition { name, comment } = column;
                CreateViewColumnOptions {
                    name,
                    data_type: field.data_type().clone(),
                    nullable: field.is_nullable(),
                    comment,
                }
            })
            .collect())
    } else {
        Ok(names()?
            .into_iter()
            .zip(schema.fields().iter())
            .map(|(name, field)| CreateViewColumnOptions {
                name,
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                comment: None,
            })
            .collect())
    }
}

pub(super) fn set_namespace(properties: &mut Vec<(String, String)>, namespace: Vec<String>) {
    // Use Spark's existing catalog properties, preserving unrelated entries and
    // their ordering. Caller resolves the namespace after column validation.
    properties
        .retain(|(key, _)| key != VIEW_NAMESPACE_SIZE && !key.starts_with(VIEW_NAMESPACE_PART));
    properties.push((VIEW_NAMESPACE_SIZE.to_string(), namespace.len().to_string()));
    properties.extend(
        namespace
            .into_iter()
            .enumerate()
            .map(|(index, part)| (format!("{VIEW_NAMESPACE_PART}{index}"), part)),
    );
}

#[cfg(test)]
mod tests;
