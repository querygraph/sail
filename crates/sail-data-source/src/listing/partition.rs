use datafusion::arrow::datatypes::FieldRef;
use datafusion::logical_expr::Expr;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Result, plan_err};

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
