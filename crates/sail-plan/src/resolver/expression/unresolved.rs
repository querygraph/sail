//! Structured missing-column diagnostics after native attribute resolution fails.
use std::collections::{HashMap, HashSet};

use datafusion_common::{DFSchemaRef, TableReference};
use sail_common::spec;
use sail_common_datafusion::error::StructuredSparkError;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) fn unresolved_attribute(
        &self,
        name: &spec::ObjectName,
        schema: &DFSchemaRef,
        state: &PlanResolverState,
    ) -> PlanResult<PlanError> {
        let mut candidates = vec![];
        let mut seen = HashSet::new();
        for (qualifier, field) in schema.iter() {
            let info = state.get_field_info(field.name())?;
            if info.is_hidden() || !seen.insert((qualifier.cloned(), info.origin())) {
                continue;
            }
            let mut parts = match qualifier {
                None => vec![],
                Some(TableReference::Bare { table }) => vec![table.to_string()],
                Some(TableReference::Partial { schema, table }) => {
                    vec![schema.to_string(), table.to_string()]
                }
                Some(TableReference::Full {
                    catalog,
                    schema,
                    table,
                }) => {
                    vec![catalog.to_string(), schema.to_string(), table.to_string()]
                }
            };
            parts.push(info.name().to_string());
            candidates.push(parts);
        }
        let parts = name
            .parts()
            .iter()
            .map(|p| p.as_ref().to_string())
            .collect::<Vec<_>>();
        let object = quoted(&parts);
        let proposals = suggestions(&parts, &candidates);
        let mut parameters = HashMap::from([("objectName".into(), object.clone())]);
        let message = format!(
            "A column, variable, or function parameter with name {object} cannot be resolved."
        );
        let (condition, message) = if proposals.is_empty() {
            ("UNRESOLVED_COLUMN.WITHOUT_SUGGESTION", message)
        } else {
            let proposal = proposals.into_iter().take(5).collect::<Vec<_>>().join(", ");
            parameters.insert("proposal".into(), proposal.clone());
            (
                "UNRESOLVED_COLUMN.WITH_SUGGESTION",
                format!("{message} Did you mean one of the following? [{proposal}]."),
            )
        };
        Ok(PlanError::Structured(Box::new(StructuredSparkError {
            class: "org.apache.spark.sql.AnalysisException".into(),
            condition: condition.into(),
            sql_state: "42703".into(),
            message,
            parameters,
        })))
    }
}

fn quoted(parts: &[String]) -> String {
    parts
        .iter()
        .map(|part| format!("`{}`", part.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".")
}

fn common_prefix(candidates: &[Vec<String>], tail: usize) -> bool {
    let Some(first) = candidates.first() else {
        return false;
    };
    let prefix = &first[..first.len().saturating_sub(tail)];
    candidates
        .iter()
        .all(|parts| &parts[..parts.len().saturating_sub(tail)] == prefix)
}

/// Spark strips shared qualifiers before stable distance ordering, not afterwards.
fn suggestions(base: &[String], candidates: &[Vec<String>]) -> Vec<String> {
    let tail = if base.len() == 1 && common_prefix(candidates, 1) {
        1
    } else if base.len() <= 2 && common_prefix(candidates, 2) {
        2
    } else {
        usize::MAX
    };
    // UnresolvedAttribute.sql quotes only parts that are not plain identifiers.
    let base = base
        .iter()
        .map(|part| {
            let mut chars = part.chars();
            let simple = chars
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
            if simple {
                part.clone()
            } else {
                quoted(std::slice::from_ref(part))
            }
        })
        .collect::<Vec<_>>()
        .join(".");
    let mut values = candidates
        .iter()
        .map(|parts| quoted(&parts[parts.len().saturating_sub(tail)..]))
        .collect::<Vec<_>>();
    values.sort_by_cached_key(|candidate| distance(candidate, &base));
    values
}

/// Apache Commons Text's Levenshtein implementation compares Java UTF-16 units.
fn distance(left: &str, right: &str) -> usize {
    let right = right.encode_utf16().collect::<Vec<_>>();
    let mut row = (0..=right.len()).collect::<Vec<_>>();
    for (i, left) in left.encode_utf16().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for (j, right) in right.iter().enumerate() {
            let old = row[j + 1];
            row[j + 1] = (row[j] + 1)
                .min(old + 1)
                .min(diagonal + usize::from(left != *right));
            diagonal = old;
        }
    }
    row[right.len()]
}

#[cfg(test)]
#[path = "unresolved_tests.rs"]
mod tests;
