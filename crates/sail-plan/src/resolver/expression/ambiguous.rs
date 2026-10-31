//! Diagnostics for ambiguity already established by native root resolution.
use std::collections::HashMap;

use datafusion_common::TableReference;
use sail_common::spec;
use sail_common_datafusion::error::{StructuredSparkError, quote_sql_identifier};

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) fn ambiguous_attribute(
        &self,
        name: &spec::ObjectName,
        root: &str,
        qualifiers: Vec<Option<&TableReference>>,
    ) -> PlanResult<PlanError> {
        // Spark's UnresolvedAttribute.name quotes parts containing dots; its
        // diagnostic toSQLId(String) then parses that name again.
        let unresolved = name
            .parts()
            .iter()
            .map(|part| {
                let part = part.as_ref();
                if part.contains('.') {
                    format!("`{part}`")
                } else {
                    part.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(".");
        let name = quote_sql_identifier(&unresolved)?;
        let mut references = qualifiers
            .into_iter()
            .map(|qualifier| {
                let mut parts = match qualifier {
                    None => vec![],
                    Some(TableReference::Bare { table }) => vec![table.as_ref()],
                    Some(TableReference::Partial { schema, table }) => {
                        vec![schema.as_ref(), table.as_ref()]
                    }
                    Some(TableReference::Full {
                        catalog,
                        schema,
                        table,
                    }) => vec![catalog.as_ref(), schema.as_ref(), table.as_ref()],
                };
                // AttributeSeq collects matches with the requested spelling, even
                // when two different attributes then display the same qualified name.
                parts.push(root);
                let qualified = parts
                    .iter()
                    .map(|part| format!("`{}`", part.replace('`', "``")))
                    .collect::<Vec<_>>()
                    .join(".");
                quote_sql_identifier(&qualified)
            })
            .collect::<Result<Vec<_>, _>>()?;
        references.sort_by(|left, right| left.encode_utf16().cmp(right.encode_utf16()));
        let references = format!("[{}]", references.join(", "));
        Ok(PlanError::Structured(Box::new(StructuredSparkError {
            class: "org.apache.spark.sql.AnalysisException".into(),
            condition: "AMBIGUOUS_REFERENCE".into(),
            sql_state: "42704".into(),
            message: format!("Reference {name} is ambiguous, could be: {references}."),
            parameters: HashMap::from([
                ("name".into(), name),
                ("referenceNames".into(), references),
            ]),
        })))
    }
}

#[cfg(test)]
#[path = "ambiguous_tests.rs"]
mod tests;
