use std::collections::HashMap;

use super::StructuredSparkError;

/// SQL supplies an unquoted identifier; the protocol supplies its raw name.
/// The rejection's diagnostic spelling is independent of graph-key normalization.
pub fn multipart_flow_name_error(name: &str) -> Box<StructuredSparkError> {
    Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "MULTIPART_FLOW_NAME_NOT_SUPPORTED".into(),
        sql_state: "0A000".into(),
        message: format!("Flow with multipart name '{name}' is not supported."),
        parameters: HashMap::from([("flowName".into(), name.into())]),
    })
}

/// Spark's IdentifierHelper converts only one-, two- or three-part names to
/// TableIdentifier. This failure precedes the named-flow policy check.
pub fn pipeline_identifier_arity_error(parts: &[String]) -> Option<Box<StructuredSparkError>> {
    if (1..=3).contains(&parts.len()) {
        return None;
    }
    Some(Box::new(StructuredSparkError {
        class: "java.lang.UnsupportedOperationException".into(),
        condition: String::new(),
        sql_state: String::new(),
        message: format!(
            "4+ part table identifier {} is not supported.",
            parts.join(".")
        ),
        parameters: HashMap::new(),
    }))
}
