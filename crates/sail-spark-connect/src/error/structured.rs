use std::collections::HashMap;

use sail_common_datafusion::error::StructuredSparkError;
use tonic::Status;

pub(super) fn into_status(error: StructuredSparkError) -> Status {
    let mut metadata = HashMap::from([
        ("errorClass".into(), error.condition.clone()),
        (
            "messageParameters".into(),
            serde_json::json!(error.parameters).to_string(),
        ),
    ]);
    if !error.sql_state.is_empty() {
        metadata.insert("sqlState".into(), error.sql_state.clone());
    }
    super::spark_status(&error.class, &error.to_string(), metadata)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod pipeline_tests;
