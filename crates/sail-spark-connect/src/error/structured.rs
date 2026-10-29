use std::collections::HashMap;

use sail_common_datafusion::error::StructuredSparkError;
use tonic::Status;

pub(super) fn into_status(error: StructuredSparkError) -> Status {
    super::spark_status(
        &error.class,
        &error.to_string(),
        HashMap::from([
            ("errorClass".into(), error.condition),
            ("sqlState".into(), error.sql_state),
            (
                "messageParameters".into(),
                serde_json::json!(error.parameters).to_string(),
            ),
        ]),
    )
}

#[cfg(test)]
mod tests;
