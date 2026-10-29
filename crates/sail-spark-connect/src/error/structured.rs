use std::collections::HashMap;

use thiserror::Error;
use tonic::Status;

/// Spark error details consumed by Connect clients independently of the message.
#[derive(Debug, Error)]
#[error("[{condition}] {message}")]
pub struct StructuredSparkError {
    pub(crate) class: &'static str,
    pub(crate) condition: &'static str,
    pub(crate) sql_state: &'static str,
    pub(crate) message: String,
    pub(crate) parameters: HashMap<String, String>,
}

impl From<StructuredSparkError> for Status {
    fn from(error: StructuredSparkError) -> Self {
        super::spark_status(
            error.class,
            &error.to_string(),
            HashMap::from([
                ("errorClass".into(), error.condition.into()),
                ("sqlState".into(), error.sql_state.into()),
                (
                    "messageParameters".into(),
                    serde_json::json!(error.parameters).to_string(),
                ),
            ]),
        )
    }
}
