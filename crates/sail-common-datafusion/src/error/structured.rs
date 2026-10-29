use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Spark error details preserved independently of their transport representation.
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
#[error("[{condition}] {message}")]
pub struct StructuredSparkError {
    pub class: String,
    pub condition: String,
    pub sql_state: String,
    pub message: String,
    pub parameters: HashMap<String, String>,
}
