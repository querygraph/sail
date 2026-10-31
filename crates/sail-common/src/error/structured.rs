use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Spark error details preserved independently of their transport representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredSparkError {
    pub class: String,
    /// Empty when Spark defines no error condition for an unclassified exception.
    pub condition: String,
    /// Empty when Spark defines no SQLSTATE (for example, legacy parser errors).
    pub sql_state: String,
    pub message: String,
    pub parameters: HashMap<String, String>,
}

impl fmt::Display for StructuredSparkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.condition.is_empty() {
            write!(f, "[{}] ", self.condition)?;
        }
        f.write_str(&self.message)
    }
}

impl std::error::Error for StructuredSparkError {}
