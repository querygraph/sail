use datafusion_common::{DataFusionError, Result};

/// Format a Spark diagnostic identifier, preserving typed malformed-name errors.
pub fn quote_sql_identifier(name: &str) -> Result<String> {
    sail_common::error::quote_sql_identifier(name).map_err(|error| DataFusionError::External(error))
}
