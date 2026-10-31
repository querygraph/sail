use std::collections::HashMap;

use datafusion_common::DataFusionError;

use super::{StructuredSparkError, quote_sql_identifier};

/// Preserve Spark's diagnostic for an explicit catalog load.
pub fn catalog_not_found_error(name: &str) -> DataFusionError {
    let catalog = match quote_sql_identifier(name) {
        Ok(catalog) => catalog,
        Err(error) => return error,
    };
    let config = format!("\"spark.sql.catalog.{name}\"");
    DataFusionError::External(Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.connector.catalog.CatalogNotFoundException".into(),
        condition: "CATALOG_NOT_FOUND".into(),
        sql_state: "42P08".into(),
        message: format!(
            "The catalog {catalog} not found. Consider to set the SQL config {config} to a catalog plugin."
        ),
        parameters: HashMap::from([("catalogName".into(), catalog), ("config".into(), config)]),
    }))
}
