use std::collections::HashMap;

use sail_catalog::utils::quote_name_if_needed;
use sail_common_datafusion::catalog::{CatalogPartitionField, PartitionTransform};

use crate::error::{SparkError, StructuredSparkError};

pub(super) fn partition_and_cluster() -> SparkError {
    Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED".into(),
        sql_state: "42908".into(),
        message: "Cannot specify both CLUSTER BY and PARTITIONED BY.".into(),
        parameters: HashMap::new(),
    })
    .into()
}

pub(super) fn partition_change(
    existing: &[CatalogPartitionField],
    requested: &[String],
) -> SparkError {
    let existing = existing
        .iter()
        .map(transform)
        .collect::<Vec<_>>()
        .join(", ");
    let requested = requested
        .iter()
        .map(|column| {
            transform(&CatalogPartitionField {
                column: column.clone(),
                transform: Some(PartitionTransform::Identity),
            })
        })
        .collect::<Vec<_>>()
        .join(", ");
    Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "CANNOT_UPDATE_PARTITION_COLUMNS".into(),
        sql_state: "42000".into(),
        message: format!(
            "Declared partitioning {requested} conflicts with existing table partitioning {existing}.\n\
             Please delete the table or change the declared partitioning to match its partitions."
        ),
        parameters: HashMap::from([
            ("existingPartitionColumns".into(), existing),
            ("requestedPartitionColumns".into(), requested),
        ]),
    })
    .into()
}

// Catalog partition fields carry decoded column names. Reuse Sail's Spark
// identifier quoting and Spark Transform.toString spelling for diagnostics.
fn transform(field: &CatalogPartitionField) -> String {
    let column = quote_name_if_needed(&field.column);
    match field.transform.unwrap_or_default() {
        PartitionTransform::Identity => format!("identity({column})"),
        PartitionTransform::Year => format!("years({column})"),
        PartitionTransform::Month => format!("months({column})"),
        PartitionTransform::Day => format!("days({column})"),
        PartitionTransform::Hour => format!("hours({column})"),
        PartitionTransform::Bucket(n) => format!("bucket({n}, {column})"),
        PartitionTransform::Truncate(n) => format!("truncate({n}, {column})"),
    }
}

pub(super) fn cluster_change(existing: &[String], requested: &[String]) -> SparkError {
    let format = |columns: &[String]| format!("[{}]", columns.join(", "));
    let (existing, requested) = (format(existing), format(requested));
    Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "CANNOT_UPDATE_PARTITION_COLUMNS".into(),
        sql_state: "42000".into(),
        message: format!(
            "Declared clustering {requested} conflicts with existing table clustering {existing}.\n\
             Please delete the table or change the declared clustering to match its columns."
        ),
        parameters: HashMap::from([
            ("existingPartitionColumns".into(), existing),
            ("requestedPartitionColumns".into(), requested),
        ]),
    })
    .into()
}

pub(super) fn missing_cluster_column(column: &str, table: &[String]) -> SparkError {
    SparkError::invalid(format!(
        "Cluster column `{column}` is not defined in the table {}.",
        table.join(".")
    ))
}

pub(super) fn all_partition_columns() -> SparkError {
    Box::new(StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "ALL_PARTITION_COLUMNS_NOT_ALLOWED".into(),
        sql_state: "KD005".into(),
        message: "Cannot use all columns for partition columns.".into(),
        parameters: HashMap::new(),
    })
    .into()
}

/// Reported by the flow, on each attempt, as Spark's table write does.
pub(in crate::pipelines) fn missing_partition_column(
    column: &str,
    table: &[String],
    columns: &[String],
) -> StructuredSparkError {
    let quote = |name: &String| format!("`{name}`");
    let table = table.iter().map(quote).collect::<Vec<_>>().join(".");
    let defined = columns.iter().map(quote).collect::<Vec<_>>().join(", ");
    StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "COLUMN_NOT_DEFINED_IN_TABLE".into(),
        sql_state: "42703".into(),
        message: format!(
            "partition column `{column}` is not defined in table {table}, defined table columns \
             are: {defined}."
        ),
        parameters: HashMap::from([
            ("colName".into(), format!("`{column}`")),
            ("tableName".into(), table),
            ("tableCols".into(), defined),
        ]),
    }
}

pub(in crate::pipelines) fn ambiguous_reference(column: &str) -> StructuredSparkError {
    let names = format!("[`{column}`, `{column}`]");
    StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "AMBIGUOUS_REFERENCE".into(),
        sql_state: "42704".into(),
        message: format!("Reference `{column}` is ambiguous, could be: {names}."),
        parameters: HashMap::from([
            ("name".into(), format!("`{column}`")),
            ("referenceNames".into(), names),
        ]),
    }
}
