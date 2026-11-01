//! `SHOW CREATE TABLE`, following Spark's `ShowCreateTableCommandBase`.

use std::collections::HashMap;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::DataFusionError;
use sail_common_datafusion::catalog::{
    CLUSTERING_COLUMNS_PROPERTY, CatalogPartitionField, PartitionTransform, TableColumnStatus,
    TableKind, TableStatus,
};
use sail_common_datafusion::datasource::is_lakehouse_format;
use sail_common_datafusion::error::StructuredSparkError;
use sail_common_datafusion::session::plan::PlanFormatter;

use crate::error::{CatalogError, CatalogResult};
use crate::utils::{quote_name_if_needed, quote_names_if_needed};

const OPTION_PREFIX: &str = "option.";
/// Reserved keys that are stated in their own clause.
const RESERVED_PROPERTIES: [&str; 2] = ["comment", "provider"];
/// `CatalogTable.VIEW_PREFIX`, which Spark leaves out of a view's statement.
const VIEW_PROPERTY_PREFIX: &str = "view.";

pub fn show_create_table(
    name: &[String],
    status: &TableStatus,
    as_serde: bool,
    formatter: &dyn PlanFormatter,
) -> CatalogResult<String> {
    if status.kind.is_temporary() {
        return Err(temporary_view(name));
    }
    match &status.kind {
        TableKind::Table { .. } if as_serde => Err(data_source_as_serde(status)),
        TableKind::Table { .. } => show_table(status, formatter),
        _ => Ok(show_view(status)),
    }
}

fn show_table(status: &TableStatus, formatter: &dyn PlanFormatter) -> CatalogResult<String> {
    let TableKind::Table {
        comment,
        location,
        format,
        partition_by,
        sort_by,
        bucket_by,
        properties,
        is_external,
        ..
    } = &status.kind
    else {
        return Err(CatalogError::Internal("expected a table".to_string()));
    };
    // Spark keeps file table columns nullable, whatever was declared.
    let keep_nullability = is_lakehouse_format(format);
    let columns = status
        .kind
        .columns()
        .iter()
        .map(|column| column_definition(column, keep_nullability, formatter))
        .collect::<CatalogResult<Vec<_>>>()?;

    let mut output = format!("CREATE TABLE {} ", qualified_name(status));
    output.push_str(&multiline(&columns));
    output.push_str(&format!("USING {format}\n"));

    let mut options = vec![];
    let mut table_properties = vec![];
    let mut clustering = vec![];
    for (key, value) in properties {
        if let Some(option) = key.strip_prefix(OPTION_PREFIX) {
            options.push((option.to_string(), value.clone()));
        } else if key == CLUSTERING_COLUMNS_PROPERTY {
            clustering = value.split(',').map(str::to_string).collect();
            table_properties.push((key.clone(), clustering_property(&clustering)?));
        } else if !RESERVED_PROPERTIES.contains(&key.as_str()) {
            table_properties.push((key.clone(), value.clone()));
        }
    }
    options.retain(|(key, _)| !key.eq_ignore_ascii_case("path"));
    if !options.is_empty() {
        output.push_str("OPTIONS ");
        output.push_str(&multiline(&key_values(options)));
    }
    let mut partitions = partition_by.iter().map(partition).collect::<Vec<_>>();
    if !clustering.is_empty() {
        partitions.push(format!("cluster_by({})", column_list(&clustering)));
    }
    if !partitions.is_empty() {
        output.push_str(&format!("PARTITIONED BY ({})\n", partitions.join(", ")));
    }
    if let Some(bucket_by) = bucket_by {
        output.push_str(&format!(
            "CLUSTERED BY ({})\n",
            column_list(&bucket_by.columns)
        ));
        if !sort_by.is_empty() {
            let columns = sort_by.iter().map(|x| x.column.clone()).collect::<Vec<_>>();
            output.push_str(&format!("SORTED BY ({})\n", column_list(&columns)));
        }
        output.push_str(&format!("INTO {} BUCKETS\n", bucket_by.num_buckets));
    }
    if let Some(location) = location.as_ref().filter(|_| *is_external) {
        output.push_str(&format!("LOCATION '{}'\n", escape(location)));
    }
    if let Some(comment) = comment {
        output.push_str(&format!("COMMENT '{}'\n", escape(comment)));
    }
    if !table_properties.is_empty() {
        output.push_str("TBLPROPERTIES ");
        output.push_str(&multiline(&key_values(table_properties)));
    }
    Ok(output)
}

/// Spark names a view without its catalog and always states its schema binding.
fn show_view(status: &TableStatus) -> String {
    let name: Vec<&str> = status
        .database
        .iter()
        .map(String::as_str)
        .chain([status.name.as_str()])
        .collect();
    let mut output = format!("CREATE VIEW {} ", quote_names_if_needed(&name));
    let columns = status
        .kind
        .columns()
        .iter()
        .map(|column| {
            let comment = column
                .comment
                .as_ref()
                .map(|comment| format!(" COMMENT '{}'", escape(comment)))
                .unwrap_or_default();
            format!("{}{comment}", quote_name_if_needed(&column.name))
        })
        .collect::<Vec<_>>();
    if !columns.is_empty() {
        output.push_str(&multiline(&columns));
    }
    if let Some(comment) = status.kind.comment() {
        output.push_str(&format!("COMMENT '{}'\n", escape(&comment)));
    }
    let properties = status
        .kind
        .properties()
        .iter()
        .filter(|(key, _)| {
            !RESERVED_PROPERTIES.contains(&key.as_str()) && !key.starts_with(VIEW_PROPERTY_PREFIX)
        })
        .cloned()
        .collect::<Vec<_>>();
    if !properties.is_empty() {
        output.push_str("TBLPROPERTIES ");
        output.push_str(&multiline(&key_values(properties)));
    }
    output.push_str("WITH SCHEMA COMPENSATION\n");
    output.push_str(&format!(
        "AS {}\n",
        status.kind.view_definition().unwrap_or_default()
    ));
    output
}

fn qualified_name(status: &TableStatus) -> String {
    let name: Vec<&str> = status
        .catalog
        .iter()
        .chain(&status.database)
        .map(String::as_str)
        .chain([status.name.as_str()])
        .collect();
    quote_names_if_needed(&name)
}

fn column_definition(
    column: &TableColumnStatus,
    keep_nullability: bool,
    formatter: &dyn PlanFormatter,
) -> CatalogResult<String> {
    let mut output = format!(
        "{} {}",
        quote_name_if_needed(&column.name),
        sql_type(&column.data_type, formatter)?
    );
    if keep_nullability && !column.nullable {
        output.push_str(" NOT NULL");
    }
    if let Some(default) = &column.default {
        output.push_str(&format!(" DEFAULT {default}"));
    }
    if let Some(expression) = &column.generated_always_as {
        output.push_str(&format!(" GENERATED ALWAYS AS ({expression})"));
    }
    if let Some(comment) = &column.comment {
        output.push_str(&format!(" COMMENT '{}'", escape(comment)));
    }
    Ok(output)
}

/// `DataType.sql`, whose strings state the collation.
fn sql_type(data_type: &DataType, formatter: &dyn PlanFormatter) -> CatalogResult<String> {
    Ok(match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            "STRING COLLATE UTF8_BINARY".to_string()
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => {
            format!("ARRAY<{}>", sql_type(field.data_type(), formatter)?)
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(CatalogError::Internal(
                    "map entries must be a struct".to_string(),
                ));
            };
            let [key, value] = fields.as_ref() else {
                return Err(CatalogError::Internal(
                    "map entries must have two fields".to_string(),
                ));
            };
            format!(
                "MAP<{}, {}>",
                sql_type(key.data_type(), formatter)?,
                sql_type(value.data_type(), formatter)?
            )
        }
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|field| {
                    Ok(format!(
                        "{}: {}",
                        quote_name_if_needed(field.name()),
                        sql_type(field.data_type(), formatter)?
                    ))
                })
                .collect::<CatalogResult<Vec<_>>>()?;
            format!("STRUCT<{}>", fields.join(", "))
        }
        other => formatter
            .data_type_to_simple_string(other)
            .map_err(CatalogError::DataFusionError)?
            .to_uppercase(),
    })
}

fn partition(field: &CatalogPartitionField) -> String {
    let column = quote_name_if_needed(&field.column);
    match field.transform {
        None | Some(PartitionTransform::Identity) => column,
        Some(PartitionTransform::Year) => format!("years({column})"),
        Some(PartitionTransform::Month) => format!("months({column})"),
        Some(PartitionTransform::Day) => format!("days({column})"),
        Some(PartitionTransform::Hour) => format!("hours({column})"),
        Some(PartitionTransform::Bucket(buckets)) => format!("bucket({buckets}, {column})"),
        Some(PartitionTransform::Truncate(width)) => format!("truncate({column}, {width})"),
    }
}

/// Spark records clustering columns as JSON, one array of name parts per column.
fn clustering_property(columns: &[String]) -> CatalogResult<String> {
    let parts = columns.iter().map(|x| vec![x]).collect::<Vec<_>>();
    serde_json::to_string(&parts).map_err(|e| CatalogError::Internal(e.to_string()))
}

fn column_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_name_if_needed(column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn key_values(mut entries: Vec<(String, String)>) -> Vec<String> {
    entries.sort();
    entries
        .iter()
        .map(|(key, value)| format!("'{}' = '{}'", escape(key), escape(value)))
        .collect()
}

fn multiline(lines: &[String]) -> String {
    format!("(\n  {})\n", lines.join(",\n  "))
}

/// `escapeSingleQuotedString`.
fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

fn temporary_view(name: &[String]) -> CatalogError {
    let name = name
        .iter()
        .map(|part| format!("`{}`", part.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".");
    structured(StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "EXPECT_PERMANENT_VIEW_NOT_TEMP".into(),
        sql_state: "42809".into(),
        message: format!("'SHOW CREATE TABLE' expects a permanent view but {name} is a temp view."),
        parameters: HashMap::from([
            ("viewName".into(), name),
            ("operation".into(), "SHOW CREATE TABLE".into()),
        ]),
    })
}

fn data_source_as_serde(status: &TableStatus) -> CatalogError {
    let name = status
        .catalog
        .iter()
        .chain(&status.database)
        .chain([&status.name])
        .map(|part| format!("`{}`", part.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".");
    structured(StructuredSparkError {
        class: "org.apache.spark.sql.AnalysisException".into(),
        condition: "UNSUPPORTED_SHOW_CREATE_TABLE.ON_DATA_SOURCE_TABLE_WITH_AS_SERDE".into(),
        sql_state: "0A000".into(),
        message: format!(
            "Unsupported a SHOW CREATE TABLE command. The table {name} is a Spark data source \
             table. Please use SHOW CREATE TABLE without AS SERDE instead."
        ),
        parameters: HashMap::from([("tableName".into(), name)]),
    })
}

fn structured(error: StructuredSparkError) -> CatalogError {
    CatalogError::DataFusionError(DataFusionError::External(Box::new(error)))
}
