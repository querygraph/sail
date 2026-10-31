//! Spark 4.2.1 Auto CDC error conditions, with their exact classes, SQL
//! states, message templates and parameter names from
//! `common/utils/src/main/resources/error/error-conditions.json`.

use std::collections::HashMap;

use crate::error::{SparkError, StructuredSparkError};

const ANALYSIS_EXCEPTION: &str = "org.apache.spark.sql.AnalysisException";
const PARSE_EXCEPTION: &str = "org.apache.spark.sql.catalyst.parser.ParseException";

fn structured(
    class: &str,
    condition: &str,
    sql_state: &str,
    message: String,
    parameters: &[(&str, &str)],
) -> SparkError {
    Box::new(StructuredSparkError {
        class: class.into(),
        condition: condition.into(),
        sql_state: sql_state.into(),
        message,
        parameters: parameters
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect::<HashMap<_, _>>(),
    })
    .into()
}

fn analysis(
    condition: &str,
    sql_state: &str,
    message: String,
    parameters: &[(&str, &str)],
) -> SparkError {
    structured(
        ANALYSIS_EXCEPTION,
        condition,
        sql_state,
        message,
        parameters,
    )
}

pub(super) fn multipart_column_identifier(column_name: &str, name_parts: &[String]) -> SparkError {
    let parts = name_parts.join(", ");
    analysis(
        "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER",
        "22023",
        format!(
            "Expected a single column identifier; got the multi-part identifier {column_name} (parts: {parts})."
        ),
        &[("columnName", column_name), ("nameParts", &parts)],
    )
}

pub(super) fn columns_not_found_in_schema(
    case_sensitivity: &str,
    schema_name: &str,
    missing: &[String],
    available: &[String],
) -> SparkError {
    let missing = missing.join(", ");
    let available = available.join(", ");
    analysis(
        "AUTOCDC_COLUMNS_NOT_FOUND_IN_SCHEMA",
        "42703",
        format!(
            "Using {case_sensitivity} column name comparison, the following columns are not present in the {schema_name} schema: {missing}. Available columns: {available}."
        ),
        &[
            ("caseSensitivity", case_sensitivity),
            ("schemaName", schema_name),
            ("missingColumns", &missing),
            ("availableColumns", &available),
        ],
    )
}

pub(super) fn empty_keys() -> SparkError {
    analysis(
        "AUTOCDC_EMPTY_KEYS",
        "22023",
        "AutoCDC requires at least one key column to identify rows, but received an empty key set."
            .into(),
        &[],
    )
}

pub(super) fn missing_source() -> SparkError {
    analysis(
        "AUTOCDC_MISSING_SOURCE",
        "22023",
        "AutoCDC flow is missing a required `source` table name. Specify the name of the streaming source table the flow should read from."
            .into(),
        &[],
    )
}

pub(super) fn missing_sequence_by() -> SparkError {
    analysis(
        "AUTOCDC_MISSING_SEQUENCE_BY",
        "22023",
        "AutoCDC flow is missing a required `sequence_by` expression. Specify a `sequence_by` column or expression that orders incoming change events."
            .into(),
        &[],
    )
}

pub(super) fn non_column_identifier(expression: &str) -> SparkError {
    analysis(
        "AUTOCDC_NON_COLUMN_IDENTIFIER",
        "22023",
        format!(
            "Expected a column identifier; got the non-attribute expression `{expression}`. AutoCDC keys, sequence_by, column_list, and except_column_list must reference unqualified column names."
        ),
        &[("expression", expression)],
    )
}

pub(super) fn both_column_list_and_except_column_list() -> SparkError {
    analysis(
        "AUTOCDC_BOTH_COLUMN_LIST_AND_EXCEPT_COLUMN_LIST",
        "42613",
        "AutoCDC flow specifies both `column_list` and `except_column_list`; at most one may be provided."
            .into(),
        &[],
    )
}

pub(super) fn key_not_in_selected_schema(case_sensitivity: &str, key: &str) -> SparkError {
    analysis(
        "AUTOCDC_KEY_NOT_IN_SELECTED_SCHEMA",
        "22023",
        format!(
            "Using {case_sensitivity} column name comparison, the AutoCDC key column `{key}` is not present in the flow's selected source schema. AutoCDC requires every key column to be present in the source change-data feed and retained by any configured column selection."
        ),
        &[
            ("caseSensitivity", case_sensitivity),
            ("keyColumnName", key),
        ],
    )
}

pub(super) fn reserved_column_name_prefix_conflict(
    case_sensitivity: &str,
    column_name: &str,
    schema_name: &str,
    prefix: &str,
) -> SparkError {
    analysis(
        "AUTOCDC_RESERVED_COLUMN_NAME_PREFIX_CONFLICT",
        "42710",
        format!(
            "The column `{column_name}` in the {schema_name} schema collides with the reserved AutoCDC column name prefix `{prefix}` (using {case_sensitivity} column name comparison). Rename or remove the column."
        ),
        &[
            ("caseSensitivity", case_sensitivity),
            ("columnName", column_name),
            ("schemaName", schema_name),
            ("reservedColumnNamePrefix", prefix),
        ],
    )
}

pub(super) fn scd2_not_supported() -> SparkError {
    analysis(
        "AUTOCDC_SCD2_NOT_SUPPORTED",
        "0A000",
        "AutoCDC flows do not currently support SCD Type 2 transformations.".into(),
        &[],
    )
}

pub(super) fn multiple_flows_to_target(table_name: &str, flows: &str) -> SparkError {
    analysis(
        "AUTOCDC_MULTIPLE_FLOWS_TO_TARGET",
        "42000",
        format!(
            "Invalid AutoCDC destination {table_name} with multiple flows: {flows}. An AutoCDC target table must have exactly one flow writing to it."
        ),
        &[("tableName", table_name), ("flows", flows)],
    )
}

pub(super) fn relation_for_temporary_view(flow: &str, view: &str) -> SparkError {
    analysis(
        "INVALID_FLOW_QUERY_TYPE.AUTOCDC_RELATION_FOR_TEMPORARY_VIEW",
        "42000",
        format!(
            "Flow {flow} returns an invalid relation type. AutoCDC flows must target a streaming table because their reconciliation semantics require a streaming-table sink, but the flow {flow} attempts to write an AutoCDC relation to the temporary view {view}."
        ),
        &[("flowIdentifier", flow), ("viewIdentifier", view)],
    )
}

/// Spark's SQL parser rejects an empty identifier before any Auto CDC check.
pub(super) fn parse_empty_statement() -> SparkError {
    structured(
        PARSE_EXCEPTION,
        "PARSE_EMPTY_STATEMENT",
        "42617",
        "Syntax error, unexpected empty statement.".into(),
        &[],
    )
}

/// `DataTypeErrors.attributeNameSyntaxError`, raised by the Connect planner's
/// `UnresolvedAttribute.quotedString` before Auto CDC sees the name.
pub(super) fn invalid_attribute_name_syntax(name: &str) -> SparkError {
    analysis(
        "INVALID_ATTRIBUTE_NAME_SYNTAX",
        "42601",
        format!(
            "Syntax error in the attribute name: {name}. Check that backticks appear in pairs, a quoted string is a complete name part and use a backtick only inside quoted name parts."
        ),
        &[("name", name)],
    )
}

/// `IdentifierHelper.toTableIdentifier`: an `UnsupportedOperationException`
/// rather than a structured error, with the parts as the SQL parser read them.
pub(super) fn four_part_table_identifier(parts: &[String]) -> SparkError {
    SparkError::unsupported(format!(
        "4+ part table identifier {} is not supported.",
        parts.join(".")
    ))
}

const MICROBATCH_VALIDATION: &str = "AUTOCDC_MICROBATCH_VALIDATION";
const MICROBATCH_VALIDATION_STATE: &str = "22000";

fn microbatch_validation(
    sub_class: &str,
    table_name: &str,
    batch_id: &str,
    detail: &str,
    parameters: &[(&str, &str)],
) -> SparkError {
    let mut all: Vec<(&str, &str)> = vec![("tableName", table_name), ("batchId", batch_id)];
    all.extend_from_slice(parameters);
    analysis(
        &format!("{MICROBATCH_VALIDATION}.{sub_class}"),
        MICROBATCH_VALIDATION_STATE,
        format!(
            "AutoCDC flow on table {table_name} in batch {batch_id} failed microbatch validation. {detail}"
        ),
        &all,
    )
}

/// `ScdBatchValidator.validateMicrobatch`: the sequencing type is not orderable.
pub(super) fn non_orderable_sequence(
    table_name: &str,
    batch_id: &str,
    data_type: &str,
) -> SparkError {
    microbatch_validation(
        "NON_ORDERABLE_SEQUENCE",
        table_name,
        batch_id,
        &format!(
            "The sequencing column has non-orderable type {data_type}. The sequencing column must be of a type that supports ordering."
        ),
        &[("dataType", data_type)],
    )
}

/// `ScdBatchValidator.validateMicrobatch`: rows with a null sequencing value.
pub(super) fn null_sequence(table_name: &str, batch_id: &str, null_count: &str) -> SparkError {
    microbatch_validation(
        "NULL_SEQUENCE",
        table_name,
        batch_id,
        &format!(
            "The microbatch contains {null_count} row(s) with a null sequencing value. All rows must have a non-null sequencing value."
        ),
        &[("nullCount", null_count)],
    )
}

/// `ScdBatchValidator.validateMicrobatch`: rows with a null key column.
pub(super) fn null_key(table_name: &str, batch_id: &str, null_key_counts: &str) -> SparkError {
    microbatch_validation(
        "NULL_KEY",
        table_name,
        batch_id,
        &format!(
            "The microbatch contains rows with null values in the following key column(s): {null_key_counts}. All rows must have non-null values for every key column."
        ),
        &[("nullKeyCounts", null_key_counts)],
    )
}
