//! Ports of `AutoCdcScd1KeyDriftSuite.scala`, the merge-support case of
//! `AutoCdcScd1SinglePipelineSuite.scala:168–216` and the schema-order cases
//! of `AutoCdcScd1AuxiliaryTableDurabilitySuite.scala`, as checks over the
//! pure auxiliary-state contract. The streaming halves (MemoryStream runs,
//! checkpoints, the row results) need the SCD1 write path.

use sail_common::spec;

use super::super::UnqualifiedColumnName;
use super::super::auxiliary::*;
use super::super::schema::Case;
use crate::error::{SparkError, StructuredSparkError};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const FLOW: &str = "cat.ns1.flow_v2";
const AUX: &str = "cat.ns1.__spark_autocdc_aux_state_target";
const TARGET: &str = "`cat`.`ns1`.`target`";

fn field(name: &str, data_type: spec::DataType, nullable: bool) -> spec::Field {
    spec::Field {
        name: name.into(),
        data_type,
        nullable,
        metadata: vec![],
    }
}

fn key(name: &str, data_type: spec::DataType, nullable: bool) -> KeyField {
    KeyField::from_field(&field(name, data_type, nullable))
}

fn recorded(names: &[&str]) -> Vec<(String, String)> {
    let names: Vec<String> = names.iter().map(|name| name.to_string()).collect();
    vec![(
        KEY_COLUMN_NAMES_PROPERTY.into(),
        serialize_key_column_names(&names),
    )]
}

fn structured(result: Result<(), SparkError>) -> Result<StructuredSparkError, String> {
    match result {
        Err(SparkError::Structured(error)) => Ok(*error),
        other => Err(format!("expected a structured error, got {other:?}")),
    }
}

fn param(error: &StructuredSparkError, name: &str) -> String {
    error
        .parameters
        .get(name)
        .cloned()
        .unwrap_or_else(|| format!("<missing parameter {name}>"))
}

fn drift(
    expected: &[KeyField],
    existing: &[spec::Field],
    names: &[&str],
    case: Case,
) -> Result<StructuredSparkError, String> {
    let error = structured(validate_no_key_drift(
        FLOW,
        AUX,
        expected,
        existing,
        &recorded(names),
        case,
    ))?;
    assert_eq!(error.condition, "AUTOCDC_INVALID_STATE.KEY_SCHEMA_DRIFT");
    assert_eq!(error.sql_state, "42000");
    assert_eq!(error.class, "org.apache.spark.sql.AnalysisException");
    assert_eq!(param(&error, "flowName"), FLOW);
    assert_eq!(param(&error, "auxTableName"), AUX);
    assert!(error.message.starts_with(&format!(
        "AutoCDC flow {FLOW} detected an invalid state: The AutoCDC flow's current key columns "
    )));
    Ok(error)
}

// KeyDriftSuite: "adds a key column" (expected `region STRING,id INT NOT NULL`).
#[test]
fn adding_a_key_column_is_drift() -> TestResult {
    let error = drift(
        &[
            key("region", spec::DataType::Utf8, true),
            key("id", spec::DataType::Int32, false),
        ],
        &[field("id", spec::DataType::Int32, false)],
        &["id"],
        Case::Insensitive,
    )?;
    assert_eq!(
        param(&error, "expectedKeySchema"),
        "region STRING,id INT NOT NULL"
    );
    assert_eq!(param(&error, "recordedKeySchema"), "id INT NOT NULL");
    Ok(())
}

// KeyDriftSuite: "drops a key column".
#[test]
fn dropping_a_key_column_is_drift() -> TestResult {
    let error = drift(
        &[key("id", spec::DataType::Int32, false)],
        &[
            field("region", spec::DataType::Utf8, true),
            field("id", spec::DataType::Int32, false),
        ],
        &["region", "id"],
        Case::Insensitive,
    )?;
    assert_eq!(param(&error, "expectedKeySchema"), "id INT NOT NULL");
    assert_eq!(
        param(&error, "recordedKeySchema"),
        "region STRING,id INT NOT NULL"
    );
    Ok(())
}

// KeyDriftSuite: "swaps a key ... same arity".
#[test]
fn swapping_a_key_name_at_equal_arity_is_drift() -> TestResult {
    let error = drift(
        &[
            key("id", spec::DataType::Int32, false),
            key("country", spec::DataType::Utf8, true),
        ],
        &[
            field("id", spec::DataType::Int32, false),
            field("region", spec::DataType::Utf8, true),
            field("country", spec::DataType::Utf8, true),
        ],
        &["id", "region"],
        Case::Insensitive,
    )?;
    assert_eq!(
        param(&error, "expectedKeySchema"),
        "id INT NOT NULL,country STRING"
    );
    assert_eq!(
        param(&error, "recordedKeySchema"),
        "id INT NOT NULL,region STRING"
    );
    Ok(())
}

// KeyDriftSuite: "recorded aux key dataType differs".
#[test]
fn a_different_recorded_type_is_drift() -> TestResult {
    let error = drift(
        &[key("id", spec::DataType::Int32, false)],
        &[field("id", spec::DataType::Int64, false)],
        &["id"],
        Case::Insensitive,
    )?;
    assert_eq!(param(&error, "expectedKeySchema"), "id INT NOT NULL");
    assert_eq!(param(&error, "recordedKeySchema"), "id BIGINT NOT NULL");
    Ok(())
}

// KeyDriftSuite: reorder, nullability/metadata and backtick changes are not drift.
#[test]
fn reorder_nullability_and_backticks_are_not_drift() -> TestResult {
    let int = spec::DataType::Int32;
    validate_no_key_drift(
        FLOW,
        AUX,
        &[key("b", int.clone(), false), key("a", int.clone(), false)],
        &[
            field("a", int.clone(), false),
            field("b", int.clone(), false),
        ],
        &recorded(&["a", "b"]),
        Case::Insensitive,
    )?;
    validate_no_key_drift(
        FLOW,
        AUX,
        &[key("id", int.clone(), true)],
        &[field("id", int.clone(), false)],
        &recorded(&["id"]),
        Case::Insensitive,
    )?;
    let schema = spec::Fields::from(vec![
        field("id", int.clone(), false),
        field("version", spec::DataType::Int64, false),
    ]);
    for spelling in ["`id`", "id"] {
        let keys = vec![UnqualifiedColumnName::parse(spelling)?];
        let expected = expected_key_fields(&keys, &schema, Case::Insensitive, FLOW, TARGET)?;
        assert_eq!(expected, vec![key("id", int.clone(), false)]);
        validate_no_key_drift(
            FLOW,
            AUX,
            &expected,
            &[field("id", int.clone(), false)],
            &recorded(&["id"]),
            Case::Insensitive,
        )?;
    }
    Ok(())
}

// KeyDriftSuite: a case-only difference drifts only under case-sensitive analysis.
#[test]
fn case_only_difference_follows_the_resolver() -> TestResult {
    let expected = [key("Id", spec::DataType::Int32, false)];
    let existing = [field("id", spec::DataType::Int32, false)];
    let error = drift(&expected, &existing, &["id"], Case::Sensitive)?;
    assert_eq!(param(&error, "expectedKeySchema"), "Id INT NOT NULL");
    assert_eq!(param(&error, "recordedKeySchema"), "id INT NOT NULL");
    validate_no_key_drift(
        FLOW,
        AUX,
        &expected,
        &existing,
        &recorded(&["id"]),
        Case::Insensitive,
    )?;
    Ok(())
}

// KeyDriftSuite: property missing, malformed, recorded key absent.
#[test]
fn corrupt_auxiliary_metadata_has_its_own_conditions() -> TestResult {
    let flow = "cat.ns1.flow";
    let expected = [key("id", spec::DataType::Int32, false)];
    let existing = [field("id", spec::DataType::Int32, false)];

    let error = structured(validate_no_key_drift(
        flow,
        AUX,
        &expected,
        &existing,
        &[],
        Case::Insensitive,
    ))?;
    assert_eq!(
        error.condition,
        "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MISSING"
    );
    assert_eq!(error.sql_state, "42000");
    assert_eq!(param(&error, "flowName"), flow);
    assert_eq!(param(&error, "auxTableName"), AUX);
    assert_eq!(
        param(&error, "propertyName"),
        "pipelines.autocdc.keyColumnNames"
    );
    assert_eq!(error.parameters.len(), 3);

    let malformed = vec![(
        KEY_COLUMN_NAMES_PROPERTY.to_string(),
        "not-a-json-array".to_string(),
    )];
    let error = structured(validate_no_key_drift(
        flow,
        AUX,
        &expected,
        &existing,
        &malformed,
        Case::Insensitive,
    ))?;
    assert_eq!(
        error.condition,
        "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_PROPERTY_MALFORMED"
    );
    assert_eq!(param(&error, "rawValue"), "not-a-json-array");
    assert_eq!(
        param(&error, "propertyName"),
        "pipelines.autocdc.keyColumnNames"
    );
    assert!(error.message.contains("(e.g. '[\"id\",\"region\"]')"));

    let error = structured(validate_no_key_drift(
        flow,
        AUX,
        &expected,
        &existing,
        &recorded(&["region"]),
        Case::Insensitive,
    ))?;
    assert_eq!(
        error.condition,
        "AUTOCDC_INVALID_STATE.AUXILIARY_TABLE_KEY_COLUMN_MISSING"
    );
    assert_eq!(param(&error, "keyColumnName"), "region");
    assert_eq!(
        param(&error, "propertyName"),
        "pipelines.autocdc.keyColumnNames"
    );
    assert_eq!(error.parameters.len(), 4);
    Ok(())
}

// SinglePipelineSuite:168–216: a parquet target cannot be an AutoCDC target.
#[test]
fn a_target_without_row_level_operations_is_rejected() -> TestResult {
    let table = "`spark_catalog`.`default`.`target_no_merge`";
    let error = structured(require_merge_support(table, None, Some("parquet"), false))?;
    assert_eq!(error.condition, "AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE");
    assert_eq!(error.sql_state, "0A000");
    assert_eq!(param(&error, "tableName"), table);
    assert_eq!(param(&error, "format"), "parquet");
    assert_eq!(
        error.message,
        format!(
            "Cannot start AutoCDC flow: the target table {table} (format: parquet) does not support row-level operations. AutoCDC requires a target backed by a connector that supports MERGE."
        )
    );
    let error = structured(require_merge_support(
        table,
        Some("delta"),
        Some("parquet"),
        false,
    ))?;
    assert_eq!(param(&error, "format"), "delta");
    let error = structured(require_merge_support(table, None, None, false))?;
    assert_eq!(param(&error, "format"), "<unknown>");
    require_merge_support(table, None, Some("parquet"), true)?;
    Ok(())
}

// DurabilitySuite: keys first, in declared order, then the metadata column.
#[test]
fn auxiliary_schema_places_declared_keys_first_then_metadata() -> TestResult {
    let schema = spec::Fields::from(vec![
        field("payload", spec::DataType::Utf8, true),
        field("id", spec::DataType::Int32, false),
        field("region", spec::DataType::Utf8, false),
        field("version", spec::DataType::Int64, false),
    ]);
    let keys = vec![
        UnqualifiedColumnName::parse("region")?,
        UnqualifiedColumnName::parse("id")?,
    ];
    let expected = expected_key_fields(&keys, &schema, Case::Insensitive, FLOW, TARGET)?;
    let aux = auxiliary_schema(&expected, &spec::DataType::Int64);
    assert_eq!(
        aux.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
        vec!["region", "id", "__spark_autocdc_metadata"]
    );
    assert!(!aux[0].nullable && !aux[1].nullable);
    let spec::DataType::Struct { fields } = &aux[2].data_type else {
        return Err("metadata column must be a struct".into());
    };
    assert_eq!(
        fields
            .iter()
            .map(|f| (f.name.as_str(), &f.data_type, f.nullable))
            .collect::<Vec<_>>(),
        vec![
            ("deleteSequence", &spec::DataType::Int64, true),
            ("upsertSequence", &spec::DataType::Int64, true)
        ]
    );
    let Err(missing) = expected_key_fields(
        &[UnqualifiedColumnName::parse("absent")?],
        &schema,
        Case::Insensitive,
        FLOW,
        TARGET,
    ) else {
        return Err("expected an internal error for an absent key".into());
    };
    assert!(
        missing
            .to_string()
            .contains("AutoCDC key column 'absent' is missing from the auxiliary table schema")
    );
    Ok(())
}

// DurabilitySuite: identifiers with special characters render quoted in DDL.
#[test]
fn key_schema_ddl_quotes_names_that_need_it() -> TestResult {
    let error = drift(
        &[
            key("name with spaces", spec::DataType::Utf8, false),
            key("it's", spec::DataType::Utf8, false),
        ],
        &[field("name with spaces", spec::DataType::Utf8, false)],
        &["name with spaces"],
        Case::Insensitive,
    )?;
    assert_eq!(
        param(&error, "expectedKeySchema"),
        "`name with spaces` STRING NOT NULL,`it's` STRING NOT NULL"
    );
    assert_eq!(
        param(&error, "recordedKeySchema"),
        "`name with spaces` STRING NOT NULL"
    );
    Ok(())
}
