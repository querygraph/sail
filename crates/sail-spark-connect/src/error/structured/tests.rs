use std::collections::HashMap;

use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use sail_common_datafusion::error::{CommonErrorCause, RemoteError, StructuredSparkError};
use tonic::Status;
use tonic_types::StatusExt;

use super::rendered;
use crate::error::{SparkError, SparkThrowable};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn fixture() -> StructuredSparkError {
    StructuredSparkError {
        class: "java.lang.NumberFormatException".into(),
        condition: "CAST_INVALID_INPUT".into(),
        sql_state: "22018".into(),
        message: "malformed 雪\nvalue".into(),
        parameters: HashMap::from([
            ("expression".into(), "'雪\n'".into()),
            ("sourceType".into(), "\"STRING\"".into()),
            ("targetType".into(), "\"INT\"".into()),
            ("ansiConfig".into(), "\"spark.sql.ansi.enabled\"".into()),
        ]),
    }
}

fn assert_status(status: Status) -> TestResult {
    let expected = fixture();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), rendered(&expected));
    let details = status.get_error_details();
    let info = details.error_info().ok_or("missing error metadata")?;
    assert_eq!(info.reason, expected.class);
    assert_eq!(info.domain, "org.apache.spark");
    assert_eq!(info.metadata.len(), 4);
    assert_eq!(
        info.metadata["classes"],
        format!("[\"{}\"]", expected.class)
    );
    assert_eq!(info.metadata["errorClass"], expected.condition);
    assert_eq!(info.metadata["sqlState"], expected.sql_state);
    assert_eq!(
        serde_json::from_str::<HashMap<String, String>>(&info.metadata["messageParameters"])?,
        expected.parameters
    );
    Ok(())
}

#[test]
fn direct_and_wrapped_structured_errors_preserve_complete_connect_metadata() -> TestResult {
    let errors = [
        sail_plan::error::PlanError::Structured(Box::new(fixture())).into(),
        SparkError::Structured(Box::new(fixture())),
        SparkError::DataFusionError(DataFusionError::External(Box::new(fixture()))),
        SparkError::ArrowError(ArrowError::ExternalError(Box::new(fixture()))),
        SparkError::DataFusionError(DataFusionError::External(Box::new(RemoteError {
            cause: CommonErrorCause::Structured(Box::new(fixture())),
        }))),
    ];
    for error in errors {
        assert_status(error.into())?;
    }
    Ok(())
}

#[test]
fn stored_throwable_exposes_native_message_class_and_metadata() -> TestResult {
    let error = fixture();
    let throwable = SparkThrowable::from(CommonErrorCause::Structured(Box::new(error.clone())));
    assert_eq!(throwable.message(), rendered(&error));
    assert_eq!(throwable.class_name(), error.class);
    assert_status(throwable.into())
}

#[test]
fn missing_catalog_execution_errors_keep_spark_metadata() -> TestResult {
    use sail_common_datafusion::error::catalog_not_found_error;

    for (name, quoted) in [
        ("missing_catalog", "`missing_catalog`"),
        ("a.b", "`a`.`b`"),
        ("a\"b", "`a\"b`"),
        ("雪", "`雪`"),
        ("", "``"),
    ] {
        let errors = [
            SparkError::DataFusionError(catalog_not_found_error(name)),
            SparkError::DataFusionError(DataFusionError::Shared(std::sync::Arc::new(
                DataFusionError::Context(
                    "catalog command".into(),
                    Box::new(catalog_not_found_error(name)),
                ),
            ))),
        ];
        for error in errors {
            let status: Status = error.into();
            let details = status.get_error_details();
            let info = details.error_info().ok_or("missing error info")?;
            assert_eq!(
                info.reason,
                "org.apache.spark.sql.connector.catalog.CatalogNotFoundException"
            );
            assert_eq!(info.metadata["errorClass"], "CATALOG_NOT_FOUND");
            assert_eq!(info.metadata["sqlState"], "42P08");
            assert_eq!(
                serde_json::from_str::<Vec<String>>(&info.metadata["classes"])?,
                vec![
                    "org.apache.spark.sql.connector.catalog.CatalogNotFoundException",
                    "org.apache.spark.SparkException",
                    "java.lang.Exception",
                    "java.lang.Throwable",
                    "java.lang.Object",
                ]
            );
            let config = format!("\"spark.sql.catalog.{name}\"");
            assert_eq!(
                serde_json::from_str::<HashMap<String, String>>(
                    &info.metadata["messageParameters"]
                )?,
                HashMap::from([
                    ("catalogName".into(), quoted.into()),
                    ("config".into(), config.clone()),
                ])
            );
            assert_eq!(
                status.message(),
                format!(
                    "[CATALOG_NOT_FOUND] The catalog {quoted} not found. Consider to set the SQL config {config} to a catalog plugin. SQLSTATE: 42P08"
                )
            );
        }
    }
    Ok(())
}

#[test]
fn catalog_diagnostics_preserve_invalid_names_and_other_missing_object_kinds() -> TestResult {
    use sail_catalog::error::{CatalogError, CatalogObject};
    use sail_plan::error::PlanError;

    let invalid = SparkError::DataFusionError(
        sail_common_datafusion::error::catalog_not_found_error("a`b"),
    );
    let status: Status = invalid.into();
    let details = status.get_error_details();
    let info = details.error_info().ok_or("missing invalid-name details")?;
    assert_eq!(info.reason, "org.apache.spark.sql.AnalysisException");
    assert_eq!(info.metadata["errorClass"], "INVALID_ATTRIBUTE_NAME_SYNTAX");
    assert_eq!(info.metadata["sqlState"], "42601");
    assert_eq!(
        serde_json::from_str::<HashMap<String, String>>(&info.metadata["messageParameters"])?,
        HashMap::from([("name".into(), "a`b".into())])
    );
    for object in [
        CatalogObject::Catalog,
        CatalogObject::Table,
        CatalogObject::View,
        CatalogObject::Database,
    ] {
        let error = PlanError::from(CatalogError::NotFound(object, "input".into()));
        assert!(matches!(error, PlanError::AnalysisError(_)), "{error}");
        assert!(!error.to_string().contains("CATALOG_NOT_FOUND"));
    }
    Ok(())
}

#[test]
fn sql_parse_errors_survive_direct_and_plan_connect_conversion() -> TestResult {
    for via_plan in [false, true] {
        let error = sail_sql_analyzer::statement::from_ast_statement(
            sail_sql_analyzer::parser::parse_one_statement(
                "CREATE TEMPORARY VIEW some_database.tv AS SELECT 1",
            )?,
        )
        .err()
        .ok_or("expected parse error")?;
        let error = if via_plan {
            SparkError::from(sail_plan::error::PlanError::from(error))
        } else {
            SparkError::from(error)
        };
        let status: Status = error.into();
        let details = status.get_error_details();
        let info = details.error_info().ok_or("missing parse metadata")?;
        assert_eq!(
            info.reason,
            "org.apache.spark.sql.catalyst.parser.ParseException"
        );
        assert_eq!(info.metadata["errorClass"], "INVALID_TEMP_OBJ_QUALIFIER");
        assert_eq!(info.metadata["sqlState"], "42602");
        assert_eq!(
            serde_json::from_str::<HashMap<String, String>>(&info.metadata["messageParameters"])?,
            HashMap::from([
                ("objectType".into(), "VIEW".into()),
                ("objectName".into(), "`tv`".into()),
                ("qualifier".into(), "`some_database`".into()),
            ])
        );
    }
    Ok(())
}
