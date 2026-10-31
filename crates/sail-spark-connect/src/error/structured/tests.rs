use std::collections::HashMap;

use datafusion::arrow::error::ArrowError;
use datafusion::common::DataFusionError;
use sail_common_datafusion::error::{CommonErrorCause, RemoteError, StructuredSparkError};
use tonic::Status;
use tonic_types::StatusExt;

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
    assert_eq!(status.message(), expected.to_string());
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
    assert_eq!(throwable.message(), error.to_string());
    assert_eq!(throwable.class_name(), error.class);
    assert_status(throwable.into())
}
