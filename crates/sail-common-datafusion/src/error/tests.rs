use std::collections::HashMap;
use std::sync::Arc;

use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct NoPython;

impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn std::error::Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}

fn fixture() -> StructuredSparkError {
    StructuredSparkError {
        class: "java.lang.NumberFormatException".into(),
        condition: "CAST_INVALID_INPUT".into(),
        sql_state: "22018".into(),
        message: "malformed value '雪\n'".into(),
        parameters: HashMap::from([
            ("expression".into(), "'雪\n'".into()),
            ("targetType".into(), "\"INT\"".into()),
        ]),
    }
}

fn assert_fixture(cause: CommonErrorCause) -> TestResult {
    let CommonErrorCause::Structured(error) = cause else {
        return Err(format!("structured cause lost: {cause:?}").into());
    };
    assert_eq!(
        serde_json::to_value(error)?,
        serde_json::to_value(fixture())?
    );
    Ok(())
}

#[test]
fn structured_cause_survives_datafusion_arrow_context_and_shared_wrappers() -> TestResult {
    let errors = [
        DataFusionError::External(Box::new(fixture())),
        DataFusionError::ArrowError(
            Box::new(ArrowError::ExternalError(Box::new(fixture()))),
            None,
        ),
        DataFusionError::Shared(Arc::new(DataFusionError::Context(
            "projection".into(),
            Box::new(DataFusionError::External(Box::new(fixture()))),
        ))),
    ];
    for error in errors {
        assert_fixture(CommonErrorCause::new::<NoPython>(&error))?;
    }
    Ok(())
}

#[test]
fn structured_cause_survives_serde_and_remote_source() -> TestResult {
    let cause = CommonErrorCause::new::<NoPython>(&fixture());
    assert!(cause.is_retryable());
    let message = cause.to_string();
    assert_eq!(message, fixture().to_string());
    let cause = serde_json::from_str(&serde_json::to_string(&cause)?)?;
    let error = DataFusionError::External(Box::new(RemoteError { cause }));
    assert_fixture(CommonErrorCause::new::<NoPython>(&error))
}

#[test]
fn unknown_commit_keeps_precedence_over_structured_execution_error() {
    let error = DataFusionError::Collection(vec![
        DataFusionError::External(Box::new(fixture())),
        DataFusionError::External(Box::new(CommitStateUnknownError("response lost".into()))),
    ]);
    let cause = CommonErrorCause::new::<NoPython>(&error);
    assert!(!cause.is_retryable());
    assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
}

fn uncertain_storage() -> object_store::Error {
    object_store::Error::Generic {
        store: "owned-operation",
        source: Box::new(DataFusionError::Collection(vec![
            DataFusionError::External(Box::new(fixture())),
            DataFusionError::External(Box::new(CommitStateUnknownError("response lost".into()))),
        ])),
    }
}

#[test]
fn storage_format_and_io_wrappers_keep_nonretryable_commit_uncertainty() {
    use datafusion::parquet::errors::ParquetError;
    let errors = [
        DataFusionError::ObjectStore(Box::new(uncertain_storage())),
        DataFusionError::IoError(std::io::Error::other(uncertain_storage())),
        DataFusionError::ParquetError(Box::new(ParquetError::External(Box::new(
            std::io::Error::other(uncertain_storage()),
        )))),
        DataFusionError::ArrowError(
            Box::new(ArrowError::IoError(
                "writer".into(),
                std::io::Error::other(uncertain_storage()),
            )),
            None,
        ),
    ];
    for error in errors {
        let cause = CommonErrorCause::new::<NoPython>(&error);
        assert!(!cause.is_retryable(), "lost nested policy: {error:?}");
        assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
    }
    let direct = ArrowError::IoError("writer".into(), std::io::Error::other(uncertain_storage()));
    assert!(!CommonErrorCause::new::<NoPython>(&direct).is_retryable());
}

#[test]
fn ordinary_storage_format_and_io_categories_and_messages_are_unchanged() -> TestResult {
    use datafusion::parquet::errors::ParquetError;
    let errors = [
        DataFusionError::ObjectStore(Box::new(object_store::Error::Generic {
            store: "ordinary",
            source: Box::new(fixture()),
        })),
        DataFusionError::IoError(std::io::Error::other(fixture())),
        DataFusionError::ArrowError(
            Box::new(ArrowError::IoError(
                "writer".into(),
                std::io::Error::other(fixture()),
            )),
            None,
        ),
        DataFusionError::ParquetError(Box::new(ParquetError::External(Box::new(fixture())))),
    ];
    for error in errors {
        let expected = match &error {
            DataFusionError::ObjectStore(e) => CommonErrorCause::Io(e.to_string()),
            DataFusionError::IoError(e) => CommonErrorCause::Io(e.to_string()),
            DataFusionError::ArrowError(e, _) => {
                let ArrowError::IoError(_, inner) = e.as_ref() else {
                    unreachable!()
                };
                // Unchanged production baseline: the boxed Arrow wrapper's
                // source traversal reaches its inner display-only fallback.
                CommonErrorCause::Unknown(inner.to_string())
            }
            DataFusionError::ParquetError(e) => CommonErrorCause::FormatParquet(e.to_string()),
            _ => unreachable!(),
        };
        let actual = CommonErrorCause::new::<NoPython>(&error);
        assert_eq!(
            serde_json::to_value(&actual)?,
            serde_json::to_value(&expected)?
        );
        assert!(actual.is_retryable());
    }
    let direct = ArrowError::IoError("writer".into(), std::io::Error::other(fixture()));
    let CommonErrorCause::Io(message) = CommonErrorCause::new::<NoPython>(&direct) else {
        return Err("direct Arrow I/O category changed".into());
    };
    assert_eq!(message, format!("writer: {}", fixture()));
    Ok(())
}
