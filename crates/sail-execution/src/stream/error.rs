use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_flight::error::FlightError;
use sail_common_datafusion::error::{CommonErrorCause, RemoteError};
use sail_python_udf::error::PyErrExtractor;
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};

pub type TaskStreamResult<T> = Result<T, TaskStreamError>;

#[derive(Debug, Clone)]
pub enum TaskStreamError {
    Unknown(String),
    External(Arc<dyn std::error::Error + Send + Sync + 'static>),
}

impl fmt::Display for TaskStreamError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TaskStreamError::Unknown(x) => write!(f, "{x}"),
            TaskStreamError::External(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for TaskStreamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TaskStreamError::Unknown(_) => None,
            TaskStreamError::External(e) => Some(e.as_ref()),
        }
    }
}

impl From<Status> for TaskStreamError {
    fn from(status: Status) -> Self {
        if status.code() == Code::Internal {
            let details = status.get_error_details();
            if let Some(info) = details.error_info()
                && let ("task stream", "sail", Some(cause)) = (
                    info.reason.as_str(),
                    info.domain.as_str(),
                    info.metadata.get("cause"),
                )
                && let Ok(cause) = serde_json::from_str::<CommonErrorCause>(cause)
            {
                return cause.into();
            }
        }
        Self::Unknown(status.message().to_string())
    }
}

impl From<TaskStreamError> for Status {
    fn from(error: TaskStreamError) -> Self {
        let mut metadata = HashMap::new();
        if let Ok(cause) = serde_json::to_string(&CommonErrorCause::from(error)) {
            metadata.insert("cause".into(), cause);
        }

        let mut details = ErrorDetails::new();
        details.set_error_info("task stream", "sail", metadata);

        Status::with_error_details(Code::Internal, "task stream error", details)
    }
}

impl From<FlightError> for TaskStreamError {
    fn from(value: FlightError) -> Self {
        match value {
            FlightError::Arrow(e) => Self::External(Arc::new(e)),
            FlightError::Tonic(status) => (*status).into(),
            FlightError::NotYetImplemented(x)
            | FlightError::ProtocolError(x)
            | FlightError::DecodeError(x) => Self::Unknown(x),
            FlightError::ExternalError(e) => Self::External(Arc::from(e)),
        }
    }
}

impl From<CommonErrorCause> for TaskStreamError {
    fn from(value: CommonErrorCause) -> Self {
        Self::External(Arc::new(RemoteError { cause: value }))
    }
}

impl From<TaskStreamError> for CommonErrorCause {
    fn from(value: TaskStreamError) -> Self {
        match value {
            TaskStreamError::Unknown(x) => CommonErrorCause::Unknown(x),
            TaskStreamError::External(e) => CommonErrorCause::new::<PyErrExtractor>(e.as_ref()),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::error::ArrowError;
    use datafusion::common::DataFusionError;
    use sail_common_datafusion::error::StructuredSparkError;

    use super::*;

    #[test]
    fn structured_cause_survives_flight_status_and_error_sources()
    -> Result<(), Box<dyn std::error::Error>> {
        let error = StructuredSparkError {
            class: "java.lang.NumberFormatException".into(),
            condition: "CAST_INVALID_INPUT".into(),
            sql_state: "22018".into(),
            message: "malformed 雪\nvalue".into(),
            parameters: HashMap::from([("expression".into(), "'雪\n'".into())]),
        };
        let expected = serde_json::to_value(&error)?;
        let status: Status =
            TaskStreamError::from(CommonErrorCause::Structured(Box::new(error.clone()))).into();
        let sources = [
            FlightError::Tonic(Box::new(status)),
            FlightError::Arrow(ArrowError::ExternalError(Box::new(error.clone()))),
            FlightError::ExternalError(Box::new(error)),
        ];
        for source in sources {
            let source = DataFusionError::External(Box::new(TaskStreamError::from(source)));
            let CommonErrorCause::Structured(error) =
                CommonErrorCause::new::<PyErrExtractor>(&source)
            else {
                return Err(format!("structured Flight cause lost: {source:?}").into());
            };
            assert_eq!(serde_json::to_value(error)?, expected);
        }
        Ok(())
    }
}
