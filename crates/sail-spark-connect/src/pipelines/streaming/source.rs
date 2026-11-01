//! The bounded source contract lives in `sail_common_datafusion::streaming::bounded`
//! so adapters in lower crates can implement it; this module re-exports it for
//! the pipeline control model and keeps the Spark-facing error conversion.

pub use sail_common_datafusion::streaming::bounded::{
    BoundedSource, Cursor, Incarnation, Partition, PartitionId, Range, ScanError, SourceDescriptor,
    SourceError,
};

use crate::error::SparkError;

impl From<SourceError> for SparkError {
    fn from(error: SourceError) -> Self {
        let message = format!("streaming source: {error}");
        match error {
            SourceError::Unavailable { .. }
            | SourceError::Cancelled
            | SourceError::Internal { .. } => SparkError::internal(message),
            SourceError::Refused(_)
            | SourceError::Denied { .. }
            | SourceError::NotFound { .. }
            | SourceError::Corrupt { .. } => SparkError::invalid(message),
        }
    }
}

impl From<ScanError> for SparkError {
    fn from(error: ScanError) -> Self {
        SourceError::from(error).into()
    }
}
