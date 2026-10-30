//! The bounded source contract lives in `sail_common_datafusion::streaming::bounded`
//! so adapters in lower crates can implement it; this module re-exports it for
//! the pipeline control model and keeps the Spark-facing error conversion.

pub use sail_common_datafusion::streaming::bounded::{
    BoundedSource, Cursor, Incarnation, Partition, PartitionId, Range, ScanError, SourceDescriptor,
};

use crate::error::SparkError;

impl From<ScanError> for SparkError {
    fn from(error: ScanError) -> Self {
        SparkError::invalid(format!("streaming source: {error}"))
    }
}
