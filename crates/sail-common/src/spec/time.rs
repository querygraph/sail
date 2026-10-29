use serde::{Deserialize, Serialize};

use crate::error::{CommonError, CommonResult};

pub const SAIL_SPARK_TIME_METADATA_KEY: &str = "SAIL::spark::time";

/// Spark's declared TIME precision, independent of Arrow's storage resolution.
///
/// Fields carrying this metadata use Arrow Time64(Nanosecond). The precision
/// describes the Spark type; it does not request rounding or truncation of data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SparkTimeMetadata {
    precision: i32,
}

impl SparkTimeMetadata {
    pub fn try_new(precision: i32) -> CommonResult<Self> {
        if !(0..=6).contains(&precision) {
            return Err(CommonError::invalid(format!(
                "Spark TIME precision must be between 0 and 6: {precision}"
            )));
        }
        Ok(Self { precision })
    }

    pub fn precision(self) -> i32 {
        self.precision
    }

    pub fn from_json(value: &str) -> CommonResult<Self> {
        let metadata = serde_json::from_str::<Self>(value)
            .map_err(|error| CommonError::invalid(format!("Spark TIME metadata: {error}")))?;
        Self::try_new(metadata.precision)
    }

    pub fn to_json(self) -> CommonResult<String> {
        let metadata = Self::try_new(self.precision)?;
        serde_json::to_string(&metadata)
            .map_err(|error| CommonError::internal(format!("Spark TIME metadata: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataType, Literal, TimeUnit, data_type_to_null_literal};

    #[test]
    fn spark_time_metadata_preserves_every_supported_precision() -> CommonResult<()> {
        for precision in 0..=6 {
            let metadata = SparkTimeMetadata::try_new(precision)?;
            assert_eq!(metadata.precision(), precision);
            let json = metadata.to_json()?;
            assert_eq!(json, format!(r#"{{"precision":{precision}}}"#));
            assert_eq!(SparkTimeMetadata::from_json(&json)?, metadata);
        }
        Ok(())
    }

    #[test]
    fn spark_time_metadata_rejects_invalid_precision() {
        for precision in [i32::MIN, -1, 7, 9, i32::MAX] {
            assert!(SparkTimeMetadata::try_new(precision).is_err());
            assert!(
                SparkTimeMetadata::from_json(&format!(r#"{{"precision":{precision}}}"#)).is_err()
            );
            assert!(SparkTimeMetadata { precision }.to_json().is_err());
        }
    }

    #[test]
    fn spark_time_metadata_rejects_malformed_json() {
        for json in [
            "invalid",
            "{}",
            "null",
            "[]",
            r#"{"precision":"3"}"#,
            r#"{"precision":1.5}"#,
            r#"{"precision":null}"#,
            r#"{"precision":1,"precision":2}"#,
            r#"{"precision":3,"unexpected":true}"#,
        ] {
            assert!(SparkTimeMetadata::from_json(json).is_err(), "{json}");
        }
    }

    #[test]
    fn spark_time_spec_and_typed_null_retain_declared_precision() -> CommonResult<()> {
        for precision in 0..=6 {
            let data_type = DataType::Time { precision };
            let json = serde_json::to_string(&data_type)
                .map_err(|error| CommonError::internal(error.to_string()))?;
            assert_eq!(json, format!(r#"{{"time":{{"precision":{precision}}}}}"#));
            let restored: DataType = serde_json::from_str(&json)
                .map_err(|error| CommonError::internal(error.to_string()))?;
            assert_eq!(restored, data_type);
            let literal = data_type_to_null_literal(data_type)?;
            assert_eq!(
                literal,
                Literal::Time {
                    nanoseconds: None,
                    precision
                }
            );
            assert_eq!(
                literal
                    .spark_time_metadata()?
                    .map(SparkTimeMetadata::precision),
                Some(precision)
            );
        }
        for precision in [-1, 7, 9] {
            assert!(data_type_to_null_literal(DataType::Time { precision }).is_err());
            assert!(
                Literal::Time {
                    nanoseconds: None,
                    precision
                }
                .spark_time_metadata()
                .is_err()
            );
        }
        assert_eq!(
            data_type_to_null_literal(DataType::Time64 {
                time_unit: TimeUnit::Microsecond
            })?,
            Literal::Time64Microsecond { microseconds: None }
        );
        Ok(())
    }
}
