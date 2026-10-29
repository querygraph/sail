use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef};
use arrow::error::ArrowError;
use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datafusion_common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::error::StructuredSparkError;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

/// ANSI primitive string casts retain the offending value before error transport.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAnsiStringCast {
    signature: Signature,
    target_type: DataType,
    sql_target: String,
}

impl SparkAnsiStringCast {
    pub fn target_name(target: &DataType) -> Option<&'static str> {
        match target {
            DataType::Int8 => Some("TINYINT"),
            DataType::Int16 => Some("SMALLINT"),
            DataType::Int32 => Some("INT"),
            DataType::Int64 => Some("BIGINT"),
            DataType::Float32 => Some("FLOAT"),
            DataType::Float64 => Some("DOUBLE"),
            DataType::Boolean => Some("BOOLEAN"),
            _ => None,
        }
    }

    pub fn try_new(target_type: DataType, sql_target: String) -> Result<Self> {
        if Self::target_name(&target_type) != Some(sql_target.as_str()) {
            return plan_err!("invalid ANSI string cast target: {target_type} ({sql_target})");
        }
        Ok(Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            target_type,
            sql_target,
        })
    }

    pub fn target_type(&self) -> &DataType {
        &self.target_type
    }

    pub fn sql_target(&self) -> &str {
        &self.sql_target
    }

    fn invalid_input(&self, value: &str) -> DataFusionError {
        let expression = format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"));
        let source_type = "\"STRING\"";
        let target_type = format!("\"{}\"", self.sql_target);
        let message = format!(
            "The value {expression} of the type {source_type} cannot be cast to {target_type} because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead."
        );
        DataFusionError::External(Box::new(StructuredSparkError {
            class: if self.target_type == DataType::Boolean {
                "org.apache.spark.SparkRuntimeException"
            } else {
                "java.lang.NumberFormatException"
            }
            .into(),
            condition: "CAST_INVALID_INPUT".into(),
            sql_state: "22018".into(),
            message,
            parameters: HashMap::from([
                ("expression".into(), expression),
                ("sourceType".into(), source_type.into()),
                ("targetType".into(), target_type),
                ("ansiConfig".into(), "\"spark.sql.ansi.enabled\"".into()),
            ]),
        }))
    }

    fn failed_value(&self, input: &ColumnarValue) -> Result<Option<String>> {
        match input {
            ColumnarValue::Scalar(value) => Ok(value.try_as_str().flatten().map(str::to_owned)),
            ColumnarValue::Array(array) => {
                // Failure only: inspect retained values, never evaluate the child again.
                for index in 0..array.len() {
                    if array.is_null(index) {
                        continue;
                    }
                    let value = ScalarValue::try_from_array(array, index)?;
                    if let Err(error) =
                        value.cast_to_with_options(&self.target_type, &DEFAULT_CAST_OPTIONS)
                    {
                        if !is_cast_error(&error) {
                            return Err(error);
                        }
                        return Ok(value.try_as_str().flatten().map(str::to_owned));
                    }
                }
                Ok(None)
            }
        }
    }
}

fn is_cast_error(error: &DataFusionError) -> bool {
    matches!(error.find_root(), DataFusionError::ArrowError(error, _) if matches!(error.as_ref(), ArrowError::CastError(_)))
}

impl ScalarUDFImpl for SparkAnsiStringCast {
    fn name(&self) -> &str {
        "spark_ansi_string_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                args.arg_fields.len(),
            ));
        };
        // Match type-only Cast's source nullability and non-extension metadata.
        let mut metadata = input.metadata().clone();
        metadata.remove(EXTENSION_TYPE_NAME_KEY);
        metadata.remove(EXTENSION_TYPE_METADATA_KEY);
        Ok(Arc::new(
            input
                .as_ref()
                .clone()
                .with_name(self.name())
                .with_data_type(self.target_type.clone())
                .with_metadata(metadata),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                args.args.len(),
            ));
        };
        let source = input.data_type();
        if !matches!(
            source,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return Err(unsupported_data_type_exec_err(
                self.name(),
                "STRING",
                &source,
            ));
        }
        match input.cast_to(&self.target_type, Some(&DEFAULT_CAST_OPTIONS)) {
            Ok(value) => Ok(value),
            Err(error) if is_cast_error(&error) => match self.failed_value(input)? {
                Some(value) => Err(self.invalid_input(&value)),
                None => Err(error),
            },
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests;
