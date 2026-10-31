use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Decimal128Array;
use arrow::datatypes::{DataType, Decimal128Type, FieldRef, validate_decimal_precision_and_scale};
use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common_datafusion::error::StructuredSparkError;

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};

mod parse;

/// Spark STRING-to-DECIMAL semantics. Policies are captured at planning time;
/// legacy CAST and TRY_CAST both use `fail_on_error = false`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkStringToDecimal {
    signature: Signature,
    precision: u8,
    scale: i8,
    fail_on_error: bool,
    allow_negative_scale: bool,
}

impl SparkStringToDecimal {
    pub fn try_new(
        precision: u8,
        scale: i8,
        fail_on_error: bool,
        allow_negative_scale: bool,
    ) -> Result<Self> {
        validate_decimal_precision_and_scale::<Decimal128Type>(precision, scale)?;
        Ok(Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            precision,
            scale,
            fail_on_error,
            allow_negative_scale,
        })
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }
    pub fn scale(&self) -> i8 {
        self.scale
    }
    pub fn fail_on_error(&self) -> bool {
        self.fail_on_error
    }
    /// This policy also changes range classification for positive-scale targets:
    /// Spark permits a parsed negative scale beyond 38 integral digits when set.
    pub fn allow_negative_scale(&self) -> bool {
        self.allow_negative_scale
    }

    fn cast_value(&self, value: Option<&str>) -> Result<Option<i128>> {
        let Some(value) = value else {
            return Ok(None);
        };
        match parse::cast(value, self.precision, self.scale, self.allow_negative_scale) {
            Ok(result) => Ok(Some(result)),
            Err(_) if !self.fail_on_error => Ok(None),
            Err(parse::CastError::Invalid) => Err(super::cast_error::invalid_string_cast(
                value,
                &format!("DECIMAL({},{})", self.precision, self.scale),
                false,
            )),
            Err(parse::CastError::UnsupportedRange) => {
                Err(DataFusionError::External(Box::new(StructuredSparkError {
                    class: "org.apache.spark.SparkArithmeticException".into(),
                    condition: "NUMERIC_OUT_OF_SUPPORTED_RANGE".into(),
                    sql_state: "22003".into(),
                    message: format!(
                        "The value {value} cannot be interpreted as a numeric since it has more than 38 digits."
                    ),
                    parameters: HashMap::from([("value".into(), value.into())]),
                })))
            }
            Err(parse::CastError::Overflow(decimal)) => {
                let value = decimal.error_value();
                Err(DataFusionError::External(Box::new(StructuredSparkError {
                    class: "org.apache.spark.SparkArithmeticException".into(),
                    condition: "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION".into(),
                    sql_state: "22003".into(),
                    message: format!(
                        "{value} cannot be represented as Decimal({}, {}). If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error, and return NULL instead.",
                        self.precision, self.scale
                    ),
                    parameters: HashMap::from([
                        ("value".into(), value),
                        ("precision".into(), self.precision.to_string()),
                        ("scale".into(), self.scale.to_string()),
                        ("config".into(), "\"spark.sql.ansi.enabled\"".into()),
                    ]),
                })))
            }
        }
    }

    fn cast_array<'a>(
        &self,
        values: impl Iterator<Item = Option<&'a str>>,
    ) -> Result<ColumnarValue> {
        let values = values
            .map(|v| self.cast_value(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(ColumnarValue::Array(Arc::new(
            Decimal128Array::from(values).with_precision_and_scale(self.precision, self.scale)?,
        )))
    }
}

impl ScalarUDFImpl for SparkStringToDecimal {
    fn name(&self) -> &str {
        "spark_string_to_decimal"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Decimal128(self.precision, self.scale))
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                args.arg_fields.len(),
            ));
        };
        let mut metadata = input.metadata().clone();
        metadata.remove(EXTENSION_TYPE_NAME_KEY);
        metadata.remove(EXTENSION_TYPE_METADATA_KEY);
        Ok(Arc::new(
            input
                .as_ref()
                .clone()
                .with_name(self.name())
                .with_data_type(DataType::Decimal128(self.precision, self.scale))
                .with_nullable(true)
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
        if !matches!(
            input.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return Err(unsupported_data_type_exec_err(
                self.name(),
                "STRING",
                &input.data_type(),
            ));
        }
        match input {
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                self.cast_value(value.try_as_str().flatten())?,
                self.precision,
                self.scale,
            ))),
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Utf8 => self.cast_array(as_string_array(array)?.iter()),
                DataType::LargeUtf8 => self.cast_array(as_large_string_array(array)?.iter()),
                DataType::Utf8View => self.cast_array(as_string_view_array(array)?.iter()),
                _ => Err(unsupported_data_type_exec_err(
                    self.name(),
                    "STRING",
                    array.data_type(),
                )),
            },
        }
    }
}

#[cfg(test)]
mod tests;
