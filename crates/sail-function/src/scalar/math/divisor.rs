use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::compute::kernels::cmp::eq;
use datafusion::arrow::compute::{nullif, or};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::scalar::math::zero_division::ZeroDivisionKind;

/// Check an already evaluated numeric divisor without changing its datatype.
/// Keeping the check in one function avoids CASE nullability evaluation and
/// repeated evaluation of volatile/window operands.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SafeNumericDivisor {
    kind: ZeroDivisionKind,
    ansi: bool,
    signature: Signature,
}

impl SafeNumericDivisor {
    pub fn new(kind: ZeroDivisionKind, ansi: bool) -> Self {
        Self {
            kind,
            ansi,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
    fn validate(&self, data_type: &DataType) -> Result<()> {
        if data_type.is_numeric() || matches!(data_type, DataType::Null) {
            Ok(())
        } else {
            Err(unsupported_data_type_exec_err(
                self.name(),
                "numeric or NULL",
                data_type,
            ))
        }
    }
}

impl ScalarUDFImpl for SafeNumericDivisor {
    fn name(&self) -> &str {
        match (self.kind, self.ansi) {
            (ZeroDivisionKind::Divide, true) => "spark_divisor_ansi_divide",
            (ZeroDivisionKind::Remainder, true) => "spark_divisor_ansi_remainder",
            (ZeroDivisionKind::Divide, false) => "spark_divisor_legacy_divide",
            (ZeroDivisionKind::Remainder, false) => "spark_divisor_legacy_remainder",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        let [data_type] = types else {
            return Err(invalid_arg_count_exec_err(self.name(), (1, 1), types.len()));
        };
        self.validate(data_type)?;
        Ok(data_type.clone())
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                args.arg_fields.len(),
            ));
        };
        self.validate(field.data_type())?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return Err(invalid_arg_count_exec_err(
                self.name(),
                (1, 1),
                args.args.len(),
            ));
        };
        let data_type = input.data_type();
        self.validate(&data_type)?;
        if data_type == DataType::Null {
            return Ok(input.clone());
        }
        let array = input.to_array(1)?;
        let zeros = numeric_zero_mask(&array)?;
        if zeros.true_count() == 0 {
            return Ok(input.clone());
        }
        if self.ansi {
            return Err(self.kind.error());
        }
        match input {
            ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                &data_type,
            )?)),
            ColumnarValue::Array(_) => Ok(ColumnarValue::Array(nullif(array.as_ref(), &zeros)?)),
        }
    }
}

/// Numeric value equality with zero, including both floating-point signs.
pub(crate) fn numeric_zero_mask(array: &ArrayRef) -> Result<BooleanArray> {
    let zero = ScalarValue::new_zero(array.data_type())?;
    let zeros = eq(array, &zero.to_scalar()?)?;
    if array.data_type().is_floating() {
        // Arrow total-order equality distinguishes the two signs of zero.
        Ok(or(
            &zeros,
            &eq(array, &zero.arithmetic_negate()?.to_scalar()?)?,
        )?)
    } else {
        Ok(zeros)
    }
}
