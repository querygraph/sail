use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF};

use super::divisor::SafeNumericDivisor;
use super::zero_division::ZeroDivisionKind;

fn invoke(guard: &ScalarUDF, value: ColumnarValue) -> Result<ColumnarValue> {
    let field = Arc::new(Field::new("divisor", value.data_type(), true));
    let rows = match &value {
        ColumnarValue::Array(a) => a.len(),
        _ => 1,
    };
    guard.invoke_with_args(ScalarFunctionArgs {
        args: vec![value],
        arg_fields: vec![field.clone()],
        number_rows: rows,
        return_field: field,
        config_options: Arc::new(ConfigOptions::default()),
    })
}

#[test]
fn guard_preserves_numeric_types_nulls_and_scalar_representation() -> Result<()> {
    for data_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal32(6, 2),
        DataType::Decimal64(12, 2),
        DataType::Decimal128(20, 5),
        DataType::Decimal256(45, 5),
        DataType::Null,
    ] {
        for ansi in [true, false] {
            let guard = ScalarUDF::from(SafeNumericDivisor::new(ZeroDivisionKind::Divide, ansi));
            let null = ScalarValue::try_new_null(&data_type)?;
            let ColumnarValue::Scalar(actual) =
                invoke(&guard, ColumnarValue::Scalar(null.clone()))?
            else {
                return datafusion_common::internal_err!("scalar null became an array");
            };
            assert_eq!(actual, null);
            let field = Arc::new(
                Field::new("divisor", data_type.clone(), false).with_metadata(
                    std::collections::HashMap::from([("owner".into(), "test".into())]),
                ),
            );
            let output = guard.return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&field),
                scalar_arguments: &[None],
            })?;
            assert_eq!(output.data_type(), &data_type);
            assert!(output.is_nullable());
            assert_eq!(output.metadata(), field.metadata());
            if data_type == DataType::Null {
                continue;
            }
            let zero = ScalarValue::new_zero(&data_type)?;
            let result = invoke(&guard, ColumnarValue::Scalar(zero));
            if ansi {
                assert!(
                    result
                        .err()
                        .is_some_and(|e| e.to_string().contains("DIVIDE_BY_ZERO"))
                );
            } else {
                let ColumnarValue::Scalar(actual) = result? else {
                    return datafusion_common::internal_err!("scalar zero became an array");
                };
                assert_eq!(actual, null);
            }
        }
    }
    Ok(())
}

#[test]
fn guard_handles_signed_zero_nan_infinity_and_sliced_arrays() -> Result<()> {
    let values = Float64Array::from(vec![
        Some(99.0),
        None,
        Some(0.0),
        Some(-0.0),
        Some(2.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
    ])
    .slice(1, 6);
    let guard = ScalarUDF::from(SafeNumericDivisor::new(ZeroDivisionKind::Remainder, false));
    let ColumnarValue::Array(result) = invoke(&guard, ColumnarValue::Array(Arc::new(values)))?
    else {
        return datafusion_common::internal_err!("array became scalar");
    };
    let actual = result
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| datafusion_common::internal_datafusion_err!("changed float datatype"))?;
    assert_eq!(actual.len(), 6);
    for i in 0..3 {
        assert!(actual.is_null(i));
    }
    assert_eq!(actual.value(3), 2.0);
    assert!(actual.value(4).is_nan());
    assert_eq!(actual.value(5), f64::INFINITY);
    let ansi = ScalarUDF::from(SafeNumericDivisor::new(ZeroDivisionKind::Remainder, true));
    for value in [0.0, -0.0] {
        let error = invoke(
            &ansi,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(value))),
        )
        .err()
        .ok_or_else(|| datafusion_common::internal_datafusion_err!("zero was accepted"))?;
        assert!(error.to_string().contains("REMAINDER_BY_ZERO"));
    }
    for value in [f64::NAN, f64::INFINITY, 2.0] {
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(actual))) = invoke(
            &ansi,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(value))),
        )?
        else {
            return datafusion_common::internal_err!("changed scalar representation");
        };
        assert_eq!(actual.to_bits(), value.to_bits());
    }
    let empty = Arc::new(Float64Array::from(Vec::<f64>::new()));
    let ColumnarValue::Array(result) = invoke(&ansi, ColumnarValue::Array(empty))? else {
        return datafusion_common::internal_err!("empty array became scalar");
    };
    assert_eq!(result.len(), 0);
    Ok(())
}
