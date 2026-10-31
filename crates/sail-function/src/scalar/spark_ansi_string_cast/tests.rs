use std::collections::HashMap;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::Field;
use datafusion_common::config::ConfigOptions;
use datafusion_common::exec_err;
use sail_common_datafusion::error::{
    CommonErrorCause, PythonErrorCause, PythonErrorCauseExtractor,
};

use super::*;

struct NoPython;
impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn std::error::Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}

fn function(target: DataType) -> Result<SparkAnsiStringCast> {
    let name = SparkAnsiStringCast::target_name(&target)
        .ok_or_else(|| datafusion_common::plan_datafusion_err!("unsupported test type"))?;
    SparkAnsiStringCast::try_new(target, name.into())
}

fn invoke(function: &SparkAnsiStringCast, input: ColumnarValue) -> Result<ColumnarValue> {
    let number_rows = match &input {
        ColumnarValue::Array(array) => array.len(),
        _ => 1,
    };
    function.invoke_with_args(ScalarFunctionArgs {
        arg_fields: vec![Arc::new(Field::new("value", input.data_type(), true))],
        args: vec![input],
        number_rows,
        return_field: Arc::new(Field::new("result", function.target_type().clone(), true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
}

fn rejected(value: Result<ColumnarValue>) -> Result<DataFusionError> {
    match value {
        Err(error) => Ok(error),
        Ok(_) => exec_err!("malformed cast accepted"),
    }
}

fn assert_invalid(error: DataFusionError, target: &DataType, value: &str) -> Result<()> {
    let CommonErrorCause::Structured(error) = CommonErrorCause::new::<NoPython>(&error) else {
        return exec_err!("missing structured cast failure: {error}");
    };
    assert_eq!(error.condition, "CAST_INVALID_INPUT");
    assert_eq!(error.sql_state, "22018");
    assert_eq!(
        error.class,
        if target == &DataType::Boolean {
            "org.apache.spark.SparkRuntimeException"
        } else {
            "java.lang.NumberFormatException"
        }
    );
    let quoted = format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"));
    assert_eq!(
        error.parameters,
        HashMap::from([
            ("expression".into(), quoted.clone()),
            ("sourceType".into(), "\"STRING\"".into()),
            (
                "targetType".into(),
                format!(
                    "\"{}\"",
                    SparkAnsiStringCast::target_name(target).unwrap_or("unexpected")
                )
            ),
            ("ansiConfig".into(), "\"spark.sql.ansi.enabled\"".into()),
        ])
    );
    assert!(error.message.contains(&quoted));
    Ok(())
}

#[test]
fn primitive_successes_preserve_kernel_values_for_scalars_arrays_nulls_and_empty_arrays()
-> Result<()> {
    for target in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
    ] {
        let function = function(target.clone())?;
        let value = if target == DataType::Boolean {
            "true"
        } else {
            "12"
        };
        for source in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            for input in [
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.into()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Array(Arc::new(StringArray::from(vec![Some(value), None]))),
                ColumnarValue::Array(Arc::new(StringArray::from(Vec::<Option<&str>>::new()))),
            ] {
                let input = input.cast_to(&source, None)?;
                let expected = input.cast_to(&target, Some(&DEFAULT_CAST_OPTIONS))?;
                let actual = invoke(&function, input)?;
                match (actual, expected) {
                    (ColumnarValue::Scalar(actual), ColumnarValue::Scalar(expected)) => {
                        assert_eq!(actual, expected)
                    }
                    (ColumnarValue::Array(actual), ColumnarValue::Array(expected)) => {
                        assert_eq!(actual.to_data(), expected.to_data())
                    }
                    _ => return exec_err!("cast changed scalar/array representation"),
                }
            }
        }
    }
    Ok(())
}

#[test]
fn malformed_scalars_keep_native_class_and_complete_parameters() -> Result<()> {
    for target in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
    ] {
        let function = function(target.clone())?;
        for source in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let value = "a'b\\雪\n";
            let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.into())))
                .cast_to(&source, None)?;
            assert_invalid(rejected(invoke(&function, input))?, &target, value)?;
        }
    }
    let function = function(DataType::Int32)?;
    assert_invalid(
        rejected(invoke(
            &function,
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("2147483648".into()))),
        ))?,
        &DataType::Int32,
        "2147483648",
    )?;
    Ok(())
}

#[test]
fn sliced_arrays_report_first_failing_non_null_retained_value() -> Result<()> {
    for source in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let input = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("outside"),
            Some("1"),
            None,
            Some("bad"),
            Some("later"),
        ])))
        .cast_to(&source, None)?;
        let ColumnarValue::Array(input) = input else {
            return exec_err!("expected array");
        };
        let input: ArrayRef = input.slice(1, 4);
        assert_invalid(
            rejected(invoke(
                &function(DataType::Int32)?,
                ColumnarValue::Array(input),
            ))?,
            &DataType::Int32,
            "bad",
        )?;
    }
    Ok(())
}

#[test]
fn target_constructor_rejects_unsupported_or_mislabeled_types() {
    for (target, name) in [
        (DataType::Int32, "INTEGER"),
        (DataType::Int32, "BOOLEAN"),
        (DataType::Decimal128(8, 2), "DECIMAL(8,2)"),
        (DataType::Date32, "DATE"),
        (DataType::Utf8, "STRING"),
    ] {
        assert!(SparkAnsiStringCast::try_new(target, name.into()).is_err());
    }
}

#[test]
fn return_field_preserves_metadata_and_spark_string_cast_nullability() -> Result<()> {
    for source in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let bad = match source {
            DataType::Utf8 => ScalarValue::Utf8(Some("bad".into())),
            DataType::LargeUtf8 => ScalarValue::LargeUtf8(Some("bad".into())),
            _ => ScalarValue::Utf8View(Some("bad".into())),
        };
        for nullable in [false, true] {
            let input = Arc::new(Field::new("value", source.clone(), nullable).with_metadata(
                HashMap::from([
                    ("comment".into(), "source comment".into()),
                    (EXTENSION_TYPE_NAME_KEY.into(), "arrow.json".into()),
                    (EXTENSION_TYPE_METADATA_KEY.into(), "{}".into()),
                ]),
            ));
            for target in [
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::Float32,
                DataType::Float64,
                DataType::Boolean,
            ] {
                // Field inference is type-only, even for a known malformed literal.
                for value in [None, Some(&bad)] {
                    let actual =
                        function(target.clone())?.return_field_from_args(ReturnFieldArgs {
                            arg_fields: std::slice::from_ref(&input),
                            scalar_arguments: &[value],
                        })?;
                    assert_eq!(actual.data_type(), &target);
                    assert!(actual.is_nullable());
                    assert_eq!(
                        actual.metadata(),
                        &HashMap::from([("comment".into(), "source comment".into()),])
                    );
                }
            }
        }
    }
    Ok(())
}
