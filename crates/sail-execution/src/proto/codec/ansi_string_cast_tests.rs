use std::collections::HashMap;

use datafusion::arrow::array::StringArray;
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
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

fn invoke(function: &ScalarUDF, target: &DataType, input: ColumnarValue) -> Result<ColumnarValue> {
    let number_rows = match &input {
        ColumnarValue::Array(array) => array.len(),
        _ => 1,
    };
    function.invoke_with_args(ScalarFunctionArgs {
        arg_fields: vec![Arc::new(Field::new("value", input.data_type(), true))],
        args: vec![input],
        number_rows,
        return_field: Arc::new(Field::new("result", target.clone(), true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
}

#[test]
fn decoded_ansi_cast_executes_scalar_and_array_successes_and_native_failures() -> Result<()> {
    for target in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
    ] {
        let name = SparkAnsiStringCast::target_name(&target)
            .ok_or_else(|| plan_datafusion_err!("unsupported test target"))?;
        let original = ScalarUDF::from(SparkAnsiStringCast::try_new(target.clone(), name.into())?);
        let mut encoded = vec![];
        RemoteExecutionCodec.try_encode_udf(&original, &mut encoded)?;
        assert!(!encoded.is_empty());
        let decoded = RemoteExecutionCodec.try_decode_udf(original.name(), &encoded)?;
        let implementation = decoded
            .inner()
            .downcast_ref::<SparkAnsiStringCast>()
            .ok_or_else(|| plan_datafusion_err!("decoded wrong ANSI function"))?;
        assert_eq!(implementation.target_type(), &target);
        assert_eq!(implementation.sql_target(), name);
        let valid = if target == DataType::Boolean {
            "true"
        } else {
            "12"
        };
        for input in [
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(valid.into()))),
            ColumnarValue::Array(Arc::new(StringArray::from(vec![None, Some(valid)]))),
        ] {
            let expected = input.cast_to(&target, None)?;
            match (invoke(&decoded, &target, input)?, expected) {
                (ColumnarValue::Scalar(actual), ColumnarValue::Scalar(expected)) => {
                    assert_eq!(actual, expected)
                }
                (ColumnarValue::Array(actual), ColumnarValue::Array(expected)) => {
                    assert_eq!(actual.to_data(), expected.to_data())
                }
                _ => return plan_err!("decoded cast changed columnar representation"),
            }
        }
        for input in [
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("bad".into()))),
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                None,
                Some(valid),
                Some("bad"),
            ]))),
        ] {
            let Err(error) = invoke(&decoded, &target, input) else {
                return plan_err!("decoded malformed cast accepted");
            };
            let CommonErrorCause::Structured(error) = CommonErrorCause::new::<NoPython>(&error)
            else {
                return plan_err!("decoded cast lost structured failure: {error}");
            };
            assert_eq!(error.condition, "CAST_INVALID_INPUT");
            assert_eq!(error.sql_state, "22018");
            assert_eq!(
                error.parameters,
                HashMap::from([
                    ("expression".into(), "'bad'".into()),
                    ("sourceType".into(), "\"STRING\"".into()),
                    ("targetType".into(), format!("\"{name}\"")),
                    ("ansiConfig".into(), "\"spark.sql.ansi.enabled\"".into()),
                ])
            );
        }
    }
    Ok(())
}

#[test]
fn ansi_cast_codec_rejects_malformed_unsupported_and_mislabeled_payloads() -> Result<()> {
    let int = RemoteExecutionCodec.try_encode_data_type(&DataType::Int32)?;
    let date = RemoteExecutionCodec.try_encode_data_type(&DataType::Date32)?;
    for (target_type, sql_target, name) in [
        (vec![0xff], "INT", "spark_ansi_string_cast"),
        (date, "DATE", "spark_ansi_string_cast"),
        (int.clone(), "BOOLEAN", "spark_ansi_string_cast"),
        (int, "INT", "another_function"),
    ] {
        let payload = ExtendedScalarUdf {
            udf_kind: Some(r#gen::extended_scalar_udf::UdfKind::SparkAnsiStringCast(
                r#gen::SparkAnsiStringCastUdf {
                    target_type,
                    sql_target: sql_target.into(),
                },
            )),
        }
        .encode_to_vec();
        assert!(RemoteExecutionCodec.try_decode_udf(name, &payload).is_err());
    }
    assert!(
        RemoteExecutionCodec
            .try_decode_udf("spark_ansi_string_cast", &[])
            .is_err()
    );
    Ok(())
}
