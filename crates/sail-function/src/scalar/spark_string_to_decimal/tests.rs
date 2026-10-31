use arrow::array::{Array, StringArray};
use arrow::datatypes::Field;
use datafusion_common::config::ConfigOptions;
use datafusion_common::exec_err;
use sail_common_datafusion::error::{
    CommonErrorCause, PythonErrorCause, PythonErrorCauseExtractor,
};

use super::*;

mod boundaries;

struct NoPython;
impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn std::error::Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}

fn invoke(function: &SparkStringToDecimal, input: ColumnarValue) -> Result<ColumnarValue> {
    let rows = match &input {
        ColumnarValue::Array(a) => a.len(),
        _ => 1,
    };
    function.invoke_with_args(ScalarFunctionArgs {
        arg_fields: vec![Arc::new(Field::new("source", input.data_type(), true))],
        args: vec![input],
        number_rows: rows,
        return_field: Arc::new(Field::new(
            "result",
            DataType::Decimal128(function.precision(), function.scale()),
            true,
        )),
        config_options: Arc::new(ConfigOptions::default()),
    })
}

#[derive(serde::Deserialize)]
struct Case {
    source: Option<String>,
    precision: u8,
    scale: i8,
    fail_on_error: bool,
    #[serde(default)]
    allow_negative_scale: bool,
    expected: Option<i128>,
    condition: Option<String>,
    sql_state: Option<String>,
    parameters: HashMap<String, String>,
}

fn assert_case(result: Result<ColumnarValue>, case: &Case, scalar: bool) -> Result<()> {
    match (result, &case.condition) {
        (Err(error), Some(condition)) => {
            let CommonErrorCause::Structured(error) = CommonErrorCause::new::<NoPython>(&error)
            else {
                return exec_err!("missing structured decimal error: {error}");
            };
            assert_eq!(&error.condition, condition);
            assert_eq!(Some(&error.sql_state), case.sql_state.as_ref());
            assert_eq!(error.parameters, case.parameters);
            assert_eq!(
                error.class,
                if condition == "CAST_INVALID_INPUT" {
                    "java.lang.NumberFormatException"
                } else {
                    "org.apache.spark.SparkArithmeticException"
                }
            );
        }
        (Ok(value), None) => {
            assert_eq!(matches!(value, ColumnarValue::Scalar(_)), scalar);
            let value = match value {
                ColumnarValue::Scalar(v) => v,
                ColumnarValue::Array(a) => ScalarValue::try_from_array(&a, 0)?,
            };
            assert_eq!(
                value,
                ScalarValue::Decimal128(case.expected, case.precision, case.scale),
                "{:?}",
                case.source
            );
        }
        (result, condition) => {
            return exec_err!(
                "decimal outcome mismatch for {:?}: {result:?} vs {condition:?}",
                case.source
            );
        }
    }
    Ok(())
}

#[test]
fn spark_421_contract_matches_scalars_and_arrays_for_all_string_layouts() -> Result<()> {
    // 96 distinct policies/inputs, losslessly deduplicated from the recorded
    // Spark 4.2.1 192-cell ANSI/legacy CAST/TRY_CAST oracle. Legacy and TRY agree.
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/contract.json"))
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    assert_eq!(cases.len(), 96);
    check_cases(cases)
}

#[test]
fn spark_421_negative_scale_policy_contract_matches_all_string_layouts() -> Result<()> {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/policy-contract.json"))
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    assert_eq!(cases.len(), 32);
    check_cases(cases)
}

fn check_cases(cases: Vec<Case>) -> Result<()> {
    for case in cases {
        let function = SparkStringToDecimal::try_new(
            case.precision,
            case.scale,
            case.fail_on_error,
            case.allow_negative_scale,
        )?;
        for source in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let scalar = ColumnarValue::Scalar(ScalarValue::Utf8(case.source.clone()))
                .cast_to(&source, None)?;
            assert_case(invoke(&function, scalar), &case, true)?;
            let array =
                ColumnarValue::Array(Arc::new(StringArray::from(vec![case.source.as_deref()])))
                    .cast_to(&source, None)?;
            assert_case(invoke(&function, array), &case, false)?;
        }
    }
    Ok(())
}

#[test]
fn arrays_preserve_nulls_slices_empty_batches_and_first_error() -> Result<()> {
    for source in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let values = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("unused"),
            Some("1.235"),
            None,
            Some("bad"),
            Some("2e1"),
        ])))
        .cast_to(&source, None)?
        .into_array(5)?;
        let function = SparkStringToDecimal::try_new(5, 2, false, false)?;
        for array in [values.clone(), values.slice(1, 4), values.slice(0, 0)] {
            let result = invoke(&function, ColumnarValue::Array(Arc::clone(&array)))?
                .into_array(array.len())?;
            let expected = if array.len() == 5 {
                vec![None, Some(124), None, None, Some(2000)]
            } else if array.is_empty() {
                vec![]
            } else {
                vec![Some(124), None, None, Some(2000)]
            };
            assert_eq!(
                result.as_ref(),
                &Decimal128Array::from(expected).with_precision_and_scale(5, 2)? as &dyn Array
            );
        }
        let strict = SparkStringToDecimal::try_new(5, 2, true, false)?;
        let values = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            None,
            Some("first"),
            Some("second"),
        ])))
        .cast_to(&source, None)?;
        let error = invoke(&strict, values)
            .err()
            .ok_or_else(|| datafusion_common::exec_datafusion_err!("expected first invalid row"))?;
        let CommonErrorCause::Structured(error) = CommonErrorCause::new::<NoPython>(&error) else {
            return exec_err!("unstructured: {error}");
        };
        assert_eq!(
            error.parameters.get("expression").map(String::as_str),
            Some("'first'")
        );
    }
    Ok(())
}
