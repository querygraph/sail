use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ScalarFunctionArgs, Volatility};
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

#[test]
fn zero_division_codec_preserves_kind_type_and_structured_failure() -> Result<()> {
    for (kind, name, condition) in [
        (
            ZeroDivisionKind::Divide,
            "spark_divide_by_zero",
            "DIVIDE_BY_ZERO",
        ),
        (
            ZeroDivisionKind::Remainder,
            "spark_remainder_by_zero",
            "REMAINDER_BY_ZERO",
        ),
    ] {
        let original = ScalarUDF::from(SparkZeroDivisionError::new(kind));
        let mut bytes = vec![];
        RemoteExecutionCodec.try_encode_udf(&original, &mut bytes)?;
        // Stateless Sail functions still use the existing StandardUdf envelope.
        let definition = ExtendedScalarUdf::decode(bytes.as_slice())
            .map_err(|error| plan_datafusion_err!("invalid standard definition: {error}"))?;
        assert!(matches!(
            definition.udf_kind,
            Some(UdfKind::Standard(r#gen::StandardUdf {}))
        ));
        let decoded = RemoteExecutionCodec.try_decode_udf(original.name(), &bytes)?;
        assert!(decoded.inner().is::<SparkZeroDivisionError>());
        assert_eq!(decoded.name(), name);
        assert_eq!(decoded.signature().volatility, Volatility::Immutable);
        assert_eq!(decoded.return_type(&[])?, DataType::Null);
        let result = decoded.invoke_with_args(ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("error", DataType::Null, true)),
            config_options: Arc::new(ConfigOptions::default()),
        });
        let error = result
            .err()
            .ok_or_else(|| plan_datafusion_err!("decoded arithmetic error succeeded"))?;
        let CommonErrorCause::Structured(error) = CommonErrorCause::new::<NoPython>(&error) else {
            return plan_err!("arithmetic error lost its structured cause: {error}");
        };
        assert_eq!(error.class, "org.apache.spark.SparkArithmeticException");
        assert_eq!(error.condition, condition);
        assert_eq!(error.sql_state, "22012");
        assert_eq!(
            error.parameters.get("config").map(String::as_str),
            Some("\"spark.sql.ansi.enabled\"")
        );
    }
    Ok(())
}
