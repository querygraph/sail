use std::time::Duration;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::ScalarValue;
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use sail_common_datafusion::error::{
    CommonErrorCause, PythonErrorCause, PythonErrorCauseExtractor,
};
use sail_function::scalar::spark_string_to_decimal::SparkStringToDecimal;
use tonic_types::StatusExt;

use super::*;
use crate::session::{SparkSession, SparkSessionOptions};

type TestResult = Result<(), Box<dyn std::error::Error>>;
const KEY: &str = SparkConfigKey::SPARK_SQL_LEGACY_ALLOW_NEGATIVE_SCALE_OF_DECIMAL;
const ANSI: &str = SparkConfigKey::SPARK_SQL_ANSI_ENABLED;

fn session() -> SparkResult<SparkSession> {
    pyo3::Python::initialize();
    SparkSession::try_new(
        "decimal-config".into(),
        "test-user".into(),
        SparkSessionOptions {
            execution_heartbeat_interval: Duration::from_secs(1),
        },
    )
}
fn set(spark: &SparkSession, key: &str, value: &str) -> SparkResult<()> {
    spark.set_config(vec![ConfigKeyValue {
        key: key.into(),
        value: Some(value.into()),
    }])
}
fn assert_invalid(error: SparkError, value: &str) -> TestResult {
    let status: tonic::Status = error.into();
    let details = status.get_error_details();
    let info = details
        .error_info()
        .ok_or("missing config error metadata")?;
    assert_eq!(
        info.reason,
        "org.apache.spark.SparkIllegalArgumentException"
    );
    assert_eq!(
        info.metadata["errorClass"],
        "INVALID_CONF_VALUE.TYPE_MISMATCH"
    );
    assert_eq!(info.metadata["sqlState"], "22022");
    assert_eq!(
        serde_json::from_str::<HashMap<String, String>>(&info.metadata["messageParameters"])?,
        HashMap::from([
            ("confName".into(), KEY.into()),
            ("confValue".into(), value.into()),
            ("confType".into(), "boolean".into()),
        ])
    );
    assert_eq!(
        serde_json::from_str::<Vec<String>>(&info.metadata["classes"])?,
        [
            "org.apache.spark.SparkIllegalArgumentException",
            "java.lang.IllegalArgumentException",
            "java.lang.RuntimeException",
            "java.lang.Exception",
            "java.lang.Throwable",
            "java.lang.Object",
        ]
    );
    Ok(())
}

#[test]
fn decimal_policy_uses_spark_42_defaults_and_java_boolean_syntax() -> TestResult {
    // Pin metadata explicitly; these expectations must not silently use the
    // Rust test process's best-effort PySpark-version fallback.
    let mut runtime = SparkRuntimeConfig {
        entries: &SPARK_CONFIG_V4_2,
        config: HashMap::new(),
    };
    assert!(!PlanConfig::default().legacy_allow_negative_scale_of_decimal);
    assert!(!PlanConfig::try_from(&runtime)?.legacy_allow_negative_scale_of_decimal);
    for (value, expected) in [
        ("true", true),
        ("false", false),
        (" TrUe\t", true),
        ("\0true\0", true),
        ("falſe", false),
        ("FALſE", false),
    ] {
        runtime.set(KEY.into(), value.into())?;
        assert_eq!(runtime.get(KEY)?, Some(value));
        assert_eq!(
            PlanConfig::try_from(&runtime)?.legacy_allow_negative_scale_of_decimal,
            expected
        );
    }
    runtime.unset(KEY)?;
    assert!(!PlanConfig::try_from(&runtime)?.legacy_allow_negative_scale_of_decimal);
    Ok(())
}

struct NoPython;
impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn std::error::Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}
fn range_result(config: &PlanConfig) -> Result<String, Box<dyn std::error::Error>> {
    // This is the callable component consuming the actual captured settings;
    // SQL resolver routing is intentionally a separate unit.
    let function = SparkStringToDecimal::try_new(
        5,
        2,
        config.ansi_mode,
        config.legacy_allow_negative_scale_of_decimal,
    )?;
    let result = function.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "1e39".into(),
        )))],
        arg_fields: vec![Arc::new(Field::new("source", DataType::Utf8, false))],
        number_rows: 1,
        return_field: Arc::new(Field::new("result", DataType::Decimal128(5, 2), true)),
        config_options: Arc::new(ConfigOptions::default()),
    });
    match result {
        Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(None, 5, 2))) => Ok("NULL".into()),
        Err(error) => {
            let CommonErrorCause::Structured(error) = CommonErrorCause::new::<NoPython>(&error)
            else {
                return Err(format!("unstructured range error: {error}").into());
            };
            Ok(error.condition)
        }
        result => Err(format!("unexpected decimal result: {result:?}").into()),
    }
}

#[test]
fn decimal_session_and_override_snapshots_keep_independent_semantics() -> TestResult {
    let spark = session()?;
    set(&spark, ANSI, "true")?;
    let initial = spark.plan_config()?;
    set(&spark, KEY, "true")?;
    let allowed = spark.plan_config()?;
    let overridden = spark.plan_config_with_overrides([(KEY.into(), "false".into())])?;
    assert_eq!(range_result(&initial)?, "NUMERIC_OUT_OF_SUPPORTED_RANGE");
    assert_eq!(
        range_result(&allowed)?,
        "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION"
    );
    assert_eq!(range_result(&overridden)?, "NUMERIC_OUT_OF_SUPPORTED_RANGE");
    assert_eq!(
        range_result(spark.plan_config()?.as_ref())?,
        "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION"
    );
    let null_on_error = spark.plan_config_with_overrides([(ANSI.into(), "false".into())])?;
    assert_eq!(range_result(&null_on_error)?, "NULL");
    spark.unset_config(vec![KEY.into()])?;
    assert_eq!(
        range_result(spark.plan_config()?.as_ref())?,
        "NUMERIC_OUT_OF_SUPPORTED_RANGE"
    );
    assert_eq!(
        range_result(&allowed)?,
        "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION"
    );
    let allow_override = spark.plan_config_with_overrides([(KEY.into(), "true".into())])?;
    assert_eq!(
        range_result(&allow_override)?,
        "NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION"
    );
    assert!(!spark.plan_config()?.legacy_allow_negative_scale_of_decimal);
    assert_eq!(allowed.session_user_id, "test-user");
    Ok(())
}

#[test]
fn invalid_decimal_set_and_override_preserve_prior_session_value() -> TestResult {
    let spark = session()?;
    set(&spark, KEY, "true")?;
    for value in ["1", "0", "yes", "", "\u{a0}true\u{a0}", "truſe"] {
        let error = set(&spark, KEY, value)
            .err()
            .ok_or("invalid SET accepted")?;
        assert_invalid(error, value)?;
        assert!(spark.plan_config()?.legacy_allow_negative_scale_of_decimal);
        assert_eq!(
            spark
                .get_config(vec![KEY.into()])?
                .first()
                .and_then(|v| v.value.as_deref()),
            Some("true")
        );
        let error = spark
            .plan_config_with_overrides([(KEY.into(), value.into())])
            .err()
            .ok_or("invalid override accepted")?;
        assert_invalid(error, value)?;
        assert!(spark.plan_config()?.legacy_allow_negative_scale_of_decimal);
    }
    Ok(())
}
