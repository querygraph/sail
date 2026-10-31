use std::collections::HashMap;

use datafusion_common::DataFusionError;
use sail_common_datafusion::error::StructuredSparkError;

pub(super) fn invalid_string_cast(value: &str, target: &str, boolean: bool) -> DataFusionError {
    let expression = format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"));
    let source_type = "\"STRING\"";
    let target_type = format!("\"{target}\"");
    let message = format!(
        "The value {expression} of the type {source_type} cannot be cast to {target_type} because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead."
    );
    DataFusionError::External(Box::new(StructuredSparkError {
        class: if boolean {
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
