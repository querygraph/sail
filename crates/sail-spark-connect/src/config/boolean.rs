use std::collections::HashMap;

use sail_data_source::error::DataSourceResult;
use sail_data_source::options::parsers::parse_spark_bool;

use crate::error::{SparkError, SparkResult, StructuredSparkError};

/// SQLConf uses Java String.trim before Scala's strict boolean parser.
pub(super) fn parse_sql_bool(key: &str, value: &str) -> DataSourceResult<bool> {
    parse_spark_bool(key, value.trim_matches(|c| c <= '\u{20}'))
}

pub(super) fn parse_config_bool(key: &str, value: &str) -> SparkResult<bool> {
    parse_sql_bool(key, value).map_err(|_| SparkError::Structured(Box::new(StructuredSparkError {
        class: "org.apache.spark.SparkIllegalArgumentException".into(),
        condition: "INVALID_CONF_VALUE.TYPE_MISMATCH".into(),
        sql_state: "22022".into(),
        message: format!("The value '{value}' in the config \"{key}\" is invalid. It should be a/an 'boolean' value."),
        parameters: HashMap::from([
            ("confName".into(), key.into()),
            ("confValue".into(), value.into()),
            ("confType".into(), "boolean".into()),
        ]),
    })))
}
