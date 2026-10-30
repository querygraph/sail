use sail_data_source::formats::orc::{
    OrcListingSessionDefaults, OrcReadSessionModes, OrcWriteCompression, OrcWriteSessionDefaults,
};
use sail_data_source::options::parsers::parse_spark_bool;

use super::SparkRuntimeConfig;
use crate::error::{SparkError, SparkResult};
use crate::spark::config::SparkConfigKey;

pub(crate) const ORC_COMPRESSION: &str = "spark.sql.orc.compression.codec";

const FORCE_POSITIONAL_EVOLUTION: &str = "orc.force.positional.evolution";

pub(crate) fn affects_orc_config(key: &str) -> bool {
    matches!(
        key,
        SparkConfigKey::SPARK_SQL_ORC_MERGE_SCHEMA
            | SparkConfigKey::SPARK_SQL_ORC_IMPL
            | FORCE_POSITIONAL_EVOLUTION
            | ORC_COMPRESSION
    )
}

pub(super) fn parse_merge_schema(value: &str) -> SparkResult<bool> {
    // SQLConf applies Java String.trim (only characters <= U+0020) before
    // Scala's strict boolean parser. Operation options do not trim.
    parse_spark_bool(
        SparkConfigKey::SPARK_SQL_ORC_MERGE_SCHEMA,
        value.trim_matches(|character| character <= '\u{20}'),
    )
    .map_err(|_| {
        SparkError::invalid(format!(
            "{} must be a boolean: {value}",
            SparkConfigKey::SPARK_SQL_ORC_MERGE_SCHEMA
        ))
    })
}

impl SparkRuntimeConfig {
    pub(crate) fn orc_read_modes(&self) -> OrcReadSessionModes {
        OrcReadSessionModes {
            implementation: self
                .get_option(SparkConfigKey::SPARK_SQL_ORC_IMPL)
                .unwrap_or("native")
                .to_string(),
            // SessionState copies exact explicit SQLConf names into Hadoop;
            // OrcConf then uses untrimmed Java Boolean.parseBoolean.
            force_positional_evolution: self
                .get_option(FORCE_POSITIONAL_EVOLUTION)
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
        }
    }

    pub(crate) fn orc_listing_defaults(&self) -> SparkResult<OrcListingSessionDefaults> {
        let merge_schema = self
            .get_option(SparkConfigKey::SPARK_SQL_ORC_MERGE_SCHEMA)
            .map(parse_merge_schema)
            .transpose()?
            .unwrap_or_default();
        Ok(OrcListingSessionDefaults { merge_schema })
    }
}

pub(super) fn parse_write_compression(value: &str) -> SparkResult<OrcWriteCompression> {
    OrcWriteCompression::resolve(None, None, Some(value))
        .map_err(|error| SparkError::invalid(error.to_string()))
}

impl SparkRuntimeConfig {
    pub(crate) fn orc_write_defaults(&self) -> SparkResult<OrcWriteSessionDefaults> {
        Ok(OrcWriteSessionDefaults {
            compression: self
                .get_option(ORC_COMPRESSION)
                .map(parse_write_compression)
                .transpose()?
                .unwrap_or_default(),
        })
    }
}
