use std::collections::BTreeMap;

use crate::error::{SparkError, SparkResult};

const RESET_ALLOWED: &str = "pipelines.reset.allowed";

pub(super) fn validate_reset(value: Option<&String>) -> SparkResult<()> {
    if let Some(value) = value
        && !value.eq_ignore_ascii_case("true")
        && !value.eq_ignore_ascii_case("false")
    {
        return Err(SparkError::invalid(format!(
            "Could not parse value '{value}' for table property '{RESET_ALLOWED}'"
        )));
    }
    Ok(())
}

pub(super) fn canonicalize(
    properties: BTreeMap<String, String>,
) -> SparkResult<BTreeMap<String, String>> {
    // Spark validates by exact canonical key before canonicalizing storage.
    // In particular, a mixed-case key does not participate in this lookup.
    validate_reset(properties.get(RESET_ALLOWED))?;
    Ok(properties
        .into_iter()
        .filter_map(|(key, value)| {
            let lower = key.to_ascii_lowercase();
            if lower == RESET_ALLOWED {
                Some((lower, value))
            } else if lower.starts_with("pipelines.") {
                log::warn!("Unknown pipelines table property '{key}'.");
                None
            } else {
                if lower == "reset.allowed" {
                    log::warn!(
                        "Table property '{key}' resembles '{RESET_ALLOWED}'; include the pipelines prefix to set the pipeline property."
                    );
                }
                Some((key, value))
            }
        })
        .collect())
}
