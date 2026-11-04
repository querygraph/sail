use std::borrow::Cow;
use std::sync::Arc;

use datafusion_common::{DataFusionError, Result, plan_err};
use orc_rust::error::{OutOfSpecSnafu, Result as OrcResult};
use orc_rust::{TimestampTimeZone, TimestampTimeZoneOptions, TimestampTimeZoneResolver};
use serde::{Deserialize, Serialize};

use super::profile::{
    OFFSET_DOMAIN_MILLIS, PROFILE_BUNDLE_SHA256, ValidatedProfiles, embedded_profiles,
};

const SEMANTICS_VERSION: u32 = 1;

/// Captured reader rules for the bundled Java timezone compatibility profile.
///
/// The driver supplies its reader timezone once. Workers validate this descriptor
/// against their compiled profile instead of consulting their local timezone.
/// The SQL session's Arrow/display timezone is independent of these reader rules.
/// Deserialization must be followed by `native_options` to validate the identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrcJavaTimeZoneConfig {
    semantics_version: u32,
    profile_bundle_sha256: String,
    reader_timezone: String,
}

impl OrcJavaTimeZoneConfig {
    pub fn for_reader_timezone(reader_timezone: String) -> Result<Self> {
        let config = Self {
            semantics_version: SEMANTICS_VERSION,
            profile_bundle_sha256: PROFILE_BUNDLE_SHA256.to_string(),
            reader_timezone,
        };
        config.native_options()?;
        Ok(config)
    }

    pub fn reader_timezone(&self) -> &str {
        &self.reader_timezone
    }

    pub fn profile_bundle_sha256(&self) -> &str {
        &self.profile_bundle_sha256
    }

    pub fn native_options(&self) -> Result<TimestampTimeZoneOptions> {
        if self.semantics_version != SEMANTICS_VERSION {
            return plan_err!(
                "Unsupported ORC Java timezone semantics version {}",
                self.semantics_version
            );
        }
        if self.profile_bundle_sha256 != PROFILE_BUNDLE_SHA256 {
            return plan_err!(
                "ORC Java timezone profile identity mismatch: requested {}, compiled {}",
                self.profile_bundle_sha256,
                PROFILE_BUNDLE_SHA256
            );
        }
        let resolver = Arc::new(JavaTimeZoneResolver {
            profiles: embedded_profiles()?,
        });
        let reader = resolver
            .resolve(&self.reader_timezone)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(TimestampTimeZoneOptions::new(reader, resolver))
    }
}

#[derive(Debug)]
struct JavaTimeZoneResolver {
    profiles: Arc<ValidatedProfiles>,
}

impl JavaTimeZoneResolver {
    // Java preserves named aliases but normalizes custom offsets. Return the
    // resolved name separately from shared rules so the oracle checks both.
    fn resolve_name(&self, name: &str) -> (Arc<dyn TimestampTimeZone>, Cow<'_, str>) {
        if let Some(named) = self.profiles.exact_name(name) {
            return (named.profile, Cow::Borrowed(named.resolved_id));
        }
        if let Some(custom) = parse_custom_gmt(name) {
            let epoch = self.profiles.utc_epoch_seconds() - i64::from(custom.offset_millis / 1_000);
            return (
                Arc::new(FixedOffsetZone {
                    offset_millis: custom.offset_millis,
                    orc_epoch_seconds: epoch,
                }),
                Cow::Owned(custom.normalized_id),
            );
        }
        // This is Java TimeZone.getTimeZone(String)'s deliberate fallback, not
        // an error recovery policy for invalid profile data or numeric ranges.
        (self.profiles.gmt(), Cow::Borrowed("GMT"))
    }
}

impl TimestampTimeZoneResolver for JavaTimeZoneResolver {
    fn resolve(&self, name: &str) -> OrcResult<Arc<dyn TimestampTimeZone>> {
        Ok(self.resolve_name(name).0)
    }
}

#[derive(Debug)]
struct FixedOffsetZone {
    offset_millis: i32,
    orc_epoch_seconds: i64,
}

impl TimestampTimeZone for FixedOffsetZone {
    fn orc_epoch_seconds(&self) -> i64 {
        self.orc_epoch_seconds
    }

    fn offset_millis(&self, utc_millis: i64) -> OrcResult<i32> {
        if !(OFFSET_DOMAIN_MILLIS[0]..=OFFSET_DOMAIN_MILLIS[1]).contains(&utc_millis) {
            return OutOfSpecSnafu {
                msg: format!(
                    "ORC Java timezone query {utc_millis} is outside the qualified millisecond \
                     domain {}..={}",
                    OFFSET_DOMAIN_MILLIS[0], OFFSET_DOMAIN_MILLIS[1]
                ),
            }
            .fail();
        }
        Ok(self.offset_millis)
    }
}

struct CustomOffset {
    offset_millis: i32,
    normalized_id: String,
}

/// Java 20's string API accepts compact hours/HHMM and colon forms with seconds.
/// In the three-part form the middle component may have one or two digits; the
/// final component always needs two. All parsing is case-sensitive and ASCII.
fn parse_custom_gmt(name: &str) -> Option<CustomOffset> {
    let (sign, rest) = name.as_bytes().strip_prefix(b"GMT")?.split_first()?;
    let negative = match sign {
        b'+' => false,
        b'-' => true,
        _ => return None,
    };
    let mut components = rest.split(|byte| *byte == b':');
    let first = components.next()?;
    let second = components.next();
    let third = components.next();
    if components.next().is_some() {
        return None;
    }
    let (hours, minutes, seconds) = match (second, third) {
        (None, None) => {
            let value = digits(first, 1, 4)?;
            if first.len() <= 2 {
                (value, 0, 0)
            } else {
                (value / 100, value % 100, 0)
            }
        }
        (Some(minutes), None) => (digits(first, 1, 2)?, digits(minutes, 2, 2)?, 0),
        (Some(minutes), Some(seconds)) => (
            digits(first, 1, 2)?,
            digits(minutes, 1, 2)?,
            digits(seconds, 2, 2)?,
        ),
        (None, Some(_)) => return None,
    };
    if hours > 23 || minutes > 59 || seconds > 59 {
        return None;
    }
    let magnitude = (hours * 3_600 + minutes * 60 + seconds) * 1_000;
    let sign = if negative { '-' } else { '+' };
    let mut normalized_id = format!("GMT{sign}{hours:02}:{minutes:02}");
    if seconds != 0 {
        normalized_id.push_str(&format!(":{seconds:02}"));
    }
    Some(CustomOffset {
        offset_millis: if negative { -magnitude } else { magnitude },
        normalized_id,
    })
}

fn digits(value: &[u8], minimum: usize, maximum: usize) -> Option<i32> {
    if !(minimum..=maximum).contains(&value.len()) || !value.iter().all(u8::is_ascii_digit) {
        return None;
    }
    Some(
        value
            .iter()
            .fold(0, |value, digit| value * 10 + i32::from(digit - b'0')),
    )
}

#[cfg(test)]
#[path = "resolver_tests.rs"]
mod tests;
