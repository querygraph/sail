use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use datafusion_common::{DataFusionError, Result};
use orc_rust::TimestampTimeZone;
use orc_rust::error::{OutOfSpecSnafu, Result as OrcResult};
use serde::Deserialize;

pub(super) const PROFILE_BUNDLE_SHA256: &str =
    "1f90695f80f862138bf1635b8727281eb7b0a76d838513eaf09675cb6ee7212b";
pub(super) const OFFSET_DOMAIN_MILLIS: [i64; 2] = [-9_223_380_626_789_368, 9_223_380_626_789_367];
const UTC_EPOCH_SECONDS: i64 = 1_420_070_400;
const CYCLE_PERIOD_MILLIS: i64 = 12_622_780_800_000;
const EMBEDDED_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/data/orc/timezone/java-timezone-profiles.json"
));

// This parser is private and accepts only the trusted compiled asset. Its exact
// bytes, canonical object hashes and duplicate keys are checked by the asset
// generation gate. Runtime validation is not arbitrary-file hash verification.
static PROFILES: LazyLock<std::result::Result<Arc<ValidatedProfiles>, String>> =
    LazyLock::new(|| parse_embedded(EMBEDDED_JSON).map(Arc::new));

pub(super) fn embedded_profiles() -> Result<Arc<ValidatedProfiles>> {
    PROFILES.as_ref().map(Arc::clone).map_err(|message| {
        DataFusionError::Internal(format!(
            "Invalid embedded ORC Java timezone bundle: {message}"
        ))
    })
}

#[derive(Debug)]
pub(super) struct ValidatedProfiles {
    zones: BTreeMap<String, NamedZone>,
    gmt: Arc<Profile>,
}

#[derive(Debug)]
struct NamedZone {
    profile: Arc<Profile>,
    resolved_id: String,
}

pub(super) struct ResolvedNamedZone<'a> {
    pub(super) profile: Arc<dyn TimestampTimeZone>,
    pub(super) resolved_id: &'a str,
}

impl ValidatedProfiles {
    pub(super) fn exact_name(&self, name: &str) -> Option<ResolvedNamedZone<'_>> {
        self.zones.get(name).map(|zone| ResolvedNamedZone {
            profile: zone.profile.clone(),
            resolved_id: &zone.resolved_id,
        })
    }

    pub(super) fn gmt(&self) -> Arc<dyn TimestampTimeZone> {
        self.gmt.clone()
    }

    pub(super) fn utc_epoch_seconds(&self) -> i64 {
        UTC_EPOCH_SECONDS
    }
}

#[derive(Debug)]
struct Profile {
    id: String,
    raw_prefix_offset_millis: i32,
    orc_epoch_seconds: i64,
    transitions: Box<[(i64, i32)]>,
    future: Future,
}

#[derive(Debug)]
enum Future {
    Fixed(i32),
    Cycle(Arc<Cycle>),
}

#[derive(Debug)]
struct Cycle {
    anchor_millis: i64,
    rows: Box<[(i64, i32)]>,
}

impl TimestampTimeZone for Profile {
    fn orc_epoch_seconds(&self) -> i64 {
        self.orc_epoch_seconds
    }

    fn offset_millis(&self, utc_millis: i64) -> OrcResult<i32> {
        if !(OFFSET_DOMAIN_MILLIS[0]..=OFFSET_DOMAIN_MILLIS[1]).contains(&utc_millis) {
            return Err(OutOfSpecSnafu {
                msg: format!(
                    "ORC Java timezone profile {}: instant {utc_millis} is outside qualified millisecond domain {:?}",
                    self.id, OFFSET_DOMAIN_MILLIS
                ),
            }.build());
        }
        let index = self
            .transitions
            .partition_point(|(millis, _)| *millis <= utc_millis);
        if index == 0 {
            return Ok(self.raw_prefix_offset_millis);
        }
        let (last_millis, offset) = self.transitions[index - 1];
        if index < self.transitions.len() || utc_millis == last_millis {
            return Ok(offset);
        }
        Ok(match &self.future {
            Future::Fixed(offset) => *offset,
            Future::Cycle(cycle) => {
                let position = (i128::from(utc_millis) - i128::from(cycle.anchor_millis))
                    .rem_euclid(i128::from(CYCLE_PERIOD_MILLIS));
                // Validated cycle rows start at zero, so this index is positive.
                let index = cycle
                    .rows
                    .partition_point(|(millis, _)| i128::from(*millis) <= position);
                cycle.rows[index - 1].1
            }
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BundleDto {
    format_version: u32,
    configuration: ConfigurationDto,
    zones: BTreeMap<String, ZoneDto>,
    profiles: BTreeMap<String, ProfileDto>,
    cycles: BTreeMap<String, CycleDto>,
    java_aliases: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigurationDto {
    java_runtime_version: String,
    java_vendor: String,
    tzdb_versions: Vec<String>,
    legacy_id_mapping: bool,
    format_locale: String,
    epoch_calendar: String,
    epoch_text: String,
    utc_epoch_seconds: i64,
    offset_domain_millis: [i64; 2],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ZoneDto {
    profile: String,
    resolved_id: String,
    available: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileDto {
    raw_prefix_offset_millis: i32,
    orc_epoch_seconds: i64,
    transitions: Vec<(i64, i32)>,
    future: FutureDto,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
enum FutureDto {
    Fixed { offset_millis: i32 },
    Cycle { id: String },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CycleDto {
    anchor_millis: i64,
    period_millis: i64,
    rows: Vec<(i64, i32)>,
}

fn parse_embedded(json: &str) -> std::result::Result<ValidatedProfiles, String> {
    let bundle: BundleDto = serde_json::from_str(json).map_err(|error| error.to_string())?;
    validate_configuration(bundle.format_version, &bundle.configuration)?;
    let mut cycles = BTreeMap::new();
    for (id, cycle) in bundle.cycles {
        validate_id(&id)?;
        if cycle.period_millis != CYCLE_PERIOD_MILLIS
            || !(OFFSET_DOMAIN_MILLIS[0]..=OFFSET_DOMAIN_MILLIS[1]).contains(&cycle.anchor_millis)
            || cycle.rows.first().map(|row| row.0) != Some(0)
        {
            return Err(format!(
                "cycle {id} has an invalid period, anchor or first row"
            ));
        }
        validate_rows(&cycle.rows, 0, CYCLE_PERIOD_MILLIS - 1)
            .map_err(|error| format!("cycle {id}: {error}"))?;
        cycles.insert(
            id,
            Arc::new(Cycle {
                anchor_millis: cycle.anchor_millis,
                rows: cycle.rows.into_boxed_slice(),
            }),
        );
    }
    let mut profiles = BTreeMap::new();
    for (id, profile) in bundle.profiles {
        validate_id(&id)?;
        validate_rows(
            &profile.transitions,
            OFFSET_DOMAIN_MILLIS[0],
            OFFSET_DOMAIN_MILLIS[1],
        )
        .map_err(|error| format!("profile {id}: {error}"))?;
        let epoch_millis = i128::from(profile.orc_epoch_seconds) * 1_000;
        if !(i128::from(OFFSET_DOMAIN_MILLIS[0])..=i128::from(OFFSET_DOMAIN_MILLIS[1]))
            .contains(&epoch_millis)
        {
            return Err(format!(
                "profile {id} ORC epoch is outside the qualified domain"
            ));
        }
        if profile.transitions.is_empty()
            && !matches!(&profile.future, FutureDto::Fixed { offset_millis } if *offset_millis == profile.raw_prefix_offset_millis)
        {
            return Err(format!(
                "profile {id} without history requires a fixed future equal to its raw prefix"
            ));
        }
        let future = match profile.future {
            FutureDto::Fixed { offset_millis } => Future::Fixed(offset_millis),
            FutureDto::Cycle { id: cycle } => Future::Cycle(
                cycles
                    .get(&cycle)
                    .ok_or_else(|| format!("profile {id} references missing cycle {cycle}"))?
                    .clone(),
            ),
        };
        profiles.insert(
            id.clone(),
            Arc::new(Profile {
                id,
                raw_prefix_offset_millis: profile.raw_prefix_offset_millis,
                orc_epoch_seconds: profile.orc_epoch_seconds,
                transitions: profile.transitions.into_boxed_slice(),
                future,
            }),
        );
    }
    // Aliases are retained as provenance, never traversed to resolve a name.
    for (alias, target) in bundle.java_aliases {
        if !bundle.zones.contains_key(&alias) || !bundle.zones.contains_key(&target) {
            return Err(format!(
                "provenance alias {alias} references an absent exact name {target}"
            ));
        }
    }
    let mut zones = BTreeMap::new();
    for (name, zone) in bundle.zones {
        if name.is_empty() || zone.resolved_id.is_empty() || !zone.available {
            return Err(format!(
                "zone {name:?} is not an available named zone in the selected baseline"
            ));
        }
        let profile = profiles
            .get(&zone.profile)
            .ok_or_else(|| format!("zone {name:?} references missing profile {}", zone.profile))?
            .clone();
        zones.insert(
            name,
            NamedZone {
                profile,
                resolved_id: zone.resolved_id,
            },
        );
    }
    let gmt = zones.get("GMT").ok_or("missing GMT profile")?;
    if gmt.resolved_id != "GMT"
        || gmt.profile.raw_prefix_offset_millis != 0
        || gmt.profile.orc_epoch_seconds != UTC_EPOCH_SECONDS
        || !gmt.profile.transitions.is_empty()
        || !matches!(&gmt.profile.future, Future::Fixed(0))
    {
        return Err("invalid GMT fallback profile".to_string());
    }
    let gmt = gmt.profile.clone();
    Ok(ValidatedProfiles { zones, gmt })
}

fn validate_configuration(
    version: u32,
    config: &ConfigurationDto,
) -> std::result::Result<(), String> {
    if version != 1
        || config.java_runtime_version != "20.0.2+9-78"
        || config.java_vendor != "Oracle Corporation"
        || config.tzdb_versions != ["2023c"]
        || config.legacy_id_mapping
        || config.format_locale != "en-US"
        || config.epoch_calendar != "java.util.GregorianCalendar"
        || config.epoch_text != "2015-01-01 00:00:00"
        || config.utc_epoch_seconds != UTC_EPOCH_SECONDS
        || config.offset_domain_millis != OFFSET_DOMAIN_MILLIS
    {
        return Err("unsupported format or Java timezone configuration".to_string());
    }
    Ok(())
}

fn validate_id(id: &str) -> std::result::Result<(), String> {
    if id.len() != 64
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!("invalid profile/cycle identity {id:?}"));
    }
    Ok(())
}

fn validate_rows(rows: &[(i64, i32)], low: i64, high: i64) -> std::result::Result<(), String> {
    if rows
        .iter()
        .any(|(millis, _)| !(low..=high).contains(millis))
        || rows.windows(2).any(|pair| pair[0].0 >= pair[1].0)
    {
        return Err(
            "transition positions must be strictly increasing and inside their domain".to_string(),
        );
    }
    Ok(())
}

#[cfg(test)]
#[path = "profile_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "profile_oracle_tests.rs"]
mod oracle_tests;
