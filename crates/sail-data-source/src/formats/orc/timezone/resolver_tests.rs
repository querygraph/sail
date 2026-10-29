#![expect(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::fmt::Write;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::*;

const ORACLE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/orc/timezone/java-timezone-resolver-oracle.json"
));
const PROFILES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/data/orc/timezone/java-timezone-profiles.json"
));

#[derive(Deserialize)]
struct Oracle {
    case_count: usize,
    oracle_sha256: String,
    examples: BTreeMap<String, OracleCase>,
}

#[derive(Deserialize)]
struct OracleCase {
    resolved_id: String,
    offset_at_unix_epoch_millis: i32,
    orc_epoch_seconds: i64,
}

fn resolver() -> JavaTimeZoneResolver {
    JavaTimeZoneResolver {
        profiles: embedded_profiles().unwrap(),
    }
}

#[test]
fn retained_java_examples_report_exact_names_offsets_and_epochs() {
    let resolver = resolver();
    let oracle: Oracle = serde_json::from_str(ORACLE).unwrap();
    assert_eq!(oracle.examples.len(), 31);
    for (name, expected) in oracle.examples {
        let (profile, resolved_id) = resolver.resolve_name(&name);
        assert_eq!(resolved_id, expected.resolved_id, "{name:?}");
        assert_eq!(
            profile.offset_millis(0).unwrap(),
            expected.offset_at_unix_epoch_millis,
            "{name:?}"
        );
        assert_eq!(
            profile.orc_epoch_seconds(),
            expected.orc_epoch_seconds,
            "{name:?}"
        );
    }
}

#[test]
fn complete_resolver_sequence_matches_retained_public_java_digest() {
    #[derive(Deserialize)]
    struct Names {
        zones: BTreeMap<String, serde_json::Value>,
    }

    let names: Names = serde_json::from_str(PROFILES).unwrap();
    let oracle: Oracle = serde_json::from_str(ORACLE).unwrap();
    let resolver = resolver();
    let mut digest = Sha256::new();
    let mut count = 0;
    visit_oracle_cases(&names.zones.into_keys().collect::<Vec<_>>(), |name| {
        let (profile, resolved_id) = resolver.resolve_name(name);
        let row = (
            name,
            resolved_id.as_ref(),
            profile.offset_millis(0).unwrap(),
            profile.orc_epoch_seconds(),
        );
        digest.update(serde_json::to_vec(&row).unwrap());
        digest.update(b"\n");
        count += 1;
    });
    assert_eq!(count, 315_351);
    assert_eq!(count, oracle.case_count);
    let mut observed = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(observed, "{byte:02x}").unwrap();
    }
    assert_eq!(observed, oracle.oracle_sha256);
}

// This enumerates the retained Java oracle's input sequence. Expected results
// are the public Java digest, not values computed by the Rust custom-GMT parser.
fn visit_oracle_cases(names: &[String], mut visit: impl FnMut(&str)) {
    for sign in ['+', '-'] {
        let mut limit = 10;
        for width in 1..=4 {
            for value in 0..limit {
                visit(&format!("GMT{sign}{value:0width$}"));
            }
            limit *= 10;
        }
        for hour in 0..24 {
            for h in variants(hour) {
                for minute in 0..60 {
                    visit(&format!("GMT{sign}{h}:{minute:02}"));
                    for m in variants(minute) {
                        for second in 0..60 {
                            visit(&format!("GMT{sign}{h}:{m}:{second:02}"));
                        }
                    }
                }
            }
        }
        for h in ["", "24", "99", "000", "-1", "A", "１"] {
            for m in ["", "0", "60", "99", "000", "-1", "B", "０"] {
                for s in ["", "0", "60", "99", "000", "-1", "C", "０"] {
                    visit(&format!("GMT{sign}{h}:{m}:{s}"));
                }
            }
        }
    }
    for name in [
        "",
        "Z",
        "UT",
        "utc",
        "gmt",
        " UTC",
        "UTC ",
        "Not/AZone",
        "America/los_angeles",
        "UTC+05:30",
        "+05:30",
        "-0530",
        "GMT",
        "GMT+",
        "GMT-",
        "GMT+05:30:15",
        "GMT-00:00:01",
        "GMT+23:59:59",
        "GMT+05:30:00",
        "GMT+00530",
        "GMT+24:00",
        "GMT+05:60",
        "GMT+05:30:60",
        "GMT+1:2:03",
        "GMT+01:2:03",
        "GMT+1:2",
        "GMT+1::03",
        "GMT+01:02:03:04",
        "GMT+01:02\n",
        "GMT+\0",
        "GMT+１２:00",
    ] {
        visit(name);
    }
    for name in names {
        visit(name);
        visit(&name.to_lowercase());
        visit(&name.to_uppercase());
        visit(&format!(" {name}"));
    }
}

fn variants(value: i32) -> Vec<String> {
    if value < 10 {
        vec![value.to_string(), format!("{value:02}")]
    } else {
        vec![value.to_string()]
    }
}

#[test]
fn named_aliases_keep_java_ids_and_stored_2015_epochs() {
    // Additional direct Java observations from JavaTimezoneNamesEpoch.java,
    // retained with the ORC timezone mechanism evidence. Windhoek's 2015 epoch
    // uses +02:00 even though its prehistory raw prefix is +01:00.
    let resolver = resolver();
    for (name, epoch) in [
        ("PST", 1_420_099_200),
        ("EST", 1_420_088_400),
        ("IST", 1_420_050_600),
        ("Asia/Katmandu", 1_420_049_700),
        ("Australia/Lord_Howe", 1_420_030_800),
        ("Pacific/Chatham", 1_420_020_900),
        ("Africa/Windhoek", 1_420_063_200),
    ] {
        let (profile, resolved_id) = resolver.resolve_name(name);
        assert_eq!(resolved_id, name);
        assert_eq!(profile.orc_epoch_seconds(), epoch, "{name}");
    }
}

#[test]
fn fixed_offsets_cover_qualified_endpoints_and_reject_outside_queries() {
    let resolver = resolver();
    for (name, expected) in [("GMT+23:59:59", 86_399_000), ("GMT-00:00:01", -1_000)] {
        let zone = resolver.resolve(name).unwrap();
        for millis in [OFFSET_DOMAIN_MILLIS[0], 0, OFFSET_DOMAIN_MILLIS[1]] {
            assert_eq!(zone.offset_millis(millis).unwrap(), expected);
        }
        for millis in [
            i64::MIN,
            OFFSET_DOMAIN_MILLIS[0] - 1,
            OFFSET_DOMAIN_MILLIS[1] + 1,
            i64::MAX,
        ] {
            assert!(zone.offset_millis(millis).is_err());
        }
    }
}

#[test]
fn config_roundtrip_preserves_captured_name_and_pinned_identity() {
    for name in ["PST", "GMT-0", "GMT+1:2:03", "Not/AZone", ""] {
        let config = OrcJavaTimeZoneConfig::for_reader_timezone(name.to_string()).unwrap();
        assert_eq!(config.reader_timezone(), name);
        assert_eq!(config.profile_bundle_sha256(), PROFILE_BUNDLE_SHA256);
        let value = serde_json::to_value(&config).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "semantics_version": 1,
                "profile_bundle_sha256": PROFILE_BUNDLE_SHA256,
                "reader_timezone": name,
            })
        );
        let decoded: OrcJavaTimeZoneConfig = serde_json::from_value(value).unwrap();
        assert_eq!(decoded, config);
        decoded.native_options().unwrap();
    }
}

#[test]
fn config_rejects_unknown_semantics_or_bundle_before_creating_native_options() {
    for version in [0, SEMANTICS_VERSION + 1, u32::MAX] {
        let config = OrcJavaTimeZoneConfig {
            semantics_version: version,
            profile_bundle_sha256: PROFILE_BUNDLE_SHA256.to_string(),
            reader_timezone: "UTC".to_string(),
        };
        assert!(config.native_options().is_err());
    }
    for identity in [
        String::new(),
        "0".repeat(64),
        PROFILE_BUNDLE_SHA256.to_uppercase(),
        format!("{PROFILE_BUNDLE_SHA256} "),
    ] {
        let config = OrcJavaTimeZoneConfig {
            semantics_version: SEMANTICS_VERSION,
            profile_bundle_sha256: identity,
            reader_timezone: "UTC".to_string(),
        };
        assert!(config.native_options().is_err());
    }
}

#[test]
fn config_json_rejects_missing_fields_unknown_fields_and_wrong_types() {
    let valid = serde_json::json!({
        "semantics_version": SEMANTICS_VERSION,
        "profile_bundle_sha256": PROFILE_BUNDLE_SHA256,
        "reader_timezone": "UTC",
    });
    for field in [
        "semantics_version",
        "profile_bundle_sha256",
        "reader_timezone",
    ] {
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<OrcJavaTimeZoneConfig>(missing).is_err());
        let mut wrong_type = valid.clone();
        wrong_type[field] = serde_json::Value::Null;
        assert!(serde_json::from_value::<OrcJavaTimeZoneConfig>(wrong_type).is_err());
    }
    let mut unknown = valid;
    unknown["session_timezone"] = serde_json::json!("UTC");
    assert!(serde_json::from_value::<OrcJavaTimeZoneConfig>(unknown).is_err());
}
