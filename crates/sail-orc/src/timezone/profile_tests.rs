#![expect(clippy::unwrap_used)]

use std::fmt::Write;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::*;

#[test]
fn embedded_identity_and_shared_profiles_match_selected_baseline() {
    let mut observed = String::new();
    for byte in Sha256::digest(EMBEDDED_JSON.as_bytes()) {
        write!(observed, "{byte:02x}").unwrap();
    }
    assert_eq!(observed, PROFILE_BUNDLE_SHA256);
    let first = embedded_profiles().unwrap();
    let second = embedded_profiles().unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(first.zones.len(), 631);
    let la = first.exact_name("America/Los_Angeles").unwrap();
    let alias = first.exact_name("US/Pacific").unwrap();
    assert!(Arc::ptr_eq(&la.profile, &alias.profile));
    assert_eq!(la.resolved_id, "America/Los_Angeles");
    assert_eq!(alias.resolved_id, "US/Pacific");
    assert!(first.exact_name("america/los_angeles").is_none());
    // The exported Java-parsed 2015 epoch differs from UTC minus the raw
    // pre-history offset in Windhoek. Do not derive it from that prefix.
    let windhoek = first.exact_name("Africa/Windhoek").unwrap();
    assert_eq!(windhoek.profile.orc_epoch_seconds(), 1_420_063_200);
    assert_eq!(first.utc_epoch_seconds(), 1_420_070_400);
}

#[test]
fn qualified_domain_includes_endpoints_and_rejects_adjacent_instants() {
    assert_eq!(
        OFFSET_DOMAIN_MILLIS,
        [
            i64::MIN.div_euclid(1_000) - 4 * (1_i64 << 31),
            i64::MAX.div_euclid(1_000) + 4 * (1_i64 << 31),
        ]
    );
    let profiles = embedded_profiles().unwrap();
    for name in [
        "GMT",
        "America/Los_Angeles",
        "Africa/Windhoek",
        "Asia/Kathmandu",
    ] {
        let zone = profiles.exact_name(name).unwrap().profile;
        for millis in OFFSET_DOMAIN_MILLIS {
            assert!(zone.offset_millis(millis).is_ok(), "{name}/{millis}");
        }
        for millis in [
            OFFSET_DOMAIN_MILLIS[0] - 1,
            OFFSET_DOMAIN_MILLIS[1] + 1,
            i64::MIN,
            i64::MAX,
        ] {
            let error = zone.offset_millis(millis).unwrap_err().to_string();
            assert!(
                error.contains("outside qualified millisecond domain"),
                "{error}"
            );
        }
    }
    let gmt = profiles.gmt();
    assert_eq!(gmt.orc_epoch_seconds(), 1_420_070_400);
    for millis in [OFFSET_DOMAIN_MILLIS[0], 0, OFFSET_DOMAIN_MILLIS[1]] {
        assert_eq!(gmt.offset_millis(millis).unwrap(), 0);
    }
}

// These synthetic tables exercise validation only. Offset semantics are checked
// against the retained Java oracle in profile_oracle_tests.rs.
const GMT_ID: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const PROFILE_ID: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const CYCLE_ID: &str = "2222222222222222222222222222222222222222222222222222222222222222";

fn validation_bundle() -> Value {
    json!({
        "format_version": 1,
        "configuration": {
            "java_runtime_version": "20.0.2+9-78", "java_vendor": "Oracle Corporation",
            "tzdb_versions": ["2023c"], "legacy_id_mapping": false,
            "format_locale": "en-US", "epoch_calendar": "java.util.GregorianCalendar",
            "epoch_text": "2015-01-01 00:00:00", "utc_epoch_seconds": UTC_EPOCH_SECONDS,
            "offset_domain_millis": OFFSET_DOMAIN_MILLIS,
        },
        "zones": {
            "GMT": {"profile": GMT_ID, "resolved_id": "GMT", "available": true},
            "Test": {"profile": PROFILE_ID, "resolved_id": "Test", "available": true},
        },
        "java_aliases": {},
        "profiles": {
            (GMT_ID): {"raw_prefix_offset_millis": 0, "orc_epoch_seconds": UTC_EPOCH_SECONDS,
                "transitions": [], "future": {"kind": "fixed", "offset_millis": 0}},
            (PROFILE_ID): {"raw_prefix_offset_millis": 0, "orc_epoch_seconds": UTC_EPOCH_SECONDS,
                "transitions": [[0, 0], [1000, 1]], "future": {"kind": "cycle", "id": CYCLE_ID}},
        },
        "cycles": {(CYCLE_ID): {"anchor_millis": 0, "period_millis": CYCLE_PERIOD_MILLIS,
            "rows": [[0, 0], [1000, 1]]}},
    })
}

fn reject_at(path: &str, value: Value) {
    let mut bundle = validation_bundle();
    *bundle.pointer_mut(path).unwrap() = value;
    assert!(
        parse_embedded(&bundle.to_string()).is_err(),
        "accepted {path}: {bundle}"
    );
}

#[test]
fn loader_rejects_wrong_configuration_and_shape() {
    assert!(parse_embedded(&validation_bundle().to_string()).is_ok());
    for (path, value) in [
        ("/format_version", json!(2)),
        ("/configuration/java_runtime_version", json!("other")),
        ("/configuration/java_vendor", json!("other")),
        ("/configuration/tzdb_versions", json!(["2024a"])),
        ("/configuration/legacy_id_mapping", json!(true)),
        ("/configuration/format_locale", json!("th-TH")),
        ("/configuration/epoch_calendar", json!("BuddhistCalendar")),
        ("/configuration/epoch_text", json!("2015-01-02 00:00:00")),
        ("/configuration/utc_epoch_seconds", json!(0)),
        ("/configuration/offset_domain_millis", json!([0, 1])),
        ("/zones/GMT/resolved_id", json!("UTC")),
        ("/zones/Test/available", json!(false)),
        ("/zones/Test/resolved_id", json!("")),
        ("/zones/Test/profile", json!("missing")),
        ("/java_aliases", json!({"Test": "missing"})),
    ] {
        reject_at(path, value);
    }
    let mut bundle = validation_bundle();
    bundle["unexpected"] = json!(true);
    assert!(parse_embedded(&bundle.to_string()).is_err());
    let mut bundle = validation_bundle();
    bundle["zones"].as_object_mut().unwrap().remove("GMT");
    assert!(parse_embedded(&bundle.to_string()).is_err());
    let mut bundle = validation_bundle();
    bundle["configuration"]
        .as_object_mut()
        .unwrap()
        .remove("format_locale");
    assert!(parse_embedded(&bundle.to_string()).is_err());
    let mut bundle = validation_bundle();
    let invalid_profile = bundle["profiles"][GMT_ID].clone();
    bundle["profiles"]
        .as_object_mut()
        .unwrap()
        .insert("bad-id".to_string(), invalid_profile);
    assert!(parse_embedded(&bundle.to_string()).is_err());
}

#[test]
fn loader_rejects_malformed_tables_and_integer_ranges() {
    for rows in [
        json!([[1, 0], [1, 1]]),
        json!([[2, 0], [1, 1]]),
        json!([[OFFSET_DOMAIN_MILLIS[0] - 1, 0]]),
        json!([[OFFSET_DOMAIN_MILLIS[1] + 1, 0]]),
        json!([[0, 2_147_483_648_i64]]),
        json!([[9_223_372_036_854_775_808_u64, 0]]),
    ] {
        reject_at(&format!("/profiles/{PROFILE_ID}/transitions"), rows);
    }
    for rows in [
        json!([]),
        json!([[1, 0]]),
        json!([[0, 0], [0, 1]]),
        json!([[0, 0], [-1, 1]]),
        json!([[0, 0], [CYCLE_PERIOD_MILLIS, 1]]),
    ] {
        reject_at(&format!("/cycles/{CYCLE_ID}/rows"), rows);
    }
    reject_at(&format!("/cycles/{CYCLE_ID}/period_millis"), json!(0));
    reject_at(
        &format!("/cycles/{CYCLE_ID}/anchor_millis"),
        json!(i64::MIN),
    );
    reject_at(
        &format!("/profiles/{PROFILE_ID}/orc_epoch_seconds"),
        json!(i64::MAX),
    );
    reject_at(
        &format!("/profiles/{PROFILE_ID}/raw_prefix_offset_millis"),
        json!(-2_147_483_649_i64),
    );
    reject_at(
        &format!("/profiles/{PROFILE_ID}/future/id"),
        json!("missing"),
    );
    reject_at(
        &format!("/profiles/{PROFILE_ID}/future/kind"),
        json!("unknown"),
    );
    reject_at(&format!("/profiles/{PROFILE_ID}/transitions"), json!([]));
    reject_at(
        &format!("/profiles/{GMT_ID}/future/offset_millis"),
        json!(1),
    );
}
