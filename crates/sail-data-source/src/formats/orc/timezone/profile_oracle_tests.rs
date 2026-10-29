#![expect(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::*;

const VERIFICATION: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/orc/timezone/java-timezone-profile-verification.json"
));

#[derive(Deserialize)]
struct OraclePlan {
    baseline_millis: Vec<i64>,
    cycle_block_starts_millis: Vec<i64>,
    per_zone: BTreeMap<String, ZonePlan>,
    offset_checks: usize,
    offset_oracle_sha256: String,
}

#[derive(Deserialize)]
struct ZonePlan {
    offset_checks: usize,
    full_cycle_checked: bool,
}

fn queries(profile: &ProfileDto, cycle: Option<&CycleDto>, plan: &OraclePlan) -> BTreeSet<i64> {
    let mut points = plan
        .baseline_millis
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    for (index, &(millis, _)) in profile.transitions.iter().enumerate() {
        points.extend([millis - 1, millis, millis + 1]);
        if index > 0 {
            let previous = profile.transitions[index - 1].0;
            points.insert(previous + (millis - previous) / 2);
        }
    }
    if let Some(cycle) = cycle {
        let history_end = profile.transitions.last().unwrap().0;
        for &anchor in &plan.cycle_block_starts_millis {
            for (index, &(position, _)) in cycle.rows.iter().enumerate() {
                let end = cycle
                    .rows
                    .get(index + 1)
                    .map_or(cycle.period_millis, |row| row.0);
                for millis in [
                    anchor + position - 1,
                    anchor + position,
                    anchor + position + 1,
                    anchor + position + (end - position) / 2,
                ] {
                    if millis > history_end
                        && (OFFSET_DOMAIN_MILLIS[0]..=OFFSET_DOMAIN_MILLIS[1]).contains(&millis)
                    {
                        points.insert(millis);
                    }
                }
            }
        }
    }
    points
}

#[test]
fn all_exported_profiles_match_the_retained_java_offset_oracle() {
    // Replay the exact exporter query sequence, also independently reproduced
    // by verify_java_timezone_profiles.py. Expected offsets come only from the
    // retained public-Java digest, never from a second Rust table evaluator.
    let bundle: BundleDto = serde_json::from_str(EMBEDDED_JSON).unwrap();
    let plan: OraclePlan = serde_json::from_str(VERIFICATION).unwrap();
    let profiles = embedded_profiles().unwrap();
    let mut seen_cycles = BTreeSet::new();
    let mut digest = Sha256::new();
    let mut checks = 0;
    for (name, zone) in &bundle.zones {
        let profile = &bundle.profiles[&zone.profile];
        let cycle = match &profile.future {
            FutureDto::Cycle { id } if seen_cycles.insert(id.clone()) => Some(&bundle.cycles[id]),
            _ => None,
        };
        let points = queries(profile, cycle, &plan);
        assert_eq!(points.len(), plan.per_zone[name].offset_checks, "{name}");
        assert_eq!(
            cycle.is_some(),
            plan.per_zone[name].full_cycle_checked,
            "{name}"
        );
        let runtime = profiles.exact_name(name).unwrap().profile;
        for millis in points {
            let offset = runtime.offset_millis(millis).unwrap();
            // Tuple JSON is the canonical compact [name,millis,offset] array.
            digest.update(serde_json::to_vec(&(name, millis, offset)).unwrap());
            digest.update(b"\n");
            checks += 1;
        }
    }
    assert_eq!(checks, 830_644);
    assert_eq!(checks, plan.offset_checks);
    let mut observed = String::new();
    for byte in digest.finalize() {
        write!(observed, "{byte:02x}").unwrap();
    }
    assert_eq!(observed, plan.offset_oracle_sha256);
    assert_eq!(
        observed,
        "0f2dcdcac7e66981ac47663cb4584ecd0e30a626ce1f7930087cc9023c4af4bd"
    );
    assert_eq!(seen_cycles.len(), 36);
}
