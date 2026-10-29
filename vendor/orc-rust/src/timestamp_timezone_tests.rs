// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::{TimestampPrecisionLoss, TimestampReadOptions};
use arrow::datatypes::TimeUnit;

#[derive(Debug)]
struct Fixed(i32);

impl TimestampTimeZone for Fixed {
    fn orc_epoch_seconds(&self) -> i64 {
        1_420_070_400 - i64::from(self.0) / 1000
    }

    fn offset_millis(&self, _: i64) -> Result<i32> {
        Ok(self.0)
    }
}

// A deliberately bounded test profile, not a timezone database implementation.
// The 2020 Los Angeles transition instants and expectations below were checked
// against Java 20.0.2 / tzdb 2023c / ORC 2.3.0.
#[derive(Debug)]
struct LosAngeles2020;

impl TimestampTimeZone for LosAngeles2020 {
    fn orc_epoch_seconds(&self) -> i64 {
        1_420_099_200
    }

    fn offset_millis(&self, utc_millis: i64) -> Result<i32> {
        Ok(
            if (1_583_661_600_000..1_604_221_200_000).contains(&utc_millis) {
                -25_200_000
            } else {
                -28_800_000
            },
        )
    }
}

fn conversion(
    writer: impl TimestampTimeZone + 'static,
    reader: impl TimestampTimeZone + 'static,
) -> TimestampTimeZoneConversion {
    TimestampTimeZoneConversion {
        writer: Arc::new(writer),
        reader: Arc::new(reader),
    }
}

#[test]
fn same_rules_preserve_both_fold_instants_without_resolving_local_time() {
    // Distinct Arc allocations exercise the formula, not the pointer fast path.
    let conversion = conversion(LosAngeles2020, LosAngeles2020);
    for micros in [1_604_219_400_000_000_i64, 1_604_223_000_000_000] {
        let nanos = i128::from(micros) * 1000;
        assert_eq!(conversion.convert(nanos).unwrap(), nanos);
    }
}

#[test]
fn java_two_offset_rule_matches_cross_zone_gap_and_fold() {
    let conversion = conversion(Fixed(0), LosAngeles2020);
    for (input_millis, expected_millis) in [
        (1_583_634_600_000_i64, 1_583_659_800_000_i64),
        (1_604_194_200_000, 1_604_219_400_000),
    ] {
        assert_eq!(
            conversion
                .convert(i128::from(input_millis) * 1_000_000)
                .unwrap(),
            i128::from(expected_millis) * 1_000_000
        );
    }
}

#[test]
fn nonhour_offsets_and_supplied_orc_epoch_are_used() {
    let conversion = conversion(Fixed(20_700_000), Fixed(-18_000_000));
    assert_eq!(conversion.orc_epoch_seconds(), 1_420_049_700);
    assert_eq!(conversion.convert(0).unwrap(), 38_700_000_000_000);
}

#[test]
fn conversion_retains_full_microsecond_range_before_narrowing() {
    let unchanged = conversion(Fixed(0), Fixed(0));
    let precision = TimestampReadOptions::new();
    for micros in [
        i64::MIN,
        -62_135_596_800_000_000,
        253_402_300_799_999_999,
        i64::MAX,
    ] {
        let nanos = unchanged.convert(i128::from(micros) * 1000).unwrap();
        assert_eq!(
            precision.convert(nanos, TimeUnit::Microsecond).unwrap(),
            micros
        );
    }
    let west = conversion(Fixed(0), Fixed(-28_800_000));
    let nanos = west.convert(i128::from(i64::MAX) * 1000).unwrap();
    assert!(precision.convert(nanos, TimeUnit::Microsecond).is_err());
    // The reader-zone conversion can cross year 9999 without a calendar limit.
    assert_eq!(
        west.convert(253_402_300_799_999_999_000).unwrap(),
        253_402_329_599_999_999_000
    );
}

#[test]
fn precision_policy_is_applied_after_timezone_conversion() {
    let conversion = conversion(Fixed(0), Fixed(0));
    let truncate =
        TimestampReadOptions::new().with_precision_loss(TimestampPrecisionLoss::Truncate);
    for (nanos, expected) in [(1001, 1), (-1001, -2), (-1, -1)] {
        let nanos = conversion.convert(nanos).unwrap();
        assert!(TimestampReadOptions::new()
            .convert(nanos, TimeUnit::Microsecond)
            .is_err());
        assert_eq!(
            truncate.convert(nanos, TimeUnit::Microsecond).unwrap(),
            expected
        );
    }
}

#[derive(Debug)]
struct Unsupported;

impl TimestampTimeZone for Unsupported {
    fn orc_epoch_seconds(&self) -> i64 {
        1_420_070_400
    }

    fn offset_millis(&self, _: i64) -> Result<i32> {
        Err(invalid("profile does not support this instant"))
    }
}

#[test]
fn offset_errors_and_checked_candidate_overflow_are_reported() {
    assert!(conversion(Unsupported, Fixed(0))
        .convert(0)
        .unwrap_err()
        .to_string()
        .contains("profile does not support this instant"));
    let conversion = conversion(Fixed(i32::MAX), Fixed(i32::MIN));
    assert!(conversion
        .convert(i128::from(i64::MAX) * 1_000_000)
        .unwrap_err()
        .to_string()
        .contains("candidate exceeds"));
    assert!(conversion.convert(i128::MAX).is_err());
}

#[derive(Debug)]
struct Resolver;

impl TimestampTimeZoneResolver for Resolver {
    fn resolve(&self, name: &str) -> Result<Arc<dyn TimestampTimeZone>> {
        match name {
            "custom" => Ok(Arc::new(Fixed(20_700_000))),
            _ => Err(invalid("unknown test zone")),
        }
    }
}

#[test]
fn missing_names_retain_current_writer_and_resolution_errors_do_not_replace_it() {
    let mut state = TimestampTimeZoneState::new(TimestampTimeZoneOptions::new(
        Arc::new(Fixed(0)),
        Arc::new(Resolver),
    ));
    assert_eq!(
        state.start_stripe(None).unwrap().orc_epoch_seconds(),
        1_420_070_400
    );
    assert_eq!(
        state
            .start_stripe(Some("custom"))
            .unwrap()
            .orc_epoch_seconds(),
        1_420_049_700
    );
    assert!(state.start_stripe(Some("invalid")).is_err());
    for name in [None, Some("")] {
        assert_eq!(
            state.start_stripe(name).unwrap().orc_epoch_seconds(),
            1_420_049_700
        );
    }
}
