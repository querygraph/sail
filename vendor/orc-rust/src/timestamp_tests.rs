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

fn options() -> TimestampReadOptions {
    TimestampReadOptions::new().with_precision_loss(TimestampPrecisionLoss::Truncate)
}

#[test]
fn default_options_keep_strict_precision() {
    let options = TimestampReadOptions::default();
    assert_eq!(options.precision_loss(), TimestampPrecisionLoss::Error);
    for nanos in [1, -1, 1_876_543_211, -1_876_543_211] {
        assert!(options.convert(nanos, TimeUnit::Microsecond).is_err());
    }
    assert_eq!(options.convert(-1_000, TimeUnit::Microsecond).unwrap(), -1);
}

#[test]
fn fractional_truncation_floors_negative_instants() {
    for (nanos, micros) in [
        (1, 0),
        (-1, -1),
        (1_876_543_211, 1_876_543),
        (-1_876_543_211, -1_876_544),
    ] {
        assert_eq!(
            options().convert(nanos, TimeUnit::Microsecond).unwrap(),
            micros
        );
    }
}

#[test]
fn microsecond_range_is_not_limited_to_nanosecond_or_chrono_range() {
    for micros in [
        i64::MIN,
        -62_135_596_800_000_000,
        253_402_300_799_999_999,
        i64::MAX,
    ] {
        assert_eq!(
            options()
                .convert(i128::from(micros) * 1_000, TimeUnit::Microsecond)
                .unwrap(),
            micros
        );
    }
    assert!(options()
        .convert((i128::from(i64::MAX) + 1) * 1_000, TimeUnit::Microsecond)
        .is_err());
}

#[test]
fn checked_decoder_retains_orc_epoch_edge_and_rejects_invalid_nanos() {
    // ORC cannot distinguish this encoded positive value from the historical
    // -1 microsecond writer case. Do not silently repair already-written files.
    assert_eq!(
        options()
            .decode(0, 0, 999_999_000 << 3, TimeUnit::Microsecond)
            .unwrap(),
        999_999
    );
    for nanos in [-1, 1_000_000_000_i64 << 3, i64::MAX] {
        assert!(options()
            .decode(0, 0, nanos, TimeUnit::Microsecond)
            .is_err());
    }
}
