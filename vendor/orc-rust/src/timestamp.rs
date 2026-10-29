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

//! Opt-in handling of timestamp precision loss.

use arrow::datatypes::TimeUnit;
use snafu::ensure;

use crate::error::{OutOfSpecSnafu, Result};

/// Handling of fractional digits that the requested Arrow unit cannot retain.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TimestampPrecisionLoss {
    /// Reject values that cannot be represented exactly (the existing default).
    #[default]
    Error,
    /// Discard fractional digits, rounding negative instants toward negative infinity.
    Truncate,
}

/// Optional precision handling for ORC timestamp readers.
///
/// The default rejects fractional digits that the requested Arrow unit cannot
/// retain. Truncation uses checked wide arithmetic before narrowing the result;
/// it does not impose the range of a nanosecond timestamp on microsecond reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TimestampReadOptions {
    precision_loss: TimestampPrecisionLoss,
}

impl TimestampReadOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_precision_loss(mut self, precision_loss: TimestampPrecisionLoss) -> Self {
        self.precision_loss = precision_loss;
        self
    }

    pub fn precision_loss(&self) -> TimestampPrecisionLoss {
        self.precision_loss
    }

    pub(crate) fn decode(
        self,
        base: i64,
        encoded_seconds: i64,
        encoded_nanos: i64,
        unit: TimeUnit,
    ) -> Result<i64> {
        self.convert(decode_nanos(base, encoded_seconds, encoded_nanos)?, unit)
    }

    pub(crate) fn convert(self, nanos: i128, unit: TimeUnit) -> Result<i64> {
        let divisor = match unit {
            TimeUnit::Second => 1_000_000_000,
            TimeUnit::Millisecond => 1_000_000,
            TimeUnit::Microsecond => 1_000,
            TimeUnit::Nanosecond => 1,
        };
        ensure!(
            self.precision_loss == TimestampPrecisionLoss::Truncate || nanos % divisor == 0,
            OutOfSpecSnafu {
                msg: format!("timestamp loses precision at {unit:?}")
            }
        );
        i64::try_from(nanos.div_euclid(divisor))
            .map_err(|_| invalid("timestamp exceeds requested Arrow unit range"))
    }
}

pub(crate) fn decode_nanos(base: i64, encoded_seconds: i64, encoded_nanos: i64) -> Result<i128> {
    let encoded_nanos = u64::try_from(encoded_nanos).map_err(|_| invalid("negative nanos"))?;
    let zeros = encoded_nanos & 7;
    let nanos = (encoded_nanos >> 3)
        .checked_mul(if zeros == 0 {
            1
        } else {
            10_u64.pow(zeros as u32 + 1)
        })
        .ok_or_else(|| invalid("nanosecond expansion overflow"))?;
    ensure!(
        nanos < 1_000_000_000,
        OutOfSpecSnafu {
            msg: "timestamp nanos exceed one second"
        }
    );
    let mut seconds = i128::from(base) + i128::from(encoded_seconds);
    // Preserve the Java ORC/ORC-763 decoding rule, including its epoch-edge behavior.
    if seconds < 0 && nanos > 999_999 {
        seconds -= 1;
    }
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|x| x.checked_add(i128::from(nanos)))
        .ok_or_else(|| invalid("timestamp expansion overflow"))
}

fn invalid(message: &str) -> crate::error::OrcError {
    OutOfSpecSnafu {
        msg: message.to_owned(),
    }
    .build()
}

#[cfg(test)]
#[path = "timestamp_tests.rs"]
mod tests;
