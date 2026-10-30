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

use std::io;
use std::mem::size_of;

use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{IoSnafu, OffsetOverflowSnafu, OutOfSpecSnafu, Result};

fn check_allocation_size<T>(count: usize) -> Result<()> {
    let bytes = count.checked_mul(size_of::<T>()).context(OutOfSpecSnafu {
        msg: "Array buffer byte size overflows usize",
    })?;
    ensure!(
        bytes <= isize::MAX as usize,
        OutOfSpecSnafu {
            msg: "Array buffer byte size exceeds addressable memory"
        }
    );
    Ok(())
}

fn check_offset_buffer_size<O>(count: usize) -> Result<()> {
    let offsets = count.checked_add(1).context(OutOfSpecSnafu {
        msg: "Array offset count overflows usize",
    })?;
    check_allocation_size::<O>(offsets)
}

/// Check both scratch and eventual offset-buffer addressability before decoding
/// PRESENT. Row count is not limited by the offset value type.
pub(super) fn check_length_buffer_size<O>(count: usize) -> Result<()> {
    check_offset_buffer_size::<O>(count)?;
    check_allocation_size::<i64>(count)
}

/// Allocate the length scratch only after the caller has decoded PRESENT.
pub(super) fn new_length_buffer<O>(count: usize) -> Result<Vec<i64>> {
    check_length_buffer_size::<O>(count)?;
    let mut lengths = Vec::new();
    lengths
        .try_reserve_exact(count)
        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))
        .context(IoSnafu)?;
    lengths.resize(count, 0);
    Ok(lengths)
}

/// Validate decoded unsigned lengths held in the decoder's signed representation.
/// The total is an i32 offset into the materialized child array or byte payload,
/// not a limit on the number of rows or on cumulative work across batches.
pub(super) fn checked_total_length(lengths: &[i64]) -> Result<usize> {
    let mut total = 0_i64;
    for &length in lengths {
        ensure!(
            length >= 0,
            OutOfSpecSnafu {
                msg: "Decoded array length is negative"
            }
        );
        total = total.checked_add(length).context(OutOfSpecSnafu {
            msg: "Decoded array lengths overflow i64",
        })?;
    }
    ensure!(
        total <= i64::from(i32::MAX),
        OffsetOverflowSnafu {
            total_length: total,
            max_size: i32::MAX,
            batch_size: lengths.len(),
        }
    );
    usize::try_from(total).map_err(|_| {
        OutOfSpecSnafu {
            msg: "Decoded array length does not fit usize",
        }
        .build()
    })
}

#[cfg(test)]
pub(super) mod tests;
