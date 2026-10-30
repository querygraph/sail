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

use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::ArrayRef;
use arrow::buffer::NullBuffer;

use super::*;
use crate::array_decoder::ArrayBatchDecoder;
use crate::encoding::integer::{get_unsigned_int_decoder, RleVersion};
use crate::encoding::PrimitiveValueDecoder;
use crate::error::OrcError;

#[derive(Clone, Copy)]
pub(crate) enum ExpectedError {
    Negative,
    SumOverflow,
    OffsetOverflow(i64),
}

pub(crate) struct Case {
    bytes: Vec<u8>,
    version: RleVersion,
    pub(crate) count: usize,
    pub(crate) expected: ExpectedError,
}

impl Case {
    pub(crate) fn decoder(&self) -> Box<dyn PrimitiveValueDecoder<i64> + Send> {
        get_unsigned_int_decoder(Cursor::new(self.bytes.clone()), self.version)
    }
}

pub(crate) fn unsigned_literals(values: &[u64]) -> Vec<u8> {
    assert!(!values.is_empty() && values.len() <= 128);
    let mut bytes = vec![(256 - values.len()) as u8];
    for &value in values {
        let mut value = value;
        while value >= 128 {
            bytes.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        bytes.push(value as u8);
    }
    bytes
}

pub(crate) fn malformed_cases() -> Vec<Case> {
    let mut cases = vec![];
    for (values, expected) in [
        (vec![u64::MAX], ExpectedError::Negative),
        (vec![u64::MAX, 1], ExpectedError::Negative),
        (vec![i64::MAX as u64, 1], ExpectedError::SumOverflow),
        (
            vec![i32::MAX as u64 + 1],
            ExpectedError::OffsetOverflow(i64::from(i32::MAX) + 1),
        ),
        (
            vec![i32::MAX as u64, 1],
            ExpectedError::OffsetOverflow(i64::from(i32::MAX) + 1),
        ),
    ] {
        cases.push(Case {
            bytes: unsigned_literals(&values),
            version: RleVersion::V1,
            count: values.len(),
            expected,
        });
        // RLEv2 direct, 64-bit values, one or two literals.
        let mut bytes = vec![0x7e, (values.len() - 1) as u8];
        for value in &values {
            bytes.extend(value.to_be_bytes());
        }
        cases.push(Case {
            bytes,
            version: RleVersion::V2,
            count: values.len(),
            expected,
        });
    }
    // RLEv2 short repeat: three copies of an eight-byte all-ones value.
    let mut bytes = vec![0x38];
    bytes.extend(u64::MAX.to_be_bytes());
    cases.push(Case {
        bytes,
        version: RleVersion::V2,
        count: 3,
        expected: ExpectedError::Negative,
    });
    cases
}

pub(crate) fn negative_cases() -> impl Iterator<Item = Case> {
    malformed_cases()
        .into_iter()
        .filter(|case| matches!(case.expected, ExpectedError::Negative))
}

pub(crate) fn many_unsigned_literals(values: &[u64]) -> Vec<u8> {
    values.chunks(128).flat_map(unsigned_literals).collect()
}

pub(crate) struct Skipped(pub(crate) Arc<Mutex<Vec<usize>>>);

impl ArrayBatchDecoder for Skipped {
    fn next_batch(&mut self, _size: usize, _parent: Option<&NullBuffer>) -> Result<ArrayRef> {
        panic!("skip-count tests do not materialize children");
    }

    fn skip_values(&mut self, n: usize, parent: Option<&NullBuffer>) -> Result<()> {
        assert!(parent.is_none());
        self.0.lock().unwrap().push(n);
        Ok(())
    }
}

pub(crate) fn assert_error(error: OrcError, expected: ExpectedError) {
    match (error, expected) {
        (OrcError::OutOfSpec { msg, .. }, ExpectedError::Negative) => {
            assert_eq!(msg, "Decoded array length is negative");
        }
        (OrcError::OutOfSpec { msg, .. }, ExpectedError::SumOverflow) => {
            assert_eq!(msg, "Decoded array lengths overflow i64");
        }
        (
            OrcError::OffsetOverflow {
                total_length,
                max_size,
                ..
            },
            ExpectedError::OffsetOverflow(expected),
        ) => {
            assert_eq!(total_length, expected);
            assert_eq!(max_size, i32::MAX);
        }
        (error, _) => panic!("unexpected error: {error}"),
    }
}

/// Returning a distinct error also catches dependent work without allocating
/// any length-controlled child array, even if a regression bypasses admission.
pub(crate) struct Unread(pub(crate) Arc<AtomicUsize>);

impl PrimitiveValueDecoder<i64> for Unread {
    fn decode(&mut self, _out: &mut [i64]) -> Result<()> {
        self.0.fetch_add(1, Ordering::Relaxed);
        OutOfSpecSnafu {
            msg: "dependent length decoder was invoked",
        }
        .fail()
    }

    fn skip(&mut self, _n: usize) -> Result<()> {
        panic!("materialization tests do not skip");
    }
}

impl ArrayBatchDecoder for Unread {
    fn next_batch(&mut self, _size: usize, _parent: Option<&NullBuffer>) -> Result<ArrayRef> {
        self.0.fetch_add(1, Ordering::Relaxed);
        OutOfSpecSnafu {
            msg: "dependent child decoder was invoked",
        }
        .fail()
    }

    fn skip_values(&mut self, _n: usize, _parent: Option<&NullBuffer>) -> Result<()> {
        panic!("materialization tests do not skip");
    }
}

#[test]
fn intrinsic_boundaries_are_checked_without_allocating_large_buffers() {
    assert_eq!(
        checked_total_length(&[i64::from(i32::MAX)]).unwrap(),
        i32::MAX as usize
    );
    assert_eq!(checked_total_length(&[0, 0]).unwrap(), 0);
    assert!(check_offset_buffer_size::<i32>(usize::MAX).is_err());
    assert!(check_allocation_size::<i64>(usize::MAX / 8 + 1).is_err());
    assert!(check_allocation_size::<i64>(isize::MAX as usize / 8 + 1).is_err());
    check_offset_buffer_size::<i32>(isize::MAX as usize / 4 - 1).unwrap();
    // More than i32::MAX rows is not an offset-value overflow: e.g. all-empty
    // lists can have zero child values. Check only buffer addressability.
    #[cfg(target_pointer_width = "64")]
    check_offset_buffer_size::<i32>(i32::MAX as usize + 1).unwrap();
}

#[test]
fn skip_lengths_use_bounded_chunks_and_preserve_the_next_length() {
    struct BoundedDecoder(Box<dyn PrimitiveValueDecoder<i64> + Send>);
    impl PrimitiveValueDecoder<i64> for BoundedDecoder {
        fn decode(&mut self, out: &mut [i64]) -> Result<()> {
            assert!(out.len() <= SKIP_BATCH_SIZE);
            self.0.decode(out)
        }

        fn skip(&mut self, _n: usize) -> Result<()> {
            panic!("lengths must be decoded and checked");
        }
    }

    let count = SKIP_BATCH_SIZE + 3;
    let mut values = vec![1; count];
    values.push(7);
    let mut decoder = BoundedDecoder(get_unsigned_int_decoder(
        Cursor::new(many_unsigned_literals(&values)),
        RleVersion::V1,
    ));
    let mut skips = vec![];
    skip_lengths(&mut decoder, count, |n| {
        skips.push(n);
        Ok(())
    })
    .unwrap();
    assert_eq!(skips, vec![SKIP_BATCH_SIZE, 3]);
    let mut next = [0];
    decoder.decode(&mut next).unwrap();
    assert_eq!(next, [7]);
}

#[test]
fn truncated_skip_lengths_fail_before_dependent_work_without_request_sized_scratch() {
    for bytes in [vec![], vec![0xff], vec![0xfe, 1]] {
        let mut decoder = get_unsigned_int_decoder(Cursor::new(bytes), RleVersion::V1);
        let mut calls = 0;
        assert!(skip_lengths(decoder.as_mut(), usize::MAX, |_| {
            calls += 1;
            Ok(())
        })
        .is_err());
        assert_eq!(calls, 0);
    }
}

#[cfg(target_pointer_width = "64")]
#[test]
fn skip_totals_split_before_usize_overflow_without_an_arrow_offset_limit() {
    for (values, expected) in [
        (vec![i32::MAX as u64, 1], vec![i32::MAX as usize + 1]),
        (vec![i64::MAX as u64, 1], vec![i64::MAX as usize + 1]),
        (
            vec![i64::MAX as u64, i64::MAX as u64, 2],
            vec![usize::MAX - 1, 2],
        ),
    ] {
        let mut decoder =
            get_unsigned_int_decoder(Cursor::new(unsigned_literals(&values)), RleVersion::V1);
        let mut skips = vec![];
        skip_lengths(decoder.as_mut(), values.len(), |n| {
            skips.push(n);
            Ok(())
        })
        .unwrap();
        assert_eq!(skips, expected);
    }
}

#[cfg(target_pointer_width = "32")]
#[test]
fn individual_skip_length_must_fit_the_child_count_type() {
    assert!(checked_skip_length(i64::from(u32::MAX) + 1).is_err());
}
