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

use std::io::{self, Cursor};

use super::*;
use crate::encoding::integer::{rle_v2::RleV2Decoder, UnsignedEncoding};
use crate::encoding::PrimitiveValueDecoder;
use crate::error::OrcError;

// Independent byte fixtures: ORC zigzag/base-128 encodings of signed extrema.
// No native writer or derive_delta_header is used to construct these runs.
const MIN_ZIGZAG: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
const MAX_ZIGZAG: &[u8] = &[0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];

fn run(header: [u8; 2], base: &[u8], delta: &[u8], packed: &[u8]) -> Vec<u8> {
    [header.as_slice(), base, delta, packed].concat()
}

fn decode<N: NInt, S: EncodingSign>(bytes: &[u8], count: usize) -> Result<Vec<N>> {
    let mut decoder = RleV2Decoder::<N, _, S>::new(Cursor::new(bytes));
    let mut out = vec![N::zero(); count];
    decoder.decode(&mut out)?;
    Ok(out)
}

fn assert_width_error(error: OrcError) {
    assert!(matches!(error, OrcError::OutOfSpec { ref msg, .. }
        if msg == "over/underflow when decoding delta integer"));
}

#[test]
fn variable_singletons_fail_before_payload_or_output() {
    for encoded_width in 1..=31 {
        let header = 0xc0 | (encoded_width << 1);
        // The incomplete base varint must remain unread: the run length alone
        // is invalid. No output or delta scratch should change.
        let mut reader = Cursor::new([0x00, 0x80]);
        let mut out = vec![7_i64];
        let mut scratch = vec![9_i64];
        let error =
            read_delta_values::<_, _, SignedEncoding>(&mut reader, &mut out, &mut scratch, header)
                .unwrap_err();
        assert!(matches!(error, OrcError::OutOfSpec { ref msg, .. }
            if msg == "variable delta run must contain at least two values"));
        assert_eq!(reader.position(), 1);
        assert_eq!(out, [7]);
        assert_eq!(scratch, [9]);

        let bytes = [header, 0x00];
        assert!(decode::<i64, SignedEncoding>(&bytes, 1).is_err());
        let mut decoder = RleV2Decoder::<i64, _, UnsignedEncoding>::new(Cursor::new(bytes));
        assert!(decoder.skip(1).is_err());
    }
}

#[test]
fn fixed_singletons_keep_the_base_and_consume_the_signed_delta() {
    for delta in [MIN_ZIGZAG, &[0x00]] {
        let bytes = run([0xc0, 0x00], &[0x0a], delta, &[]);
        let mut reader = Cursor::new(&bytes[1..]);
        let mut out = vec![];
        read_delta_values::<i64, _, SignedEncoding>(&mut reader, &mut out, &mut vec![], bytes[0])
            .unwrap();
        assert_eq!(out, [5]);
        assert_eq!(reader.position(), (bytes.len() - 1) as u64);
    }
}

#[test]
fn fixed_minimum_delta_preserves_extreme_values_and_long_wrapping() {
    let bytes = run([0xc0, 0x01], &[0x00], MIN_ZIGZAG, &[]);
    assert_eq!(
        decode::<i64, SignedEncoding>(&bytes, 2).unwrap(),
        [0, i64::MIN]
    );
    let bytes = run([0xc0, 0x02], &[0x00], MIN_ZIGZAG, &[]);
    assert_eq!(
        decode::<i64, SignedEncoding>(&bytes, 3).unwrap(),
        [0, i64::MIN, 0]
    );
}

#[test]
fn variable_minimum_first_delta_preserves_a_decreasing_sequence() {
    let bytes = run(
        [0xfe, 0x02],
        MAX_ZIGZAG,
        MIN_ZIGZAG,
        &i64::MAX.to_be_bytes(),
    );
    assert_eq!(
        decode::<i64, SignedEncoding>(&bytes, 3).unwrap(),
        [i64::MAX, -1, i64::MIN]
    );
}

#[test]
fn packed_unsigned_magnitudes_keep_all_sixty_four_bits() {
    // These are monotonic signed sequences with all results representable.
    // Neither final magnitude is representable as a positive i64.
    for (magnitude, increasing_last, decreasing_last) in
        [(1_u64 << 63, 1, -2), (u64::MAX - 1, i64::MAX, i64::MIN)]
    {
        let magnitude = magnitude.to_be_bytes();
        let increasing = run([0xfe, 0x02], MIN_ZIGZAG, &[0x02], &magnitude);
        assert_eq!(
            decode::<i64, SignedEncoding>(&increasing, 3).unwrap(),
            [i64::MIN, i64::MIN + 1, increasing_last]
        );
        let decreasing = run([0xfe, 0x02], MAX_ZIGZAG, &[0x01], &magnitude);
        assert_eq!(
            decode::<i64, SignedEncoding>(&decreasing, 3).unwrap(),
            [i64::MAX, i64::MAX - 1, decreasing_last]
        );
    }
}

#[test]
fn signed_boundaries_wrap_for_fixed_and_variable_delta_runs() {
    for (base, delta, expected) in [
        (MAX_ZIGZAG, &[0x02][..], [i64::MAX, i64::MIN, i64::MIN + 1]),
        (MIN_ZIGZAG, &[0x01][..], [i64::MIN, i64::MAX, i64::MAX - 1]),
    ] {
        assert_eq!(
            decode::<i64, SignedEncoding>(&run([0xc0, 0x02], base, delta, &[]), 3).unwrap(),
            expected
        );
        assert_eq!(
            decode::<i64, SignedEncoding>(&run([0xc2, 0x02], base, delta, &[0x40]), 3).unwrap(),
            expected
        );
    }
}

#[test]
fn unsigned_base_bits_and_zero_delta_direction_follow_java_long_arithmetic() {
    // The all-ones varint is u64::MAX when the base is unsigned.
    let bytes = run([0xc2, 0x02], MIN_ZIGZAG, &[0x02], &[0x40]);
    assert_eq!(
        decode::<i64, UnsignedEncoding>(&bytes, 3).unwrap(),
        [-1, 0, 1]
    );
    // Although the writer avoids a zero first delta for varying runs, the
    // Java reader treats it as increasing. Packed magnitudes are 1, 2, 3.
    let bytes = run([0xc2, 0x04], &[0x0a], &[0x00], &[0x6c]);
    assert_eq!(
        decode::<i64, SignedEncoding>(&bytes, 5).unwrap(),
        [5, 5, 6, 8, 11]
    );
}

#[test]
fn narrow_types_allow_large_deltas_between_representable_values() {
    let bytes = run([0xc0, 0x01], &[0xff, 0xff, 0x03], &[0xfe, 0xff, 0x07], &[]);
    assert_eq!(
        decode::<i16, SignedEncoding>(&bytes, 2).unwrap(),
        [i16::MIN, i16::MAX]
    );
    let bytes = run(
        [0xc0, 0x01],
        &[0xff, 0xff, 0xff, 0xff, 0x0f],
        &[0xfe, 0xff, 0xff, 0xff, 0x1f],
        &[],
    );
    assert_eq!(
        decode::<i32, SignedEncoding>(&bytes, 2).unwrap(),
        [i32::MIN, i32::MAX]
    );
}

#[test]
fn narrow_result_overflow_remains_an_error() {
    for bytes in [
        run([0xc0, 0x01], &[0xfe, 0xff, 0x03], &[0x02], &[]),
        run([0xc0, 0x01], &[0xff, 0xff, 0x03], &[0x01], &[]),
        run([0xc2, 0x02], &[0xfc, 0xff, 0x03], &[0x02], &[0x40]),
    ] {
        assert_width_error(decode::<i16, SignedEncoding>(&bytes, 3).unwrap_err());
    }
    for bytes in [
        run([0xc0, 0x01], &[0xfe, 0xff, 0xff, 0xff, 0x0f], &[0x02], &[]),
        run([0xc0, 0x01], &[0xff, 0xff, 0xff, 0xff, 0x0f], &[0x01], &[]),
        run(
            [0xc2, 0x02],
            &[0xfc, 0xff, 0xff, 0xff, 0x0f],
            &[0x02],
            &[0x40],
        ),
    ] {
        assert_width_error(decode::<i32, SignedEncoding>(&bytes, 3).unwrap_err());
    }
}

#[test]
fn truncated_delta_payloads_preserve_the_io_error() {
    for bytes in [
        &[0xc0][..],
        &[0xc0, 0x01, 0x80],
        &[0xc0, 0x01, 0x00, 0x80],
        &[0xfe, 0x02, 0x00, 0x02, 0x80],
    ] {
        let OrcError::IoError { source, .. } = decode::<i64, SignedEncoding>(bytes, 3).unwrap_err()
        else {
            panic!("expected the original truncated-payload I/O error");
        };
        assert_eq!(source.kind(), io::ErrorKind::UnexpectedEof);
    }
}

#[test]
fn extreme_and_two_value_runs_keep_the_next_run_aligned_when_skipping() {
    let mut bytes = run([0xc0, 0x02], &[0x00], MIN_ZIGZAG, &[]);
    // A two-value varying run has no packed payload, even with width 64.
    bytes.extend(run([0xfe, 0x01], &[0x0a], &[0x02], &[]));
    bytes.extend([0xc0, 0x01, 0x0e, 0x02]);
    let mut decoder = RleV2Decoder::<i64, _, SignedEncoding>::new(Cursor::new(bytes));
    let mut first = [0];
    decoder.decode(&mut first).unwrap();
    assert_eq!(first, [0]);
    decoder.skip(2).unwrap();
    let mut rest = [0; 4];
    decoder.decode(&mut rest).unwrap();
    assert_eq!(rest, [5, 6, 7, 8]);
}
