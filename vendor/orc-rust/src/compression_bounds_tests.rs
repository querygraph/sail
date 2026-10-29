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

fn compression(kind: CompressionType, limit: u64) -> Option<Compression> {
    Compression::from_proto(kind.to_proto(), Some(limit))
}

fn block(header: CompressionHeader, payload: &[u8]) -> Bytes {
    let mut encoded = header.encode().to_vec();
    encoded.extend_from_slice(payload);
    Bytes::from(encoded)
}

fn decode(bytes: Bytes, kind: CompressionType, limit: u64) -> std::io::Result<Vec<u8>> {
    let mut output = Vec::new();
    Decompressor::new(bytes, compression(kind, limit), Vec::new()).read_to_end(&mut output)?;
    Ok(output)
}

#[test]
fn rejects_incomplete_block_framing_with_typed_cause() {
    for encoded in [
        Bytes::from_static(&[1]),
        Bytes::from_static(&[1, 0]),
        block(CompressionHeader::Original(5), b"a"),
        block(CompressionHeader::Compressed(5), b"a"),
        Bytes::from_static(&[0xff, 0xff, 0xff]),
    ] {
        let error = decode(encoded, CompressionType::Zstd, 64).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(matches!(
            error
                .get_ref()
                .and_then(|cause| cause.downcast_ref::<OrcError>()),
            Some(OrcError::OutOfSpec { .. })
        ));
    }
}

#[test]
fn read_errors_preserve_invalid_codec_cause() {
    let error = decode(
        block(CompressionHeader::Compressed(3), &[0xff; 3]),
        CompressionType::Zstd,
        64,
    )
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error
        .get_ref()
        .unwrap()
        .downcast_ref::<OrcError>()
        .is_some());
}

#[test]
fn zero_length_read_does_not_consume_input() {
    let mut reader = Decompressor::new(
        Bytes::from_static(&[0xff]),
        compression(CompressionType::Zstd, 64),
        Vec::new(),
    );
    assert_eq!(reader.read(&mut []).unwrap(), 0);
    assert_eq!(
        reader.read(&mut [0]).unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
}

#[test]
fn empty_blocks_do_not_end_the_stream() {
    let mut bytes = block(CompressionHeader::Original(0), &[]).to_vec();
    bytes.extend_from_slice(&block(CompressionHeader::Original(0), &[]));
    bytes.extend_from_slice(&block(CompressionHeader::Original(2), b"ab"));
    bytes.extend_from_slice(&block(CompressionHeader::Original(0), &[]));
    bytes.extend_from_slice(&block(CompressionHeader::Original(1), b"c"));
    bytes.extend_from_slice(&block(CompressionHeader::Original(0), &[]));
    let mut reader = Decompressor::new(
        Bytes::from(bytes),
        compression(CompressionType::Zstd, 64),
        Vec::new(),
    );
    let mut byte = [0];
    for expected in b"abc" {
        assert_eq!(reader.read(&mut byte).unwrap(), 1);
        assert_eq!(byte[0], *expected);
    }
    assert_eq!(reader.read(&mut byte).unwrap(), 0);
    assert_eq!(reader.read(&mut byte).unwrap(), 0);
}

#[test]
fn rejects_original_blocks_larger_than_declared_size() {
    assert!(decode(
        block(CompressionHeader::Original(4), b"abcd"),
        CompressionType::Zstd,
        3,
    )
    .is_err());
    assert_eq!(
        decode(
            block(CompressionHeader::Original(4), b"abcd"),
            CompressionType::Zstd,
            4,
        )
        .unwrap(),
        b"abcd"
    );
}

#[test]
fn rejects_invalid_block_sizes_before_allocation() {
    for limit in [0, MAX_COMPRESSION_BLOCK_SIZE, u64::MAX] {
        let error = decode(
            block(CompressionHeader::Original(1), b"x"),
            CompressionType::Zstd,
            limit,
        )
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
}

#[test]
fn all_codecs_enforce_decoded_block_limit() {
    let input = vec![b'a'; 64];
    for kind in [
        CompressionType::Zlib,
        CompressionType::Zstd,
        CompressionType::Snappy,
        CompressionType::Lz4,
        CompressionType::Lzo,
    ] {
        let mut encoded = Vec::new();
        if matches!(kind, CompressionType::Lzo) {
            // A valid LZO literal run followed by the end-of-stream marker.
            encoded.push(17 + input.len() as u8);
            encoded.extend_from_slice(&input);
            encoded.extend_from_slice(&[17, 0, 0]);
        } else {
            get_compressor_variant(kind)
                .unwrap()
                .compress_block(&input, &mut encoded)
                .unwrap();
        }
        let framed = block(
            CompressionHeader::Compressed(encoded.len() as u32),
            &encoded,
        );
        assert_eq!(decode(framed.clone(), kind, 64).unwrap(), input, "{kind}");
        assert!(
            decode(framed, kind, 63).is_err(),
            "{kind} exceeded the declared size"
        );
        let mut scratch = Vec::new();
        assert!(get_decompressor_variant(kind)
            .decompress_block(&encoded, &mut scratch, 63)
            .is_err());
        assert!(
            scratch.len() <= 64,
            "{kind} decoded past the one-byte overflow probe"
        );
    }
}

#[test]
fn malformed_lzo_back_reference_is_an_error() {
    assert!(decode(
        block(CompressionHeader::Compressed(3), &[0x80, 0xff, 0]),
        CompressionType::Lzo,
        64,
    )
    .is_err());
}

fn compressed_block(kind: CompressionType, input: &[u8]) -> Bytes {
    let mut encoded = Vec::new();
    if matches!(kind, CompressionType::Lzo) {
        assert!(input.len() <= 238);
        if !input.is_empty() {
            encoded.push(17 + input.len() as u8);
            encoded.extend_from_slice(input);
        }
        encoded.extend_from_slice(&[17, 0, 0]);
    } else {
        get_compressor_variant(kind)
            .unwrap()
            .compress_block(input, &mut encoded)
            .unwrap();
    }
    block(
        CompressionHeader::Compressed(encoded.len() as u32),
        &encoded,
    )
}

#[test]
fn total_budget_covers_multiple_original_blocks() {
    let mut encoded = block(CompressionHeader::Original(2), b"ab").to_vec();
    encoded.extend_from_slice(&block(CompressionHeader::Original(2), b"cd"));
    encoded.extend_from_slice(&block(CompressionHeader::Original(0), b""));
    for (budget, should_pass) in [(4, true), (5, true), (3, false), (0, false)] {
        let mut reader = Decompressor::new_with_limit(
            Bytes::from(encoded.clone()),
            compression(CompressionType::Zstd, 64),
            Vec::new(),
            budget,
        );
        let mut decoded = Vec::new();
        let outcome = reader.read_to_end(&mut decoded);
        assert_eq!(outcome.is_ok(), should_pass, "budget={budget}");
        if should_pass {
            assert_eq!(decoded, b"abcd");
        } else {
            assert!(decoded.len() <= budget);
            assert_eq!(outcome.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
        }
    }
    // The original constructor has no aggregate output cap.
    assert_eq!(
        decode(Bytes::from(encoded), CompressionType::Zstd, 2).unwrap(),
        b"abcd"
    );
}

#[test]
fn total_budget_covers_compressed_and_original_blocks_for_every_codec() {
    for kind in [
        CompressionType::Zlib,
        CompressionType::Zstd,
        CompressionType::Snappy,
        CompressionType::Lz4,
        CompressionType::Lzo,
    ] {
        let mut encoded = compressed_block(kind, b"abc").to_vec();
        encoded.extend_from_slice(&block(CompressionHeader::Original(2), b"de"));
        encoded.extend_from_slice(&compressed_block(kind, b"fgh"));
        encoded.extend_from_slice(&compressed_block(kind, b""));
        encoded.extend_from_slice(&block(CompressionHeader::Original(0), b""));
        let mut exact = Decompressor::new_with_limit(
            Bytes::from(encoded.clone()),
            compression(kind, 64),
            Vec::new(),
            8,
        );
        let mut decoded = Vec::new();
        exact.read_to_end(&mut decoded).unwrap();
        assert_eq!(decoded, b"abcdefgh", "{kind}");
        assert_eq!(exact.read(&mut [0]).unwrap(), 0);

        let mut short = Decompressor::new_with_limit(
            Bytes::from(encoded),
            compression(kind, 64),
            Vec::new(),
            7,
        );
        let mut decoded = Vec::new();
        assert!(short.read_to_end(&mut decoded).is_err(), "{kind}");
        assert_eq!(
            decoded, b"abcde",
            "{kind}: oversized final block was emitted"
        );
    }
}

#[test]
fn zero_budget_accepts_only_empty_output() {
    for kind in [
        CompressionType::Zlib,
        CompressionType::Zstd,
        CompressionType::Snappy,
        CompressionType::Lz4,
        CompressionType::Lzo,
    ] {
        let mut empty = block(CompressionHeader::Original(0), b"").to_vec();
        empty.extend_from_slice(&compressed_block(kind, b""));
        empty.extend_from_slice(&block(CompressionHeader::Original(0), b""));
        for next in [None, Some(false), Some(true)] {
            let mut input = empty.clone();
            if let Some(compressed) = next {
                input.extend_from_slice(&if compressed {
                    compressed_block(kind, b"x")
                } else {
                    block(CompressionHeader::Original(1), b"x")
                });
            }
            let mut reader = Decompressor::new_with_limit(
                Bytes::from(input),
                compression(kind, 64),
                Vec::new(),
                0,
            );
            let mut decoded = Vec::new();
            assert_eq!(
                reader.read_to_end(&mut decoded).is_ok(),
                next.is_none(),
                "{kind}"
            );
            assert!(decoded.is_empty());
        }
    }
}

#[test]
fn total_budget_applies_to_uncompressed_input_before_emitting() {
    for (input, budget, should_pass) in [
        (b"".as_slice(), 0, true),
        (b"a".as_slice(), 0, false),
        (b"abc".as_slice(), 3, true),
        (b"abc".as_slice(), 2, false),
    ] {
        let mut reader =
            Decompressor::new_with_limit(Bytes::copy_from_slice(input), None, Vec::new(), budget);
        let mut decoded = Vec::new();
        assert_eq!(reader.read_to_end(&mut decoded).is_ok(), should_pass);
        assert_eq!(decoded, if should_pass { input } else { b"" });
    }
}

#[test]
fn small_remaining_budget_limits_codec_scratch_before_decoding() {
    for kind in [
        CompressionType::Zlib,
        CompressionType::Zstd,
        CompressionType::Snappy,
        CompressionType::Lz4,
        CompressionType::Lzo,
    ] {
        let mut input = block(CompressionHeader::Original(2), b"ab").to_vec();
        input.extend_from_slice(&compressed_block(kind, &[b'x'; 64]));
        let mut reader = Decompressor::new_with_limit(
            Bytes::from(input),
            compression(kind, MAX_COMPRESSION_BLOCK_SIZE - 1),
            Vec::new(),
            3,
        );
        let mut decoded = Vec::new();
        assert!(reader.read_to_end(&mut decoded).is_err(), "{kind}");
        assert_eq!(decoded, b"ab");
        assert!(reader.decompressor.scratch.len() <= 2, "{kind}");
        assert!(
            reader.decompressor.scratch.capacity() < 1024,
            "{kind} allocated the declared block size"
        );
    }
}
