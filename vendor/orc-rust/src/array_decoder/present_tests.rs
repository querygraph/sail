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

use std::io::{self, Cursor, Read};
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Array, StringArray, StructArray};
use arrow::error::ArrowError;
use bytes::Bytes;
use prost::Message;

use super::*;
use crate::encoding::integer::RleVersion;
use crate::error::OrcError;
use crate::{proto, ArrowReaderBuilder};

struct CountedReader {
    bytes: Cursor<Vec<u8>>,
    reads: Arc<AtomicUsize>,
}

impl Read for CountedReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.bytes.read(out)
    }
}

fn assert_unexpected_eof(error: &OrcError) {
    let OrcError::IoError { source, .. } = error else {
        panic!("expected PRESENT I/O error, got {error}");
    };
    assert_eq!(source.kind(), io::ErrorKind::UnexpectedEof);
}

#[test]
fn truncated_present_stops_before_primitive_data_with_and_without_parent_nulls() {
    // Byte RLE: absent header, absent literal byte, and a two-byte literal run
    // missing its second byte. These are real malformed PRESENT encodings.
    for present_bytes in [vec![], vec![0xff], vec![0xfe, 0xff]] {
        for parent in [None, Some(NullBuffer::from(vec![true, false]))] {
            let reads = Arc::new(AtomicUsize::new(0));
            let data = CountedReader {
                // Two signed RLEv1 literal values: 7, 8.
                bytes: Cursor::new(vec![0xfe, 14, 16]),
                reads: reads.clone(),
            };
            let present = PresentDecoder {
                inner: Box::new(BooleanDecoder::new(Cursor::new(present_bytes.clone()))),
            };
            let mut decoder =
                Int64ArrayDecoder::new(get_signed_int_decoder(data, RleVersion::V1), Some(present));
            let error = decoder
                .next_primitive_batch(2, parent.as_ref())
                .unwrap_err();
            assert_unexpected_eof(&error);
            assert_eq!(reads.load(Ordering::Relaxed), 0);
        }
    }
}

// A complete, uncompressed one-stripe ORC fixture. The nested form has a
// STRUCT column containing one STRING, exercising merged parent/child validity.
fn string_file(
    nested: bool,
    rows: u64,
    parent_present: Option<&[u8]>,
    child_present: Option<&[u8]>,
    lengths: &[u8],
    data: &[u8],
) -> Bytes {
    let string_id = if nested { 2 } else { 1 };
    let struct_type = |name: &str, child| proto::Type {
        kind: Some(proto::r#type::Kind::Struct.into()),
        field_names: vec![name.into()],
        subtypes: vec![child],
        ..Default::default()
    };
    let mut types = vec![struct_type("value", 1)];
    if nested {
        types.push(struct_type("text", string_id));
    }
    types.push(proto::Type {
        kind: Some(proto::r#type::Kind::String.into()),
        ..Default::default()
    });

    let mut file = b"ORC".to_vec();
    let mut streams = vec![];
    let mut add_stream = |column, kind: Kind, bytes: &[u8]| {
        streams.push(proto::Stream {
            kind: Some(kind.into()),
            column: Some(column),
            length: Some(bytes.len() as u64),
        });
        file.extend_from_slice(bytes);
    };
    if let Some(present) = parent_present {
        assert!(nested);
        add_stream(1, Kind::Present, present);
    }
    if let Some(present) = child_present {
        add_stream(string_id, Kind::Present, present);
    }
    add_stream(string_id, Kind::Length, lengths);
    add_stream(string_id, Kind::Data, data);

    let data_length = file.len() as u64 - 3;
    let stripe_footer = proto::StripeFooter {
        streams,
        columns: vec![
            proto::ColumnEncoding {
                kind: Some(proto::column_encoding::Kind::Direct.into()),
                ..Default::default()
            };
            types.len()
        ],
        ..Default::default()
    }
    .encode_to_vec();
    let footer = proto::Footer {
        stripes: vec![proto::StripeInformation {
            offset: Some(3),
            index_length: Some(0),
            data_length: Some(data_length),
            footer_length: Some(stripe_footer.len() as u64),
            number_of_rows: Some(rows),
            ..Default::default()
        }],
        types,
        number_of_rows: Some(rows),
        ..Default::default()
    }
    .encode_to_vec();
    let postscript = proto::PostScript {
        footer_length: Some(footer.len() as u64),
        metadata_length: Some(0),
        version: vec![0, 12],
        ..Default::default()
    }
    .encode_to_vec();
    file.extend(stripe_footer);
    file.extend(footer);
    file.extend_from_slice(&postscript);
    file.push(u8::try_from(postscript.len()).unwrap());
    file.into()
}

fn read(file: Bytes) -> std::result::Result<Vec<RecordBatch>, ArrowError> {
    ArrowReaderBuilder::try_new(file).unwrap().build().collect()
}

fn native_error(error: &ArrowError) -> &OrcError {
    let ArrowError::ExternalError(error) = error else {
        panic!("expected native ORC error, got {error}");
    };
    error.downcast_ref::<OrcError>().unwrap()
}

#[test]
fn truncated_string_and_struct_present_precedes_a_distinct_length_error() {
    // RLEv1 one literal whose varint exceeds 64 bits. With valid PRESENT this
    // must fail as VarintTooLarge, distinguishing length decoding from PRESENT.
    let mut bad_lengths = vec![0xff];
    bad_lengths.extend([0x80; 11]);
    let parent_bytes = [0xff, 0xa0]; // true, false, true
    for nested in [false, true] {
        let parent = nested.then_some(parent_bytes.as_slice());
        let file = string_file(nested, 3, parent, Some(&[0xff]), &bad_lengths, b"x");
        assert_unexpected_eof(native_error(&read(file).unwrap_err()));

        let file = string_file(nested, 3, parent, Some(&[0xff, 0xe0]), &bad_lengths, b"x");
        assert!(matches!(
            native_error(&read(file).unwrap_err()),
            OrcError::VarintTooLarge { .. }
        ));
    }
    let file = string_file(true, 3, Some(&[0xff]), None, &bad_lengths, b"x");
    assert_unexpected_eof(native_error(&read(file).unwrap_err()));
}

#[test]
fn valid_string_nulls_and_nested_parent_child_nulls_are_preserved() {
    let flat = read(string_file(
        false,
        3,
        None,
        Some(&[0xff, 0xa0]), // true, false, true
        &[0xfe, 1, 1],
        b"ab",
    ))
    .unwrap();
    let actual = flat[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(actual, &StringArray::from(vec![Some("a"), None, Some("b")]));

    let nested = read(string_file(
        true,
        3,
        Some(&[0xff, 0xa0]), // parent: true, false, true
        Some(&[0xff, 0x40]), // child among non-null parents: false, true
        &[0xff, 1],
        b"b",
    ))
    .unwrap();
    let actual = nested[0]
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        actual.nulls(),
        Some(&NullBuffer::from(vec![true, false, true]))
    );
    let child = actual
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(child, &StringArray::from(vec![None, None, Some("b")]));

    let all_valid = [0xff, 0xe0];
    for present in [None, Some(all_valid.as_slice())] {
        let batches = read(string_file(
            false,
            3,
            None,
            present,
            &[0xfd, 1, 1, 1],
            b"abc",
        ))
        .unwrap();
        let actual = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(actual, &StringArray::from(vec!["a", "b", "c"]));
        assert!(actual.nulls().is_none());
    }
}

#[test]
fn skip_present_counts_bounded_chunks_and_preserves_the_next_bit() {
    struct BoundedBoolean(BooleanDecoder<Cursor<Vec<u8>>>);
    impl PrimitiveValueDecoder<bool> for BoundedBoolean {
        fn decode(&mut self, out: &mut [bool]) -> Result<()> {
            assert!(out.len() <= length::SKIP_BATCH_SIZE);
            self.0.decode(out)
        }

        fn skip(&mut self, _n: usize) -> Result<()> {
            panic!("PRESENT must be decoded to count non-null values");
        }
    }

    // 1,024 alternating bits, followed by true, false, true, then false.
    let mut bytes = vec![0x80]; // Byte RLE: 128 literal bytes.
    bytes.extend([0xaa; 128]);
    bytes.extend([0xff, 0xa0]);
    let mut present = Some(PresentDecoder {
        inner: Box::new(BoundedBoolean(BooleanDecoder::new(Cursor::new(bytes)))),
    });
    assert_eq!(
        skip_present_and_get_non_null_count(&mut present, None, 1027).unwrap(),
        514
    );
    assert_eq!(
        present.as_mut().unwrap().next_buffer(1).unwrap(),
        NullBuffer::from(vec![false])
    );
}

#[test]
fn skip_present_preserves_absent_and_nested_parent_null_semantics() {
    let mut absent = None;
    assert_eq!(
        skip_present_and_get_non_null_count(&mut absent, None, usize::MAX).unwrap(),
        usize::MAX
    );
    let parent = NullBuffer::from(vec![true, false, true]);
    assert_eq!(
        skip_present_and_get_non_null_count(&mut absent, Some(&parent), 3).unwrap(),
        2
    );

    let mut present = Some(PresentDecoder {
        // false, true among the two non-null parents; next value is true.
        inner: Box::new(BooleanDecoder::new(Cursor::new(vec![0xff, 0x60]))),
    });
    assert_eq!(
        skip_present_and_get_non_null_count(&mut present, Some(&parent), 3).unwrap(),
        1
    );
    assert_eq!(
        present.as_mut().unwrap().next_buffer(1).unwrap(),
        NullBuffer::from(vec![true])
    );
}
