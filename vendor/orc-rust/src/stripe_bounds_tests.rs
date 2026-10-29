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

use std::cell::RefCell;
use std::io::{self, Cursor};

use super::*;
use crate::projection::ProjectionMask;
use crate::reader::metadata::read_metadata;

fn assert_out_of_spec(error: error::OrcError, expected: &str) {
    let error::OrcError::OutOfSpec { msg, .. } = error else {
        panic!("expected OutOfSpec, got {error}");
    };
    assert_eq!(msg, expected);
}

#[test]
fn stripe_metadata_checks_both_footer_offset_additions_and_footer_end() {
    for (offset, index, data, footer, expected) in [
        (u64::MAX, 1, 0, 0, "Stripe footer offset overflows u64"),
        (u64::MAX - 1, 1, 1, 0, "Stripe footer offset overflows u64"),
        (0, u64::MAX, 1, 0, "Stripe footer offset overflows u64"),
        (u64::MAX - 2, 1, 1, 1, "Stripe footer end overflows u64"),
    ] {
        let info = proto::StripeInformation {
            offset: Some(offset),
            index_length: Some(index),
            data_length: Some(data),
            footer_length: Some(footer),
            ..Default::default()
        };
        assert_out_of_spec(StripeMetadata::try_from(&info).unwrap_err(), expected);
        assert_out_of_spec(
            StripeMetadata::try_from((&info, &proto::StripeStatistics::default())).unwrap_err(),
            expected,
        );
    }
}

#[test]
fn stripe_metadata_preserves_missing_defaults_and_exact_u64_boundaries() {
    let info = proto::StripeInformation::default();
    for metadata in [
        StripeMetadata::try_from(&info).unwrap(),
        StripeMetadata::try_from((&info, &proto::StripeStatistics::default())).unwrap(),
    ] {
        assert_eq!(metadata.offset(), 0);
        assert_eq!(metadata.index_length(), 0);
        assert_eq!(metadata.data_length(), 0);
        assert_eq!(metadata.footer_offset(), 0);
        assert_eq!(metadata.footer_length(), 0);
        assert_eq!(metadata.number_of_rows(), 0);
    }
    let statistics = proto::StripeStatistics {
        col_stats: vec![proto::ColumnStatistics {
            number_of_values: Some(0),
            ..Default::default()
        }],
    };
    let metadata = StripeMetadata::try_from((&info, &statistics)).unwrap();
    assert_eq!(metadata.column_statistics().len(), 1);
    assert_eq!(metadata.column_statistics()[0].number_of_values(), 0);
    for (offset, index, data, footer) in [
        (u64::MAX, 0, 0, 0),
        (u64::MAX - 3, 1, 1, 1),
        (0, u64::MAX - 1, 1, 0),
    ] {
        let info = proto::StripeInformation {
            offset: Some(offset),
            index_length: Some(index),
            data_length: Some(data),
            footer_length: Some(footer),
            ..Default::default()
        };
        let metadata = StripeMetadata::try_from(&info).unwrap();
        assert_eq!(metadata.footer_offset(), u64::MAX - footer);
    }
}

fn file_metadata() -> FileMetadata {
    let footer = proto::Footer {
        types: vec![
            proto::Type {
                kind: Some(proto::r#type::Kind::Struct.into()),
                subtypes: vec![1, 2],
                field_names: vec!["selected".into(), "omitted".into()],
                ..Default::default()
            },
            proto::Type {
                kind: Some(proto::r#type::Kind::Long.into()),
                ..Default::default()
            },
            proto::Type {
                kind: Some(proto::r#type::Kind::Long.into()),
                ..Default::default()
            },
        ],
        ..Default::default()
    }
    .encode_to_vec();
    let postscript = proto::PostScript {
        footer_length: Some(footer.len() as u64),
        metadata_length: Some(0),
        ..Default::default()
    }
    .encode_to_vec();
    let mut file = b"ORC".to_vec();
    file.extend(footer);
    file.extend_from_slice(&postscript);
    file.push(u8::try_from(postscript.len()).unwrap());
    read_metadata(&mut Bytes::from(file)).unwrap()
}

struct TestReader {
    footer_offset: u64,
    footer: Bytes,
    requests: RefCell<Vec<(u64, u64)>>,
}

impl TestReader {
    fn read_bytes(&self, offset: u64, length: u64) -> io::Result<Bytes> {
        self.requests.borrow_mut().push((offset, length));
        if offset == self.footer_offset && length == self.footer.len() as u64 {
            return Ok(self.footer.clone());
        }
        // Synthetic sparse readers exercise u64 offsets without allocating
        // attacker-declared sizes. Any unguarded oversized read fails this test.
        assert!(length <= 16, "unexpected large range: {offset}, {length}");
        Ok(vec![7; usize::try_from(length).unwrap()].into())
    }
}

impl ChunkReader for TestReader {
    type T = Cursor<Vec<u8>>;

    fn len(&self) -> u64 {
        unreachable!("stripe construction must not query the file length")
    }

    fn get_read(&self, _offset: u64) -> io::Result<Self::T> {
        unreachable!("stripe construction uses get_bytes")
    }

    fn get_bytes(&self, offset: u64, length: u64) -> io::Result<Bytes> {
        self.read_bytes(offset, length)
    }
}

#[cfg(feature = "async")]
impl crate::reader::AsyncChunkReader for TestReader {
    fn len(&mut self) -> futures::future::BoxFuture<'_, io::Result<u64>> {
        unreachable!("stripe construction must not query the file length")
    }

    fn get_bytes(
        &mut self,
        offset: u64,
        length: u64,
    ) -> futures::future::BoxFuture<'_, io::Result<Bytes>> {
        Box::pin(async move { self.read_bytes(offset, length) })
    }
}

struct Case {
    info: proto::StripeInformation,
    reader: TestReader,
    expected_error: Option<&'static str>,
    expected_requests: Vec<(u64, u64)>,
    selected_length: Option<usize>,
}

fn case(offset: u64, index: u64, data: u64, streams: &[(u32, u64)]) -> Case {
    let footer = Bytes::from(
        StripeFooter {
            streams: streams
                .iter()
                .map(|&(column, length)| proto::Stream {
                    kind: Some(Kind::Data.into()),
                    column: Some(column),
                    length: Some(length),
                })
                .collect(),
            ..Default::default()
        }
        .encode_to_vec(),
    );
    let footer_offset = offset
        .checked_add(index)
        .unwrap()
        .checked_add(data)
        .unwrap();
    let footer_length = footer.len() as u64;
    Case {
        info: proto::StripeInformation {
            offset: Some(offset),
            index_length: Some(index),
            data_length: Some(data),
            footer_length: Some(footer_length),
            number_of_rows: Some(2),
            ..Default::default()
        },
        reader: TestReader {
            footer_offset,
            footer,
            requests: RefCell::new(vec![]),
        },
        expected_error: None,
        expected_requests: vec![(footer_offset, footer_length)],
        selected_length: None,
    }
}

fn cases() -> Vec<Case> {
    let mut cases = vec![];
    for streams in [vec![(1, 9)], vec![(2, 9)], vec![(2, 5), (1, 4)]] {
        let mut case = case(3, 2, 6, &streams);
        case.expected_error = Some("Stripe stream exceeds the declared index and data sections");
        cases.push(case);
    }
    let mut overflow = case(u64::MAX - 1024, 0, 100, &[(2, 2048)]);
    overflow.expected_error = Some("Stripe stream end overflows u64");
    cases.push(overflow);

    let unaddressable = isize::MAX as u64 + 1;
    let mut footer = case(3, 0, 0, &[]);
    footer.info.footer_length = Some(unaddressable);
    footer.expected_error = Some("Stripe buffer length exceeds addressable memory");
    footer.expected_requests.clear();
    cases.push(footer);

    let mut selected = case(3, 0, unaddressable, &[(1, unaddressable)]);
    selected.expected_error = Some("Stripe buffer length exceeds addressable memory");
    cases.push(selected);
    // Unselected encoded ranges need no buffer: large stripe declarations alone
    // do not introduce an arbitrary stripe-size cap.
    cases.push(case(3, 0, unaddressable, &[(2, unaddressable)]));

    // The first stream is skipped, but still advances the selected range's start.
    // Both an exact end and remaining declared padding are permitted.
    for data in [6, 8] {
        let mut valid = case(3, 2, data, &[(2, 2), (1, 6)]);
        valid.expected_requests.push((5, 6));
        valid.selected_length = Some(6);
        cases.push(valid);
    }
    let mut zero = case(3, 0, 0, &[(1, 0)]);
    zero.expected_requests.push((3, 0));
    zero.selected_length = Some(0);
    cases.push(zero);

    let mut rows = case(3, 0, 0, &[]);
    rows.info.number_of_rows = Some(u64::MAX);
    if usize::try_from(u64::MAX).is_err() {
        rows.expected_error = Some("Stripe row count does not fit usize");
        rows.expected_requests.clear();
    }
    cases.push(rows);
    cases
}

fn assert_result(case: &Case, result: Result<Stripe>) {
    match case.expected_error {
        Some(message) => assert_out_of_spec(result.unwrap_err(), message),
        None => {
            let stripe = result.unwrap();
            assert_eq!(
                stripe.number_of_rows(),
                usize::try_from(case.info.number_of_rows()).unwrap()
            );
            assert_eq!(stripe.columns().len(), 1);
            let stream = stripe
                .stream_map()
                .get_opt(&stripe.columns()[0], Kind::Data);
            match case.selected_length {
                Some(length) => {
                    let mut bytes = vec![];
                    stream.unwrap().read_to_end(&mut bytes).unwrap();
                    assert_eq!(bytes, vec![7; length]);
                }
                None => assert!(stream.is_none()),
            }
        }
    }
    assert_eq!(*case.reader.requests.borrow(), case.expected_requests);
}

#[test]
fn sync_stripe_ranges_are_checked_before_reading_or_skipping() {
    let metadata = file_metadata();
    let projection = metadata
        .root_data_type()
        .project(&ProjectionMask::roots(metadata.root_data_type(), [1]));
    for mut case in cases() {
        let info = StripeMetadata::try_from(&case.info).unwrap();
        let result = Stripe::new(&mut case.reader, &metadata, &projection, &info);
        assert_result(&case, result);
    }
}

#[cfg(feature = "async")]
#[tokio::test]
async fn async_stripe_range_checks_and_requests_match_sync() {
    let metadata = file_metadata();
    let projection = metadata
        .root_data_type()
        .project(&ProjectionMask::roots(metadata.root_data_type(), [1]));
    for mut case in cases() {
        let info = StripeMetadata::try_from(&case.info).unwrap();
        let result = Stripe::new_async(&mut case.reader, &metadata, &projection, &info).await;
        assert_result(&case, result);
    }
}

#[test]
fn buffer_addressability_accepts_the_exact_platform_boundary() {
    check_buffer_length(0).unwrap();
    check_buffer_length(isize::MAX as u64).unwrap();
    assert_out_of_spec(
        check_buffer_length(isize::MAX as u64 + 1).unwrap_err(),
        "Stripe buffer length exceeds addressable memory",
    );
}
