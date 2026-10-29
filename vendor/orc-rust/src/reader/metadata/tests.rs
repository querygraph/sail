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

#[derive(Clone, Copy, Debug)]
enum Fault {
    Empty,
    Short,
    Extra,
}

struct TestReader {
    data: Bytes,
    requests: RefCell<Vec<(u64, u64)>>,
    fault: Option<(usize, Fault)>,
}

impl TestReader {
    fn new(data: Bytes, fault: Option<(usize, Fault)>) -> Self {
        Self {
            data,
            requests: RefCell::new(vec![]),
            fault,
        }
    }

    fn read_bytes(&self, offset: u64, length: u64) -> io::Result<Bytes> {
        let mut requests = self.requests.borrow_mut();
        let call = requests.len();
        requests.push((offset, length));
        let range = usize::try_from(offset).unwrap()..usize::try_from(offset + length).unwrap();
        let mut bytes = self
            .data
            .get(range)
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?
            .to_vec();
        if let Some((fault_call, fault)) = self.fault {
            if call == fault_call {
                match fault {
                    Fault::Empty => bytes.clear(),
                    Fault::Short => {
                        bytes.pop();
                    }
                    Fault::Extra => bytes.push(0),
                }
            }
        }
        Ok(bytes.into())
    }
}

impl ChunkReader for TestReader {
    type T = Cursor<Vec<u8>>;

    fn len(&self) -> u64 {
        self.data.len() as u64
    }

    fn get_read(&self, _offset: u64) -> io::Result<Self::T> {
        unreachable!("metadata reader uses get_bytes")
    }

    fn get_bytes(&self, offset: u64, length: u64) -> io::Result<Bytes> {
        self.read_bytes(offset, length)
    }
}

#[cfg(feature = "async")]
impl crate::reader::AsyncChunkReader for TestReader {
    fn len(&mut self) -> futures::future::BoxFuture<'_, io::Result<u64>> {
        Box::pin(async move { Ok(self.data.len() as u64) })
    }

    fn get_bytes(
        &mut self,
        offset: u64,
        length: u64,
    ) -> futures::future::BoxFuture<'_, io::Result<Bytes>> {
        Box::pin(async move { self.read_bytes(offset, length) })
    }
}

fn with_postscript(mut bytes: Vec<u8>, postscript: PostScript) -> Bytes {
    let postscript = postscript.encode_to_vec();
    assert!(postscript.len() < 256);
    bytes.extend_from_slice(&postscript);
    bytes.push(postscript.len() as u8);
    bytes.into()
}

fn valid_file(padding: usize) -> Bytes {
    let footer = Footer {
        types: vec![proto::Type {
            kind: Some(proto::r#type::Kind::Struct.into()),
            ..Default::default()
        }],
        metadata: vec![proto::UserMetadataItem {
            name: Some("padding".into()),
            value: Some(vec![7; padding]),
        }],
        number_of_rows: Some(0),
        ..Default::default()
    }
    .encode_to_vec();
    let postscript = PostScript {
        footer_length: Some(footer.len() as u64),
        metadata_length: Some(0),
        version: vec![0, 12],
        ..Default::default()
    };
    let mut bytes = b"ORC".to_vec();
    bytes.extend(footer);
    with_postscript(bytes, postscript)
}

fn malformed_files() -> Vec<(Bytes, &'static str)> {
    let lengths = |footer_length, metadata_length| {
        with_postscript(
            b"ORC".to_vec(),
            PostScript {
                footer_length: Some(footer_length),
                metadata_length: Some(metadata_length),
                ..Default::default()
            },
        )
    };
    vec![
        (Bytes::from_static(b"ORC\xff"), "postscript length"),
        (lengths(u64::MAX, 1), "lengths overflow"),
        (lengths(u64::MAX, 0), "lengths exceed the file"),
        (lengths(4, 0), "lengths exceed the file"),
        (lengths(1, 0), "overlap the file magic"),
        (lengths(0, 1), "overlap the file magic"),
    ]
}

fn check_metadata(metadata: FileMetadata, padding: usize) {
    assert_eq!(metadata.number_of_rows(), 0);
    assert_eq!(metadata.file_format_version(), "0.12");
    assert_eq!(metadata.user_custom_metadata()["padding"], vec![7; padding]);
}

#[test]
fn small_and_large_metadata_tails_keep_one_or_two_reads() {
    for (padding, reads) in [(0, 1), (2 * DEFAULT_FOOTER_SIZE as usize, 2)] {
        let mut reader = TestReader::new(valid_file(padding), None);
        check_metadata(read_metadata(&mut reader).unwrap(), padding);
        assert_eq!(reader.requests.borrow().len(), reads);
    }
}

#[test]
fn malformed_tail_lengths_return_errors_before_another_read() {
    for (bytes, message) in malformed_files() {
        let mut reader = TestReader::new(bytes, None);
        let error = read_metadata(&mut reader).unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
        assert_eq!(reader.requests.borrow().len(), 1);
    }
}

#[test]
fn unexpected_tail_read_lengths_are_rejected() {
    for call in [0, 1] {
        for fault in [Fault::Empty, Fault::Short, Fault::Extra] {
            let mut reader = TestReader::new(
                valid_file(2 * DEFAULT_FOOTER_SIZE as usize),
                Some((call, fault)),
            );
            let error = read_metadata(&mut reader).unwrap_err();
            assert!(
                error.to_string().contains("unexpected number of bytes"),
                "{error}"
            );
            assert_eq!(reader.requests.borrow().len(), call + 1);
        }
    }
}

#[test]
fn empty_file_does_not_issue_a_read() {
    let mut reader = TestReader::new(Bytes::new(), None);
    assert!(matches!(
        read_metadata(&mut reader),
        Err(error::OrcError::EmptyFile { .. })
    ));
    assert!(reader.requests.borrow().is_empty());
}

#[cfg(feature = "async")]
#[tokio::test]
async fn async_tail_checks_match_sync_reader() {
    for (padding, reads) in [(0, 1), (2 * DEFAULT_FOOTER_SIZE as usize, 2)] {
        let mut reader = TestReader::new(valid_file(padding), None);
        check_metadata(read_metadata_async(&mut reader).await.unwrap(), padding);
        assert_eq!(reader.requests.borrow().len(), reads);
    }
    for (bytes, message) in malformed_files() {
        let mut reader = TestReader::new(bytes, None);
        let error = read_metadata_async(&mut reader).await.unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
        assert_eq!(reader.requests.borrow().len(), 1);
    }
    for call in [0, 1] {
        for fault in [Fault::Empty, Fault::Short, Fault::Extra] {
            let mut reader = TestReader::new(
                valid_file(2 * DEFAULT_FOOTER_SIZE as usize),
                Some((call, fault)),
            );
            let error = read_metadata_async(&mut reader).await.unwrap_err();
            assert!(
                error.to_string().contains("unexpected number of bytes"),
                "{error}"
            );
            assert_eq!(reader.requests.borrow().len(), call + 1);
        }
    }
    let mut reader = TestReader::new(Bytes::new(), None);
    assert!(matches!(
        read_metadata_async(&mut reader).await,
        Err(error::OrcError::EmptyFile { .. })
    ));
    assert!(reader.requests.borrow().is_empty());
}

struct BoundedFixture {
    data: Bytes,
    encoded: usize,
    decoded: usize,
    footer_decoded: usize,
    types: Vec<proto::Type>,
}

fn bounded_fixture(compressed: bool, padding: usize) -> BoundedFixture {
    let types = vec![
        proto::Type {
            kind: Some(proto::r#type::Kind::Struct.into()),
            subtypes: vec![1],
            field_names: vec!["items".into()],
            ..Default::default()
        },
        proto::Type {
            kind: Some(proto::r#type::Kind::List.into()),
            subtypes: vec![2],
            ..Default::default()
        },
        proto::Type {
            kind: Some(proto::r#type::Kind::Long.into()),
            attributes: vec![proto::StringPair {
                key: Some("spark.sql.catalyst.type".into()),
                value: Some("timestamp_ntz".into()),
            }],
            ..Default::default()
        },
    ];
    let footer = Footer {
        types: types.clone(),
        stripes: vec![proto::StripeInformation {
            offset: Some(3),
            index_length: Some(0),
            data_length: Some(0),
            footer_length: Some(0),
            number_of_rows: Some(0),
            ..Default::default()
        }],
        metadata: vec![proto::UserMetadataItem {
            name: Some("padding".into()),
            value: Some(vec![7; padding]),
        }],
        ..Default::default()
    }
    .encode_to_vec();
    let metadata = Metadata {
        stripe_stats: vec![proto::StripeStatistics::default()],
    }
    .encode_to_vec();
    let footer_decoded = footer.len();
    let decoded = footer.len() + metadata.len();
    assert!(decoded > footer_decoded);
    let kind = compressed.then_some(crate::compression::CompressionType::Zstd);
    let mut compressor = crate::compression::Compressor::new(kind, Some(64)).unwrap();
    let footer = compressor.compress(&footer).unwrap().into_owned();
    let metadata = compressor.compress(&metadata).unwrap().into_owned();
    let encoded = footer.len() + metadata.len();
    let postscript = PostScript {
        footer_length: Some(footer.len() as u64),
        metadata_length: Some(metadata.len() as u64),
        compression: Some(
            if compressed {
                proto::CompressionKind::Zstd
            } else {
                proto::CompressionKind::None
            }
            .into(),
        ),
        compression_block_size: Some(64),
        ..Default::default()
    };
    let mut bytes = b"ORC".to_vec();
    bytes.extend(metadata);
    bytes.extend(footer);
    BoundedFixture {
        data: with_postscript(bytes, postscript),
        encoded,
        decoded,
        footer_decoded,
        types,
    }
}

fn exact_limits(fixture: &BoundedFixture) -> MetadataReadOptions {
    MetadataReadOptions {
        max_encoded_metadata_bytes: Some(fixture.encoded),
        max_decoded_metadata_bytes: Some(fixture.decoded),
        max_schema_depth: Some(2),
    }
}

#[test]
fn metadata_options_enforce_combined_decoded_and_encoded_limits() {
    for compressed in [false, true] {
        let fixture = bounded_fixture(compressed, 512);
        let options = exact_limits(&fixture);
        let mut reader = TestReader::new(fixture.data.clone(), None);
        let metadata = read_metadata_with_options(&mut reader, options).unwrap();
        assert_eq!(metadata.column_types(), fixture.types);
        assert_eq!(reader.requests.borrow().len(), 1);
        for decoded in [0, fixture.footer_decoded, fixture.decoded - 1] {
            let options = MetadataReadOptions {
                max_decoded_metadata_bytes: Some(decoded),
                ..options
            };
            let mut reader = TestReader::new(fixture.data.clone(), None);
            assert!(
                read_metadata_with_options(&mut reader, options).is_err(),
                "compressed={compressed}, decoded={decoded}"
            );
        }
        let options = MetadataReadOptions {
            max_encoded_metadata_bytes: Some(fixture.encoded - 1),
            ..options
        };
        let mut reader = TestReader::new(fixture.data, None);
        assert!(read_metadata_with_options(&mut reader, options)
            .unwrap_err()
            .to_string()
            .contains("Encoded footer"));
    }
}

#[test]
fn encoded_limit_rejects_large_tail_before_second_read() {
    let fixture = bounded_fixture(false, 2 * DEFAULT_FOOTER_SIZE as usize);
    let options = MetadataReadOptions {
        max_encoded_metadata_bytes: Some(fixture.encoded - 1),
        ..exact_limits(&fixture)
    };
    let mut reader = TestReader::new(fixture.data.clone(), None);
    assert!(read_metadata_with_options(&mut reader, options).is_err());
    assert_eq!(reader.requests.borrow().len(), 1);
    let mut reader = TestReader::new(fixture.data.clone(), None);
    assert_eq!(
        read_metadata_with_options(&mut reader, exact_limits(&fixture))
            .unwrap()
            .column_types(),
        fixture.types
    );
    assert_eq!(reader.requests.borrow().len(), 2);
}

#[test]
fn bounded_builder_validates_depth_before_schema_construction() {
    let fixture = bounded_fixture(true, 128);
    let options = exact_limits(&fixture);
    let reader = TestReader::new(fixture.data.clone(), None);
    let builder = crate::ArrowReaderBuilder::try_new_with_options(reader, options).unwrap();
    assert_eq!(builder.file_metadata().column_types(), fixture.types);
    let options = MetadataReadOptions {
        max_schema_depth: Some(1),
        ..options
    };
    let reader = TestReader::new(fixture.data, None);
    assert!(crate::ArrowReaderBuilder::try_new_with_options(reader, options).is_err());
}

#[test]
fn validated_metadata_can_be_reused_without_another_tail_read() {
    let mut reader = TestReader::new(valid_file(0), None);
    let metadata = Arc::new(read_metadata(&mut reader).unwrap());
    let builder = crate::ArrowReaderBuilder::new_with_metadata(reader, metadata.clone());
    assert!(Arc::ptr_eq(&metadata, &builder.file_metadata));
    assert_eq!(builder.schema().fields().len(), 0);
    assert_eq!(builder.reader.requests.borrow().len(), 1);
}

#[cfg(feature = "async")]
#[tokio::test]
async fn async_metadata_options_match_sync_limits_and_attributes() {
    for compressed in [false, true] {
        let fixture = bounded_fixture(compressed, 2 * DEFAULT_FOOTER_SIZE as usize);
        let options = exact_limits(&fixture);
        let mut reader = TestReader::new(fixture.data.clone(), None);
        let metadata = read_metadata_async_with_options(&mut reader, options)
            .await
            .unwrap();
        assert_eq!(metadata.column_types(), fixture.types);
        for decoded in [0, fixture.footer_decoded, fixture.decoded - 1] {
            let options = MetadataReadOptions {
                max_decoded_metadata_bytes: Some(decoded),
                ..options
            };
            let mut reader = TestReader::new(fixture.data.clone(), None);
            assert!(read_metadata_async_with_options(&mut reader, options)
                .await
                .is_err());
        }
        let options = MetadataReadOptions {
            max_encoded_metadata_bytes: Some(fixture.encoded - 1),
            ..options
        };
        let mut reader = TestReader::new(fixture.data.clone(), None);
        assert!(read_metadata_async_with_options(&mut reader, options)
            .await
            .is_err());
        assert_eq!(reader.requests.borrow().len(), 1);
        let reader = TestReader::new(fixture.data.clone(), None);
        let builder =
            crate::ArrowReaderBuilder::try_new_async_with_options(reader, exact_limits(&fixture))
                .await
                .unwrap();
        assert_eq!(builder.file_metadata().column_types(), fixture.types);
        let options = MetadataReadOptions {
            max_schema_depth: Some(1),
            ..exact_limits(&fixture)
        };
        let reader = TestReader::new(fixture.data, None);
        assert!(
            crate::ArrowReaderBuilder::try_new_async_with_options(reader, options)
                .await
                .is_err()
        );
    }
}
