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

use std::io::Read;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use prost::Message;

use crate::compression::{CompressionType, Decompressor};
use crate::proto;
use crate::reader::metadata::{read_metadata, MetadataReadOptions};
use crate::{ArrowReaderBuilder, ArrowWriterBuilder};

#[derive(Clone, Copy, Debug)]
enum Encoding {
    Uncompressed,
    Zlib,
    OriginalBlocks,
}

struct Fixture {
    file: Bytes,
    batch: RecordBatch,
    footer_size: usize,
}

fn fixture(encoding: Encoding) -> Fixture {
    let schema = Arc::new(Schema::new(
        (0..32)
            .map(|index| Field::new(format!("value_{index}"), DataType::Int64, true))
            .collect::<Vec<_>>(),
    ));
    let values = Arc::new(Int64Array::from(vec![Some(7), None, Some(-3)])) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), vec![values; 32]).unwrap();
    let mut file = Vec::new();
    let mut builder = ArrowWriterBuilder::new(&mut file, schema);
    match encoding {
        Encoding::Uncompressed => {}
        Encoding::Zlib => {
            builder = builder
                .with_compression(CompressionType::Zlib)
                .with_compression_block_size(64);
        }
        Encoding::OriginalBlocks => {
            // A one-byte block cannot benefit from deflate, so the writer must
            // emit original blocks despite the file's compressed encoding.
            builder = builder
                .with_compression(CompressionType::Zlib)
                .with_compression_block_size(1);
        }
    }
    let mut writer = builder.try_build().unwrap();
    for _ in 0..2 {
        writer.write(&batch).unwrap();
        writer.flush_stripe().unwrap();
    }
    writer.close().unwrap();

    let file = Bytes::from(file);
    let metadata = read_metadata(&mut file.clone()).unwrap();
    assert_eq!(metadata.stripe_metadatas().len(), 2);
    let mut footer_sizes = Vec::new();
    for stripe in metadata.stripe_metadatas() {
        let start = stripe.footer_offset() as usize;
        let encoded = file.slice(start..start + stripe.footer_length() as usize);
        if !matches!(encoding, Encoding::Uncompressed) {
            let mut offset = 0;
            let mut compressed_blocks = 0;
            let mut blocks = 0;
            while offset < encoded.len() {
                let header = u32::from_le_bytes([
                    encoded[offset],
                    encoded[offset + 1],
                    encoded[offset + 2],
                    0,
                ]);
                compressed_blocks += usize::from(header & 1 == 0);
                blocks += 1;
                offset += 3 + (header >> 1) as usize;
            }
            assert_eq!(offset, encoded.len());
            assert!(blocks > 1);
            match encoding {
                Encoding::Zlib => assert!(compressed_blocks > 0),
                Encoding::OriginalBlocks => assert_eq!(compressed_blocks, 0),
                Encoding::Uncompressed => unreachable!(),
            }
        }
        let mut decoded = Vec::new();
        Decompressor::new(encoded, metadata.compression(), vec![])
            .read_to_end(&mut decoded)
            .unwrap();
        footer_sizes.push(decoded.len());
    }
    assert!(footer_sizes[0] > 0);
    assert_eq!(footer_sizes[0], footer_sizes[1]);
    Fixture {
        file,
        batch,
        footer_size: footer_sizes[0],
    }
}

fn metadata_options() -> MetadataReadOptions {
    MetadataReadOptions {
        max_encoded_metadata_bytes: Some(64 * 1024),
        max_decoded_metadata_bytes: Some(64 * 1024),
        max_schema_depth: Some(1),
    }
}

fn assert_limit_error(error: impl std::fmt::Display) {
    let message = error.to_string();
    assert!(
        message.contains("exceeds the configured output limit")
            || message.contains("exceeds the allowed output size"),
        "{message}"
    );
}

#[test]
fn sync_stripe_footer_limits_are_per_stripe_and_independent_of_file_metadata() {
    for encoding in [
        Encoding::Uncompressed,
        Encoding::Zlib,
        Encoding::OriginalBlocks,
    ] {
        let fixture = fixture(encoding);
        for limit in [None, Some(fixture.footer_size)] {
            let mut builder =
                ArrowReaderBuilder::try_new_with_options(fixture.file.clone(), metadata_options())
                    .unwrap();
            if let Some(limit) = limit {
                builder = builder.with_max_decoded_stripe_footer_bytes(limit);
            }
            let batches = builder.build().collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(batches, vec![fixture.batch.clone(); 2]);
        }
        for limit in [fixture.footer_size - 1, 0] {
            let builder =
                ArrowReaderBuilder::try_new_with_options(fixture.file.clone(), metadata_options())
                    .unwrap();
            let error = builder
                .with_max_decoded_stripe_footer_bytes(limit)
                .build()
                .collect::<Result<Vec<_>, _>>()
                .unwrap_err();
            assert_limit_error(error);
        }
        assert!(ArrowReaderBuilder::try_new_with_options(
            fixture.file,
            MetadataReadOptions {
                max_decoded_metadata_bytes: Some(0),
                ..metadata_options()
            },
        )
        .is_err());
    }
}

// A real ORC file with a zero-column, two-row stripe needs no streams or column
// encodings. Its StripeFooter protobuf is empty, but its file footer is not.
fn empty_stripe_footer_file() -> Bytes {
    let footer = proto::Footer {
        stripes: vec![proto::StripeInformation {
            offset: Some(3),
            index_length: Some(0),
            data_length: Some(0),
            footer_length: Some(0),
            number_of_rows: Some(2),
            ..Default::default()
        }],
        types: vec![proto::Type {
            kind: Some(proto::r#type::Kind::Struct.into()),
            ..Default::default()
        }],
        number_of_rows: Some(2),
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
    let mut file = b"ORC".to_vec();
    file.extend(footer);
    file.extend_from_slice(&postscript);
    file.push(u8::try_from(postscript.len()).unwrap());
    file.into()
}

#[test]
fn sync_zero_budget_accepts_an_empty_stripe_footer() {
    let batches = ArrowReaderBuilder::try_new(empty_stripe_footer_file())
        .unwrap()
        .with_max_decoded_stripe_footer_bytes(0)
        .build()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(batches[0].num_columns(), 0);
}

#[cfg(feature = "async")]
#[tokio::test]
async fn async_stripe_footer_limits_match_sync_limits() {
    use futures::TryStreamExt;
    use std::io::Cursor;

    for encoding in [
        Encoding::Uncompressed,
        Encoding::Zlib,
        Encoding::OriginalBlocks,
    ] {
        let fixture = fixture(encoding);
        for limit in [None, Some(fixture.footer_size)] {
            let mut builder = ArrowReaderBuilder::try_new_async_with_options(
                Cursor::new(fixture.file.clone()),
                metadata_options(),
            )
            .await
            .unwrap();
            if let Some(limit) = limit {
                builder = builder.with_max_decoded_stripe_footer_bytes(limit);
            }
            let batches = builder.build_async().try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(batches, vec![fixture.batch.clone(); 2]);
        }
        for limit in [fixture.footer_size - 1, 0] {
            let builder = ArrowReaderBuilder::try_new_async_with_options(
                Cursor::new(fixture.file.clone()),
                metadata_options(),
            )
            .await
            .unwrap();
            let error = builder
                .with_max_decoded_stripe_footer_bytes(limit)
                .build_async()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err();
            assert_limit_error(error);
        }
        assert!(ArrowReaderBuilder::try_new_async_with_options(
            Cursor::new(fixture.file),
            MetadataReadOptions {
                max_decoded_metadata_bytes: Some(0),
                ..metadata_options()
            },
        )
        .await
        .is_err());
    }

    let batches = ArrowReaderBuilder::try_new_async(Cursor::new(empty_stripe_footer_file()))
        .await
        .unwrap()
        .with_max_decoded_stripe_footer_bytes(0)
        .build_async()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(batches[0].num_columns(), 0);
}
