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

//! Parse ORC file tail metadata structures from file.
//!
//! File tail structure:
//!
//! ```text
//! ------------------
//! |    Metadata    |
//! |                |
//! ------------------
//! |     Footer     |
//! |                |
//! ------------------
//! |  Postscript  |X|
//! ------------------
//! ```
//!
//! Where X is last byte in file indicating
//! Postscript length in bytes.
//!
//! Footer and Metadata lengths are encoded in Postscript.
//! Postscript is never compressed, Footer and Metadata
//! may be compressed depending Postscript config value.
//!
//! If they are compressed then their lengths indicate their
//! compressed lengths.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use prost::Message;
use snafu::{ensure, OptionExt, ResultExt};

use crate::compression::{Compression, Decompressor};
use crate::error::{self, EmptyFileSnafu, OutOfSpecSnafu, Result};
use crate::proto::{self, Footer, Metadata, PostScript};
use crate::schema::RootDataType;
use crate::statistics::ColumnStatistics;
use crate::stripe::StripeMetadata;

use crate::reader::ChunkReader;

const DEFAULT_FOOTER_SIZE: u64 = 16 * 1024;

/// Optional limits applied while reading the ORC file tail.
///
/// Defaults preserve the existing unrestricted metadata API. Applications reading
/// untrusted files should set all three limits. These bound encoded/decoded byte
/// counts and schema nesting, not total heap usage: protobuf objects, retained
/// metadata, codec history, buffer capacity and scratch space also consume memory.
#[derive(Debug, Clone, Copy, Default)]
pub struct MetadataReadOptions {
    /// Maximum combined encoded footer and metadata length. Checked before the
    /// second range read; the initial tail read is at most 16 KiB regardless.
    pub max_encoded_metadata_bytes: Option<usize>,
    /// Maximum combined decoded footer and metadata length. The remaining budget
    /// is passed into block decoding before allocating its output. Streaming
    /// codecs may decode one extra byte to detect an exceeded limit.
    pub max_decoded_metadata_bytes: Option<usize>,
    /// Maximum schema depth, with the root STRUCT at depth zero. Checked before
    /// recursively constructing the schema, including unprojected columns.
    pub max_schema_depth: Option<usize>,
}

/// The file's metadata.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    compression: Option<Compression>,
    root_data_type: RootDataType,
    column_types: Arc<[proto::Type]>,
    number_of_rows: u64,
    file_format_version: String,
    /// Statistics of columns across entire file
    column_statistics: Vec<ColumnStatistics>,
    stripes: Vec<StripeMetadata>,
    user_custom_metadata: HashMap<String, Vec<u8>>,
    /// The maximum number of rows in each index entry (default 10,000)
    row_index_stride: Option<u32>,
}

impl FileMetadata {
    fn from_proto(
        postscript: &proto::PostScript,
        footer: &proto::Footer,
        metadata: &proto::Metadata,
        options: MetadataReadOptions,
    ) -> Result<Self> {
        let compression =
            Compression::from_proto(postscript.compression(), postscript.compression_block_size);
        let root_data_type = match options.max_schema_depth {
            Some(depth) => RootDataType::from_proto_with_max_depth(&footer.types, Some(depth))?,
            None => RootDataType::from_proto(&footer.types)?,
        };
        let number_of_rows = footer.number_of_rows();
        let column_statistics = footer
            .statistics
            .iter()
            .map(TryFrom::try_from)
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            metadata.stripe_stats.is_empty() || metadata.stripe_stats.len() == footer.stripes.len(),
            OutOfSpecSnafu {
                msg: "stripe stats length must equal the number of stripes"
            }
        );
        // TODO: confirm if this is valid
        let stripes = if metadata.stripe_stats.is_empty() {
            footer
                .stripes
                .iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>>>()?
        } else {
            footer
                .stripes
                .iter()
                .zip(metadata.stripe_stats.iter())
                .map(TryFrom::try_from)
                .collect::<Result<Vec<_>>>()?
        };
        let user_custom_metadata = footer
            .metadata
            .iter()
            .map(|kv| (kv.name().to_owned(), kv.value().to_vec()))
            .collect::<HashMap<_, _>>();

        let file_format_version = postscript
            .version
            .iter()
            .map(|v| v.to_string() + ".")
            .collect::<String>();
        let file_format_version = file_format_version
            .strip_suffix('.')
            .unwrap_or("")
            .to_string();

        Ok(Self {
            compression,
            root_data_type,
            column_types: footer.types.clone().into(),
            number_of_rows,
            file_format_version,
            column_statistics,
            stripes,
            user_custom_metadata,
            row_index_stride: footer.row_index_stride,
        })
    }

    pub fn number_of_rows(&self) -> u64 {
        self.number_of_rows
    }

    pub fn compression(&self) -> Option<Compression> {
        self.compression
    }

    pub fn root_data_type(&self) -> &RootDataType {
        &self.root_data_type
    }

    /// Validated ORC type records indexed by column ID, including their original
    /// attributes. This preserves logical annotations without reparsing the tail.
    pub fn column_types(&self) -> &[proto::Type] {
        &self.column_types
    }

    pub fn column_file_statistics(&self) -> &[ColumnStatistics] {
        &self.column_statistics
    }

    pub fn stripe_metadatas(&self) -> &[StripeMetadata] {
        &self.stripes
    }

    pub fn user_custom_metadata(&self) -> &HashMap<String, Vec<u8>> {
        &self.user_custom_metadata
    }

    /// Get the row index stride (rows per row group)
    ///
    /// Returns the number of rows per row group used for row-level indexes.
    /// Default is 10,000 according to ORC spec.
    ///
    /// If `None` is returned, it means row indexes are not enabled for this file.
    pub fn row_index_stride(&self) -> Option<usize> {
        self.row_index_stride.map(|s| s as usize)
    }

    pub fn file_format_version(&self) -> &str {
        &self.file_format_version
    }
}

/// Validated locations of the encoded metadata and footer within a file tail.
/// Both reader modes use this before requesting or slicing the declared sections.
struct EncodedTail {
    postscript: PostScript,
    bytes: Bytes,
    offset: u64,
    length: usize,
    footer_length: usize,
    metadata_length: usize,
}

impl EncodedTail {
    fn parse(file_len: u64, mut bytes: Bytes) -> Result<Self> {
        ensure!(
            bytes.len() as u64 == file_len.min(DEFAULT_FOOTER_SIZE),
            OutOfSpecSnafu {
                msg: "Tail read returned an unexpected number of bytes"
            }
        );
        let postscript_len = usize::from(*bytes.last().context(OutOfSpecSnafu {
            msg: "File tail is empty",
        })?);
        bytes.truncate(bytes.len() - 1);
        let postscript_start = bytes
            .len()
            .checked_sub(postscript_len)
            .context(OutOfSpecSnafu {
                msg: "File too small for given postscript length",
            })?;
        let postscript =
            PostScript::decode(&bytes[postscript_start..]).context(error::DecodeProtoSnafu)?;
        bytes.truncate(postscript_start);

        let footer_length = postscript.footer_length.context(OutOfSpecSnafu {
            msg: "Footer length is empty",
        })?;
        let metadata_length = postscript.metadata_length.context(OutOfSpecSnafu {
            msg: "Metadata length is empty",
        })?;
        let length = footer_length
            .checked_add(metadata_length)
            .context(OutOfSpecSnafu {
                msg: "Footer and metadata lengths overflow",
            })?;
        let offset = file_len
            .checked_sub(1 + postscript_len as u64)
            .and_then(|end| end.checked_sub(length))
            .context(OutOfSpecSnafu {
                msg: "Footer and metadata lengths exceed the file",
            })?;
        ensure!(
            offset >= 3,
            OutOfSpecSnafu {
                msg: "Footer and metadata overlap the file magic"
            }
        );
        // Validate conversions before issuing a second read, whose default
        // ChunkReader implementation allocates a usize-sized buffer.
        let length = isize::try_from(length).ok().context(OutOfSpecSnafu {
            msg: "Footer and metadata lengths exceed the addressable size",
        })? as usize;
        Ok(Self {
            postscript,
            bytes,
            offset,
            length,
            // Each section is no larger than the validated sum.
            footer_length: footer_length as usize,
            metadata_length: metadata_length as usize,
        })
    }

    fn missing_length(&self) -> usize {
        self.length.saturating_sub(self.bytes.len())
    }

    fn check_encoded_limit(&self, options: MetadataReadOptions) -> Result<()> {
        if let Some(limit) = options.max_encoded_metadata_bytes {
            ensure!(
                self.length <= limit,
                OutOfSpecSnafu {
                    msg: "Encoded footer and metadata exceed the configured limit"
                }
            );
        }
        Ok(())
    }

    fn decode(self, prefix: Bytes, options: MetadataReadOptions) -> Result<FileMetadata> {
        ensure!(
            prefix.len() == self.missing_length(),
            OutOfSpecSnafu {
                msg: "Metadata read returned an unexpected number of bytes"
            }
        );
        let mut bytes = if prefix.is_empty() {
            self.bytes
        } else {
            let mut bytes = BytesMut::with_capacity(self.length);
            bytes.extend_from_slice(&prefix);
            bytes.extend_from_slice(&self.bytes);
            bytes.freeze()
        };
        let compression = Compression::from_proto(
            self.postscript.compression(),
            self.postscript.compression_block_size,
        );
        let footer_start = bytes.len() - self.footer_length;
        let mut remaining = options.max_decoded_metadata_bytes;
        let footer: Footer =
            decode_section(bytes.slice(footer_start..), compression, &mut remaining)?;
        bytes.truncate(footer_start);
        let metadata: Metadata = decode_section(
            bytes.slice(bytes.len() - self.metadata_length..),
            compression,
            &mut remaining,
        )?;
        FileMetadata::from_proto(&self.postscript, &footer, &metadata, options)
    }
}

pub fn read_metadata<R: ChunkReader>(reader: &mut R) -> Result<FileMetadata> {
    read_metadata_with_options(reader, MetadataReadOptions::default())
}

/// Read metadata with explicit tail byte and schema-depth limits.
pub fn read_metadata_with_options<R: ChunkReader>(
    reader: &mut R,
    options: MetadataReadOptions,
) -> Result<FileMetadata> {
    let file_len = reader.len();
    if file_len == 0 {
        return EmptyFileSnafu.fail();
    }
    // At most two reads: first the usual tail size, then any missing prefix.
    let tail_length = file_len.min(DEFAULT_FOOTER_SIZE);
    let bytes = reader
        .get_bytes(file_len - tail_length, tail_length)
        .context(error::IoSnafu)?;
    let tail = EncodedTail::parse(file_len, bytes)?;
    tail.check_encoded_limit(options)?;
    let prefix = if tail.missing_length() == 0 {
        Bytes::new()
    } else {
        reader
            .get_bytes(tail.offset, tail.missing_length() as u64)
            .context(error::IoSnafu)?
    };
    tail.decode(prefix, options)
}

#[cfg(feature = "async")]
pub async fn read_metadata_async<R: super::AsyncChunkReader>(
    reader: &mut R,
) -> Result<FileMetadata> {
    read_metadata_async_with_options(reader, MetadataReadOptions::default()).await
}

/// Asynchronously read metadata with the same limits as the synchronous reader.
#[cfg(feature = "async")]
pub async fn read_metadata_async_with_options<R: super::AsyncChunkReader>(
    reader: &mut R,
    options: MetadataReadOptions,
) -> Result<FileMetadata> {
    let file_len = reader.len().await.context(error::IoSnafu)?;
    if file_len == 0 {
        return EmptyFileSnafu.fail();
    }
    let tail_length = file_len.min(DEFAULT_FOOTER_SIZE);
    let bytes = reader
        .get_bytes(file_len - tail_length, tail_length)
        .await
        .context(error::IoSnafu)?;
    let tail = EncodedTail::parse(file_len, bytes)?;
    tail.check_encoded_limit(options)?;
    let prefix = if tail.missing_length() == 0 {
        Bytes::new()
    } else {
        reader
            .get_bytes(tail.offset, tail.missing_length() as u64)
            .await
            .context(error::IoSnafu)?
    };
    tail.decode(prefix, options)
}

fn decode_section<T: Message + Default>(
    bytes: Bytes,
    compression: Option<Compression>,
    remaining: &mut Option<usize>,
) -> Result<T> {
    let mut decoder = match *remaining {
        Some(limit) => Decompressor::new_with_limit(bytes, compression, vec![], limit),
        None => Decompressor::new(bytes, compression, vec![]),
    };
    let mut buffer = vec![];
    decoder.read_to_end(&mut buffer).context(error::IoSnafu)?;
    if let Some(limit) = remaining {
        *limit = limit.checked_sub(buffer.len()).context(OutOfSpecSnafu {
            msg: "Decoded footer and metadata exceed the configured limit",
        })?;
    }
    T::decode(buffer.as_slice()).context(error::DecodeProtoSnafu)
}

#[cfg(test)]
mod tests;
