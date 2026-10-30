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

use std::io::{self, Read};
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{ArrayRef, DictionaryArray, GenericByteArray, StringArray};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow::compute::kernels::cast;
use arrow::datatypes::{ByteArrayType, DataType, GenericBinaryType, GenericStringType};
use snafu::ResultExt;

use crate::array_decoder::derive_present_vec;
use crate::column::Column;
use crate::compression::Decompressor;
use crate::encoding::integer::get_unsigned_int_decoder;
use crate::encoding::PrimitiveValueDecoder;
use crate::error::{ArrowSnafu, IoSnafu, Result};
use crate::proto::column_encoding::Kind as ColumnEncodingKind;
use crate::proto::stream::Kind;
use crate::stripe::Stripe;

use super::length::{
    check_length_buffer_size, checked_total_length, new_length_buffer, skip_lengths,
};
use super::{ArrayBatchDecoder, Int64ArrayDecoder, PresentDecoder};

mod dictionary;

// TODO: reduce duplication with string below
pub fn new_binary_decoder(column: &Column, stripe: &Stripe) -> Result<Box<dyn ArrayBatchDecoder>> {
    let present = PresentDecoder::from_stripe(stripe, column);

    let lengths = stripe.stream_map().get(column, Kind::Length);
    let lengths = get_unsigned_int_decoder(lengths, column.rle_version());

    let bytes = Box::new(stripe.stream_map().get(column, Kind::Data));
    Ok(Box::new(BinaryArrayDecoder::new(bytes, lengths, present)))
}

pub fn new_string_decoder(column: &Column, stripe: &Stripe) -> Result<Box<dyn ArrayBatchDecoder>> {
    let kind = column.encoding().kind();
    let present = PresentDecoder::from_stripe(stripe, column);

    let lengths = stripe.stream_map().get(column, Kind::Length);
    let lengths = get_unsigned_int_decoder(lengths, column.rle_version());

    match kind {
        ColumnEncodingKind::Direct | ColumnEncodingKind::DirectV2 => {
            let bytes = Box::new(stripe.stream_map().get(column, Kind::Data));
            Ok(Box::new(DirectStringArrayDecoder::new(
                bytes, lengths, present,
            )))
        }
        ColumnEncodingKind::Dictionary | ColumnEncodingKind::DictionaryV2 => {
            let mut bytes = stripe.stream_map().get(column, Kind::DictionaryData);
            let mut lengths = lengths;
            // TODO: is this always guaranteed to be set for all dictionaries?
            let dictionary_size = column.dictionary_size();
            let dictionary_strings =
                dictionary::read_dictionary(&mut bytes, lengths.as_mut(), dictionary_size)?;
            let dictionary_strings = Arc::new(dictionary_strings);

            let indexes = stripe.stream_map().get(column, Kind::Data);
            let indexes = get_unsigned_int_decoder(indexes, column.rle_version());
            let indexes = Int64ArrayDecoder::new(indexes, present);

            Ok(Box::new(DictionaryStringArrayDecoder::new(
                indexes,
                dictionary_strings,
            )?))
        }
    }
}

// TODO: check this offset size type
pub type DirectStringArrayDecoder = GenericByteArrayDecoder<GenericStringType<i32>>;
pub type BinaryArrayDecoder = GenericByteArrayDecoder<GenericBinaryType<i32>>;

pub struct GenericByteArrayDecoder<T: ByteArrayType> {
    bytes: Box<Decompressor>,
    lengths: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    present: Option<PresentDecoder>,
    phantom: PhantomData<T>,
}

impl<T: ByteArrayType> GenericByteArrayDecoder<T> {
    fn new(
        bytes: Box<Decompressor>,
        lengths: Box<dyn PrimitiveValueDecoder<i64> + Send>,
        present: Option<PresentDecoder>,
    ) -> Self {
        Self {
            bytes,
            lengths,
            present,
            phantom: Default::default(),
        }
    }

    fn next_byte_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<GenericByteArray<T>> {
        check_length_buffer_size::<T::Offset>(batch_size)?;
        let present =
            derive_present_vec(&mut self.present, parent_present, batch_size).transpose()?;

        let mut lengths = new_length_buffer::<T::Offset>(batch_size)?;
        if let Some(present) = &present {
            self.lengths.decode_spaced(&mut lengths, present)?;
        } else {
            self.lengths.decode(&mut lengths)?;
        }
        let total_length = checked_total_length(&lengths)?;
        // Fetch all data bytes at once
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(total_length)
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))
            .context(IoSnafu)?;
        self.bytes
            .by_ref()
            .take(total_length as u64)
            .read_to_end(&mut bytes)
            .context(IoSnafu)?;
        let bytes = Buffer::from(bytes);
        let offsets =
            OffsetBuffer::<T::Offset>::from_lengths(lengths.into_iter().map(|l| l as usize));

        let null_buffer = match present {
            // Edge case where keys of map cannot have a null buffer
            Some(present) if present.null_count() == 0 => None,
            _ => present,
        };
        let array =
            GenericByteArray::<T>::try_new(offsets, bytes, null_buffer).context(ArrowSnafu)?;
        Ok(array)
    }
}

impl<T: ByteArrayType> ArrayBatchDecoder for GenericByteArrayDecoder<T> {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let array = self.next_byte_batch(batch_size, parent_present)?;
        let array = Arc::new(array) as ArrayRef;
        Ok(array)
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        use crate::array_decoder::skip_present_and_get_non_null_count;

        let non_null_count =
            skip_present_and_get_non_null_count(&mut self.present, parent_present, n)?;

        skip_lengths(self.lengths.as_mut(), non_null_count, |length| {
            let length = u64::try_from(length).map_err(|_| {
                crate::error::OutOfSpecSnafu {
                    msg: "Skipped byte count does not fit u64",
                }
                .build()
            })?;
            // TODO: can we use the decompressor to skip the bytes?
            let copied = io::copy(&mut self.bytes.by_ref().take(length), &mut io::sink())
                .context(IoSnafu)?;
            if copied != length {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "ORC byte payload ended before all skipped bytes were consumed",
                ))
                .context(IoSnafu);
            }
            Ok(())
        })
    }
}

pub struct DictionaryStringArrayDecoder {
    indexes: Int64ArrayDecoder,
    dictionary: Arc<StringArray>,
}

impl DictionaryStringArrayDecoder {
    fn new(indexes: Int64ArrayDecoder, dictionary: Arc<StringArray>) -> Result<Self> {
        Ok(Self {
            indexes,
            dictionary,
        })
    }
}

impl ArrayBatchDecoder for DictionaryStringArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        let keys = self
            .indexes
            .next_primitive_batch(batch_size, parent_present)?;
        // TODO: ORC spec states: For dictionary encodings the dictionary is sorted
        //       (in lexicographical order of bytes in the UTF-8 encodings).
        //       So we can set the is_ordered property here?
        let array = DictionaryArray::try_new(keys, self.dictionary.clone()).context(ArrowSnafu)?;
        // Cast back to StringArray to ensure all stripes have consistent datatype
        // TODO: Is there anyway to preserve the dictionary encoding?
        //       This costs performance.
        let array = cast(&array, &DataType::Utf8).context(ArrowSnafu)?;

        let array = Arc::new(array);
        Ok(array)
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        self.indexes.skip_values(n, parent_present)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::BinaryArray;
    use bytes::Bytes;

    use super::*;
    use crate::array_decoder::length::tests::{
        assert_error, malformed_cases, many_unsigned_literals, negative_cases, unsigned_literals,
        Unread,
    };
    use crate::compression::Compression;
    use crate::encoding::boolean::BooleanDecoder;
    use crate::encoding::integer::RleVersion;
    use crate::error::OrcError;

    fn malformed<T: ByteArrayType>() {
        let payload = Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]);
        for case in malformed_cases() {
            let mut decoder = GenericByteArrayDecoder::<T>::new(
                Box::new(Decompressor::new(payload.clone(), None, vec![])),
                case.decoder(),
                None,
            );
            assert_error(
                decoder.next_byte_batch(case.count, None).unwrap_err(),
                case.expected,
            );
            // The actual payload decompressor remains at its original position.
            let mut remaining = vec![];
            decoder.bytes.read_to_end(&mut remaining).unwrap();
            assert_eq!(remaining.as_slice(), payload.as_ref());
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let mut decoder = GenericByteArrayDecoder::<T>::new(
            Box::new(Decompressor::empty()),
            Box::new(Unread(calls.clone())),
            None,
        );
        assert!(decoder.next_byte_batch(usize::MAX, None).is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn malformed_string_lengths_leave_payload_unread() {
        malformed::<GenericStringType<i32>>();
    }

    #[test]
    fn malformed_binary_lengths_leave_payload_unread() {
        malformed::<GenericBinaryType<i32>>();
    }

    fn valid<T: ByteArrayType>() -> GenericByteArray<T> {
        let mut decoder = GenericByteArrayDecoder::<T>::new(
            Box::new(Decompressor::new(Bytes::from_static(b"abc"), None, vec![])),
            get_unsigned_int_decoder(Cursor::new(unsigned_literals(&[2, 0, 1])), RleVersion::V1),
            None,
        );
        decoder
            .next_byte_batch(4, Some(&NullBuffer::from(vec![true, false, true, true])))
            .unwrap()
    }

    #[test]
    fn string_and_binary_lengths_preserve_null_and_empty_values() {
        assert_eq!(
            valid::<GenericStringType<i32>>(),
            StringArray::from(vec![Some("ab"), None, Some(""), Some("c")])
        );
        assert_eq!(
            valid::<GenericBinaryType<i32>>(),
            BinaryArray::from(vec![
                Some(b"ab".as_slice()),
                None,
                Some(b"".as_slice()),
                Some(b"c".as_slice())
            ])
        );
    }

    fn malformed_skip<T: ByteArrayType>() {
        let payload = Bytes::from_static(b"abcd");
        for case in negative_cases() {
            let mut decoder = GenericByteArrayDecoder::<T>::new(
                Box::new(Decompressor::new(payload.clone(), None, vec![])),
                case.decoder(),
                None,
            );
            assert_error(
                decoder.skip_values(case.count, None).unwrap_err(),
                case.expected,
            );
            let mut remaining = vec![];
            decoder.bytes.read_to_end(&mut remaining).unwrap();
            assert_eq!(remaining.as_slice(), payload.as_ref());
        }
    }

    #[test]
    fn malformed_string_and_binary_skip_lengths_leave_payload_unread() {
        malformed_skip::<GenericStringType<i32>>();
        malformed_skip::<GenericBinaryType<i32>>();
    }

    fn truncated_skip<T: ByteArrayType>() {
        // The second request exceeds materialized i32 offsets, but must reach
        // the payload without allocating its declared size or rejecting it.
        for lengths in [vec![2], vec![i32::MAX as u64, 1]] {
            let mut decoder = GenericByteArrayDecoder::<T>::new(
                Box::new(Decompressor::new(Bytes::from_static(b"a"), None, vec![])),
                get_unsigned_int_decoder(Cursor::new(unsigned_literals(&lengths)), RleVersion::V1),
                None,
            );
            let OrcError::IoError { source, .. } =
                decoder.skip_values(lengths.len(), None).unwrap_err()
            else {
                panic!("expected skipped payload EOF");
            };
            assert_eq!(source.kind(), io::ErrorKind::UnexpectedEof);
        }
    }

    #[test]
    fn string_and_binary_skips_require_all_promised_payload_bytes() {
        truncated_skip::<GenericStringType<i32>>();
        truncated_skip::<GenericBinaryType<i32>>();
    }

    #[test]
    fn skipped_payload_preserves_the_original_decompressor_error() {
        // An incomplete compression header fails as InvalidData, not the
        // synthesized UnexpectedEof used for a successful short payload read.
        let mut decoder = DirectStringArrayDecoder::new(
            Box::new(Decompressor::new(
                Bytes::from_static(&[0xff]),
                Compression::from_proto(crate::proto::CompressionKind::Zlib, None),
                vec![],
            )),
            get_unsigned_int_decoder(Cursor::new(unsigned_literals(&[1])), RleVersion::V1),
            None,
        );
        let OrcError::IoError { source, .. } = decoder.skip_values(1, None).unwrap_err() else {
            panic!("expected original decompressor I/O error");
        };
        assert_eq!(source.kind(), io::ErrorKind::InvalidData);
        assert!(source
            .get_ref()
            .unwrap()
            .downcast_ref::<OrcError>()
            .is_some());
    }

    fn skip_then_read<T: ByteArrayType>() -> GenericByteArray<T> {
        let count = super::super::length::SKIP_BATCH_SIZE + 1;
        let mut lengths = vec![0; count - 1];
        lengths.extend([2, 1]);
        let mut decoder = GenericByteArrayDecoder::<T>::new(
            Box::new(Decompressor::new(Bytes::from_static(b"abc"), None, vec![])),
            get_unsigned_int_decoder(
                Cursor::new(many_unsigned_literals(&lengths)),
                RleVersion::V1,
            ),
            None,
        );
        let mut parent = vec![true; count + 1];
        parent[0] = false;
        decoder
            .skip_values(count + 1, Some(&NullBuffer::from(parent)))
            .unwrap();
        decoder.next_byte_batch(1, None).unwrap()
    }

    #[test]
    fn string_and_binary_skips_across_chunks_and_nulls_preserve_the_next_value() {
        assert_eq!(
            skip_then_read::<GenericStringType<i32>>(),
            StringArray::from(vec!["c"])
        );
        assert_eq!(
            skip_then_read::<GenericBinaryType<i32>>(),
            BinaryArray::from(vec![b"c".as_slice()])
        );
    }

    #[test]
    fn skip_present_failure_precedes_lengths_and_all_null_parents_read_neither() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut decoder = DirectStringArrayDecoder::new(
            Box::new(Decompressor::empty()),
            Box::new(Unread(calls.clone())),
            Some(PresentDecoder {
                inner: Box::new(BooleanDecoder::new(Cursor::new(vec![0xff]))),
            }),
        );
        decoder
            .skip_values(3, Some(&NullBuffer::from(vec![false; 3])))
            .unwrap();
        let OrcError::IoError { source, .. } = decoder.skip_values(usize::MAX, None).unwrap_err()
        else {
            panic!("expected PRESENT EOF before LENGTH decoding");
        };
        assert_eq!(source.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}
