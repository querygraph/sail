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

use std::marker::PhantomData;

use arrow::{
    array::{Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, OffsetSizeTrait},
    buffer::NullBuffer,
    compute::filter,
    datatypes::{
        ArrowPrimitiveType, ArrowTimestampType, ByteArrayType, Date32Type, Decimal128Type,
        Float32Type, Float64Type, GenericBinaryType, GenericStringType, Int16Type, Int32Type,
        Int64Type, Int8Type,
    },
};
use bytes::{BufMut, BytesMut};
use snafu::ResultExt;

use crate::{
    encoding::{
        boolean::BooleanEncoder,
        byte::ByteRleEncoder,
        float::FloatEncoder,
        integer::{rle_v2::RleV2Encoder, NInt, SignedEncoding, UnsignedEncoding},
        timestamp::TimestampEncoder,
        PrimitiveValueEncoder,
    },
    error::{ArrowSnafu, Result},
    memory::EstimateMemory,
    writer::StreamType,
};

use super::{ColumnEncoding, Stream};

/// Encodes a specific column for a stripe. Will encode to an internal memory
/// buffer until it is finished, in which case it returns the stream bytes to
/// be serialized to a writer.
pub trait ColumnStripeEncoder: EstimateMemory {
    /// Encode entire provided [`ArrayRef`] to internal buffer.
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()>;

    /// Column encoding used for streams.
    fn column_encoding(&self) -> ColumnEncoding;

    /// Emit buffered streams to be written to the writer, and reset state
    /// in preparation for next stripe.
    fn finish(&mut self) -> Vec<Stream>;

    /// Encoders of the child columns, in column ID order.
    fn children(&self) -> &[Box<dyn ColumnStripeEncoder>] {
        &[]
    }

    /// Encoders of the child columns, in column ID order.
    fn children_mut(&mut self) -> &mut [Box<dyn ColumnStripeEncoder>] {
        &mut []
    }
}

// TODO: simplify these generics, probably overcomplicating things here

/// Encoder for primitive ORC types (e.g. int, float). Uses a specific [`PrimitiveValueEncoder`] to
/// encode the primitive values into internal memory. When finished, outputs a DATA stream and
/// optionally a PRESENT stream.
pub struct PrimitiveColumnEncoder<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> {
    encoder: E,
    column_encoding: ColumnEncoding,
    /// Lazily initialized once we encounter an [`Array`] with a [`NullBuffer`].
    present: Option<BooleanEncoder>,
    encoded_count: usize,
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> PrimitiveColumnEncoder<T, E> {
    // TODO: encode knowledge of the ColumnEncoding as part of the type, instead of requiring it
    //       to be passed at runtime
    pub fn new(column_encoding: ColumnEncoding) -> Self {
        Self {
            encoder: E::new(),
            column_encoding,
            present: None,
            encoded_count: 0,
            _phantom: Default::default(),
        }
    }
}

impl<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> EstimateMemory
    for PrimitiveColumnEncoder<T, E>
{
    fn estimate_memory_size(&self) -> usize {
        self.encoder.estimate_memory_size()
            + self
                .present
                .as_ref()
                .map(|p| p.estimate_memory_size())
                .unwrap_or(0)
    }
}

impl<T: ArrowPrimitiveType, E: PrimitiveValueEncoder<T::Native>> ColumnStripeEncoder
    for PrimitiveColumnEncoder<T, E>
{
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        // TODO: return as result instead of panicking here?
        let array = array.as_primitive::<T>();
        // Handling case where if encoding across RecordBatch boundaries, arrays
        // might introduce a NullBuffer
        match (array.nulls(), &mut self.present) {
            // Need to copy only the valid values as indicated by null_buffer
            (Some(null_buffer), Some(present)) => {
                present.extend(null_buffer);
                for index in null_buffer.valid_indices() {
                    let v = array.value(index);
                    self.encoder.write_one(v);
                }
            }
            (Some(null_buffer), None) => {
                // Lazily initiate present buffer and ensure backfill the already encoded values
                let mut present = BooleanEncoder::new();
                present.extend_present(self.encoded_count);
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    let v = array.value(index);
                    self.encoder.write_one(v);
                }
            }
            // Simple direct copy from values buffer, extending present if needed
            (None, _) => {
                let values = array.values();
                self.encoder.write_slice(values);
                if let Some(present) = self.present.as_mut() {
                    present.extend_present(array.len())
                }
            }
        }
        self.encoded_count += array.len() - array.null_count();
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        self.column_encoding
    }

    fn finish(&mut self) -> Vec<Stream> {
        let bytes = self.encoder.take_inner();
        // Return mandatory Data stream and optional Present stream
        let data = Stream {
            kind: StreamType::Data,
            bytes,
        };
        self.encoded_count = 0;
        match &mut self.present {
            Some(present) => {
                let bytes = present.finish();
                let present = Stream {
                    kind: StreamType::Present,
                    bytes,
                };
                vec![data, present]
            }
            None => vec![data],
        }
    }
}

/// Seconds from the ORC epoch of 1 January 2015 to the Unix epoch.
const ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH: i64 = 1_420_070_400;

/// Encodes an Arrow timestamp into the DATA and SECONDARY streams used by ORC.
pub struct TimestampColumnEncoder<T: ArrowTimestampType> {
    encoder: TimestampEncoder<T>,
    present: Option<BooleanEncoder>,
    encoded_count: usize,
}

impl<T: ArrowTimestampType> TimestampColumnEncoder<T> {
    pub fn new() -> Self {
        Self {
            encoder: TimestampEncoder::new(
                ORC_EPOCH_UTC_SECONDS_SINCE_UNIX_EPOCH,
                Box::new(RleV2Encoder::<i64, SignedEncoding>::new()),
                Box::new(RleV2Encoder::<i64, UnsignedEncoding>::new()),
            ),
            present: None,
            encoded_count: 0,
        }
    }
}

impl<T: ArrowTimestampType> EstimateMemory for TimestampColumnEncoder<T> {
    fn estimate_memory_size(&self) -> usize {
        self.encoder.estimate_memory_size()
            + self
                .present
                .as_ref()
                .map(|present| present.estimate_memory_size())
                .unwrap_or(0)
    }
}

impl<T: ArrowTimestampType> ColumnStripeEncoder for TimestampColumnEncoder<T> {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        let array = array.as_primitive::<T>();
        if let Some(null_buffer) = array.nulls() {
            let values = null_buffer
                .valid_indices()
                .map(|index| array.value(index))
                .collect::<Vec<_>>();
            self.encoder.encode(&values)?;

            let encoded_count = self.encoded_count;
            let present = self.present.get_or_insert_with(|| {
                let mut present = BooleanEncoder::new();
                present.extend_present(encoded_count);
                present
            });
            present.extend(null_buffer);
        } else {
            self.encoder.encode(array.values())?;
            if let Some(present) = self.present.as_mut() {
                present.extend_present(array.len());
            }
        }
        self.encoded_count += array.len() - array.null_count();
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::DirectV2
    }

    fn finish(&mut self) -> Vec<Stream> {
        let (data, secondary) = self.encoder.take_inner();
        let mut streams = vec![
            Stream {
                kind: StreamType::Data,
                bytes: data,
            },
            Stream {
                kind: StreamType::Secondary,
                bytes: secondary,
            },
        ];
        self.encoded_count = 0;
        if let Some(present) = self.present.as_mut() {
            streams.push(Stream {
                kind: StreamType::Present,
                bytes: present.finish(),
            });
        }
        streams
    }
}

pub struct BooleanColumnEncoder {
    encoder: BooleanEncoder,
    /// Lazily initialized once we encounter an [`Array`] with a [`NullBuffer`].
    present: Option<BooleanEncoder>,
    encoded_count: usize,
}

impl BooleanColumnEncoder {
    pub fn new() -> Self {
        Self {
            encoder: BooleanEncoder::new(),
            present: None,
            encoded_count: 0,
        }
    }
}

impl EstimateMemory for BooleanColumnEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.encoder.estimate_memory_size()
            + self
                .present
                .as_ref()
                .map(|p| p.estimate_memory_size())
                .unwrap_or(0)
    }
}

impl ColumnStripeEncoder for BooleanColumnEncoder {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        // TODO: return as result instead of panicking here?
        let array = array.as_boolean();
        // Handling case where if encoding across RecordBatch boundaries, arrays
        // might introduce a NullBuffer
        match (array.nulls(), &mut self.present) {
            // Need to copy only the valid values as indicated by null_buffer
            (Some(null_buffer), Some(present)) => {
                present.extend(null_buffer);
                for index in null_buffer.valid_indices() {
                    let v = array.value(index);
                    self.encoder.extend_boolean(v);
                }
            }
            (Some(null_buffer), None) => {
                // Lazily initiate present buffer and ensure backfill the already encoded values
                let mut present = BooleanEncoder::new();
                present.extend_present(self.encoded_count);
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    let v = array.value(index);
                    self.encoder.extend_boolean(v);
                }
            }
            // Simple direct copy from values buffer, extending present if needed
            (None, _) => {
                let values = array.values();
                self.encoder.extend_bb(values);
                if let Some(present) = self.present.as_mut() {
                    present.extend_present(array.len())
                }
            }
        }
        self.encoded_count += array.len() - array.null_count();
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::Direct
    }

    fn finish(&mut self) -> Vec<Stream> {
        let bytes = self.encoder.finish();
        // Return mandatory Data stream and optional Present stream
        let data = Stream {
            kind: StreamType::Data,
            bytes,
        };
        self.encoded_count = 0;
        match &mut self.present {
            Some(present) => {
                let bytes = present.finish();
                let present = Stream {
                    kind: StreamType::Present,
                    bytes,
                };
                vec![data, present]
            }
            None => vec![data],
        }
    }
}

/// Direct encodes binary/strings.
pub struct GenericBinaryColumnEncoder<T: ByteArrayType>
where
    T::Offset: NInt,
{
    string_bytes: BytesMut,
    length_encoder: RleV2Encoder<T::Offset, UnsignedEncoding>,
    present: Option<BooleanEncoder>,
    encoded_count: usize,
}

impl<T: ByteArrayType> GenericBinaryColumnEncoder<T>
where
    T::Offset: NInt,
{
    pub fn new() -> Self {
        Self {
            string_bytes: BytesMut::new(),
            length_encoder: RleV2Encoder::new(),
            present: None,
            encoded_count: 0,
        }
    }
}

impl<T: ByteArrayType> EstimateMemory for GenericBinaryColumnEncoder<T>
where
    T::Offset: NInt,
{
    fn estimate_memory_size(&self) -> usize {
        self.string_bytes.len()
            + self.length_encoder.estimate_memory_size()
            + self
                .present
                .as_ref()
                .map(|p| p.estimate_memory_size())
                .unwrap_or(0)
    }
}

impl<T: ByteArrayType> ColumnStripeEncoder for GenericBinaryColumnEncoder<T>
where
    T::Offset: NInt,
{
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        if array.is_empty() {
            return Ok(());
        }
        // TODO: return as result instead of panicking here?
        let array = array.as_bytes::<T>();
        // Handling case where if encoding across RecordBatch boundaries, arrays
        // might introduce a NullBuffer
        match (array.nulls(), &mut self.present) {
            // Need to copy only the valid values as indicated by null_buffer
            (Some(null_buffer), Some(present)) => {
                present.extend(null_buffer);
                for index in null_buffer.valid_indices() {
                    self.length_encoder.write_one(array.value_length(index));
                    self.string_bytes.put_slice(array.value(index).as_ref());
                }
            }
            (Some(null_buffer), None) => {
                // Lazily initiate present buffer and ensure backfill the already encoded values
                let mut present = BooleanEncoder::new();
                present.extend_present(self.encoded_count);
                present.extend(null_buffer);
                self.present = Some(present);
                for index in null_buffer.valid_indices() {
                    self.length_encoder.write_one(array.value_length(index));
                    self.string_bytes.put_slice(array.value(index).as_ref());
                }
            }
            // Simple direct copy from values buffer, extending present if needed
            (None, _) => {
                let offsets = array.offsets();
                let first_offset = offsets[0];

                let mut length_to_copy = <T::Offset as num::Zero>::zero();
                let mut prev_offset = first_offset;
                // Derive lengths from offsets then encode them as ints
                for &offset in offsets.iter().skip(1) {
                    let length = offset - prev_offset;
                    self.length_encoder.write_one(length);
                    length_to_copy += length;
                    prev_offset = offset;
                }
                // Copy all string bytes in a single go
                // TODO: this cast to i64 to usize can be cleaned up?
                let first_offset = first_offset.as_i64() as usize;
                let end_offset = first_offset + length_to_copy.as_i64() as usize;
                let string_bytes = &array.value_data()[first_offset..end_offset];
                self.string_bytes.put_slice(string_bytes);

                if let Some(present) = self.present.as_mut() {
                    present.extend_present(array.len())
                }
            }
        }
        self.encoded_count += array.len() - array.null_count();
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::DirectV2
    }

    fn finish(&mut self) -> Vec<Stream> {
        // TODO: throwing away allocations here
        let data_bytes = std::mem::take(&mut self.string_bytes);
        let length_bytes = self.length_encoder.take_inner();
        let data = Stream {
            kind: StreamType::Data,
            bytes: data_bytes.into(),
        };
        let length = Stream {
            kind: StreamType::Length,
            bytes: length_bytes,
        };
        self.encoded_count = 0;
        match &mut self.present {
            Some(present) => {
                let bytes = present.finish();
                let present = Stream {
                    kind: StreamType::Present,
                    bytes,
                };
                vec![data, length, present]
            }
            None => vec![data, length],
        }
    }
}

/// The PRESENT stream of a column, started at its first [`NullBuffer`], with
/// a count of the non-null values before that.
#[derive(Default)]
struct PresentEncoder {
    present: Option<BooleanEncoder>,
    encoded_count: usize,
}

impl PresentEncoder {
    fn extend(&mut self, nulls: Option<&NullBuffer>, len: usize) {
        let encoded_count = self.encoded_count;
        match (nulls, &mut self.present) {
            (Some(nulls), Some(present)) => present.extend(nulls),
            (Some(nulls), present) => {
                let mut encoder = BooleanEncoder::new();
                encoder.extend_present(encoded_count);
                encoder.extend(nulls);
                *present = Some(encoder);
            }
            (None, Some(present)) => present.extend_present(len),
            (None, None) => {}
        }
        self.encoded_count += len - nulls.map_or(0, |nulls| nulls.null_count());
    }

    fn take_stream(&mut self) -> Option<Stream> {
        self.encoded_count = 0;
        self.present.as_mut().map(|present| Stream {
            kind: StreamType::Present,
            bytes: present.finish(),
        })
    }
}

impl EstimateMemory for PresentEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.present
            .as_ref()
            .map_or(0, |present| present.estimate_memory_size())
    }
}

fn children_memory_size(children: &[Box<dyn ColumnStripeEncoder>]) -> usize {
    children.iter().map(|c| c.estimate_memory_size()).sum()
}

/// A zigzag base 128 varint. Unsigned, as a 38 digit decimal sets the top bit
/// of its zigzag form, which the signed [`NInt`] shifts would smear.
fn write_decimal_varint(writer: &mut BytesMut, value: i128) {
    let mut value = ((value << 1) ^ (value >> 127)) as u128;
    while value >= 0x80 {
        writer.put_u8(value as u8 | 0x80);
        value >>= 7;
    }
    writer.put_u8(value as u8);
}

/// Encodes a decimal as a varint in the DATA stream and its scale in the
/// SECONDARY stream. The scale is the column's for every value.
pub struct DecimalColumnEncoder {
    data: BytesMut,
    scales: RleV2Encoder<i32, SignedEncoding>,
    scale: i32,
    present: PresentEncoder,
}

impl DecimalColumnEncoder {
    pub fn new(scale: i32) -> Self {
        Self {
            data: BytesMut::new(),
            scales: RleV2Encoder::new(),
            scale,
            present: PresentEncoder::default(),
        }
    }
}

impl EstimateMemory for DecimalColumnEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.data.len() + self.scales.estimate_memory_size() + self.present.estimate_memory_size()
    }
}

impl ColumnStripeEncoder for DecimalColumnEncoder {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        let array = array.as_primitive::<Decimal128Type>();
        self.present.extend(array.nulls(), array.len());
        for value in array.iter().flatten() {
            write_decimal_varint(&mut self.data, value);
            self.scales.write_one(self.scale);
        }
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::DirectV2
    }

    fn finish(&mut self) -> Vec<Stream> {
        let mut streams = vec![
            Stream {
                kind: StreamType::Data,
                bytes: std::mem::take(&mut self.data).into(),
            },
            Stream {
                kind: StreamType::Secondary,
                bytes: self.scales.take_inner(),
            },
        ];
        streams.extend(self.present.take_stream());
        streams
    }
}

/// Encodes a struct's nulls. A child holds values only for non-null parents.
pub struct StructColumnEncoder {
    children: Vec<Box<dyn ColumnStripeEncoder>>,
    present: PresentEncoder,
}

impl StructColumnEncoder {
    pub fn new(children: Vec<Box<dyn ColumnStripeEncoder>>) -> Self {
        Self {
            children,
            present: PresentEncoder::default(),
        }
    }
}

impl EstimateMemory for StructColumnEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.present.estimate_memory_size() + children_memory_size(&self.children)
    }
}

impl ColumnStripeEncoder for StructColumnEncoder {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        let array = array.as_struct();
        self.present.extend(array.nulls(), array.len());
        let nulls = array.nulls().filter(|nulls| nulls.null_count() > 0);
        for (column, child) in array.columns().iter().zip(self.children.iter_mut()) {
            match nulls {
                Some(nulls) => {
                    let valid = BooleanArray::new(nulls.inner().clone(), None);
                    child.encode_array(&filter(column, &valid).context(ArrowSnafu)?)?;
                }
                None => child.encode_array(column)?,
            }
        }
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::Direct
    }

    fn finish(&mut self) -> Vec<Stream> {
        self.present.take_stream().into_iter().collect()
    }

    fn children(&self) -> &[Box<dyn ColumnStripeEncoder>] {
        &self.children
    }

    fn children_mut(&mut self) -> &mut [Box<dyn ColumnStripeEncoder>] {
        &mut self.children
    }
}

/// The child rows of the non-null parents. A null parent can still span child
/// rows in Arrow, but ORC stores none for it.
fn present_child_rows<O: OffsetSizeTrait>(
    rows: &ArrayRef,
    offsets: &[O],
    nulls: Option<&NullBuffer>,
) -> Result<ArrayRef> {
    let (first, last) = (offsets[0].as_usize(), offsets[offsets.len() - 1].as_usize());
    let rows = rows.slice(first, last - first);
    let Some(nulls) = nulls.filter(|nulls| nulls.null_count() > 0) else {
        return Ok(rows);
    };
    let mut keep = BooleanBufferBuilder::new(rows.len());
    for (parent, range) in offsets.windows(2).enumerate() {
        keep.append_n((range[1] - range[0]).as_usize(), nulls.is_valid(parent));
    }
    filter(&rows, &BooleanArray::new(keep.finish(), None)).context(ArrowSnafu)
}

/// The LENGTH and PRESENT streams shared by lists and maps.
#[derive(Default)]
struct LengthEncoder {
    lengths: Option<RleV2Encoder<i64, UnsignedEncoding>>,
    present: PresentEncoder,
}

impl LengthEncoder {
    fn encode<O: OffsetSizeTrait>(&mut self, offsets: &[O], nulls: Option<&NullBuffer>) {
        self.present.extend(nulls, offsets.len() - 1);
        let lengths = self.lengths.get_or_insert_with(RleV2Encoder::new);
        for (parent, range) in offsets.windows(2).enumerate() {
            if nulls.map_or(true, |nulls| nulls.is_valid(parent)) {
                lengths.write_one((range[1] - range[0]).as_usize() as i64);
            }
        }
    }

    fn take_streams(&mut self) -> Vec<Stream> {
        let bytes = self
            .lengths
            .get_or_insert_with(RleV2Encoder::new)
            .take_inner();
        let mut streams = vec![Stream {
            kind: StreamType::Length,
            bytes,
        }];
        streams.extend(self.present.take_stream());
        streams
    }
}

impl EstimateMemory for LengthEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.present.estimate_memory_size()
            + self
                .lengths
                .as_ref()
                .map_or(0, |lengths| lengths.estimate_memory_size())
    }
}

/// Encodes an Arrow list or large list, with a single child column.
pub struct ListColumnEncoder<O: OffsetSizeTrait> {
    lengths: LengthEncoder,
    children: Vec<Box<dyn ColumnStripeEncoder>>,
    _phantom: PhantomData<O>,
}

impl<O: OffsetSizeTrait> ListColumnEncoder<O> {
    pub fn new(child: Box<dyn ColumnStripeEncoder>) -> Self {
        Self {
            lengths: LengthEncoder::default(),
            children: vec![child],
            _phantom: Default::default(),
        }
    }
}

impl<O: OffsetSizeTrait> EstimateMemory for ListColumnEncoder<O> {
    fn estimate_memory_size(&self) -> usize {
        self.lengths.estimate_memory_size() + children_memory_size(&self.children)
    }
}

impl<O: OffsetSizeTrait> ColumnStripeEncoder for ListColumnEncoder<O> {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        let array = array.as_list::<O>();
        self.lengths.encode(array.value_offsets(), array.nulls());
        let rows = present_child_rows(array.values(), array.value_offsets(), array.nulls())?;
        self.children[0].encode_array(&rows)
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::DirectV2
    }

    fn finish(&mut self) -> Vec<Stream> {
        self.lengths.take_streams()
    }

    fn children(&self) -> &[Box<dyn ColumnStripeEncoder>] {
        &self.children
    }

    fn children_mut(&mut self) -> &mut [Box<dyn ColumnStripeEncoder>] {
        &mut self.children
    }
}

/// Encodes an Arrow map as ORC keys and values, each its own child column.
pub struct MapColumnEncoder {
    lengths: LengthEncoder,
    children: Vec<Box<dyn ColumnStripeEncoder>>,
}

impl MapColumnEncoder {
    pub fn new(keys: Box<dyn ColumnStripeEncoder>, values: Box<dyn ColumnStripeEncoder>) -> Self {
        Self {
            lengths: LengthEncoder::default(),
            children: vec![keys, values],
        }
    }
}

impl EstimateMemory for MapColumnEncoder {
    fn estimate_memory_size(&self) -> usize {
        self.lengths.estimate_memory_size() + children_memory_size(&self.children)
    }
}

impl ColumnStripeEncoder for MapColumnEncoder {
    fn encode_array(&mut self, array: &ArrayRef) -> Result<()> {
        let array = array.as_map();
        self.lengths.encode(array.value_offsets(), array.nulls());
        let entries: ArrayRef = std::sync::Arc::new(array.entries().clone());
        let entries = present_child_rows(&entries, array.value_offsets(), array.nulls())?;
        let entries = entries.as_struct();
        for (column, child) in entries.columns().iter().zip(self.children.iter_mut()) {
            child.encode_array(column)?;
        }
        Ok(())
    }

    fn column_encoding(&self) -> ColumnEncoding {
        ColumnEncoding::DirectV2
    }

    fn finish(&mut self) -> Vec<Stream> {
        self.lengths.take_streams()
    }

    fn children(&self) -> &[Box<dyn ColumnStripeEncoder>] {
        &self.children
    }

    fn children_mut(&mut self) -> &mut [Box<dyn ColumnStripeEncoder>] {
        &mut self.children
    }
}

pub type FloatColumnEncoder = PrimitiveColumnEncoder<Float32Type, FloatEncoder<f32>>;
pub type DoubleColumnEncoder = PrimitiveColumnEncoder<Float64Type, FloatEncoder<f64>>;
pub type ByteColumnEncoder = PrimitiveColumnEncoder<Int8Type, ByteRleEncoder>;
pub type Int16ColumnEncoder = PrimitiveColumnEncoder<Int16Type, RleV2Encoder<i16, SignedEncoding>>;
pub type Int32ColumnEncoder = PrimitiveColumnEncoder<Int32Type, RleV2Encoder<i32, SignedEncoding>>;
pub type Int64ColumnEncoder = PrimitiveColumnEncoder<Int64Type, RleV2Encoder<i64, SignedEncoding>>;
pub type DateColumnEncoder = PrimitiveColumnEncoder<Date32Type, RleV2Encoder<i32, SignedEncoding>>;
pub type StringColumnEncoder = GenericBinaryColumnEncoder<GenericStringType<i32>>;
pub type LargeStringColumnEncoder = GenericBinaryColumnEncoder<GenericStringType<i64>>;
pub type BinaryColumnEncoder = GenericBinaryColumnEncoder<GenericBinaryType<i32>>;
pub type LargeBinaryColumnEncoder = GenericBinaryColumnEncoder<GenericBinaryType<i64>>;
