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

use std::sync::Arc;

use arrow::array::{ArrayRef, MapArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{Field, Fields};
use snafu::ResultExt;

use crate::array_decoder::derive_present_vec;
use crate::column::Column;
use crate::encoding::integer::get_unsigned_int_decoder;
use crate::encoding::PrimitiveValueDecoder;
use crate::error::{ArrowSnafu, Result};
use crate::proto::stream::Kind;
use crate::stripe::Stripe;

use super::length::{
    check_length_buffer_size, checked_total_length, new_length_buffer, skip_lengths,
};
use super::{array_decoder_factory, ArrayBatchDecoder, PresentDecoder};

pub struct MapArrayDecoder {
    keys: Box<dyn ArrayBatchDecoder>,
    values: Box<dyn ArrayBatchDecoder>,
    present: Option<PresentDecoder>,
    lengths: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    fields: Fields,
}

impl MapArrayDecoder {
    pub fn new(
        column: &Column,
        keys_field: Arc<Field>,
        values_field: Arc<Field>,
        stripe: &Stripe,
    ) -> Result<Self> {
        let present = PresentDecoder::from_stripe(stripe, column);

        let keys_column = &column.children()[0];
        let keys = array_decoder_factory(keys_column, keys_field.data_type(), stripe)?;

        let values_column = &column.children()[1];
        let values = array_decoder_factory(values_column, values_field.data_type(), stripe)?;

        let reader = stripe.stream_map().get(column, Kind::Length);
        let lengths = get_unsigned_int_decoder(reader, column.rle_version());

        let fields = Fields::from(vec![keys_field, values_field]);

        Ok(Self {
            keys,
            values,
            present,
            lengths,
            fields,
        })
    }
}

impl ArrayBatchDecoder for MapArrayDecoder {
    fn next_batch(
        &mut self,
        batch_size: usize,
        parent_present: Option<&NullBuffer>,
    ) -> Result<ArrayRef> {
        check_length_buffer_size::<i32>(batch_size)?;
        let present =
            derive_present_vec(&mut self.present, parent_present, batch_size).transpose()?;

        let mut lengths = new_length_buffer::<i32>(batch_size)?;
        if let Some(present) = &present {
            self.lengths.decode_spaced(&mut lengths, present)?;
        } else {
            self.lengths.decode(&mut lengths)?;
        }
        let total_length = checked_total_length(&lengths)?;
        // Fetch key and value arrays, each with total_length elements
        // Fetch child array as one Array with total_length elements
        let keys_array = self.keys.next_batch(total_length, None)?;
        let values_array = self.values.next_batch(total_length, None)?;
        // Compose the keys + values array into a StructArray with two entries
        let entries =
            StructArray::try_new(self.fields.clone(), vec![keys_array, values_array], None)
                .context(ArrowSnafu)?;
        let offsets = OffsetBuffer::from_lengths(lengths.into_iter().map(|l| l as usize));

        let field = Arc::new(Field::new_struct("entries", self.fields.clone(), false));
        let array =
            MapArray::try_new(field, offsets, entries, present, false).context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        use super::skip_present_and_get_non_null_count;

        let non_null_count =
            skip_present_and_get_non_null_count(&mut self.present, parent_present, n)?;

        skip_lengths(self.lengths.as_mut(), non_null_count, |length| {
            self.keys.skip_values(length, None)?;
            self.values.skip_values(length, None)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::DataType;

    use super::*;
    use crate::array_decoder::length::tests::{
        assert_error, malformed_cases, many_unsigned_literals, negative_cases, unsigned_literals,
        Skipped, Unread,
    };
    use crate::array_decoder::Int64ArrayDecoder;
    use crate::encoding::integer::{get_signed_int_decoder, RleVersion};

    fn fields() -> Fields {
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]
        .into()
    }

    #[test]
    fn malformed_lengths_never_invoke_key_or_value_decoders() {
        for case in malformed_cases() {
            let calls = Arc::new(AtomicUsize::new(0));
            let mut decoder = MapArrayDecoder {
                keys: Box::new(Unread(calls.clone())),
                values: Box::new(Unread(calls.clone())),
                present: None,
                lengths: case.decoder(),
                fields: fields(),
            };
            assert_error(
                decoder.next_batch(case.count, None).unwrap_err(),
                case.expected,
            );
            assert_eq!(calls.load(Ordering::Relaxed), 0);
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let mut decoder = MapArrayDecoder {
            keys: Box::new(Unread(calls.clone())),
            values: Box::new(Unread(calls.clone())),
            present: None,
            lengths: Box::new(Unread(calls.clone())),
            fields: fields(),
        };
        assert!(decoder.next_batch(usize::MAX, None).is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn map_lengths_preserve_nulls_empty_maps_and_entries() {
        let integers = |values: &[u64]| {
            Box::new(Int64ArrayDecoder::new(
                get_signed_int_decoder(Cursor::new(unsigned_literals(values)), RleVersion::V1),
                None,
            )) as Box<dyn ArrayBatchDecoder>
        };
        let mut decoder = MapArrayDecoder {
            keys: integers(&[2, 4, 6]),
            values: integers(&[14, 16, 18]),
            present: None,
            lengths: get_unsigned_int_decoder(
                Cursor::new(unsigned_literals(&[2, 0, 1])),
                RleVersion::V1,
            ),
            fields: fields(),
        };
        let parent = NullBuffer::from(vec![true, false, true, true]);
        let output = decoder.next_batch(4, Some(&parent)).unwrap();
        let output = output.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(output.value_offsets(), &[0, 2, 2, 2, 3]);
        assert_eq!(output.nulls(), Some(&parent));
        assert_eq!(
            output.keys().as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![1, 2, 3])
        );
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![7, 8, 9])
        );
    }

    #[test]
    fn malformed_skip_lengths_never_invoke_map_keys_or_values() {
        for case in negative_cases() {
            let skips = Arc::default();
            let mut decoder = MapArrayDecoder {
                keys: Box::new(Skipped(Arc::clone(&skips))),
                values: Box::new(Skipped(Arc::clone(&skips))),
                present: None,
                lengths: case.decoder(),
                fields: fields(),
            };
            assert_error(
                decoder.skip_values(case.count, None).unwrap_err(),
                case.expected,
            );
            assert!(skips.lock().unwrap().is_empty());
        }
    }

    fn integers(values: &[u64]) -> Box<dyn ArrayBatchDecoder> {
        Box::new(Int64ArrayDecoder::new(
            get_signed_int_decoder(Cursor::new(unsigned_literals(values)), RleVersion::V1),
            None,
        ))
    }

    #[test]
    fn map_skips_across_length_chunks_and_parent_nulls_preserve_the_next_entry() {
        let count = super::super::length::SKIP_BATCH_SIZE + 1;
        let mut lengths = vec![0; count - 1];
        lengths.extend([2, 1]);
        let mut decoder = MapArrayDecoder {
            keys: integers(&[2, 4, 6]),
            values: integers(&[14, 16, 18]),
            present: None,
            lengths: get_unsigned_int_decoder(
                Cursor::new(many_unsigned_literals(&lengths)),
                RleVersion::V1,
            ),
            fields: fields(),
        };
        let mut parent = vec![true; count + 1];
        parent[0] = false;
        decoder
            .skip_values(count + 1, Some(&NullBuffer::from(parent)))
            .unwrap();
        let next = decoder.next_batch(1, None).unwrap();
        let next = next.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(next.value_offsets(), &[0, 1]);
        assert_eq!(
            next.keys().as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![3])
        );
        assert_eq!(
            next.values().as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![9])
        );
    }

    #[test]
    fn map_skip_propagates_truncated_keys_and_values() {
        for (keys, values) in [(vec![2], vec![14, 16]), (vec![2, 4], vec![14])] {
            let mut decoder = MapArrayDecoder {
                keys: integers(&keys),
                values: integers(&values),
                present: None,
                lengths: get_unsigned_int_decoder(
                    Cursor::new(unsigned_literals(&[2])),
                    RleVersion::V1,
                ),
                fields: fields(),
            };
            assert!(decoder.skip_values(1, None).is_err());
        }
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn map_skips_advance_keys_and_values_for_each_overflow_flush() {
        let keys = Arc::default();
        let values = Arc::default();
        let mut decoder = MapArrayDecoder {
            keys: Box::new(Skipped(Arc::clone(&keys))),
            values: Box::new(Skipped(Arc::clone(&values))),
            present: None,
            lengths: get_unsigned_int_decoder(
                Cursor::new(unsigned_literals(&[i64::MAX as u64, i64::MAX as u64, 2])),
                RleVersion::V1,
            ),
            fields: fields(),
        };
        decoder.skip_values(3, None).unwrap();
        assert_eq!(*keys.lock().unwrap(), vec![usize::MAX - 1, 2]);
        assert_eq!(*values.lock().unwrap(), vec![usize::MAX - 1, 2]);
    }
}
