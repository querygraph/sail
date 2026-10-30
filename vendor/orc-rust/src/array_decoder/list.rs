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

use arrow::array::{ArrayRef, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{Field, FieldRef};
use snafu::ResultExt;

use crate::array_decoder::derive_present_vec;
use crate::column::Column;
use crate::encoding::integer::get_unsigned_int_decoder;
use crate::encoding::PrimitiveValueDecoder;
use crate::proto::stream::Kind;

use crate::error::{ArrowSnafu, Result};
use crate::stripe::Stripe;

use super::length::{check_length_buffer_size, checked_total_length, new_length_buffer};
use super::{array_decoder_factory, ArrayBatchDecoder, PresentDecoder};

pub struct ListArrayDecoder {
    inner: Box<dyn ArrayBatchDecoder>,
    present: Option<PresentDecoder>,
    lengths: Box<dyn PrimitiveValueDecoder<i64> + Send>,
    field: FieldRef,
}

impl ListArrayDecoder {
    pub fn new(column: &Column, field: Arc<Field>, stripe: &Stripe) -> Result<Self> {
        let present = PresentDecoder::from_stripe(stripe, column);

        let child = &column.children()[0];
        let inner = array_decoder_factory(child, field.data_type(), stripe)?;

        let reader = stripe.stream_map().get(column, Kind::Length);
        let lengths = get_unsigned_int_decoder(reader, column.rle_version());

        Ok(Self {
            inner,
            present,
            lengths,
            field,
        })
    }
}

impl ArrayBatchDecoder for ListArrayDecoder {
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
        // Fetch child array as one Array with total_length elements
        let child_array = self.inner.next_batch(total_length, None)?;
        let offsets = OffsetBuffer::from_lengths(lengths.into_iter().map(|l| l as usize));
        let null_buffer = present;

        let array = ListArray::try_new(self.field.clone(), offsets, child_array, null_buffer)
            .context(ArrowSnafu)?;
        let array = Arc::new(array);
        Ok(array)
    }

    fn skip_values(&mut self, n: usize, parent_present: Option<&NullBuffer>) -> Result<()> {
        use super::skip_present_and_get_non_null_count;

        let non_null_count =
            skip_present_and_get_non_null_count(&mut self.present, parent_present, n)?;

        // Decode lengths to determine how many child values to skip
        let mut lengths = vec![0; non_null_count];
        self.lengths.decode(&mut lengths)?;
        let total_length: i64 = lengths.iter().sum();

        // Skip the child values (children don't have parent_present from list)
        self.inner.skip_values(total_length as usize, None)?;

        Ok(())
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
        assert_error, malformed_cases, unsigned_literals, Unread,
    };
    use crate::array_decoder::Int64ArrayDecoder;
    use crate::encoding::integer::{get_signed_int_decoder, RleVersion};

    fn field() -> FieldRef {
        Arc::new(Field::new("item", DataType::Int64, true))
    }

    #[test]
    fn malformed_lengths_never_invoke_the_child_decoder() {
        for case in malformed_cases() {
            let calls = Arc::new(AtomicUsize::new(0));
            let mut decoder = ListArrayDecoder {
                inner: Box::new(Unread(calls.clone())),
                present: None,
                lengths: case.decoder(),
                field: field(),
            };
            assert_error(
                decoder.next_batch(case.count, None).unwrap_err(),
                case.expected,
            );
            assert_eq!(calls.load(Ordering::Relaxed), 0);
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let mut decoder = ListArrayDecoder {
            inner: Box::new(Unread(calls.clone())),
            present: None,
            lengths: Box::new(Unread(calls.clone())),
            field: field(),
        };
        assert!(decoder.next_batch(usize::MAX, None).is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn nested_lists_preserve_parent_nulls_and_small_child_values() {
        let values = Box::new(Int64ArrayDecoder::new(
            get_signed_int_decoder(
                Cursor::new(unsigned_literals(&[14, 16, 18])),
                RleVersion::V1,
            ),
            None,
        ));
        let inner = ListArrayDecoder {
            inner: values,
            present: None,
            lengths: get_unsigned_int_decoder(
                Cursor::new(unsigned_literals(&[2, 0, 1])),
                RleVersion::V1,
            ),
            field: field(),
        };
        let mut outer = ListArrayDecoder {
            inner: Box::new(inner),
            present: None,
            lengths: get_unsigned_int_decoder(
                Cursor::new(unsigned_literals(&[2, 1])),
                RleVersion::V1,
            ),
            field: Arc::new(Field::new("item", DataType::List(field()), true)),
        };
        let parent = NullBuffer::from(vec![true, false, true]);
        let output = outer.next_batch(3, Some(&parent)).unwrap();
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(output.value_offsets(), &[0, 2, 2, 3]);
        assert_eq!(output.nulls(), Some(&parent));
        let child = output
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(child.value_offsets(), &[0, 2, 2, 3]);
        assert_eq!(
            child
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![7, 8, 9])
        );
    }
}
