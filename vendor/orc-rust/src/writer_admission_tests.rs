use std::io::{self, Write};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Int32Array, Int32Builder, Int64Array,
    ListArray, MapArray, MapBuilder, RecordBatch, StringArray, StringBuilder, StructArray,
    TimestampMicrosecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use bytes::Bytes;
use prost::Message;

use super::{ArrowWriterBuilder, ORC_TYPE_ATTRIBUTE_PREFIX};
use crate::proto::{self, CalendarKind};
use crate::reader::metadata::read_metadata;
use crate::ArrowReaderBuilder;

struct CountWrites(Arc<AtomicUsize>);

impl Write for CountWrites {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn rejects_unsupported_later_field_before_any_output() {
    for data_type in [
        DataType::Null,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Decimal128(12, -2),
        DataType::Decimal256(40, 2),
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), 2),
        DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
        DataType::Timestamp(TimeUnit::Microsecond, Some("Etc/UTC".into())),
        DataType::Time64(TimeUnit::Nanosecond),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let schema = Arc::new(Schema::new(vec![
            Field::new("valid", DataType::Int64, true),
            Field::new("rejected", data_type.clone(), true),
        ]));
        let error = ArrowWriterBuilder::new(CountWrites(calls.clone()), schema)
            .try_build()
            .err()
            .expect("unsupported field must return an error");
        assert!(error.to_string().contains("rejected"));
        assert!(error.to_string().contains(&format!("{data_type:?}")));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn rejects_an_unsupported_type_inside_a_nested_field_before_any_output() {
    let unsupported = Field::new("rejected", DataType::UInt8, true);
    let entries = DataType::Struct(
        vec![
            Field::new("key", DataType::Utf8, false),
            unsupported.clone(),
        ]
        .into(),
    );
    for data_type in [
        DataType::List(Arc::new(unsupported.clone())),
        DataType::LargeList(Arc::new(unsupported.clone())),
        DataType::Struct(vec![unsupported.clone()].into()),
        DataType::Map(Arc::new(Field::new("entries", entries, false)), false),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let schema = Arc::new(Schema::new(vec![Field::new("outer", data_type, true)]));
        let error = ArrowWriterBuilder::new(CountWrites(calls.clone()), schema)
            .try_build()
            .err()
            .expect("unsupported field must return an error");
        assert!(error.to_string().contains("rejected"), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn rejects_zero_batch_size_before_any_output() {
    let calls = Arc::new(AtomicUsize::new(0));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let error = ArrowWriterBuilder::new(CountWrites(calls.clone()), schema)
        .with_batch_size(0)
        .try_build()
        .err()
        .expect("zero batch size must return an error");
    assert!(error.to_string().contains("batch size"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn admitted_physical_types_can_finish_an_empty_file() {
    let mut types = vec![
        DataType::Float32,
        DataType::Float64,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Date32,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Boolean,
        DataType::Decimal128(38, 38),
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
        DataType::Struct(vec![Field::new("child", DataType::Utf8, true)].into()),
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Timestamp(unit, None));
        types.push(DataType::Timestamp(unit, Some("UTC".into())));
    }
    for data_type in types {
        let schema = Arc::new(Schema::new(vec![Field::new("value", data_type, true)]));
        let mut bytes = vec![];
        ArrowWriterBuilder::new(&mut bytes, schema)
            .try_build()
            .unwrap()
            .close()
            .unwrap();
        let batches = ArrowReaderBuilder::try_new(Bytes::from(bytes))
            .unwrap()
            .build()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }
}

#[test]
fn admitted_nullable_batches_survive_multiple_stripes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("flag", DataType::Boolean, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(7), None, Some(-9)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("alpha"), None, Some("beta")])),
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        ],
    )
    .unwrap();
    let mut bytes = vec![];
    let mut writer = ArrowWriterBuilder::new(&mut bytes, schema.clone())
        .with_batch_size(1)
        .with_stripe_byte_size(1)
        .try_build()
        .unwrap();
    writer.write(&batch).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let actual = ArrowReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .build()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    let actual = arrow::compute::concat_batches(&schema, actual.iter()).unwrap();
    let expected = arrow::compute::concat_batches(&schema, [&batch, &batch]).unwrap();
    assert_eq!(actual, expected);
}

/// One file, read back as a single batch under the writer's schema. Batches run
/// through a one row encoding unit and a one byte stripe, so that every value
/// crosses an encoder and a stripe boundary, and arrive split where asked.
fn round_trip_split(batch: &RecordBatch, split: usize) -> RecordBatch {
    let schema = batch.schema();
    let mut bytes = vec![];
    let mut writer = ArrowWriterBuilder::new(&mut bytes, schema.clone())
        .with_batch_size(1)
        .with_stripe_byte_size(1)
        .try_build()
        .unwrap();
    writer.write(&batch.slice(0, split)).unwrap();
    writer
        .write(&batch.slice(split, batch.num_rows() - split))
        .unwrap();
    writer.close().unwrap();
    let batches = ArrowReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .with_schema(schema.clone())
        .build()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    arrow::compute::concat_batches(&schema, batches.iter()).unwrap()
}

fn assert_round_trips(column: ArrayRef) {
    let batch = RecordBatch::try_from_iter([("value", column)]).unwrap();
    for split in 0..=batch.num_rows() {
        assert_eq!(round_trip_split(&batch, split), batch, "split at {split}");
    }
}

fn decimals(values: Vec<Option<i128>>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

#[test]
fn decimals_survive_at_their_limits() {
    let max = 10_i128.pow(38) - 1;
    assert_round_trips(decimals(
        vec![Some(max), Some(-max), None, Some(0), Some(1), Some(-1)],
        38,
        38,
    ));
    assert_round_trips(decimals(vec![Some(9), Some(-9), None], 1, 0));
    // The scale is declared in the footer and also stored with each value.
    assert_round_trips(decimals(vec![Some(1_230), None, Some(-5)], 10, 2));
}

fn int_list(rows: Vec<Option<Vec<Option<i32>>>>) -> ListArray {
    ListArray::from_iter_primitive::<Int32Type, _, _>(rows)
}

#[test]
fn lists_keep_null_and_empty_rows_apart() {
    let list = int_list(vec![
        Some(vec![Some(1), None, Some(3)]),
        None,
        Some(vec![]),
        Some(vec![None]),
        None,
    ]);
    assert_round_trips(Arc::new(list));
}

#[test]
fn a_column_with_no_child_values_is_readable() {
    assert_round_trips(Arc::new(int_list(vec![None, Some(vec![]), None])));
}

#[test]
fn sliced_lists_write_only_their_own_child_rows() {
    let list = int_list(vec![
        Some(vec![Some(1)]),
        Some(vec![Some(2), Some(3)]),
        None,
        Some(vec![Some(4)]),
        Some(vec![Some(5)]),
    ]);
    let expected = int_list(vec![
        Some(vec![Some(2), Some(3)]),
        None,
        Some(vec![Some(4)]),
    ]);
    let batch = RecordBatch::try_from_iter([("value", Arc::new(list.slice(1, 3)) as ArrayRef)]);
    let batch = batch.unwrap();
    let actual = round_trip_split(&batch, 1);
    assert_eq!(actual.column(0).as_ref(), &expected as &dyn Array);
}

#[test]
fn a_null_list_with_child_rows_stores_none_of_them() {
    // Valid in Arrow, and without care would shift all the later lists' values.
    let field = Arc::new(Field::new("item", DataType::Int32, true));
    let list = ListArray::new(
        field,
        OffsetBuffer::new(vec![0, 1, 3, 4].into()),
        Arc::new(Int32Array::from(vec![1, 99, 98, 2])),
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let expected = int_list(vec![Some(vec![Some(1)]), None, Some(vec![Some(2)])]);
    let batch = RecordBatch::try_from_iter([("value", Arc::new(list) as ArrayRef)]).unwrap();
    let actual = round_trip_split(&batch, 1);
    assert_eq!(actual.column(0).as_ref(), &expected as &dyn Array);
}

fn string_int_map(rows: &[Option<&[(&str, Option<i32>)]>]) -> MapArray {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    for row in rows {
        if let Some(entries) = row {
            for (key, value) in *entries {
                builder.keys().append_value(key);
                builder.values().append_option(*value);
            }
        }
        builder.append(row.is_some()).unwrap();
    }
    builder.finish()
}

#[test]
fn maps_keep_null_values_null_rows_and_empty_rows_apart() {
    let map = string_int_map(&[
        Some(&[("a", Some(1)), ("b", None)]),
        None,
        Some(&[]),
        Some(&[("c", Some(3))]),
    ]);
    assert_round_trips(Arc::new(map));
}

#[test]
fn a_sliced_map_writes_only_its_own_entries() {
    let map = string_int_map(&[
        Some(&[("a", Some(1))]),
        Some(&[("b", Some(2)), ("c", None)]),
        None,
        Some(&[("d", Some(4))]),
    ]);
    let expected = string_int_map(&[Some(&[("b", Some(2)), ("c", None)]), None]);
    let batch = RecordBatch::try_from_iter([("value", Arc::new(map.slice(1, 2)) as ArrayRef)]);
    let actual = round_trip_split(&batch.unwrap(), 1);
    assert_eq!(actual.column(0).as_ref(), &expected as &dyn Array);
}

fn struct_of_int_and_string(
    x: Vec<Option<i32>>,
    y: Vec<Option<&str>>,
    nulls: Option<NullBuffer>,
) -> StructArray {
    StructArray::new(
        vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Utf8, true),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(x)),
            Arc::new(StringArray::from(y)),
        ],
        nulls,
    )
}

#[test]
fn a_null_struct_differs_from_a_struct_of_nulls() {
    let array = struct_of_int_and_string(
        vec![Some(1), None, None, Some(4)],
        vec![Some("a"), None, None, None],
        Some(NullBuffer::from(vec![true, true, false, true])),
    );
    assert_round_trips(Arc::new(array));
}

#[test]
fn a_null_struct_stores_none_of_its_child_values() {
    // The value under the null row must not shift the row after it.
    let array = struct_of_int_and_string(
        vec![Some(1), Some(99), Some(3)],
        vec![Some("a"), Some("hidden"), Some("c")],
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let expected = struct_of_int_and_string(
        vec![Some(1), None, Some(3)],
        vec![Some("a"), None, Some("c")],
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let batch = RecordBatch::try_from_iter([("value", Arc::new(array) as ArrayRef)]).unwrap();
    let actual = round_trip_split(&batch, 1);
    assert_eq!(actual.column(0).as_ref(), &expected as &dyn Array);
}

#[test]
fn deep_nesting_assigns_column_ids_in_preorder() {
    let inner = struct_of_int_and_string(
        vec![Some(1), Some(2), None],
        vec![Some("a"), None, Some("c")],
        None,
    );
    let field = Arc::new(Field::new("item", inner.data_type().clone(), true));
    let list = ListArray::new(
        field,
        OffsetBuffer::new(vec![0, 2, 2, 3].into()),
        Arc::new(inner),
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let after = Int32Array::from(vec![7, 8, 9]);
    let batch = RecordBatch::try_from_iter([
        ("nested", Arc::new(list) as ArrayRef),
        ("after", Arc::new(after) as ArrayRef),
    ])
    .unwrap();
    assert_eq!(round_trip_split(&batch, 1), batch);
}

#[test]
fn a_timestamp_inside_a_container_sets_the_writer_timezone() {
    let values = TimestampMicrosecondArray::from(vec![Some(1_000_000), None]);
    let field = Arc::new(Field::new("item", values.data_type().clone(), true));
    let list = ListArray::new(
        field,
        OffsetBuffer::new(vec![0, 2].into()),
        Arc::new(values),
        None,
    );
    assert_round_trips(Arc::new(list));
}

#[test]
fn field_attributes_and_the_calendar_reach_the_footer() {
    let attribute = format!("{ORC_TYPE_ATTRIBUTE_PREFIX}example.key");
    let annotated = Field::new("annotated", DataType::Int64, true)
        .with_metadata([(attribute, "value".to_string())].into());
    let plain = Field::new("plain", DataType::Int64, true)
        .with_metadata([("other".to_string(), "dropped".to_string())].into());
    let child = Field::new("child", DataType::Struct(vec![annotated].into()), true);
    let schema = Arc::new(Schema::new(vec![child, plain]));
    let mut bytes = vec![];
    ArrowWriterBuilder::new(&mut bytes, schema)
        .with_calendar(CalendarKind::ProlepticGregorian)
        .try_build()
        .unwrap()
        .close()
        .unwrap();
    let file = Bytes::from(bytes);
    let metadata = read_metadata(&mut file.clone()).unwrap();
    let types = metadata.column_types();
    assert_eq!(types[0].subtypes, vec![1, 3]);
    assert_eq!(types[1].subtypes, vec![2]);
    assert_eq!(types[2].attributes[0].key.as_deref(), Some("example.key"));
    assert_eq!(types[2].attributes[0].value.as_deref(), Some("value"));
    assert!(types[3].attributes.is_empty());
    let postscript_length = file[file.len() - 1] as usize;
    let postscript_start = file.len() - 1 - postscript_length;
    let postscript = proto::PostScript::decode(&file[postscript_start..file.len() - 1]).unwrap();
    let footer_start = postscript_start - postscript.footer_length() as usize;
    let footer = proto::Footer::decode(&file[footer_start..postscript_start]).unwrap();
    assert_eq!(footer.calendar(), CalendarKind::ProlepticGregorian);
}
