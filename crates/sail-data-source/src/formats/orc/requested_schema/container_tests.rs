#![expect(clippy::unwrap_used)]

use arrow::array::{
    Array, DurationMicrosecondArray, IntervalYearMonthArray, ListArray, MapArray,
    Time64NanosecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};

use super::*;

fn map_type(key: DataType, value: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", key, false)),
                    field("value", value),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

#[test]
fn map_key_narrowing_is_rejected_during_mapping_including_nested_key_children() {
    let nested = |data_type| {
        DataType::Struct(vec![field("xs", DataType::List(field("item", data_type)))].into())
    };
    for (source_key, target_key) in [
        (DataType::Int64, DataType::Int32),
        (nested(DataType::Int64), nested(DataType::Int32)),
        (
            map_type(DataType::Utf8, DataType::Int64),
            map_type(DataType::Utf8, DataType::Int32),
        ),
    ] {
        let file = schema(vec![field("m", map_type(source_key, DataType::Utf8))]);
        let requested = schema(vec![field("m", map_type(target_key, DataType::Utf8))]);
        let error = OrcRequestedSchemaAdapter::try_new(file, requested, &[0], true).unwrap_err();
        assert!(matches!(
            error,
            datafusion_common::DataFusionError::NotImplemented(_)
        ));
        assert!(error.to_string().contains("inside MAP KEYS"));
        assert!(error.to_string().contains("not yet qualified"));
    }
}

#[test]
fn sliced_map_keeps_identity_and_widening_keys_while_values_use_safe_narrowing() {
    let source_type = map_type(DataType::Int32, DataType::Int64);
    let DataType::Map(entries_field, _) = &source_type else {
        unreachable!()
    };
    let DataType::Struct(fields) = entries_field.data_type() else {
        unreachable!()
    };
    let map = MapArray::try_new(
        entries_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 4])),
        StructArray::try_new(
            fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![99, 98, 1, i32::MAX])),
                Arc::new(Int64Array::from(vec![i64::MAX, i64::MAX, 7, i64::MAX])),
            ],
            None,
        )
        .unwrap(),
        Some(NullBuffer::from(vec![true, false, true])),
        false,
    )
    .unwrap()
    .slice(1, 2);
    let file = schema(vec![field("m", source_type)]);
    for key_type in [DataType::Int32, DataType::Int64] {
        let requested = schema(vec![field(
            "m",
            map_type(key_type.clone(), DataType::Int32),
        )]);
        let adapter =
            OrcRequestedSchemaAdapter::try_new(file.clone(), requested, &[0], true).unwrap();
        let output = adapter
            .adapt_batch(RecordBatch::try_new(file.clone(), vec![Arc::new(map.clone())]).unwrap())
            .unwrap();
        let got = output
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        assert_eq!(got.offsets(), map.offsets());
        assert_eq!(got.nulls(), map.nulls());
        assert_eq!(got.keys().null_count(), 0);
        match key_type {
            DataType::Int32 => assert!(Arc::ptr_eq(got.keys(), map.keys())),
            DataType::Int64 => assert_eq!(
                values::<Int64Type>(got.keys()),
                [Some(99), Some(98), Some(1), Some(i64::from(i32::MAX))]
            ),
            _ => unreachable!(),
        }
        let entries = got.value(1);
        assert_eq!(values::<Int32Type>(entries.column(1)), [Some(7), None]);
    }
}

#[test]
fn nested_struct_projection_preserves_slices_parent_nulls_and_disjoint_fields() {
    let children: Fields =
        vec![field("Bar", DataType::Int32), field("bar", DataType::Int32)].into();
    let data = StructArray::try_new(
        children.clone(),
        vec![
            Arc::new(Int32Array::from(vec![11, 12, 13])),
            Arc::new(Int32Array::from(vec![21, 22, 23])),
        ],
        Some(NullBuffer::from(vec![true, false, true])),
    )
    .unwrap()
    .slice(1, 2);
    let file = schema(vec![field("st", DataType::Struct(children))]);
    for targets in [
        vec![
            field("BAR", DataType::Int32),
            field("missing", DataType::Boolean),
        ],
        vec![field("missing", DataType::Boolean)],
        vec![],
    ] {
        let requested = schema(vec![field("st", DataType::Struct(targets.clone().into()))]);
        let adapter =
            OrcRequestedSchemaAdapter::try_new(file.clone(), requested, &[0], false).unwrap();
        let output = adapter
            .adapt_batch(RecordBatch::try_new(file.clone(), vec![Arc::new(data.clone())]).unwrap())
            .unwrap();
        let data = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(data.len(), 2);
        assert!(data.is_null(0) && data.is_valid(1));
        if targets.len() == 2 {
            assert_eq!(
                data.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(1),
                23
            );
        }
        if !targets.is_empty() {
            assert_eq!(data.column(targets.len() - 1).null_count(), 2);
        }
    }
}

#[test]
fn time_and_interval_requested_metadata_does_not_truncate_storage() {
    use spec::IntervalFieldType::{Day, Month, Second, Year};
    let file = OrcRequestedSchemaAdapter::nullable_schema(&schema(vec![
        time("t", 0),
        interval("ym", spec::IntervalUnit::YearMonth, Year, Month),
        interval("dt", spec::IntervalUnit::DayTime, Day, Second),
    ]));
    let requested = schema(vec![
        time("t", 3),
        interval("ym", spec::IntervalUnit::YearMonth, Year, Year),
        interval("dt", spec::IntervalUnit::DayTime, Day, Day),
    ]);
    let adapter =
        OrcRequestedSchemaAdapter::try_new(file.clone(), requested, &[0, 1, 2], true).unwrap();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Time64NanosecondArray::from(vec![
            Some(45_296_123_456_000),
            Some(86_399_999_999_000),
            None,
        ])),
        Arc::new(IntervalYearMonthArray::from(vec![
            Some(14),
            Some(-14),
            None,
        ])),
        Arc::new(DurationMicrosecondArray::from(vec![
            Some(183_845_123_456),
            Some(-183_845_123_456),
            None,
        ])),
    ];
    let output = adapter
        .adapt_batch(RecordBatch::try_new(file, columns.clone()).unwrap())
        .unwrap();
    for (source, target) in columns.iter().zip(output.columns()) {
        assert!(Arc::ptr_eq(source, target));
    }
    assert_eq!(
        output
            .schema()
            .field(0)
            .metadata()
            .get(spec::SAIL_SPARK_TIME_METADATA_KEY),
        time("t", 3)
            .metadata()
            .get(spec::SAIL_SPARK_TIME_METADATA_KEY)
    );
    assert_eq!(
        output
            .schema()
            .field(1)
            .metadata()
            .get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY),
        interval("ym", spec::IntervalUnit::YearMonth, Year, Year)
            .metadata()
            .get(spec::SAIL_SPARK_INTERVAL_METADATA_KEY)
    );
}

#[test]
fn list_and_map_slices_keep_offsets_nulls_and_semantic_metadata() {
    let element = time("item", 0);
    let list = ListArray::try_new(
        element.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 1, 3])),
        Arc::new(Time64NanosecondArray::from(vec![
            1,
            45_296_123_456_000,
            86_399_999_999_000,
        ])),
        Some(NullBuffer::from(vec![true, false, true])),
    )
    .unwrap()
    .slice(1, 2);
    let source_entries: Fields = vec![
        Arc::new(Field::new("native_keys", DataType::Utf8, false)),
        time("native_values", 0),
    ]
    .into();
    let entries = StructArray::try_new(
        source_entries.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Time64NanosecondArray::from(vec![
                1,
                45_296_123_456_000,
                86_399_999_999_000,
            ])),
        ],
        None,
    )
    .unwrap();
    let source_entry = Arc::new(Field::new(
        "native_entries",
        DataType::Struct(source_entries),
        false,
    ));
    let map = MapArray::try_new(
        source_entry.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 1, 3])),
        entries,
        Some(NullBuffer::from(vec![true, false, true])),
        false,
    )
    .unwrap()
    .slice(1, 2);
    let file = schema(vec![
        field("xs", DataType::List(element)),
        field("m", DataType::Map(source_entry, false)),
    ]);
    let target_entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(vec![field("key", DataType::Utf8), time("value", 3)].into()),
        true,
    ));
    let requested = schema(vec![
        field("xs", DataType::List(time("element", 6))),
        field("m", DataType::Map(target_entries, false)),
    ]);
    let adapter =
        OrcRequestedSchemaAdapter::try_new(file.clone(), requested, &[0, 1], true).unwrap();
    let output = adapter
        .adapt_batch(
            RecordBatch::try_new(file, vec![Arc::new(list.clone()), Arc::new(map.clone())])
                .unwrap(),
        )
        .unwrap();
    let got = output
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(got.offsets(), list.offsets());
    assert_eq!(got.nulls(), list.nulls());
    assert_eq!(
        values::<Time64NanosecondType>(got.values()),
        [Some(1), Some(45_296_123_456_000), Some(86_399_999_999_000)]
    );
    let got = output
        .column(1)
        .as_any()
        .downcast_ref::<MapArray>()
        .unwrap();
    assert_eq!(got.offsets(), map.offsets());
    assert_eq!(got.nulls(), map.nulls());
    assert_eq!(got.entries().fields()[0].name(), "key");
    assert!(!got.entries().fields()[0].is_nullable());
    assert_eq!(
        values::<Time64NanosecondType>(got.values()),
        [Some(1), Some(45_296_123_456_000), Some(86_399_999_999_000)]
    );
    assert_eq!(
        got.entries().fields()[1].metadata(),
        time("value", 3).metadata()
    );
}
