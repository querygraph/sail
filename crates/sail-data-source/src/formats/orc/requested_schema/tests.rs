#![expect(clippy::unwrap_used)]

use std::collections::HashMap;

use arrow::array::{ArrayRef, Int32Array, Int64Array, PrimitiveArray, StringArray, StructArray};
use arrow::datatypes::{ArrowPrimitiveType, Int8Type, Int32Type, Int64Type, Time64NanosecondType};
use bytes::Bytes;
use orc_rust::reader::metadata::{MetadataReadOptions, read_metadata_with_options};

use super::*;
use crate::formats::orc::{OrcJavaTimeZoneConfig, OrcSchemaBridge, infer_orc_schema};

fn field(name: &str, data_type: DataType) -> FieldRef {
    Arc::new(Field::new(name, data_type, true))
}

fn schema(fields: Vec<FieldRef>) -> SchemaRef {
    Arc::new(Schema::new(fields))
}

fn values<T: ArrowPrimitiveType>(array: &ArrayRef) -> Vec<Option<T::Native>> {
    array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .iter()
        .collect()
}

fn time(name: &str, precision: i32) -> FieldRef {
    Arc::new(
        Field::new(name, DataType::Time64(TimeUnit::Nanosecond), false).with_metadata(
            HashMap::from([(
                spec::SAIL_SPARK_TIME_METADATA_KEY.to_string(),
                spec::SparkTimeMetadata::try_new(precision)
                    .unwrap()
                    .to_json()
                    .unwrap(),
            )]),
        ),
    )
}

fn interval(
    name: &str,
    unit: spec::IntervalUnit,
    start: spec::IntervalFieldType,
    end: spec::IntervalFieldType,
) -> FieldRef {
    let data_type = match unit {
        spec::IntervalUnit::YearMonth => DataType::Interval(IntervalUnit::YearMonth),
        _ => DataType::Duration(TimeUnit::Microsecond),
    };
    Arc::new(
        Field::new(name, data_type, true).with_metadata(HashMap::from([(
            spec::SAIL_SPARK_INTERVAL_METADATA_KEY.to_string(),
            spec::SparkIntervalMetadata::try_new(unit, Some(start), Some(end))
                .unwrap()
                .unwrap()
                .to_json()
                .unwrap(),
        )])),
    )
}

#[test]
fn full_timestamp_check_precedes_projection_and_name_matching() {
    let ltz = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let ntz = DataType::Timestamp(TimeUnit::Microsecond, None);
    let file = schema(vec![field("ltz", ltz.clone()), field("ntz", ntz.clone())]);
    let selected =
        OrcRequestedSchemaAdapter::try_new(file.clone(), file.clone(), &[1], true).unwrap();
    assert_eq!(selected.file_projection(), &[1]);
    for required in [vec![], vec![0]] {
        let only_second = schema(vec![field("ntz", ntz.clone())]);
        let error = OrcRequestedSchemaAdapter::try_new(file.clone(), only_second, &required, true)
            .unwrap_err();
        assert!(error.to_string().contains("TIMESTAMP to TIMESTAMP_NTZ"));
    }
    let reordered = schema(vec![field("ntz", ntz.clone()), field("ltz", ltz.clone())]);
    assert!(OrcRequestedSchemaAdapter::try_new(file.clone(), reordered, &[1], true).is_err());
    let file_struct = schema(vec![field("st", DataType::Struct(file.fields().clone()))]);
    let bad_struct = schema(vec![field(
        "st",
        DataType::Struct(vec![field("ltz", ntz.clone()), field("ntz", ltz.clone())].into()),
    )]);
    assert!(OrcRequestedSchemaAdapter::try_new(file_struct, bad_struct, &[], true).is_err());
    // Spark's full-schema timestamp check does not recurse through arrays. A
    // selected cross-kind array conversion remains explicitly unqualified.
    let file_list = schema(vec![field("xs", DataType::List(field("item", ltz)))]);
    let requested_list = schema(vec![field("xs", DataType::List(field("item", ntz)))]);
    assert!(
        OrcRequestedSchemaAdapter::try_new(file_list.clone(), requested_list.clone(), &[], true)
            .is_ok()
    );
    let error =
        OrcRequestedSchemaAdapter::try_new(file_list, requested_list, &[0], true).unwrap_err();
    assert!(error.to_string().contains("not yet qualified"));
}

#[test]
fn projection_preserves_requested_names_duplicates_missing_values_and_safe_narrowing() {
    let file = schema(vec![
        field("Bar", DataType::Int64),
        field("Foo", DataType::Int32),
    ]);
    let requested = schema(vec![
        Arc::new(
            Field::new("foo", DataType::Int64, false)
                .with_metadata(HashMap::from([("user".into(), "retained".into())])),
        ),
        field("missing", DataType::Utf8),
        field("bar", DataType::Int8),
    ]);
    let adapter =
        OrcRequestedSchemaAdapter::try_new(file.clone(), requested, &[2, 0, 2, 1], false).unwrap();
    assert_eq!(adapter.file_projection(), &[0, 1]);
    let input = RecordBatch::try_new(
        file,
        vec![
            Arc::new(Int64Array::from(vec![Some(128), Some(-128), None])),
            Arc::new(Int32Array::from(vec![7, 8, 9])),
        ],
    )
    .unwrap();
    let output = adapter.adapt_batch(input).unwrap();
    assert_eq!(
        output
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["bar", "foo", "bar", "missing"]
    );
    assert!(output.schema().fields().iter().all(|f| f.is_nullable()));
    assert_eq!(
        output.schema().field(1).metadata().get("user").unwrap(),
        "retained"
    );
    assert_eq!(
        values::<Int8Type>(output.column(0)),
        [None, Some(-128), None]
    );
    assert_eq!(
        values::<Int64Type>(output.column(1)),
        [Some(7), Some(8), Some(9)]
    );
    assert_eq!(
        values::<Int8Type>(output.column(2)),
        [None, Some(-128), None]
    );
    assert_eq!(output.column(3).null_count(), 3);
}

#[test]
fn missing_columns_and_zero_projection_retain_row_counts() {
    let file = schema(vec![field("present", DataType::Int32)]);
    let requested = schema(vec![Arc::new(Field::new("absent", DataType::Int32, false))]);
    for required in [vec![], vec![0]] {
        let adapter =
            OrcRequestedSchemaAdapter::try_new(file.clone(), requested.clone(), &required, true)
                .unwrap();
        assert!(adapter.file_projection().is_empty());
        for rows in [0, 3] {
            let batch = RecordBatch::try_new_with_options(
                adapter.input_schema().clone(),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap();
            let output = adapter.adapt_batch(batch).unwrap();
            assert_eq!(
                (output.num_columns(), output.num_rows()),
                (required.len(), rows)
            );
            if !required.is_empty() {
                assert_eq!(output.column(0).null_count(), rows);
            }
        }
    }
}

#[test]
fn top_level_ambiguity_is_required_only_and_nested_matching_is_last_wins() {
    let file = schema(vec![
        field("Foo", DataType::Int32),
        field("foo", DataType::Int32),
        field("ok", DataType::Int32),
    ]);
    let requested = schema(vec![
        field("foo", DataType::Int32),
        field("ok", DataType::Int32),
    ]);
    assert!(
        OrcRequestedSchemaAdapter::try_new(file.clone(), requested.clone(), &[1], false).is_ok()
    );
    assert!(
        OrcRequestedSchemaAdapter::try_new(file, requested, &[0], false)
            .unwrap_err()
            .to_string()
            .contains("duplicate field")
    );
    for (names, input, name, expected) in [
        (["Bar", "bar"], [11, 22], "BAR", 22),
        (["Bar", "bar"], [11, 22], "Bar", 22),
        (["bar", "Bar"], [22, 11], "bar", 11),
        (["bar", "Bar"], [22, 11], "BAR", 11),
    ] {
        let fields: Fields = names.map(|n| field(n, DataType::Int32)).into();
        let data = StructArray::try_new(
            fields.clone(),
            input
                .map(|v| Arc::new(Int32Array::from(vec![v])) as ArrayRef)
                .into(),
            None,
        )
        .unwrap();
        let file = schema(vec![field("st", DataType::Struct(fields))]);
        let requested = schema(vec![field(
            "st",
            DataType::Struct(vec![field(name, DataType::Int32)].into()),
        )]);
        let adapter =
            OrcRequestedSchemaAdapter::try_new(file.clone(), requested, &[0], false).unwrap();
        let output = adapter
            .adapt_batch(RecordBatch::try_new(file, vec![Arc::new(data)]).unwrap())
            .unwrap();
        let data = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(values::<Int32Type>(data.column(0)), [Some(expected)]);
        assert_eq!(data.fields()[0].name(), name);
    }
}

#[test]
fn case_sensitive_exact_duplicate_top_level_names_use_last_physical_field() {
    let file = schema(vec![
        field("dup", DataType::Int32),
        field("dup", DataType::Int32),
    ]);
    let requested = schema(vec![field("dup", DataType::Int32)]);
    let adapter =
        OrcRequestedSchemaAdapter::try_new(file.clone(), requested.clone(), &[0], true).unwrap();
    assert_eq!(adapter.file_projection(), &[1]);
    let input = RecordBatch::try_new(
        file.clone(),
        vec![
            Arc::new(Int32Array::from(vec![11])),
            Arc::new(Int32Array::from(vec![22])),
        ],
    )
    .unwrap();
    let output = adapter
        .adapt_batch(input.project(adapter.file_projection()).unwrap())
        .unwrap();
    assert_eq!(values::<Int32Type>(output.column(0)), [Some(22)]);
    assert!(
        OrcRequestedSchemaAdapter::try_new(file, requested, &[0], false)
            .unwrap_err()
            .to_string()
            .contains("duplicate field")
    );
}

#[test]
fn hive_fallback_uses_literal_prefix_full_schema_width_and_requested_ordinals() {
    let file = schema(vec![
        field("_column", DataType::Int32),
        field("_col1", DataType::Utf8),
    ]);
    let requested = schema(vec![
        field("renamed", DataType::Int64),
        field("second", DataType::Utf8),
        field("missing", DataType::Int32),
    ]);
    let adapter =
        OrcRequestedSchemaAdapter::try_new(file.clone(), requested, &[2, 1], true).unwrap();
    assert_eq!(adapter.file_projection(), &[1]);
    let output = adapter
        .adapt_batch(
            RecordBatch::try_new(
                adapter.input_schema().clone(),
                vec![Arc::new(StringArray::from(vec![Some("eleven"), None]))],
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(output.column(0).null_count(), 2);
    assert_eq!(
        output
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some("eleven"), None]
    );
    let error = OrcRequestedSchemaAdapter::try_new(
        file,
        schema(vec![field("second", DataType::Utf8)]),
        &[],
        true,
    )
    .unwrap_err();
    assert!(error.to_string().contains("requires the full data schema"));
}

#[test]
fn invalid_projections_input_and_unqualified_conversions_are_explicit() {
    let file = schema(vec![field("n", DataType::Int32)]);
    assert!(OrcRequestedSchemaAdapter::try_new(file.clone(), file.clone(), &[1], true).is_err());
    let adapter =
        OrcRequestedSchemaAdapter::try_new(file.clone(), file.clone(), &[0], true).unwrap();
    let wrong = RecordBatch::try_new(
        schema(vec![field("other", DataType::Int32)]),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();
    assert!(adapter.adapt_batch(wrong).is_err());
    for (from, to) in [
        (DataType::Utf8, DataType::Int32),
        (DataType::Decimal128(10, 2), DataType::Decimal128(8, 2)),
    ] {
        let error = OrcRequestedSchemaAdapter::try_new(
            schema(vec![field("x", from)]),
            schema(vec![field("x", to)]),
            &[0],
            true,
        )
        .unwrap_err();
        assert!(error.to_string().contains("not yet qualified"));
    }
    let unicode = schema(vec![field("é", DataType::Int32)]);
    assert!(
        OrcRequestedSchemaAdapter::try_new(unicode.clone(), unicode.clone(), &[0], true).is_ok()
    );
    assert!(OrcRequestedSchemaAdapter::try_new(unicode.clone(), unicode, &[0], false).is_err());
    let wrong_time = schema(vec![Arc::new(
        time("n", 3)
            .as_ref()
            .clone()
            .with_data_type(DataType::Int32),
    )]);
    assert!(OrcRequestedSchemaAdapter::try_new(file, wrong_time, &[0], true).is_err());
    let bad_map = schema(vec![field(
        "missing",
        DataType::Map(field("entries", DataType::Int32), false),
    )]);
    assert!(OrcRequestedSchemaAdapter::try_new(schema(vec![]), bad_map, &[0], true).is_err());
}

#[test]
fn real_spark_fractional_time_file_is_reinterpreted_after_native_bridge() {
    let mut bytes = Bytes::from_static(include_bytes!(
        "../../../../tests/fixtures/orc/time_precisions.orc"
    ));
    let metadata = Arc::new(
        read_metadata_with_options(
            &mut bytes,
            MetadataReadOptions {
                max_encoded_metadata_bytes: Some(64 * 1024),
                max_decoded_metadata_bytes: Some(64 * 1024),
                max_schema_depth: Some(16),
            },
        )
        .unwrap(),
    );
    let file = infer_orc_schema(&metadata, "UTC").unwrap();
    let mut fields = file.fields().to_vec();
    fields[1] = time("t0", 3);
    let adapter = OrcRequestedSchemaAdapter::try_new(file, schema(fields), &[1], true).unwrap();
    let (builder, bridge) = OrcSchemaBridge::try_new_reader(
        bytes,
        metadata,
        "UTC",
        &OrcJavaTimeZoneConfig::for_reader_timezone("UTC".into()).unwrap(),
        Some(adapter.file_projection()),
    )
    .unwrap();
    let mut actual = Vec::new();
    for batch in builder.with_batch_size(1).build() {
        let output = adapter
            .adapt_batch(bridge.adapt_batch(batch.unwrap()).unwrap())
            .unwrap();
        assert_eq!(output.schema().fields(), adapter.schema().fields());
        actual.extend(values::<Time64NanosecondType>(output.column(0)));
    }
    assert_eq!(
        actual,
        [
            Some(0),
            Some(45_296_123_456_000),
            Some(86_399_999_999_000),
            None
        ]
    );
}

#[path = "container_tests.rs"]
mod container_tests;
