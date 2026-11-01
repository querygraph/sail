use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{BinaryArray, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{IntervalUnit, Schema};
use datafusion_common::DataFusionError;
use orc_rust::{ArrowReaderBuilder, ArrowWriterBuilder};
use sail_common_datafusion::variant::variant_metadata_field;

use super::*;

fn with_metadata(data_type: DataType, key: &str, value: &str) -> Field {
    Field::new("logical_column", data_type, true)
        .with_metadata(HashMap::from([(key.into(), value.into())]))
}

fn admit_later_field(field: Field) -> Result<OrcWriteSchema> {
    OrcWriteSchema::try_new(Arc::new(Schema::new(vec![
        Field::new("valid_first", DataType::Int64, false),
        field,
    ])))
}

#[test]
fn logical_metadata_cannot_disappear_into_supported_storage_types() {
    for key in [
        spec::SAIL_SPARK_TIME_METADATA_KEY,
        spec::SAIL_SPARK_INTERVAL_METADATA_KEY,
        spec::SAIL_SPARK_UDT_METADATA_KEY,
        "SAIL::spark::future_logical_type",
        "ARROW:extension:name",
        "ARROW:extension:metadata",
        "ARROW:extension:future_property",
    ] {
        for value in ["", "{}", "unknown.extension"] {
            for data_type in [DataType::Int64, DataType::Utf8, DataType::Binary] {
                let result = admit_later_field(with_metadata(data_type, key, value));
                assert!(
                    matches!(result, Err(DataFusionError::NotImplemented(message))
                    if message.contains("logical_column") && message.contains(key))
                );
            }
        }
    }
}

#[test]
fn spark_character_and_collation_annotations_are_not_plain_strings() {
    for metadata in [
        serde_json::json!({ (CHAR_VARCHAR_TYPE): "varchar(8)" }),
        serde_json::json!({ (CHAR_VARCHAR_TYPE): "char(3)" }),
        serde_json::json!({ "__COLLATIONS": { "logical_column": "spark.UTF8_LCASE" } }),
        serde_json::json!({ "__COLLATIONS": null }),
    ] {
        let result = admit_later_field(with_metadata(
            DataType::Utf8,
            spec::SPARK_METADATA_JSON_KEY,
            &metadata.to_string(),
        ));
        assert!(
            matches!(result, Err(DataFusionError::NotImplemented(message))
            if message.contains("logical_column") && message.contains("Spark type metadata"))
        );
    }
}

#[test]
fn malformed_spark_metadata_is_a_planning_error_with_field_context() {
    for json in ["", "invalid", "null", "[]", "1", "\"text\""] {
        let result = admit_later_field(with_metadata(
            DataType::Utf8,
            spec::SPARK_METADATA_JSON_KEY,
            json,
        ));
        assert!(matches!(result, Err(DataFusionError::Plan(message))
            if message.contains("logical_column") && message.contains("invalid Spark metadata")));
    }
}

fn item(data_type: DataType) -> Arc<Field> {
    Arc::new(Field::new("item", data_type, true))
}

fn map_of(key: DataType, value: DataType) -> DataType {
    let fields = vec![
        Field::new("key", key, false),
        Field::new("value", value, true),
    ];
    let entries = Field::new("entries", DataType::Struct(fields.into()), false);
    DataType::Map(Arc::new(entries), false)
}

#[test]
fn unsupported_types_are_rejected_before_encoding_at_any_depth() {
    for data_type in [
        DataType::Null,
        DataType::UInt8,
        DataType::UInt64,
        DataType::Float16,
        DataType::Decimal128(10, -2),
        DataType::Decimal256(40, 2),
        DataType::Date64,
        // Stored in microseconds, which would drop digits.
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Duration(TimeUnit::Microsecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::FixedSizeBinary(4),
        DataType::FixedSizeList(item(DataType::Int64), 2),
        DataType::Struct(Fields::empty()),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ] {
        for wrapped in [
            data_type.clone(),
            DataType::List(item(data_type.clone())),
            DataType::LargeList(item(data_type.clone())),
            DataType::Struct(vec![Field::new("inner", data_type.clone(), true)].into()),
            map_of(DataType::Utf8, data_type.clone()),
            map_of(data_type.clone(), DataType::Utf8),
        ] {
            let result = admit_later_field(Field::new("unsupported", wrapped, true));
            assert!(
                matches!(&result, Err(DataFusionError::NotImplemented(message))
                if message.contains(&format!("{data_type:?}"))),
                "{data_type:?}: {result:?}"
            );
        }
    }
    assert!(matches!(
        OrcWriteSchema::try_new(Arc::new(Schema::empty())),
        Err(DataFusionError::Plan(_))
    ));
}

#[test]
fn logical_metadata_cannot_disappear_inside_a_container() {
    let time = Field::new("inner", DataType::Int64, true).with_metadata(HashMap::from([(
        spec::SAIL_SPARK_INTERVAL_METADATA_KEY.into(),
        "{}".into(),
    )]));
    for data_type in [
        DataType::List(Arc::new(time.clone())),
        DataType::Struct(vec![time.clone()].into()),
        map_of(DataType::Utf8, DataType::Struct(vec![time.clone()].into())),
    ] {
        let result = admit_later_field(Field::new("outer", data_type, true));
        assert!(
            matches!(&result, Err(DataFusionError::NotImplemented(message))
            if message.contains("inner") && message.contains("logical metadata")),
            "{result:?}"
        );
    }
}

#[test]
fn variant_storage_is_refused_without_its_extension_at_any_depth() {
    let marked = variant_metadata_field(DataType::Binary, false);
    let storage =
        DataType::Struct(vec![Field::new("value", DataType::Binary, false), marked].into());
    for data_type in [
        storage.clone(),
        DataType::List(item(storage.clone())),
        map_of(DataType::Utf8, storage.clone()),
    ] {
        let result = admit_later_field(Field::new("outer", data_type, true));
        assert!(
            matches!(&result, Err(DataFusionError::NotImplemented(message))
            if message.contains("VARIANT")),
            "{result:?}"
        );
    }
}

#[test]
fn a_struct_shaped_like_a_variant_is_an_ordinary_struct() -> Result<()> {
    let data_type = DataType::Struct(
        vec![
            Field::new("value", DataType::Binary, false),
            Field::new("metadata", DataType::Binary, false),
        ]
        .into(),
    );
    assert_eq!(stored(data_type.clone())?.data_type(), &data_type);
    Ok(())
}

fn attribute(field: &Field) -> Option<&str> {
    let key = format!("{ORC_TYPE_ATTRIBUTE_PREFIX}{CATALYST_TYPE}");
    field.metadata().get(&key).map(String::as_str)
}

fn stored(data_type: DataType) -> Result<Field> {
    let schema = Arc::new(Schema::new(vec![Field::new("value", data_type, true)]));
    let admitted = OrcWriteSchema::try_new(schema)?;
    Ok(admitted.file_schema().field(0).clone())
}

#[test]
fn timestamps_are_stored_the_way_spark_stores_them() -> Result<()> {
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
    ] {
        let instant = stored(DataType::Timestamp(unit, Some("Asia/Tokyo".into())))?;
        assert_eq!(
            instant.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(attribute(&instant), Some("timestamp"));
        let wall_clock = stored(DataType::Timestamp(unit, None))?;
        assert_eq!(wall_clock.data_type(), &DataType::Int64);
        assert_eq!(attribute(&wall_clock), Some("timestamp_ntz"));
    }
    Ok(())
}

#[test]
fn storage_mapping_reaches_inside_containers() -> Result<()> {
    let ntz = DataType::Timestamp(TimeUnit::Microsecond, None);
    let list = stored(DataType::List(item(ntz.clone())))?;
    let DataType::List(child) = list.data_type() else {
        return plan_err!("expected a list: {list:?}");
    };
    assert_eq!(child.data_type(), &DataType::Int64);
    assert_eq!(attribute(child), Some("timestamp_ntz"));

    let map = stored(map_of(DataType::Utf8View, DataType::BinaryView))?;
    assert_eq!(
        map.data_type(),
        &map_of(DataType::Utf8, DataType::Binary),
        "views inside a map are stored plainly"
    );

    let structure = stored(DataType::Struct(vec![Field::new("t", ntz, true)].into()))?;
    let DataType::Struct(fields) = structure.data_type() else {
        return plan_err!("expected a struct: {structure:?}");
    };
    assert_eq!(attribute(&fields[0]), Some("timestamp_ntz"));
    Ok(())
}

#[test]
fn stored_decimals_keep_their_precision_and_scale() -> Result<()> {
    for data_type in [
        DataType::Decimal128(1, 0),
        DataType::Decimal128(1, 1),
        DataType::Decimal128(38, 0),
        DataType::Decimal128(38, 38),
    ] {
        assert_eq!(stored(data_type.clone())?.data_type(), &data_type);
    }
    Ok(())
}

#[test]
fn column_count_includes_every_nested_column() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::List(item(DataType::Int32)), true),
        Field::new("c", map_of(DataType::Utf8, DataType::Int32), true),
        Field::new(
            "d",
            DataType::Struct(vec![Field::new("x", DataType::Int32, true)].into()),
            true,
        ),
    ]));
    assert_eq!(column_count(schema.fields()), 8);
    Ok(())
}

#[test]
fn primitive_admission_preserves_original_schema_and_builds_native_writers() -> Result<()> {
    for data_type in [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
    ] {
        let field = Field::new("primitive", data_type, false).with_metadata(HashMap::from([
            (
                spec::SPARK_METADATA_JSON_KEY.into(),
                r#"{"comment":"retained annotation","custom":7}"#.into(),
            ),
            ("user.attribute".into(), "opaque".into()),
        ]));
        let original = Arc::new(Schema::new_with_metadata(
            vec![field],
            HashMap::from([("user.schema.attribute".into(), "opaque".into())]),
        ));
        let admitted = OrcWriteSchema::try_new(original.clone())?;
        assert!(Arc::ptr_eq(admitted.schema(), &original));
        let mut bytes = vec![];
        ArrowWriterBuilder::new(&mut bytes, admitted.schema().clone())
            .try_build()
            .map_err(|error| DataFusionError::External(Box::new(error)))?
            .close()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let reader = ArrowReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        assert_eq!(reader.build().count(), 0);
    }
    Ok(())
}

#[test]
fn admitted_nullable_primitives_round_trip_multiple_batches_in_one_file() -> Result<()> {
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(vec![Some(-7), None, Some(12)])) as arrow::array::ArrayRef,
        ),
        (
            "label",
            Arc::new(StringArray::from(vec![Some("é"), None, Some("")])),
        ),
        (
            "bytes",
            Arc::new(BinaryArray::from(vec![
                Some(b"\0\xff".as_slice()),
                None,
                Some(b"".as_slice()),
            ])),
        ),
        (
            "flag",
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        ),
    ])?;
    let admitted = OrcWriteSchema::try_new(batch.schema())?;
    let mut bytes = vec![];
    let mut writer = ArrowWriterBuilder::new(&mut bytes, admitted.schema().clone())
        .with_batch_size(1)
        .with_stripe_byte_size(1)
        .try_build()
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    for _ in 0..2 {
        writer
            .write(&batch)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
    }
    writer
        .close()
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let batches = ArrowReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .map_err(|error| DataFusionError::External(Box::new(error)))?
        .build()
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let actual = arrow::compute::concat_batches(&batch.schema(), &batches)?;
    let expected = arrow::compute::concat_batches(&batch.schema(), [&batch, &batch])?;
    assert_eq!(actual, expected);
    Ok(())
}
