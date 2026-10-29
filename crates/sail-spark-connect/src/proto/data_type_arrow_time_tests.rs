use std::collections::HashMap;
use std::sync::Arc;

use super::*;

fn time_field(name: &str, precision: i32, nullable: bool) -> SparkResult<adt::Field> {
    Ok(adt::Field::new(
        name,
        adt::DataType::Time64(adt::TimeUnit::Nanosecond),
        nullable,
    )
    .with_metadata(HashMap::from([(
        spec::SAIL_SPARK_TIME_METADATA_KEY.to_string(),
        spec::SparkTimeMetadata::try_new(precision)?.to_json()?,
    )])))
}

fn spark_time(precision: i32) -> DataType {
    DataType {
        kind: Some(sdt::Kind::Time(sdt::Time {
            precision: Some(precision),
            type_variation_reference: 0,
        })),
    }
}

#[test]
fn spark_time_metadata_controls_declared_precision_without_leaking() -> SparkResult<()> {
    for precision in 0..=6 {
        let field = time_field("clock", precision, false)?;
        let proto = sdt::StructField::try_from(field)?;
        assert_eq!(
            proto,
            sdt::StructField {
                name: "clock".to_string(),
                data_type: Some(spark_time(precision)),
                nullable: false,
                metadata: None,
            }
        );
    }
    Ok(())
}

#[test]
fn spark_time_metadata_preserves_user_metadata() -> SparkResult<()> {
    let mut field = time_field("clock", 2, true)?;
    let metadata = field.metadata_mut();
    metadata.insert(
        spec::SPARK_METADATA_JSON_KEY.to_string(),
        r#"{"comment":"wall clock","custom":7}"#.to_string(),
    );
    let proto = sdt::StructField::try_from(field)?;
    assert_eq!(proto.data_type, Some(spark_time(2)));
    assert_eq!(
        proto.metadata.as_deref(),
        Some(r#"{"comment":"wall clock","custom":7}"#)
    );
    Ok(())
}

#[test]
fn spark_time_metadata_rejects_incompatible_arrow_storage() -> SparkResult<()> {
    for data_type in [
        adt::DataType::Int64,
        adt::DataType::Time32(adt::TimeUnit::Second),
        adt::DataType::Time32(adt::TimeUnit::Millisecond),
        adt::DataType::Time64(adt::TimeUnit::Microsecond),
        adt::DataType::Timestamp(adt::TimeUnit::Nanosecond, None),
    ] {
        let field = time_field("clock", 3, true)?.with_data_type(data_type);
        assert!(sdt::StructField::try_from(field).is_err());
    }
    Ok(())
}

#[test]
fn spark_time_metadata_rejects_invalid_json_and_precision() {
    for metadata in [
        "invalid",
        "{}",
        r#"{"precision":-1}"#,
        r#"{"precision":7}"#,
        r#"{"precision":"3"}"#,
    ] {
        let field = adt::Field::new(
            "clock",
            adt::DataType::Time64(adt::TimeUnit::Nanosecond),
            true,
        )
        .with_metadata(HashMap::from([(
            spec::SAIL_SPARK_TIME_METADATA_KEY.to_string(),
            metadata.to_string(),
        )]));
        assert!(sdt::StructField::try_from(field).is_err(), "{metadata}");
    }
}

#[test]
fn spark_time_metadata_survives_all_list_field_conversions() -> SparkResult<()> {
    let element = Arc::new(time_field("item", 1, false)?);
    for data_type in [
        adt::DataType::List(Arc::clone(&element)),
        adt::DataType::LargeList(Arc::clone(&element)),
        adt::DataType::FixedSizeList(Arc::clone(&element), 2),
        adt::DataType::ListView(Arc::clone(&element)),
        adt::DataType::LargeListView(Arc::clone(&element)),
    ] {
        let proto = sdt::StructField::try_from(adt::Field::new("clocks", data_type, true))?;
        assert_eq!(
            proto.data_type,
            Some(DataType {
                kind: Some(sdt::Kind::Array(Box::new(sdt::Array {
                    element_type: Some(Box::new(spark_time(1))),
                    contains_null: false,
                    type_variation_reference: 0,
                }))),
            })
        );
        assert_eq!(proto.metadata, None);
    }
    Ok(())
}

#[test]
fn spark_time_metadata_survives_nested_map_and_struct_fields() -> SparkResult<()> {
    let inner = time_field("clock", 4, true)?;
    let value = adt::Field::new("value", adt::DataType::Struct(vec![inner].into()), true);
    let entries = adt::Field::new(
        "entries",
        adt::DataType::Struct(vec![time_field("key", 2, false)?, value].into()),
        false,
    );
    let mapping = adt::Field::new(
        "mapping",
        adt::DataType::Map(Arc::new(entries), false),
        true,
    );
    let root = adt::Field::new("root", adt::DataType::Struct(vec![mapping].into()), true);
    let proto = sdt::StructField::try_from(root)?;
    assert_eq!(
        proto.data_type,
        Some(DataType {
            kind: Some(sdt::Kind::Struct(sdt::Struct {
                fields: vec![sdt::StructField {
                    name: "mapping".to_string(),
                    data_type: Some(DataType {
                        kind: Some(sdt::Kind::Map(Box::new(sdt::Map {
                            key_type: Some(Box::new(spark_time(2))),
                            value_type: Some(Box::new(DataType {
                                kind: Some(sdt::Kind::Struct(sdt::Struct {
                                    fields: vec![sdt::StructField {
                                        name: "clock".to_string(),
                                        data_type: Some(spark_time(4)),
                                        nullable: true,
                                        metadata: None,
                                    }],
                                    type_variation_reference: 0,
                                })),
                            })),
                            value_contains_null: true,
                            type_variation_reference: 0,
                        }))),
                    }),
                    nullable: true,
                    metadata: None,
                }],
                type_variation_reference: 0,
            })),
        })
    );
    assert_eq!(proto.metadata, None);
    Ok(())
}

#[test]
fn spark_time_metadata_invalid_nested_storage_is_not_ignored() -> SparkResult<()> {
    let inner = time_field("item", 5, true)?
        .with_data_type(adt::DataType::Time64(adt::TimeUnit::Microsecond));
    let field = adt::Field::new("clocks", adt::DataType::List(Arc::new(inner)), true);
    assert!(sdt::StructField::try_from(field).is_err());
    Ok(())
}

#[test]
fn spark_time_unannotated_field_conversion_is_unchanged() -> SparkResult<()> {
    for (data_type, precision) in [
        (adt::DataType::Time32(adt::TimeUnit::Second), 0),
        (adt::DataType::Time32(adt::TimeUnit::Millisecond), 3),
        (adt::DataType::Time64(adt::TimeUnit::Microsecond), 6),
    ] {
        let proto = sdt::StructField::try_from(adt::Field::new("clock", data_type, true))?;
        assert_eq!(proto.data_type, Some(spark_time(precision)));
    }
    assert!(
        sdt::StructField::try_from(adt::Field::new(
            "clock",
            adt::DataType::Time64(adt::TimeUnit::Nanosecond),
            true,
        ))
        .is_err()
    );
    Ok(())
}
