use std::collections::HashMap;

use arrow::array::{
    ArrayRef, Int32Array, Int64Array, ListArray, MapArray, RecordBatch, StructArray,
    Time64NanosecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{FieldRef, Schema};

use super::casts::time_cast;
use super::*;

const NANOS: i64 = 45_296_123_456_000;
const MILLIS: i64 = 45_296_123_000_000;

fn time(name: &str, precision: i32, nullable: bool) -> PlanResult<FieldRef> {
    Ok(Arc::new(
        Field::new(name, DataType::Time64(TimeUnit::Nanosecond), nullable).with_metadata(
            HashMap::from([(
                spec::SAIL_SPARK_TIME_METADATA_KEY.to_string(),
                spec::SparkTimeMetadata::try_new(precision)?.to_json()?,
            )]),
        ),
    ))
}

fn field(name: &str, data_type: spec::DataType) -> spec::Field {
    spec::Field {
        name: name.to_string(),
        data_type,
        nullable: true,
        metadata: vec![],
    }
}

/// Use a column through optimization and physical execution. Both the output
/// values and its nested fields must survive precision conversion and renaming.
async fn assert_cast(
    input: ArrayRef,
    target: spec::DataType,
    expected: ArrayRef,
) -> PlanResult<()> {
    let context = context();
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    let mut state = PlanResolverState::new();
    let name = state.register_field_name("clock");
    let schema = Arc::new(Schema::new(vec![Field::new(
        name,
        input.data_type().clone(),
        true,
    )]));
    let input = context.read_batch(RecordBatch::try_new(schema, vec![input])?)?;
    let schema = Arc::new(input.schema().clone());
    for is_try in [false, true] {
        let expression = resolver
            .resolve_named_expression(
                time_cast(
                    spec::Expr::UnresolvedAttribute {
                        name: spec::ObjectName::bare("clock"),
                        plan_id: None,
                        is_metadata_column: false,
                    },
                    target.clone(),
                    is_try,
                ),
                &schema,
                &mut state,
            )
            .await?;
        assert_eq!(
            expression.expr.to_field(&schema)?.1.data_type(),
            expected.data_type()
        );
        let batches = input
            .clone()
            .select(vec![expression.expr.alias("result")])?
            .collect()
            .await?;
        let mut offset = 0;
        for batch in &batches {
            assert_eq!(batch.column(0).data_type(), expected.data_type());
            assert_eq!(
                batch.column(0).to_data(),
                expected.slice(offset, batch.num_rows()).to_data()
            );
            offset += batch.num_rows();
        }
        assert_eq!(offset, expected.len());
    }
    Ok(())
}

fn mixed_struct(converted: bool) -> PlanResult<StructArray> {
    let (name, precision, value, numbers) = if converted {
        (
            "after",
            3,
            MILLIS,
            Arc::new(Int64Array::from(vec![Some(7), Some(8), None])) as ArrayRef,
        )
    } else {
        (
            "before",
            0,
            NANOS,
            Arc::new(Int32Array::from(vec![Some(7), Some(8), None])) as ArrayRef,
        )
    };
    Ok(StructArray::try_new(
        vec![
            time(name, precision, true)?,
            time("same", 3, true)?,
            Arc::new(Field::new("number", numbers.data_type().clone(), true)),
        ]
        .into(),
        vec![
            Arc::new(Time64NanosecondArray::from(vec![
                Some(value),
                Some(value),
                None,
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                Some(NANOS),
                Some(NANOS),
                None,
            ])),
            numbers,
        ],
        Some(NullBuffer::from(vec![true, false, true])),
    )?)
}

fn mixed_target() -> spec::DataType {
    spec::DataType::Struct {
        fields: vec![
            field("after", spec::DataType::Time { precision: 3 }),
            field("same", spec::DataType::Time { precision: 3 }),
            field("number", spec::DataType::Int64),
        ]
        .into(),
    }
}

#[tokio::test]
async fn nested_time_cast_precedes_struct_rename_and_other_leaf_casts() -> PlanResult<()> {
    assert_cast(
        Arc::new(mixed_struct(false)?),
        mixed_target(),
        Arc::new(mixed_struct(true)?),
    )
    .await
}

#[tokio::test]
async fn nested_time_cast_preserves_null_lists_structs_and_elements() -> PlanResult<()> {
    let list = |values: ArrayRef| {
        ListArray::try_new(
            Arc::new(Field::new(
                spec::SAIL_LIST_FIELD_NAME,
                values.data_type().clone(),
                true,
            )),
            OffsetBuffer::from_lengths([2, 0, 1]),
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )
    };
    assert_cast(
        Arc::new(list(Arc::new(mixed_struct(false)?))?),
        spec::DataType::List {
            data_type: Box::new(mixed_target()),
            nullable: true,
        },
        Arc::new(list(Arc::new(mixed_struct(true)?))?),
    )
    .await?;
    for precision in [0, 3] {
        let input = Arc::new(ListArray::try_new(
            time(spec::SAIL_LIST_FIELD_NAME, 0, true)?,
            OffsetBuffer::from_lengths([2, 0, 1]),
            Arc::new(Time64NanosecondArray::from(vec![
                Some(NANOS),
                None,
                Some(NANOS),
            ])),
            Some(NullBuffer::from(vec![true, false, true])),
        )?);
        let value = if precision == 0 { NANOS } else { MILLIS };
        let expected = Arc::new(ListArray::try_new(
            time(spec::SAIL_LIST_FIELD_NAME, precision, true)?,
            OffsetBuffer::from_lengths([2, 0, 1]),
            Arc::new(Time64NanosecondArray::from(vec![
                Some(value),
                None,
                Some(value),
            ])),
            Some(NullBuffer::from(vec![true, false, true])),
        )?);
        assert_cast(
            input,
            spec::DataType::List {
                data_type: Box::new(spec::DataType::Time { precision }),
                nullable: true,
            },
            expected,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn nested_time_cast_changes_map_keys_without_truncating_identity_values() -> PlanResult<()> {
    let map = |precision, key| -> PlanResult<MapArray> {
        let entries = StructArray::try_new(
            vec![
                time(spec::SAIL_MAP_KEY_FIELD_NAME, precision, false)?,
                time(spec::SAIL_MAP_VALUE_FIELD_NAME, 3, true)?,
            ]
            .into(),
            vec![
                Arc::new(Time64NanosecondArray::from(vec![key, key])),
                Arc::new(Time64NanosecondArray::from(vec![Some(NANOS), None])),
            ],
            None,
        )?;
        Ok(MapArray::try_new(
            Arc::new(Field::new(
                spec::SAIL_MAP_FIELD_NAME,
                entries.data_type().clone(),
                false,
            )),
            OffsetBuffer::from_lengths([1, 0, 1]),
            entries,
            Some(NullBuffer::from(vec![true, false, true])),
            false,
        )?)
    };
    assert_cast(
        Arc::new(map(0, NANOS)?),
        spec::DataType::Map {
            key_type: Box::new(spec::DataType::Time { precision: 3 }),
            value_type: Box::new(spec::DataType::Time { precision: 3 }),
            value_type_nullable: true,
            keys_sorted: false,
        },
        Arc::new(map(3, MILLIS)?),
    )
    .await
}

#[tokio::test]
async fn nested_time_cast_name_uses_declared_precision() -> PlanResult<()> {
    let context = context();
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    let target = spec::DataType::List {
        data_type: Box::new(spec::DataType::Map {
            key_type: Box::new(spec::DataType::Time { precision: 1 }),
            value_type: Box::new(mixed_target()),
            value_type_nullable: true,
            keys_sorted: false,
        }),
        nullable: true,
    };
    let named = resolver
        .resolve_named_expression(
            time_cast(spec::Expr::Literal(spec::Literal::Null), target, false),
            &Arc::new(DFSchema::empty()),
            &mut PlanResolverState::new(),
        )
        .await?;
    assert_eq!(
        named.name,
        ["CAST(NULL AS ARRAY<MAP<TIME(1),STRUCT<AFTER:TIME(3),SAME:TIME(3),NUMBER:BIGINT>>>)"]
    );
    Ok(())
}
