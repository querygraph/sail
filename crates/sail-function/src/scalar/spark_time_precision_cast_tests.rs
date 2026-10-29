use std::collections::HashMap;

use datafusion::arrow::array::{Int32Array, Time64NanosecondArray};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion_common::config::ConfigOptions;

use super::*;

const NANOS: i64 = 45_296_123_456_000;

fn time(name: &str, precision: i32, nullable: bool) -> Result<FieldRef> {
    let metadata = SparkTimeMetadata::try_new(precision)
        .and_then(SparkTimeMetadata::to_json)
        .map_err(|error| plan_datafusion_err!("{error}"))?;
    Ok(Arc::new(
        Field::new(name, DataType::Time64(TimeUnit::Nanosecond), nullable).with_metadata(
            HashMap::from([(SAIL_SPARK_TIME_METADATA_KEY.to_string(), metadata)]),
        ),
    ))
}

fn invoke(function: &SparkTimePrecisionCast, input: ColumnarValue) -> Result<ColumnarValue> {
    let number_rows = match &input {
        ColumnarValue::Array(array) => array.len(),
        ColumnarValue::Scalar(_) => 1,
    };
    function.invoke_with_args(ScalarFunctionArgs {
        arg_fields: vec![Arc::clone(function.source_field())],
        args: vec![input],
        number_rows,
        return_field: Arc::clone(function.output_field()),
        config_options: Arc::new(ConfigOptions::default()),
    })
}

#[test]
fn precision_changes_truncate_but_identity_preserves_fractional_bits() -> Result<()> {
    for (source, target, expected) in [
        (0, 0, NANOS),
        (0, 1, 45_296_100_000_000),
        (0, 3, 45_296_123_000_000),
        (6, 3, 45_296_123_000_000),
    ] {
        let function =
            SparkTimePrecisionCast::try_new(time("t", source, true)?, time("t", target, true)?)?;
        for value in [Some(NANOS), None] {
            let result = invoke(
                &function,
                ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(value)),
            )?;
            let ColumnarValue::Scalar(result) = result else {
                return exec_err!("expected scalar");
            };
            assert_eq!(
                result,
                ScalarValue::Time64Nanosecond(value.map(|_| expected))
            );
        }
    }
    let source = Arc::new(Field::new(
        "t",
        DataType::Time64(TimeUnit::Microsecond),
        true,
    ));
    let function = SparkTimePrecisionCast::try_new(source, time("t", 3, true)?)?;
    let result = invoke(
        &function,
        ColumnarValue::Scalar(ScalarValue::Time64Microsecond(Some(NANOS / 1000))),
    )?;
    let ColumnarValue::Scalar(result) = result else {
        return exec_err!("expected scalar");
    };
    assert_eq!(
        result,
        ScalarValue::Time64Nanosecond(Some(45_296_123_000_000))
    );
    Ok(())
}

#[test]
fn nested_transform_preserves_source_names_other_leaves_and_struct_nulls() -> Result<()> {
    let source_fields = vec![
        time("clock", 0, true)?,
        Arc::new(Field::new("number", DataType::Int32, true)),
    ];
    let source = Arc::new(Field::new(
        "record",
        DataType::Struct(source_fields.clone().into()),
        true,
    ));
    let target = Arc::new(Field::new(
        "renamed",
        DataType::Struct(
            vec![
                time("renamed_clock", 3, true)?,
                Arc::new(Field::new("renamed_number", DataType::Int64, true)),
            ]
            .into(),
        ),
        true,
    ));
    let function = SparkTimePrecisionCast::try_new(source, target)?;
    let nulls = Some(NullBuffer::from(vec![true, false]));
    let input = Arc::new(StructArray::try_new(
        source_fields.into(),
        vec![
            Arc::new(Time64NanosecondArray::from(vec![Some(NANOS), None])),
            Arc::new(Int32Array::from(vec![7, 8])),
        ],
        nulls.clone(),
    )?);
    let output = invoke(&function, ColumnarValue::Array(input))?.into_array(2)?;
    let output = output.as_struct();
    assert_eq!(output.nulls(), nulls.as_ref());
    assert_eq!(output.fields()[0].name(), "clock");
    assert_eq!(output.fields()[1].name(), "number");
    assert_eq!(output.column(1).data_type(), &DataType::Int32);
    assert_eq!(
        output
            .column(0)
            .as_primitive::<Time64NanosecondType>()
            .value(0),
        45_296_123_000_000
    );
    assert_eq!(declared_precision(&output.fields()[0])?, Some(3));
    Ok(())
}

#[test]
fn sliced_list_keeps_offsets_nulls_and_null_elements() -> Result<()> {
    let source_element = time("item", 0, true)?;
    let source = Arc::new(Field::new(
        "values",
        DataType::List(Arc::clone(&source_element)),
        true,
    ));
    let target = Arc::new(Field::new(
        "values",
        DataType::List(time("item", 3, true)?),
        true,
    ));
    let function = SparkTimePrecisionCast::try_new(source, target)?;
    let input = ListArray::try_new(
        source_element,
        OffsetBuffer::from_lengths([2, 0, 2]),
        Arc::new(Time64NanosecondArray::from(vec![
            Some(NANOS),
            None,
            Some(NANOS),
            None,
        ])),
        Some(NullBuffer::from(vec![true, false, true])),
    )?
    .slice(1, 2);
    let offsets = input.offsets().clone();
    let nulls = input.nulls().cloned();
    let output = invoke(&function, ColumnarValue::Array(Arc::new(input)))?.into_array(2)?;
    let output = output.as_list::<i32>();
    assert_eq!(output.offsets(), &offsets);
    assert_eq!(output.nulls(), nulls.as_ref());
    let values = output.value(1);
    let values = values.as_primitive::<Time64NanosecondType>();
    assert_eq!(values.value(0), 45_296_123_000_000);
    assert!(values.is_null(1));
    Ok(())
}

#[test]
fn map_changes_only_requested_key_precision() -> Result<()> {
    let source_entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(vec![time("key", 0, false)?, time("value", 3, true)?].into()),
        false,
    ));
    let target_entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(vec![time("key", 3, false)?, time("value", 3, true)?].into()),
        false,
    ));
    let source = Arc::new(Field::new(
        "lookup",
        DataType::Map(Arc::clone(&source_entries), false),
        true,
    ));
    let target = Arc::new(Field::new(
        "lookup",
        DataType::Map(target_entries, false),
        true,
    ));
    let function = SparkTimePrecisionCast::try_new(source, target)?;
    let DataType::Struct(fields) = source_entries.data_type() else {
        return exec_err!("expected entries");
    };
    let entries = StructArray::try_new(
        fields.clone(),
        vec![
            Arc::new(Time64NanosecondArray::from(vec![NANOS])),
            Arc::new(Time64NanosecondArray::from(vec![NANOS])),
        ],
        None,
    )?;
    let input = Arc::new(MapArray::try_new(
        source_entries,
        OffsetBuffer::from_lengths([1]),
        entries,
        None,
        false,
    )?);
    let output = invoke(&function, ColumnarValue::Array(input))?.into_array(1)?;
    let output = output.as_map();
    assert_eq!(
        output
            .keys()
            .as_primitive::<Time64NanosecondType>()
            .value(0),
        45_296_123_000_000
    );
    assert_eq!(
        output
            .values()
            .as_primitive::<Time64NanosecondType>()
            .value(0),
        NANOS
    );
    Ok(())
}

#[test]
fn rejects_invalid_precision_metadata_before_execution() -> Result<()> {
    let bad = Arc::new(
        Field::new("t", DataType::Time64(TimeUnit::Nanosecond), true).with_metadata(HashMap::from(
            [(
                SAIL_SPARK_TIME_METADATA_KEY.to_string(),
                r#"{"precision":9}"#.to_string(),
            )],
        )),
    );
    assert!(SparkTimePrecisionCast::try_new(time("t", 0, true)?, bad).is_err());
    let bad_storage = Arc::new(
        time("t", 0, true)?
            .as_ref()
            .clone()
            .with_data_type(DataType::Time64(TimeUnit::Microsecond)),
    );
    assert!(SparkTimePrecisionCast::try_new(bad_storage, time("t", 3, true)?).is_err());
    Ok(())
}
