use datafusion::arrow::array::{
    Array, AsArray, Int32Array, ListArray, RecordBatch, Time64NanosecondArray,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::physical_expr::ScalarFunctionExpr;
use sail_common::spec;

use super::*;

fn list_field(precision: i32) -> Result<Arc<Field>> {
    let metadata = spec::SparkTimeMetadata::try_new(precision)
        .and_then(spec::SparkTimeMetadata::to_json)
        .map_err(|error| plan_datafusion_err!("{error}"))?;
    let element = Arc::new(
        Field::new("item", DataType::Time64(TimeUnit::Nanosecond), true).with_metadata(
            HashMap::from([(spec::SAIL_SPARK_TIME_METADATA_KEY.to_string(), metadata)]),
        ),
    );
    Ok(Arc::new(Field::new("times", DataType::List(element), true)))
}

#[test]
fn decoded_time_precision_cast_preserves_nested_fields_and_executes() -> Result<()> {
    let source = list_field(0)?;
    let target = list_field(3)?;
    let original = ScalarUDF::from(SparkTimePrecisionCast::try_new(
        Arc::clone(&source),
        Arc::clone(&target),
    )?);
    let mut encoded = vec![];
    RemoteExecutionCodec.try_encode_udf(&original, &mut encoded)?;
    assert!(!encoded.is_empty());
    let decoded = RemoteExecutionCodec.try_decode_udf(original.name(), &encoded)?;
    let implementation = decoded
        .inner()
        .downcast_ref::<SparkTimePrecisionCast>()
        .ok_or_else(|| plan_datafusion_err!("decoded wrong TIME precision function"))?;
    assert_eq!(implementation.source_field(), &source);
    assert_eq!(implementation.target_field(), &target);
    let DataType::List(element) = source.data_type() else {
        return plan_err!("expected list");
    };
    let input = Arc::new(ListArray::try_new(
        Arc::clone(element),
        OffsetBuffer::from_lengths([2]),
        Arc::new(Time64NanosecondArray::from(vec![
            Some(45_296_123_456_000),
            None,
        ])),
        None,
    )?);
    let result = decoded.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(input)],
        arg_fields: vec![source],
        number_rows: 1,
        return_field: Arc::clone(implementation.output_field()),
        config_options: Arc::new(ConfigOptions::default()),
    })?;
    let result = result.into_array(1)?;
    let values = result
        .as_list::<i32>()
        .values()
        .as_primitive::<datafusion::arrow::datatypes::Time64NanosecondType>();
    assert_eq!(values.value(0), 45_296_123_000_000);
    assert!(values.is_null(1));
    assert_eq!(result.data_type(), target.data_type());
    Ok(())
}

#[test]
fn time_precision_physical_expression_round_trip_preserves_nested_schema_and_values() -> Result<()>
{
    let source = list_field(0)?;
    let target = list_field(3)?;
    let schema = Arc::new(Schema::new(vec![
        Arc::new(Field::new("id", DataType::Int32, false)),
        Arc::clone(&source),
    ]));
    let function = ScalarUDF::from(SparkTimePrecisionCast::try_new(
        Arc::clone(&source),
        Arc::clone(&target),
    )?);
    let expression = Arc::new(ScalarFunctionExpr::try_new(
        Arc::new(function),
        vec![Arc::new(Column::new("times", 1))],
        schema.as_ref(),
        Arc::new(ConfigOptions::default()),
    )?) as Arc<dyn PhysicalExpr>;
    let bytes = try_encode_physical_expr(&RemoteExecutionCodec, &expression)?;
    assert!(!bytes.is_empty());
    let decoded = try_decode_physical_expr(
        &TaskContext::default(),
        &RemoteExecutionCodec,
        &bytes,
        schema.as_ref(),
    )?;
    assert_eq!(expression.return_field(schema.as_ref())?, target);
    assert_eq!(decoded.return_field(schema.as_ref())?, target);
    assert_eq!(
        decoded.data_type(schema.as_ref())?,
        target.data_type().clone()
    );

    let DataType::List(element) = source.data_type() else {
        return plan_err!("expected list");
    };
    let offsets = OffsetBuffer::from_lengths([2, 0, 1]);
    let nulls = Some(NullBuffer::from(vec![true, false, true]));
    let values = Arc::new(ListArray::try_new(
        Arc::clone(element),
        offsets.clone(),
        Arc::new(Time64NanosecondArray::from(vec![
            Some(45_296_123_456_000),
            None,
            Some(86_399_999_999_000),
        ])),
        nulls.clone(),
    )?);
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![10, 11, 12])), values],
    )?;
    let expected = expression.evaluate(&batch)?.into_array(batch.num_rows())?;
    let actual = decoded.evaluate(&batch)?.into_array(batch.num_rows())?;
    assert_eq!(actual.to_data(), expected.to_data());
    assert_eq!(actual.data_type(), target.data_type());
    let actual = actual.as_list::<i32>();
    assert_eq!(actual.offsets(), &offsets);
    assert_eq!(actual.nulls(), nulls.as_ref());
    assert_eq!(
        actual
            .values()
            .as_primitive::<datafusion::arrow::datatypes::Time64NanosecondType>()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(45_296_123_000_000), None, Some(86_399_999_000_000)]
    );
    Ok(())
}

#[test]
fn time_precision_codec_rejects_malformed_and_mislabeled_fields() -> Result<()> {
    let source = try_encode_field_ref(&list_field(0)?)?;
    let target = try_encode_field_ref(&list_field(3)?)?;
    for (source_field, target_field, name) in [
        (vec![0xff], target.clone(), "spark_time_precision_cast"),
        (source.clone(), vec![0xff], "spark_time_precision_cast"),
        (source, target, "another_function"),
    ] {
        let payload = ExtendedScalarUdf {
            udf_kind: Some(r#gen::extended_scalar_udf::UdfKind::SparkTimePrecisionCast(
                r#gen::SparkTimePrecisionCastUdf {
                    source_field,
                    target_field,
                },
            )),
        }
        .encode_to_vec();
        assert!(RemoteExecutionCodec.try_decode_udf(name, &payload).is_err());
    }
    Ok(())
}
