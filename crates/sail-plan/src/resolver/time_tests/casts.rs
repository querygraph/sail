use std::collections::HashMap;

use arrow::array::{
    ArrayRef, RecordBatch, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray,
};
use arrow::datatypes::Schema;

use super::*;

const NANOS: i64 = 45_296_123_456_000;

/// Resolve casts over a column, then execute through DataFrame's optimizer and
/// physical planner. Constant folding alone must not hide a value or metadata bug.
async fn execute_casts(
    values: ArrayRef,
    declared_precision: Option<i32>,
) -> PlanResult<Vec<RecordBatch>> {
    let context = context();
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    let mut state = PlanResolverState::new();
    let column_id = state.register_field_name("clock");
    let mut field = Field::new(column_id, values.data_type().clone(), true);
    if let Some(precision) = declared_precision {
        field = field.with_metadata(HashMap::from([(
            spec::SAIL_SPARK_TIME_METADATA_KEY.to_string(),
            spec::SparkTimeMetadata::try_new(precision)?.to_json()?,
        )]));
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![values])?;
    let input = context.read_batch(batch)?;
    let schema = Arc::new(input.schema().clone());
    let mut projections = vec![];
    for precision in 0..=6 {
        for is_try in [false, true] {
            let source = spec::Expr::UnresolvedAttribute {
                name: spec::ObjectName::bare("clock"),
                plan_id: None,
                is_metadata_column: false,
            };
            let named = resolver
                .resolve_named_expression(
                    time_cast(source, spec::DataType::Time { precision }, is_try),
                    &schema,
                    &mut state,
                )
                .await?;
            projections.push(named.expr.alias(format!("time_{precision}_{is_try}")));
        }
    }
    let output = input.select(projections)?;
    let batches = output.collect().await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    for batch in &batches {
        for (index, field) in batch.schema().fields().iter().enumerate() {
            assert_time_field(field, (index / 2) as i32)?;
        }
    }
    Ok(batches)
}

fn assert_cast_values(batches: &[RecordBatch], nanos: i64, source_precision: Option<i32>) {
    for index in 0..14 {
        let precision = (index / 2) as i32;
        let expected = match source_precision {
            Some(source) if source != precision => {
                let factor = 10_i64.pow(9 - precision as u32);
                nanos / factor * factor
            }
            _ => nanos,
        };
        let actual = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(index)
                    .as_primitive::<Time64NanosecondType>()
                    .iter()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![Some(expected), None], "column {index}");
    }
}

#[tokio::test]
async fn spark_time_cast_column_preserves_identity_and_truncates_changed_precision()
-> PlanResult<()> {
    for precision in 0..=6 {
        // Even TIME(0) may contain fractions after a string cast in Spark. A
        // change to TIME(1) truncates, whereas TIME(0) -> TIME(0) keeps them.
        let values = Arc::new(Time64NanosecondArray::from(vec![Some(NANOS), None]));
        let batches = execute_casts(values, Some(precision)).await?;
        assert_cast_values(&batches, NANOS, Some(precision));
    }
    Ok(())
}

#[tokio::test]
async fn spark_time_cast_column_preserves_legacy_source_units() -> PlanResult<()> {
    let inputs: Vec<(ArrayRef, i32, i64)> = vec![
        (
            Arc::new(Time32SecondArray::from(vec![Some(45_296), None])),
            0,
            45_296_000_000_000,
        ),
        (
            Arc::new(Time32MillisecondArray::from(vec![Some(45_296_123), None])),
            3,
            45_296_123_000_000,
        ),
        (
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(45_296_123_456),
                None,
            ])),
            6,
            NANOS,
        ),
        (
            Arc::new(Time64NanosecondArray::from(vec![Some(NANOS), None])),
            9,
            NANOS,
        ),
    ];
    for (values, precision, nanos) in inputs {
        let batches = execute_casts(values, None).await?;
        assert_cast_values(&batches, nanos, Some(precision));
    }
    Ok(())
}

#[tokio::test]
async fn spark_time_cast_string_column_preserves_fraction_at_every_precision() -> PlanResult<()> {
    let values = Arc::new(StringArray::from(vec![Some("12:34:56.123456"), None]));
    let batches = execute_casts(values, None).await?;
    assert_cast_values(&batches, NANOS, None);
    Ok(())
}

#[tokio::test]
async fn spark_time_cast_rejects_metadata_on_non_nanosecond_storage() {
    let values = Arc::new(Time64MicrosecondArray::from(vec![
        Some(45_296_123_456),
        None,
    ]));
    let result = execute_casts(values, Some(6)).await;
    assert!(matches!(result, Err(PlanError::InvalidArgument(message))
        if message.contains("Time64(Nanosecond)")));
}

pub(super) fn time_cast(source: spec::Expr, target: spec::DataType, is_try: bool) -> spec::Expr {
    spec::Expr::Cast {
        expr: Box::new(source),
        cast_to_type: target,
        rename: true,
        is_try,
    }
}

#[tokio::test]
async fn spark_time_casts_replace_precision_and_clear_annotations_when_casting_away()
-> PlanResult<()> {
    let context = context();
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    let schema = Arc::new(DFSchema::empty());
    for precision in 0..=6 {
        for is_try in [false, true] {
            let source = spec::Expr::Literal(spec::Literal::Time {
                nanoseconds: None,
                precision: 6,
            });
            let cast = time_cast(source, spec::DataType::Time { precision }, is_try);
            let named = resolver
                .resolve_named_expression(cast.clone(), &schema, &mut PlanResolverState::new())
                .await?;
            assert_time_field(&named.expr.to_field(&schema)?.1, precision)?;
            assert!(named.name[0].contains(&format!("AS TIME({precision})")));
            for target in [
                spec::DataType::Int64,
                spec::DataType::Time64 {
                    time_unit: spec::TimeUnit::Nanosecond,
                },
            ] {
                let named = resolver
                    .resolve_named_expression(
                        time_cast(cast.clone(), target, is_try),
                        &schema,
                        &mut PlanResolverState::new(),
                    )
                    .await?;
                assert!(
                    !named
                        .expr
                        .to_field(&schema)?
                        .1
                        .metadata()
                        .contains_key(spec::SAIL_SPARK_TIME_METADATA_KEY)
                );
                assert!(
                    !named
                        .metadata
                        .iter()
                        .any(|(key, _)| key == spec::SAIL_SPARK_TIME_METADATA_KEY)
                );
            }
        }
    }
    Ok(())
}
