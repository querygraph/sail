use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int32Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::expr::{HigherOrderFunction, LambdaVariable};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion_common::{DFSchema, Result, ScalarValue, exec_datafusion_err};
use datafusion_expr::{
    ColumnarValue, Expr, HigherOrderReturnFieldArgs, HigherOrderUDF, HigherOrderUDFImpl, ScalarUDF,
    ValueOrLambda, Volatility, col, create_udf, lambda, lit,
};

use super::div_mod_lazy::SparkDivModLazy;
use super::divisor::SafeNumericDivisor;
use super::zero_division::{SparkZeroDivisionError, ZeroDivisionKind};

type Trace = Arc<Mutex<Vec<String>>>;

fn variable(name: &str, data_type: DataType, nullable: bool) -> Expr {
    let name = format!("_divmod_{name}");
    Expr::LambdaVariable(LambdaVariable::new(
        name.to_owned(),
        Some(Arc::new(Field::new(&name, data_type, nullable))),
    ))
}

fn logical(left: Expr, right: Expr, operation: Expr, ansi: bool) -> Expr {
    Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::new(HigherOrderUDF::new_from_impl(SparkDivModLazy::new(ansi))),
        vec![
            lambda(["unused_r"], right),
            lambda(["unused_l"], left),
            lambda(["_divmod_l", "_divmod_r"], operation),
        ],
    ))
}

fn physical(expression: &Expr, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
    create_physical_expr(
        expression,
        &DFSchema::try_from(schema.clone())?,
        &ExecutionProps::new(),
        &PhysicalPlanningContext::default(),
    )
}

fn record(trace: &Trace, value: String) -> Result<()> {
    trace
        .lock()
        .map_err(|_| exec_datafusion_err!("trace lock poisoned"))?
        .push(value);
    Ok(())
}

fn observed_column(side: &'static str, trace: Trace, fail_row: Option<i32>) -> Expr {
    create_udf(
        side,
        vec![DataType::Int32, DataType::Int32],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |args| {
            let rows = args[1].to_array(1)?;
            let rows = rows
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| exec_datafusion_err!("expected row id"))?;
            for row in rows.iter().flatten() {
                record(&trace, format!("{side}{row}"))?;
                if fail_row == Some(row) {
                    return Err(exec_datafusion_err!("{side} failed on row {row}"));
                }
            }
            Ok(args[0].clone())
        }),
    )
    .call(vec![col(side), col("id")])
}

fn observed_operation(trace: Trace) -> Expr {
    create_udf(
        "operation",
        vec![DataType::Int32; 2],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |args| {
            record(&trace, "operation".to_owned())?;
            let values = ColumnarValue::values_to_arrays(args)?;
            let left = values[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| exec_datafusion_err!("expected left"))?
                .value(0);
            let right = values[1]
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| exec_datafusion_err!("expected right"))?
                .value(0);
            if right == 0 {
                return Err(ZeroDivisionKind::Divide.error());
            }
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(
                left / right,
            ))))
        }),
    )
    .call(vec![
        variable("l", DataType::Int32, true),
        variable("r", DataType::Int32, true),
    ])
}

fn batch(left: Vec<Option<i32>>, right: Vec<Option<i32>>) -> Result<RecordBatch> {
    let n = i32::try_from(left.len()).map_err(|_| exec_datafusion_err!("test row count"))?;
    RecordBatch::try_from_iter(vec![
        ("l", Arc::new(Int32Array::from(left)) as ArrayRef),
        ("r", Arc::new(Int32Array::from(right)) as ArrayRef),
        (
            "id",
            Arc::new(Int32Array::from_iter_values(0..n)) as ArrayRef,
        ),
    ])
    .map_err(Into::into)
}

#[test]
fn right_first_null_zero_skip_and_no_replay() -> Result<()> {
    let input = batch(
        vec![Some(9), None, Some(5), Some(8), None],
        vec![None, Some(0), Some(0), Some(2), Some(4)],
    )?;
    for ansi in [true, false] {
        let trace = Trace::default();
        let expr = logical(
            observed_column("l", Arc::clone(&trace), None),
            observed_column("r", Arc::clone(&trace), None),
            observed_operation(Arc::clone(&trace)),
            ansi,
        );
        let result = physical(&expr, input.schema().as_ref())?.evaluate(&input);
        if ansi {
            assert!(result.is_err());
            assert_eq!(
                *trace
                    .lock()
                    .map_err(|_| exec_datafusion_err!("trace lock"))?,
                ["r0", "r1", "l1", "r2", "l2", "operation"]
            );
        } else {
            let values = result?.into_array(5)?;
            assert_eq!(
                values.as_any().downcast_ref::<Int32Array>(),
                Some(&Int32Array::from(vec![None, None, None, Some(4), None]))
            );
            assert_eq!(
                *trace
                    .lock()
                    .map_err(|_| exec_datafusion_err!("trace lock"))?,
                ["r0", "r1", "r2", "r3", "l3", "operation", "r4", "l4"]
            );
        }
    }
    Ok(())
}

#[test]
fn stops_at_first_row_error_before_later_right_operand() -> Result<()> {
    let input = batch(vec![Some(8), Some(9)], vec![Some(2), Some(3)])?;
    let trace = Trace::default();
    let expr = logical(
        observed_column("l", Arc::clone(&trace), Some(0)),
        observed_column("r", Arc::clone(&trace), Some(1)),
        observed_operation(Arc::clone(&trace)),
        true,
    );
    let error = physical(&expr, input.schema().as_ref())?
        .evaluate(&input)
        .err()
        .ok_or_else(|| exec_datafusion_err!("expected left failure"))?;
    assert!(error.to_string().contains("l failed on row 0"), "{error}");
    assert_eq!(
        *trace
            .lock()
            .map_err(|_| exec_datafusion_err!("trace lock"))?,
        ["r0", "l0"]
    );
    Ok(())
}

#[test]
fn floating_zeros_nan_infinity_and_empty_batch() -> Result<()> {
    let input = RecordBatch::try_from_iter(vec![(
        "r",
        Arc::new(Float64Array::from(vec![
            None,
            Some(0.0),
            Some(-0.0),
            Some(2.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
        ])) as ArrayRef,
    )])?;
    let operation = ScalarUDF::from(SafeNumericDivisor::new(ZeroDivisionKind::Divide, false))
        .call(vec![variable("r", DataType::Float64, true)]);
    let expr = logical(lit(1), col("r"), operation, false);
    let physical = physical(&expr, input.schema().as_ref())?;
    let result = physical.evaluate(&input)?.into_array(6)?;
    let result = result
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| exec_datafusion_err!("expected float result"))?;
    assert!((0..3).all(|i| result.is_null(i)));
    assert_eq!(result.value(3), 2.0);
    assert!(result.value(4).is_nan());
    assert_eq!(result.value(5), f64::INFINITY);
    let empty = physical.evaluate(&input.slice(0, 0))?.into_array(0)?;
    assert_eq!(empty.data_type(), &DataType::Float64);
    assert_eq!(empty.len(), 0);
    Ok(())
}

#[test]
fn result_field_metadata_and_divisor_admission() -> Result<()> {
    let field = Arc::new(
        Field::new("operation", DataType::Int32, false).with_metadata(
            std::collections::HashMap::from([("owner".to_owned(), "native".to_owned())]),
        ),
    );
    let args = [
        ValueOrLambda::Lambda(Arc::new(Field::new("right", DataType::Int32, false))),
        ValueOrLambda::Lambda(Arc::clone(&field)),
        ValueOrLambda::Lambda(Arc::clone(&field)),
    ];
    let udf = SparkDivModLazy::new(true);
    let result = udf.return_field_from_args(HigherOrderReturnFieldArgs {
        arg_fields: &args,
        scalar_arguments: &[None, None, None],
    })?;
    assert_eq!(result.as_ref(), &field.as_ref().clone().with_nullable(true));
    assert!(udf.short_circuits());
    assert!(udf.ansi_mode());
    let invalid = logical(lit(1), lit("0"), lit(1), true);
    assert!(physical(&invalid, &Schema::empty()).is_err());
    Ok(())
}

#[tokio::test]
async fn optimizer_keeps_conditional_failure_and_captured_operation() -> Result<()> {
    let ctx = SessionContext::new();
    let error = ScalarUDF::from(SparkZeroDivisionError::new(ZeroDivisionKind::Divide)).call(vec![]);
    let skipped = logical(error, lit(ScalarValue::Int32(None)), lit(7), true);
    let result = ctx
        .read_batch(batch(vec![Some(1)], vec![Some(2)])?)?
        .select(vec![skipped])?
        .collect()
        .await?;
    assert!(result[0].column(0).is_null(0));
    let input = batch(vec![Some(3), Some(4)], vec![Some(2), Some(3)])?;
    let expr = logical(
        col("l"),
        col("r"),
        variable("r", DataType::Int32, false) + col("id"),
        true,
    );
    let result = ctx.read_batch(input)?.select(vec![expr])?.collect().await?;
    assert_eq!(
        result[0].column(0).as_any().downcast_ref::<Int32Array>(),
        Some(&Int32Array::from(vec![2, 4]))
    );
    Ok(())
}

#[tokio::test]
async fn identical_calls_keep_separate_volatile_thunk_evaluations() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let right = create_udf(
        "counting_divisor",
        vec![],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |_| {
            let value = i32::try_from(observed.fetch_add(1, Ordering::SeqCst) + 1)
                .map_err(|_| exec_datafusion_err!("counter exceeds int32"))?;
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(value))))
        }),
    )
    .call(vec![]);
    let expression = logical(
        lit(8),
        right,
        variable("l", DataType::Int32, false) / variable("r", DataType::Int32, true),
        true,
    );
    let ctx = SessionContext::new();
    let frame = ctx
        .read_batch(batch(vec![Some(1)], vec![Some(1)])?)?
        .select(vec![expression.clone().alias("a"), expression.alias("b")])?;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let batches = frame.collect().await?;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        batches[0].column(0).as_any().downcast_ref::<Int32Array>(),
        Some(&Int32Array::from(vec![8]))
    );
    assert_eq!(
        batches[0].column(1).as_any().downcast_ref::<Int32Array>(),
        Some(&Int32Array::from(vec![4]))
    );
    Ok(())
}
