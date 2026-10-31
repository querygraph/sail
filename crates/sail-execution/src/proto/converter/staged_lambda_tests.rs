use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, ScalarUDF, Volatility,
};
use datafusion::physical_expr::expressions::Literal;
use sail_function::scalar::math::zero_division::{SparkZeroDivisionError, ZeroDivisionKind};

use super::*;
use crate::proto::codec::RemoteExecutionCodec;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
enum Behavior {
    RetainNone,
    CompleteAt(usize),
    NeverComplete,
    PartialCount(usize),
    CompleteCount(usize),
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct StagedUdf {
    signature: HigherOrderSignature,
    behavior: Behavior,
}

impl StagedUdf {
    fn udf(behavior: Behavior) -> HigherOrderUDF {
        let count = if behavior == Behavior::RetainNone {
            2
        } else {
            1
        };
        let mut signature = HigherOrderSignature::exact(
            vec![ValueOrLambda::Lambda(()); count],
            Volatility::Immutable,
        );
        signature.lambda_parameters_max_iterations = match behavior {
            Behavior::CompleteAt(limit) => limit,
            Behavior::RetainNone => 2,
            _ => 1,
        };
        HigherOrderUDF::new_from_impl(Self {
            signature,
            behavior,
        })
    }
}

impl HigherOrderUDFImpl for StagedUdf {
    fn name(&self) -> &str {
        "staged_codec_test"
    }
    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }
    fn coerce_value_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        assert!(types.is_empty());
        Ok(vec![])
    }
    fn lambda_parameters(
        &self,
        step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let dummy = vec![Arc::new(Field::new("", DataType::Null, true))];
        // A physical HOF asks again at step zero with all final output fields.
        if step == 0
            && fields
                .iter()
                .all(|f| matches!(f, ValueOrLambda::Lambda(Some(_))))
            && matches!(
                self.behavior,
                Behavior::RetainNone | Behavior::CompleteAt(_)
            )
        {
            return Ok(LambdaParametersProgress::Complete(vec![
                dummy;
                fields.len()
            ]));
        }
        Ok(match self.behavior {
            Behavior::RetainNone => match step {
                0 => LambdaParametersProgress::Partial(vec![Some(dummy), None]),
                1 => LambdaParametersProgress::Partial(vec![None, Some(dummy)]),
                2 => {
                    if !fields
                        .iter()
                        .all(|f| matches!(f, ValueOrLambda::Lambda(Some(_))))
                    {
                        return plan_err!("previous lambda output was lost");
                    }
                    LambdaParametersProgress::Complete(vec![dummy.clone(), dummy])
                }
                _ => return plan_err!("unexpected resolution step"),
            },
            Behavior::CompleteAt(limit) if step == limit => {
                LambdaParametersProgress::Complete(vec![dummy])
            }
            Behavior::CompleteAt(_) | Behavior::NeverComplete => {
                LambdaParametersProgress::Partial(vec![Some(dummy)])
            }
            Behavior::PartialCount(count) => {
                LambdaParametersProgress::Partial(vec![Some(dummy); count])
            }
            Behavior::CompleteCount(count) => {
                LambdaParametersProgress::Complete(vec![dummy; count])
            }
        })
    }
    fn return_field_from_args(&self, _args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new("result", DataType::Int32, true)))
    }
    fn invoke_with_args(&self, _args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        plan_err!("decoding must not invoke the higher-order function")
    }
}

fn resolve(
    fun: &HigherOrderUDF,
    count: usize,
    failing_bodies: bool,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let converter = RemotePhysicalProtoConverter::default();
    let codec = RemoteExecutionCodec;
    let task = TaskContext::default();
    let ctx = PhysicalPlanDecodeContext::new(&task, &codec);
    let mut inputs = Vec::new();
    for _ in 0..count {
        let body: Arc<dyn PhysicalExpr> = if failing_bodies {
            let expression =
                ScalarUDF::from(SparkZeroDivisionError::new(ZeroDivisionKind::Divide)).call(vec![]);
            datafusion::physical_expr::create_physical_expr(
                &expression, &DFSchema::empty(),
                &datafusion::logical_expr::execution_props::ExecutionProps::new(),
                &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext::default(),
            )?
        } else {
            Arc::new(Literal::new(ScalarValue::Int32(Some(7))))
        };
        let lambda = Arc::new(LambdaExpr::try_new(vec!["unused".to_owned()], body)?)
            as Arc<dyn PhysicalExpr>;
        inputs.push(converter.physical_expr_to_proto(&lambda, &codec)?);
    }
    converter.decode_higher_order_args(fun, &inputs, &Schema::empty(), &ctx)
}

#[test]
fn partial_none_keeps_previous_lambda_outputs() -> Result<()> {
    let fun = Arc::new(StagedUdf::udf(Behavior::RetainNone));
    let args = resolve(fun.as_ref(), 2, false)?;
    assert_eq!(args.len(), 2);
    for arg in &args {
        let lambda = arg
            .downcast_ref::<LambdaExpr>()
            .ok_or_else(|| plan_datafusion_err!("expected lambda"))?;
        assert_eq!(lambda.body().data_type(&Schema::empty())?, DataType::Int32);
    }
    let _physical = HigherOrderFunctionExpr::try_new_with_schema(
        fun,
        args,
        &Schema::empty(),
        Arc::new(ConfigOptions::default()),
    )?;
    Ok(())
}

#[test]
fn resolution_includes_zero_and_last_allowed_steps() -> Result<()> {
    for limit in [0, 1, 2] {
        assert_eq!(
            resolve(&StagedUdf::udf(Behavior::CompleteAt(limit)), 1, false)?.len(),
            1
        );
    }
    Ok(())
}

#[test]
fn nonconvergent_parameter_resolution_is_bounded() -> Result<()> {
    let result = resolve(&StagedUdf::udf(Behavior::NeverComplete), 1, false);
    let error = result
        .err()
        .ok_or_else(|| plan_datafusion_err!("nonconvergent parameters accepted"))?;
    assert!(error.to_string().contains("resolution limit"), "{error}");
    Ok(())
}

#[test]
fn parameter_counts_preserve_physical_optional_lambda_prefixes() -> Result<()> {
    for behavior in [
        Behavior::PartialCount(0),
        Behavior::PartialCount(2),
        Behavior::CompleteCount(0),
    ] {
        let error = resolve(&StagedUdf::udf(behavior), 1, false)
            .err()
            .ok_or_else(|| plan_datafusion_err!("invalid parameter count accepted"))?;
        assert!(error.to_string().contains("parameter sets"), "{error}");
    }
    // Physical HigherOrderFunctionExpr permits unused trailing Complete sets,
    // unlike logical lambda resolution. Preserve its existing decoder contract.
    let fun = Arc::new(StagedUdf::udf(Behavior::CompleteCount(2)));
    let args = resolve(fun.as_ref(), 1, false)?;
    let _physical = HigherOrderFunctionExpr::try_new_with_schema(
        fun,
        args,
        &Schema::empty(),
        Arc::new(ConfigOptions::default()),
    )?;
    Ok(())
}

#[test]
fn staged_decode_does_not_invoke_failing_lambda_bodies() -> Result<()> {
    assert_eq!(
        resolve(&StagedUdf::udf(Behavior::RetainNone), 2, true)?.len(),
        2
    );
    Ok(())
}
