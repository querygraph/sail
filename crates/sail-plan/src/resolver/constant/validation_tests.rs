use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{DFSchema, Result, ScalarValue, exec_err};
use datafusion_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion_expr::expr::{HigherOrderFunction, LambdaVariable};
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs,
    HigherOrderSignature, HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, ValueOrLambda, Volatility, cast, col,
    create_udf, lambda, lit,
};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_function::scalar::math::div_mod_lazy::SparkDivModLazy;
use sail_function::scalar::math::divisor::SafeNumericDivisor;
use sail_function::scalar::math::zero_division::{SparkZeroDivisionError, ZeroDivisionKind};

use super::super::PlanResolver;
use crate::function::common::ScalarFunctionBuilder;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

fn parameter(name: &str, value: &Expr) -> Result<Expr> {
    let field = value.to_field(&DFSchema::empty())?.1;
    Ok(Expr::LambdaVariable(LambdaVariable::new(
        name.to_owned(),
        Some(Arc::new(field.as_ref().clone().with_name(name))),
    )))
}

fn internal(left: Expr, right: Expr, operation: Expr, ansi: bool) -> Expr {
    Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::new(HigherOrderUDF::new_from_impl(SparkDivModLazy::new(ansi))),
        vec![
            lambda(["unused_right"], right),
            lambda(["unused_left"], left),
            lambda(["_left_value", "_right_value"], operation),
        ],
    ))
}

fn division(left: Expr, right: Expr, ansi: bool) -> Result<Expr> {
    let operation = parameter("_left_value", &left)?
        / ScalarUDF::from(SafeNumericDivisor::new(ZeroDivisionKind::Divide, ansi))
            .call(vec![parameter("_right_value", &right)?]);
    Ok(internal(left, right, operation, ansi))
}

fn rejected(expression: &Expr) -> TestResult {
    let error = PlanResolver::require_constant_expression(expression)
        .err()
        .ok_or("expected constant-expression rejection")?;
    assert!(
        error
            .to_string()
            .contains("NOT_A_CONSTANT_STRING.NOT_CONSTANT"),
        "{error}"
    );
    Ok(())
}

#[test]
fn internal_constants_validate_then_evaluate_with_existing_literal_path() -> TestResult {
    let expression = division(lit(4), lit(2), true)?;
    PlanResolver::require_constant_expression(&expression)?;
    assert_eq!(expression.get_type(&DFSchema::empty())?, DataType::Int32);
    let string = cast(expression, DataType::Utf8);
    PlanResolver::require_constant_expression(&string)?;
    assert_eq!(
        LiteralEvaluator::new().evaluate(&string)?,
        ScalarValue::Utf8(Some("2".to_owned()))
    );
    Ok(())
}

#[test]
fn nested_evaluators_shadow_names_and_preserve_outer_bindings() -> TestResult {
    let left = lit(8);
    let right = lit(2);
    let inner = division(parameter("_left_value", &left)?, lit(2), true)?;
    let expression = internal(
        left,
        right.clone(),
        inner + parameter("_right_value", &right)?,
        true,
    );
    PlanResolver::require_constant_expression(&expression)?;
    assert_eq!(
        LiteralEvaluator::new().evaluate(&expression)?,
        ScalarValue::Int32(Some(6))
    );
    Ok(())
}

#[test]
fn lambda_bindings_end_at_each_body_and_argument() -> TestResult {
    let valid = division(lit(8), lit(2), true)?;
    rejected(&(valid + parameter("_left_value", &lit(8))?))?;
    let sibling = internal(parameter("unused_right", &lit(8))?, lit(2), lit(7), true);
    rejected(&sibling)?;
    for bad in [
        col("missing"),
        parameter("free", &lit(1))?,
        lambda(["x"], lit(1)),
    ] {
        rejected(&bad)?;
        rejected(&internal(bad.clone(), lit(2), lit(7), true))?;
        rejected(&internal(lit(8), lit(2), bad, true))?;
    }
    Ok(())
}

#[test]
fn volatile_and_provenance_descendants_reject_without_invocation() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let volatile = create_udf(
        "counted",
        vec![],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(1))))
        }),
    )
    .call(vec![]);
    for bad in [volatile, ScalarFunctionBuilder::mark_non_foldable(lit(7))] {
        for expression in [
            internal(bad.clone(), lit(2), lit(7), true),
            internal(lit(8), bad.clone(), lit(7), true),
            internal(lit(8), lit(2), bad, true),
        ] {
            rejected(&expression)?;
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct AsyncConstant(Signature);

impl ScalarUDFImpl for AsyncConstant {
    fn name(&self) -> &str {
        "async_constant_fixture"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn return_type(&self, _types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("must not invoke async fixture")
    }
}

#[async_trait::async_trait]
impl AsyncScalarUDFImpl for AsyncConstant {
    async fn invoke_async_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("must not invoke async fixture")
    }
}

#[test]
fn immutable_async_functions_remain_nonconstant_inside_each_scope() -> TestResult {
    let function = AsyncScalarUDF::new(Arc::new(AsyncConstant(Signature::any(
        0,
        Volatility::Immutable,
    ))))
    .into_scalar_udf();
    assert!(function.as_async().is_some());
    assert_eq!(function.signature().volatility, Volatility::Immutable);
    let call = function.call(vec![]);
    for expression in [
        call.clone(),
        internal(call.clone(), lit(2), lit(7), true),
        internal(lit(8), call.clone(), lit(7), true),
        internal(lit(8), lit(2), call, true),
    ] {
        rejected(&expression)?;
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SameName(SparkDivModLazy);

impl HigherOrderUDFImpl for SameName {
    fn name(&self) -> &str {
        "spark_divmod_lazy"
    }
    fn signature(&self) -> &HigherOrderSignature {
        self.0.signature()
    }
    fn lambda_parameters(
        &self,
        step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        self.0.lambda_parameters(step, fields)
    }
    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        self.0.return_field_from_args(args)
    }
    fn invoke_with_args(&self, _args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        exec_err!("must not invoke spoofed function")
    }
}

#[test]
fn only_exact_evaluator_and_direct_argument_shapes_are_admitted() -> TestResult {
    let valid = division(lit(8), lit(2), true)?;
    let Expr::HigherOrderFunction(original) = valid else {
        return Err("expected higher-order fixture".into());
    };
    let spoofed = Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::new(HigherOrderUDF::new_from_impl(SameName(
            SparkDivModLazy::new(true),
        ))),
        original.args.clone(),
    ));
    rejected(&spoofed)?;
    let mut missing = original.clone();
    missing.args.pop();
    rejected(&Expr::HigherOrderFunction(missing))?;
    let mut value_arg = original.clone();
    value_arg.args[0] = lit(2);
    rejected(&Expr::HigherOrderFunction(value_arg))?;
    let mut indirect_lambda = original;
    indirect_lambda.args[0] = indirect_lambda.args[0].clone().alias("lambda_alias");
    rejected(&Expr::HigherOrderFunction(indirect_lambda))?;
    Ok(())
}

#[test]
fn eligible_expressions_preserve_type_null_and_error_evaluation_phases() -> TestResult {
    let fails = division(lit(1), lit(0), true)?;
    PlanResolver::require_constant_expression(&fails)?;
    assert_eq!(fails.get_type(&DFSchema::empty())?, DataType::Int32);
    let error = LiteralEvaluator::new()
        .evaluate(&fails)
        .err()
        .ok_or("expected execution error")?;
    assert!(error.to_string().contains("DIVIDE_BY_ZERO"), "{error}");
    let null = division(lit(ScalarValue::Int32(None)), lit(0), true)?;
    PlanResolver::require_constant_expression(&null)?;
    assert_eq!(
        LiteralEvaluator::new().evaluate(&null)?,
        ScalarValue::Int32(None)
    );
    let left_error =
        ScalarUDF::from(SparkZeroDivisionError::new(ZeroDivisionKind::Divide)).call(vec![]);
    let skipped = internal(left_error, lit(ScalarValue::Int32(None)), lit(7), true);
    PlanResolver::require_constant_expression(&skipped)?;
    assert_eq!(
        LiteralEvaluator::new().evaluate(&skipped)?,
        ScalarValue::Int32(None)
    );
    Ok(())
}
