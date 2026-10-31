use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Volatility, cast,
    create_udf, lit,
};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_logical_optimizer::type_coercion::TypeCoercion;

use super::*;
use crate::config::PlanConfig;
use crate::function::constant_context::ConstantContext;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn lower(arguments: Vec<Expr>) -> PlanResult<Expr> {
    nvl2(ScalarFunctionInput {
        arguments,
        function_context: FunctionContextInput {
            argument_display_names: &[],
            plan_config: &Arc::new(PlanConfig::default()),
            session_context: &SessionContext::new(),
            schema: &Arc::new(DFSchema::empty()),
            require_foldable: false,
            constant_context: ConstantContext::Ordinary,
        },
    })
}

fn analyze(expression: Expr) -> Result<Expr, Box<dyn std::error::Error>> {
    let plan = LogicalPlanBuilder::empty(true)
        .project([expression])?
        .build()?;
    let plan = TypeCoercion::new().analyze(plan, &ConfigOptions::default())?;
    let LogicalPlan::Projection(projection) = plan else {
        return Err("expected projection".into());
    };
    Ok(projection.expr[0].clone())
}

#[test]
fn nvl2_test_type_does_not_participate_in_branch_coercion() -> TestResult {
    for (tested, expected) in [
        (lit("sentinel"), 1),
        (lit(true), 1),
        (lit(ScalarValue::Binary(Some(vec![1]))), 1),
        (lit(ScalarValue::Utf8(None)), 2),
    ] {
        let lowered = lower(vec![tested, lit(1_i32), lit(2_i32)])?;
        assert!(matches!(lowered, Expr::Case(_)));
        let analyzed = analyze(lowered)?;
        assert_eq!(
            LiteralEvaluator::new().evaluate(&analyzed)?,
            ScalarValue::Int32(Some(expected))
        );
    }
    let widened = lower(vec![lit("sentinel"), lit(1_i32), lit(2_i64)])?;
    assert_eq!(widened.get_type(&DFSchema::empty())?, DataType::Int64);
    assert_eq!(
        LiteralEvaluator::new().evaluate(&widened)?,
        ScalarValue::Int64(Some(1))
    );
    let decimal = lower(vec![
        lit("sentinel"),
        lit(1_i32),
        lit(ScalarValue::Decimal128(Some(25), 2, 1)),
    ])?;
    assert_eq!(
        decimal.get_type(&DFSchema::empty())?,
        DataType::Decimal128(11, 1)
    );
    assert_eq!(
        LiteralEvaluator::new().evaluate(&decimal)?,
        ScalarValue::Decimal128(Some(10), 11, 1)
    );
    Ok(())
}

#[test]
fn nvl2_skips_bad_branches_and_evaluates_test_once() -> TestResult {
    let bad = cast(lit("bad"), DataType::Int32);
    for (tested, yes, no, expected) in [
        (lit("sentinel"), lit(1_i32), bad.clone(), 1),
        (lit(ScalarValue::Utf8(None)), bad, lit(2_i32), 2),
    ] {
        let analyzed = analyze(lower(vec![tested, yes, no])?)?;
        assert_eq!(
            LiteralEvaluator::new().evaluate(&analyzed)?,
            ScalarValue::Int32(Some(expected))
        );
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let tested = create_udf(
        "nvl2_test_once",
        vec![],
        DataType::Utf8,
        Volatility::Volatile,
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "sentinel".into(),
            ))))
        }),
    )
    .call(vec![]);
    let analyzed = analyze(lower(vec![tested, lit(1_i32), lit(2_i32)])?)?;
    assert_eq!(
        LiteralEvaluator::new().evaluate(&analyzed)?,
        ScalarValue::Int32(Some(1))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}
