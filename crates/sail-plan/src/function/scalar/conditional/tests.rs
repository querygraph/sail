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

type ConditionalBuilder = for<'a> fn(ScalarFunctionInput<'a>) -> PlanResult<Expr>;

fn lower(arguments: Vec<Expr>) -> PlanResult<Expr> {
    lower_with(nvl2, arguments)
}

fn lower_with(builder: ConditionalBuilder, arguments: Vec<Expr>) -> PlanResult<Expr> {
    builder(ScalarFunctionInput {
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

#[test]
fn if_and_case_publish_common_types_before_analysis() -> TestResult {
    let builders: [ConditionalBuilder; 2] = [if_expr, case];
    for builder in builders {
        for condition in [Some(true), Some(false), None] {
            for (left, right, expected_type, yes, no) in [
                (
                    lit(1_i32),
                    lit(2_i64),
                    DataType::Int64,
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int64(Some(2)),
                ),
                (
                    lit(2_i64),
                    lit(1_i32),
                    DataType::Int64,
                    ScalarValue::Int64(Some(2)),
                    ScalarValue::Int64(Some(1)),
                ),
                (
                    lit(1_i32),
                    lit(ScalarValue::Decimal128(Some(25), 2, 1)),
                    DataType::Decimal128(11, 1),
                    ScalarValue::Decimal128(Some(10), 11, 1),
                    ScalarValue::Decimal128(Some(25), 11, 1),
                ),
            ] {
                let lowered = lower_with(
                    builder,
                    vec![lit(ScalarValue::Boolean(condition)), left, right],
                )?;
                assert_eq!(lowered.get_type(&DFSchema::empty())?, expected_type);
                let expected = if condition == Some(true) { yes } else { no };
                assert_eq!(LiteralEvaluator::new().evaluate(&lowered)?, expected);
                assert_eq!(
                    analyze(lowered)?.get_type(&DFSchema::empty())?,
                    expected_type
                );
            }
        }
    }
    Ok(())
}

#[test]
fn case_without_else_coerces_all_branches_and_keeps_typed_null() -> TestResult {
    for (second_condition, expected) in [
        (true, ScalarValue::Int64(Some(2))),
        (false, ScalarValue::Int64(None)),
    ] {
        let lowered = lower_with(
            case,
            vec![lit(false), lit(1_i32), lit(second_condition), lit(2_i64)],
        )?;
        assert_eq!(lowered.get_type(&DFSchema::empty())?, DataType::Int64);
        assert!(lowered.nullable(&DFSchema::empty())?);
        assert_eq!(LiteralEvaluator::new().evaluate(&lowered)?, expected);
    }
    let lowered = lower_with(
        case,
        vec![lit(false), lit(1_i32), lit(false), lit(2_i64), lit(3_i32)],
    )?;
    assert_eq!(lowered.get_type(&DFSchema::empty())?, DataType::Int64);
    assert_eq!(
        LiteralEvaluator::new().evaluate(&lowered)?,
        ScalarValue::Int64(Some(3))
    );
    Ok(())
}

#[test]
fn conditional_coercion_does_not_evaluate_branch_values() -> TestResult {
    let builders: [ConditionalBuilder; 2] = [if_expr, case];
    for builder in builders {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let bad = create_udf(
            "conditional_branch_error",
            vec![],
            DataType::Int64,
            Volatility::Immutable,
            Arc::new(move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                datafusion_common::exec_err!("selected conditional branch")
            }),
        )
        .call(vec![]);
        let lowered = lower_with(builder, vec![lit(true), lit(1_i32), bad.clone()])?;
        assert_eq!(lowered.get_type(&DFSchema::empty())?, DataType::Int64);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            LiteralEvaluator::new().evaluate(&lowered)?,
            ScalarValue::Int64(Some(1))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let selected = lower_with(builder, vec![lit(false), lit(1_i32), bad])?;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let error = LiteralEvaluator::new()
            .evaluate(&selected)
            .err()
            .ok_or("expected selected branch error")?;
        assert!(error.to_string().contains("selected conditional branch"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[test]
fn case_else_preserves_static_nullability_and_branch_values() -> TestResult {
    for (arguments, nullable, expected) in [
        (
            vec![lit(true), lit(1_i32), lit(2_i32)],
            false,
            ScalarValue::Int32(Some(1)),
        ),
        (vec![lit(false), lit(1_i32)], true, ScalarValue::Int32(None)),
        (
            vec![lit(true), lit(ScalarValue::Int32(None)), lit(2_i32)],
            true,
            ScalarValue::Int32(None),
        ),
        (
            vec![lit(false), lit(1_i32), lit(ScalarValue::Int32(None))],
            true,
            ScalarValue::Int32(None),
        ),
        (
            vec![lit(false), lit(1_i32), lit(false), lit(2_i64), lit(3_i32)],
            false,
            ScalarValue::Int64(Some(3)),
        ),
    ] {
        let lowered = lower_with(case, arguments)?;
        assert_eq!(lowered.nullable(&DFSchema::empty())?, nullable);
        assert_eq!(lowered.get_type(&DFSchema::empty())?, expected.data_type());
        assert_eq!(LiteralEvaluator::new().evaluate(&lowered)?, expected);
    }
    Ok(())
}
