use std::sync::Arc;

use arrow::error::ArrowError;
use datafusion_common::DFSchema;

use super::tests::{context, expression};
use super::*;
use crate::config::PlanConfig;
use crate::function::constant_context::ConstantContext;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn constant_type_validation_precedes_arithmetic_and_restores_ordinary_policy() -> TestResult {
    let ctx = context()?;
    for ansi_mode in [true, false] {
        let resolver = PlanResolver::new(
            &ctx,
            Arc::new(PlanConfig {
                ansi_mode,
                ..Default::default()
            }),
        );
        let mut state = PlanResolverState::new();
        let schema = Arc::new(DFSchema::empty());
        for sql in ["1 / 0", "1 DIV 0", "1 % 0"] {
            let error = resolver
                .resolve_scalar_identifier_expr(expression(sql)?, &schema, &mut state)
                .await
                .err()
                .ok_or("numeric identifier was accepted")?;
            assert!(
                error
                    .to_string()
                    .contains("NOT_A_CONSTANT_STRING.WRONG_TYPE"),
                "ansi={ansi_mode} {sql}: {error}"
            );
            assert_eq!(state.config().constant_context, ConstantContext::Ordinary);
            assert!(!state.config().require_foldable);
        }
        // The datatype must remain BIGINT even when ANSI mode would evaluate
        // division by zero to NULL. TypeOf does not evaluate its argument.
        assert_eq!(
            resolver
                .resolve_scalar_identifier_expr(
                    expression(
                        "CASE WHEN typeof(1 DIV 0) = 'bigint' THEN 'foo' ELSE 'missing' END"
                    )?,
                    &schema,
                    &mut state
                )
                .await?,
            "foo"
        );
        assert_eq!(state.config().constant_context, ConstantContext::Ordinary);
        let plan = sail_sql_analyzer::statement::from_ast_statement(
            sail_sql_analyzer::parser::parse_one_statement("SELECT 1 DIV 0")?,
        )?;
        let ordinary = resolver.resolve_named_plan(plan).await;
        if ansi_mode {
            assert!(matches!(
                ordinary,
                Err(PlanError::ArrowError(ArrowError::DivideByZero))
            ));
        } else {
            assert!(ordinary.is_ok());
        }
    }
    Ok(())
}
