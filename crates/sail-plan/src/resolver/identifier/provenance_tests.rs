use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::DataType;
use datafusion::prelude::SessionContext;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{ColumnarValue, Volatility, create_udf};
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::CreateViewOptions;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use super::tests::{context, expression};
use super::*;
use crate::config::PlanConfig;

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn input() -> Result<SessionContext, Box<dyn std::error::Error>> {
    let ctx = context()?;
    ctx.extension::<CatalogManager>()?
        .create_view(
            &["input"],
            CreateViewOptions {
                definition: "SELECT 9 AS foo".into(),
                columns: vec![],
                if_not_exists: false,
                replace: false,
                comment: None,
                properties: vec![],
            },
        )
        .await?;
    Ok(ctx)
}
fn query(expr: &str) -> PlanResult<spec::Plan> {
    Ok(sail_sql_analyzer::statement::from_ast_statement(
        sail_sql_analyzer::parser::parse_one_statement(&format!(
            "SELECT IDENTIFIER({expr}) AS value FROM input"
        ))?,
    )?)
}

#[tokio::test]
async fn scalar_session_functions_keep_nonfoldable_provenance_and_semantic_error_priority()
-> TestResult {
    let ctx = input().await?;
    let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
    for expr in [
        "CASE WHEN false THEN current_catalog() ELSE 'foo' END",
        "CASE WHEN false THEN current_database() ELSE 'foo' END",
        "CASE WHEN false THEN current_timezone() ELSE 'foo' END",
        "CASE WHEN false THEN current_user() ELSE 'foo' END",
        "CASE WHEN false THEN concat(current_catalog(), typeof(rand())) ELSE 'foo' END",
        "CASE WHEN false THEN concat(typeof(rand()), current_catalog()) ELSE 'foo' END",
    ] {
        let error = resolver
            .resolve_named_plan(query(expr)?)
            .await
            .err()
            .ok_or("nonfoldable function was erased")?;
        assert!(
            error
                .to_string()
                .contains("NOT_A_CONSTANT_STRING.NOT_CONSTANT"),
            "{expr}: {error}"
        );
    }
    for expr in [
        "concat(current_catalog(), missing)",
        "concat(missing, current_catalog())",
        "concat(current_catalog(), (SELECT missing FROM input))",
        "typeof(missing)",
        "CASE WHEN false THEN typeof((SELECT missing FROM input)) ELSE 'foo' END",
    ] {
        let error = resolver
            .resolve_named_plan(query(expr)?)
            .await
            .err()
            .ok_or("missing column was accepted")?;
        assert!(
            error
                .to_string()
                .contains("UNRESOLVED_COLUMN.WITH_SUGGESTION"),
            "{expr}: {error}"
        );
    }
    for expr in [
        "CASE WHEN false THEN typeof(current_catalog()) ELSE 'foo' END",
        "CASE WHEN false THEN typeof(concat(current_catalog(), current_database())) ELSE 'foo' END",
        "CASE WHEN false THEN typeof((SELECT foo FROM input)) ELSE 'foo' END",
        "CASE WHEN typeof(current_catalog()) = 'string' THEN 'foo' ELSE 'missing' END",
    ] {
        let named = resolver.resolve_named_plan(query(expr)?).await?;
        assert_eq!(named.fields, Some(vec!["value".into()]));
    }
    Ok(())
}

#[tokio::test]
async fn identifier_constant_context_restores_parameters_and_state_after_errors() -> TestResult {
    let ctx = context()?;
    let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
    let mut state = PlanResolverState::new();
    state.config_mut().arrow_allow_large_var_types = true;
    let schema = Arc::new(DFSchema::empty());
    let mut params = state.enter_param_values_scope(
        HashMap::from([("col".into(), ScalarValue::Utf8(Some("foo".into())))]),
        vec![],
    );
    for (expr, accepted) in [
        (
            "CASE WHEN false THEN current_catalog() ELSE :col END",
            false,
        ),
        (
            "CASE WHEN false THEN typeof(current_catalog()) ELSE :col END",
            true,
        ),
        (
            "CASE WHEN false THEN concat(current_catalog(), typeof(current_database())) ELSE :col END",
            false,
        ),
        (
            "CASE WHEN false THEN typeof(current_database()) ELSE :col END",
            true,
        ),
        (
            "CASE WHEN typeof(current_catalog()) = 'string' THEN :col ELSE 'missing' END",
            true,
        ),
    ] {
        let result = resolver
            .resolve_scalar_identifier_expr(expression(expr)?, &schema, params.state())
            .await;
        if accepted {
            assert_eq!(result?, "foo");
        } else {
            assert!(result.is_err());
        }
        assert!(!params.state().config().constant_context);
        assert!(!params.state().config().require_foldable);
        assert!(!params.state().config().table_identifier);
        assert!(params.state().config().arrow_allow_large_var_types);
        assert!(params.state().get_param_value("col").is_some());
    }
    drop(params);
    assert!(state.get_param_value("col").is_none());
    assert!(!state.config().constant_context);
    Ok(())
}

#[tokio::test]
async fn registered_function_keeps_udf_rules_without_executing_the_function() -> TestResult {
    let ctx = input().await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    ctx.extension::<CatalogManager>()?
        .register_function(create_udf(
            "probe_identity",
            vec![DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some("foo".into()))))
            }),
        ))?;
    let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
    for (expr, condition) in [
        (
            "probe_identity('literal')",
            "NOT_A_CONSTANT_STRING.NOT_CONSTANT",
        ),
        (
            "probe_identity(missing)",
            "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        ),
    ] {
        let error = resolver
            .resolve_named_plan(query(expr)?)
            .await
            .err()
            .ok_or("registered function was folded")?;
        assert!(error.to_string().contains(condition), "{expr}: {error}");
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}
