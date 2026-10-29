use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{ColumnarValue, Volatility, create_udf, lit};
use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
use sail_catalog::provider::CatalogProvider;
use sail_catalog_memory::MemoryCatalogProvider;
use sail_common::spec;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::session::plan::PlanService;
use sail_sql_analyzer::expression::from_ast_expression;
use sail_sql_analyzer::parser::parse_expression;

use super::*;
use crate::catalog::SparkCatalogObjectDisplay;
use crate::config::PlanConfig;
use crate::formatter::SparkPlanFormatter;
use crate::function::common::{FunctionContextInput, ScalarFunctionBuilder, ScalarFunctionInput};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn catalog_expression_command_emits_the_existing_catalog_operation() -> TestResult {
    let context = context()?;
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    let named = resolver
        .resolve_named_plan(spec::Plan::Command(spec::CommandPlan::new(
            spec::CommandNode::SetCurrentCatalogExpression {
                catalog: expression("CONCAT('cat', 'alog')")?,
            },
        )))
        .await?;
    assert!(named.fields.is_none());
    let datafusion_expr::LogicalPlan::Extension(extension) = named.plan else {
        return Err(PlanError::internal("expected catalog command").into());
    };
    let command = extension
        .node
        .as_any()
        .downcast_ref::<crate::catalog::CatalogCommandNode>()
        .ok_or_else(|| PlanError::internal("expected catalog command node"))?;
    assert_eq!(
        command.command(),
        &sail_catalog::command::CatalogCommand::SetCurrentCatalog {
            catalog: "catalog".into(),
        }
    );
    assert_eq!(
        context
            .extension::<CatalogManager>()?
            .default_catalog()?
            .as_ref(),
        "catalog"
    );
    Ok(())
}

fn context() -> Result<SessionContext, Box<dyn std::error::Error>> {
    let provider = MemoryCatalogProvider::new("catalog".into(), vec!["db"].try_into()?, None);
    let catalog = CatalogManager::try_new(CatalogManagerOptions {
        catalogs: HashMap::from([(
            "catalog".into(),
            Arc::new(provider) as Arc<dyn CatalogProvider>,
        )]),
        default_catalog: "catalog".into(),
        default_database: vec!["db".into()],
        global_temporary_database: vec!["global_temp".into()],
    })?;
    let config = SessionConfig::new()
        .with_extension(Arc::new(catalog))
        .with_extension(Arc::new(PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        )));
    Ok(SessionContext::new_with_state(
        SessionStateBuilder::new().with_config(config).build(),
    ))
}

fn expression(sql: &str) -> PlanResult<spec::Expr> {
    Ok(from_ast_expression(parse_expression(sql)?)?)
}

#[tokio::test]
async fn catalog_names_use_shared_expression_and_identifier_resolution() -> TestResult {
    let context = context()?;
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    for (sql, expected) in [
        ("catalog", "catalog"),
        ("'catalog'", "catalog"),
        ("CONCAT('cat', 'alog')", "catalog"),
        ("CAST('catalog' AS STRING)", "catalog"),
        ("LOWER('CATALOG')", "catalog"),
        ("CONCAT('cat', CAST(2 / 1 AS INT))", "cat2"),
        ("'`test.cat`'", "test.cat"),
        ("`test.cat`", "test.cat"),
        ("'`a``b`'", "a`b"),
        ("'``'", ""),
        ("IDENTIFIER('catalog')", "catalog"),
        ("IDENTIFIER(CONCAT('cat', 'alog'))", "catalog"),
        ("CONCAT('not_', 'configured')", "not_configured"),
    ] {
        assert_eq!(
            resolver.resolve_catalog_name(expression(sql)?).await?,
            expected,
            "{sql}"
        );
    }
    let catalog = context.extension::<CatalogManager>()?;
    assert_eq!(catalog.default_catalog()?.as_ref(), "catalog");
    assert_eq!(Vec::<String>::from(catalog.default_database()?), vec!["db"]);
    Ok(())
}

#[tokio::test]
async fn catalog_validation_preserves_foldability_type_and_null_priority() -> TestResult {
    let context = context()?;
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    for (sql, marker) in [
        ("1", "WRONG_TYPE"),
        ("NULL", "WRONG_TYPE"),
        ("1 / 0", "WRONG_TYPE"),
        ("1 DIV 0", "WRONG_TYPE"),
        ("1 % 0", "WRONG_TYPE"),
        ("CAST(NULL AS STRING)", "NOT_A_CONSTANT_STRING.NULL"),
        ("'test.cat'", "MULTI_PART_NAME"),
        ("test.cat", "MULTI_PART_NAME"),
        ("CONCAT(foo, '1')", "foo"),
        ("CAST(rand() AS STRING)", "NOT_CONSTANT"),
        ("typeof(rand())", "NOT_CONSTANT"),
        (
            "COALESCE('catalog', CAST(rand() AS STRING))",
            "NOT_CONSTANT",
        ),
        ("current_catalog()", "NOT_CONSTANT"),
        ("current_database()", "NOT_CONSTANT"),
        ("current_schema()", "NOT_CONSTANT"),
        ("current_user()", "NOT_CONSTANT"),
        ("session_user()", "NOT_CONSTANT"),
        ("user()", "NOT_CONSTANT"),
        ("current_timezone()", "NOT_CONSTANT"),
        (
            "CONCAT_WS('', TRANSFORM(ARRAY('cat', 'alog'), x -> x))",
            "NOT_CONSTANT",
        ),
        ("explode(ARRAY('catalog'))", "NOT_CONSTANT"),
    ] {
        let error = resolver
            .resolve_catalog_name(expression(sql)?)
            .await
            .err()
            .ok_or_else(|| PlanError::internal(format!("expected rejection: {sql}")))?;
        assert!(
            matches!(error, PlanError::AnalysisError(_)),
            "{sql}: {error}"
        );
        assert!(error.to_string().contains(marker), "{sql}: {error}");
    }
    assert!(
        resolver
            .resolve_catalog_name(expression("''")?)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn catalog_time_expressions_use_query_scoped_simplification() -> TestResult {
    let context = context()?;
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    let name = resolver
        .resolve_catalog_name(expression(
            "CONCAT('`', CAST(current_timestamp() AS STRING), '`')",
        )?)
        .await?;
    chrono::NaiveDateTime::parse_from_str(&name, "%Y-%m-%d %H:%M:%S%.f")?;
    Ok(())
}

#[tokio::test]
async fn catalog_scalar_errors_follow_the_selected_ansi_mode() -> TestResult {
    let context = context()?;
    let sql = "CAST(CAST('not-an-int' AS INT) AS STRING)";
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    let error = resolver
        .resolve_catalog_name(expression(sql)?)
        .await
        .err()
        .ok_or_else(|| PlanError::internal("expected ANSI cast failure"))?;
    assert!(matches!(error, PlanError::DataFusionError(_)), "{error}");
    let config = PlanConfig {
        ansi_mode: false,
        ..Default::default()
    };
    let resolver = PlanResolver::new(&context, Arc::new(config));
    let error = resolver
        .resolve_catalog_name(expression(sql)?)
        .await
        .err()
        .ok_or_else(|| PlanError::internal("expected typed-null failure"))?;
    assert!(matches!(error, PlanError::AnalysisError(_)), "{error}");
    assert!(error.to_string().contains("NOT_A_CONSTANT_STRING.NULL"));
    Ok(())
}

#[tokio::test]
async fn constant_mode_does_not_leak_into_ordinary_resolution() -> TestResult {
    let context = context()?;
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    assert!(
        resolver
            .resolve_catalog_name(expression("current_catalog()")?)
            .await
            .is_err()
    );
    let resolved = resolver
        .resolve_expression(
            expression("current_catalog()")?,
            &Arc::new(DFSchema::empty()),
            &mut PlanResolverState::new(),
        )
        .await?;
    assert_eq!(
        LiteralEvaluator::new().evaluate(&resolved)?,
        ScalarValue::Utf8(Some("catalog".into()))
    );
    Ok(())
}

#[tokio::test]
async fn constant_mode_rejects_registered_functions_before_invocation() -> TestResult {
    let context = context()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    context
        .extension::<CatalogManager>()?
        .register_function(create_udf(
            "canary",
            vec![],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "catalog".into(),
                ))))
            }),
        ))?;
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    assert!(
        resolver
            .resolve_catalog_name(expression("CONCAT(canary(), '')")?)
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let resolved = resolver
        .resolve_expression(
            expression("canary()")?,
            &Arc::new(DFSchema::empty()),
            &mut PlanResolverState::new(),
        )
        .await?;
    assert_eq!(
        LiteralEvaluator::new().evaluate(&resolved)?,
        ScalarValue::Utf8(Some("catalog".into()))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn subqueries_are_rejected_before_requiring_session_extensions() -> TestResult {
    let context = SessionContext::new();
    let resolver = PlanResolver::new(&context, Arc::new(PlanConfig::default()));
    let error = resolver
        .resolve_catalog_name(expression("(SELECT 'catalog')")?)
        .await
        .err()
        .ok_or_else(|| PlanError::internal("expected subquery rejection"))?;
    assert!(error.to_string().contains("NOT_CONSTANT"), "{error}");
    Ok(())
}

#[test]
fn non_foldable_annotation_prevents_builder_invocation() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let builder = ScalarFunctionBuilder::non_foldable(ScalarFunctionBuilder::custom(move |_| {
        observed.fetch_add(1, Ordering::SeqCst);
        Ok(lit("catalog"))
    }));
    let context = SessionContext::new();
    let config = Arc::new(PlanConfig::default());
    let schema = Arc::new(DFSchema::empty());
    for require_foldable in [true, false] {
        let result = builder(ScalarFunctionInput {
            arguments: vec![],
            function_context: FunctionContextInput {
                require_foldable,
                argument_display_names: &[],
                plan_config: &config,
                session_context: &context,
                schema: &schema,
            },
        });
        assert_eq!(result.is_err(), require_foldable);
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(!require_foldable));
    }
    Ok(())
}
