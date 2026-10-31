use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::ScalarValue;
use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
use sail_catalog::provider::{CatalogProvider, CreateViewOptions};
use sail_catalog_memory::MemoryCatalogProvider;
use sail_common::spec;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::plan::PlanService;

use super::*;
use crate::catalog::SparkCatalogObjectDisplay;
use crate::config::PlanConfig;
use crate::formatter::SparkPlanFormatter;

type TestResult = Result<(), Box<dyn std::error::Error>>;

pub(super) fn context() -> Result<SessionContext, Box<dyn std::error::Error>> {
    let provider = MemoryCatalogProvider::new("catalog".into(), vec!["db"].try_into()?, None);
    let manager = CatalogManager::try_new(CatalogManagerOptions {
        catalogs: HashMap::from([(
            "catalog".into(),
            Arc::new(provider) as Arc<dyn CatalogProvider>,
        )]),
        default_catalog: "catalog".into(),
        default_database: vec!["db".into()],
        global_temporary_database: vec!["global_temp".into()],
    })?;
    let config = SessionConfig::new()
        .with_extension(Arc::new(manager))
        .with_extension(Arc::new(DataSourceRegistry::new()))
        .with_extension(Arc::new(PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        )));
    Ok(SessionContext::new_with_state(
        SessionStateBuilder::new().with_config(config).build(),
    ))
}
pub(super) fn expression(sql: &str) -> PlanResult<spec::Expr> {
    Ok(sail_sql_analyzer::expression::from_ast_expression(
        sail_sql_analyzer::parser::parse_expression(sql)?,
    )?)
}

#[tokio::test]
async fn table_validation_precedes_subquery_analysis_type_evaluation_and_folding() -> TestResult {
    let ctx = context()?;
    let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
    let mut state = PlanResolverState::new();
    state.config_mut().arrow_allow_large_var_types = true;
    for (sql, condition) in [
        ("(SELECT missing FROM absent)", "NOT_CONSTANT"),
        (
            "CAST((SELECT missing FROM absent) AS STRING)",
            "NOT_CONSTANT",
        ),
        ("rand()", "NOT_CONSTANT"),
        ("current_catalog()", "NOT_CONSTANT"),
        ("CAST(rand() AS STRING)", "NOT_CONSTANT"),
        (
            "CASE WHEN false THEN CAST(rand() AS STRING) ELSE 'input' END",
            "NOT_CONSTANT",
        ),
        ("1 DIV 0", "WRONG_TYPE"),
        ("missing", "NOT_CONSTANT"),
        ("typeof(missing)", "NOT_CONSTANT"),
        ("typeof((SELECT missing FROM absent))", "NOT_CONSTANT"),
        ("NULL", "WRONG_TYPE"),
        ("CAST(NULL AS STRING)", "NULL"),
    ] {
        let error = resolver
            .resolve_table_identifier_expr(expression(sql)?, &mut state)
            .await
            .err()
            .ok_or("invalid identifier was accepted")?;
        assert!(
            error
                .to_string()
                .contains(&format!("NOT_A_CONSTANT_STRING.{condition}")),
            "{sql}: {error}"
        );
        assert!(!state.config().require_foldable);
        assert!(!state.config().analyzer_identifier);
        assert!(state.config().arrow_allow_large_var_types);
    }
    assert_eq!(
        resolver
            .resolve_table_identifier_expr(
                expression("CASE WHEN false THEN typeof(rand()) ELSE 'input' END")?,
                &mut state
            )
            .await?,
        "input"
    );
    assert_eq!(
        resolver
            .resolve_table_identifier_expr(expression("concat('in', 'put')")?, &mut state)
            .await?,
        "input"
    );
    assert!(!state.config().require_foldable);
    Ok(())
}

#[tokio::test]
async fn parameter_bindings_survive_guarded_table_and_scalar_evaluation() -> TestResult {
    let ctx = context()?;
    let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
    let mut state = PlanResolverState::new();
    let mut params = state.enter_param_values_scope(
        HashMap::from([
            ("tab".into(), ScalarValue::Utf8(Some("input".into()))),
            ("prefix".into(), ScalarValue::Utf8(Some("in".into()))),
        ]),
        vec![ScalarValue::Utf8(Some("input".into()))],
    );
    for expr in [
        expression(":tab")?,
        expression(":prefix || 'put'")?,
        spec::Expr::Placeholder("$1".into()),
    ] {
        assert_eq!(
            resolver
                .resolve_table_identifier_expr(expr, params.state())
                .await?,
            "input"
        );
        assert!(!params.state().config().require_foldable);
    }
    assert_eq!(
        resolver.evaluate_identifier_expr(
            Expr::Placeholder(datafusion_expr::expr::Placeholder::new_with_field(
                "$1".into(),
                None
            )),
            params.state()
        )?,
        "input"
    );
    assert!(
        resolver
            .resolve_table_identifier_expr(expression(":missing")?, params.state())
            .await
            .is_err()
    );
    assert!(!params.state().config().require_foldable);
    assert!(params.state().get_param_value("tab").is_some());
    drop(params);
    assert!(state.get_param_value("tab").is_none());
    assert!(state.get_positional_param_value(0).is_none());
    Ok(())
}

#[tokio::test]
async fn scalar_identifier_preserves_column_and_subquery_analysis_precedence() -> TestResult {
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
    let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
    for (expr, condition) in [
        ("foo", "NOT_A_CONSTANT_STRING.NOT_CONSTANT"),
        ("missing", "UNRESOLVED_COLUMN.WITH_SUGGESTION"),
        (
            "(SELECT foo FROM input)",
            "NOT_A_CONSTANT_STRING.NOT_CONSTANT",
        ),
        (
            "(SELECT missing FROM input)",
            "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        ),
    ] {
        let sql = format!("SELECT IDENTIFIER({expr}) FROM input");
        let plan = sail_sql_analyzer::statement::from_ast_statement(
            sail_sql_analyzer::parser::parse_one_statement(&sql)?,
        )?;
        let error = resolver
            .resolve_named_plan(plan)
            .await
            .err()
            .ok_or("nonconstant scalar was accepted")?;
        assert!(error.to_string().contains(condition), "{sql}: {error}");
    }
    Ok(())
}
