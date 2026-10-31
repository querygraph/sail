use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::DataFusionError;
use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
use sail_catalog::provider::CatalogProvider;
use sail_catalog_memory::MemoryCatalogProvider;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::plan::PlanService;

use super::*;
use crate::catalog::SparkCatalogObjectDisplay;
use crate::config::PlanConfig;
use crate::formatter::SparkPlanFormatter;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn native_ambiguity_matches_actual_spark_421_diagnostics_and_identity() -> TestResult {
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
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_extension(Arc::new(catalog))
            .with_extension(Arc::new(PlanService::new(
                Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
                Box::new(SparkPlanFormatter),
            ))),
    );
    // Independent public Spark observations, including duplicate display names,
    // requested spelling, nested roots, malformed names and UTF-16 ordering.
    for (sql, sensitive, condition, state, parameters) in [
        (
            "SELECT Foo FROM (SELECT 1 AS Foo, 2 AS foo) q",
            false,
            "AMBIGUOUS_REFERENCE",
            "42704",
            HashMap::from([
                ("referenceNames".into(), "[`q`.`Foo`, `q`.`Foo`]".into()),
                ("name".into(), "`Foo`".into()),
            ]),
        ),
        (
            "SELECT p.Foo FROM (SELECT 1 AS Foo, 2 AS foo) p",
            false,
            "AMBIGUOUS_REFERENCE",
            "42704",
            HashMap::from([
                ("referenceNames".into(), "[`p`.`Foo`, `p`.`Foo`]".into()),
                ("name".into(), "`p`.`Foo`".into()),
            ]),
        ),
        (
            "SELECT foo FROM (SELECT 1 AS foo) z CROSS JOIN (SELECT 2 AS foo) a",
            false,
            "AMBIGUOUS_REFERENCE",
            "42704",
            HashMap::from([
                ("referenceNames".into(), "[`a`.`foo`, `z`.`foo`]".into()),
                ("name".into(), "`foo`".into()),
            ]),
        ),
        (
            "SELECT q.s.x FROM (SELECT named_struct('x',1) AS s,named_struct('x',2) AS s) q",
            false,
            "AMBIGUOUS_REFERENCE",
            "42704",
            HashMap::from([
                ("referenceNames".into(), "[`q`.`s`, `q`.`s`]".into()),
                ("name".into(), "`q`.`s`.`x`".into()),
            ]),
        ),
        (
            "SELECT `A.B` FROM (SELECT 1 AS `A.B`,2 AS `a.b`) q",
            false,
            "AMBIGUOUS_REFERENCE",
            "42704",
            HashMap::from([
                ("referenceNames".into(), "[`q`.`A.B`, `q`.`A.B`]".into()),
                ("name".into(), "`A.B`".into()),
            ]),
        ),
        (
            "SELECT `A``B` FROM (SELECT 1 AS `A``B`,2 AS `a``b`) q",
            false,
            "INVALID_ATTRIBUTE_NAME_SYNTAX",
            "42601",
            HashMap::from([("name".into(), "A`B".into())]),
        ),
        (
            "SELECT foo FROM (SELECT 1 AS foo) `` CROSS JOIN (SELECT 2 AS foo) `𐀀`",
            false,
            "AMBIGUOUS_REFERENCE",
            "42704",
            HashMap::from([
                ("referenceNames".into(), "[`𐀀`.`foo`, ``.`foo`]".into()),
                ("name".into(), "`foo`".into()),
            ]),
        ),
        (
            "SELECT Foo FROM (SELECT 1 AS Foo,2 AS foo)",
            false,
            "AMBIGUOUS_REFERENCE",
            "42704",
            HashMap::from([
                ("referenceNames".into(), "[`Foo`, `Foo`]".into()),
                ("name".into(), "`Foo`".into()),
            ]),
        ),
    ] {
        let resolver = PlanResolver::new(
            &context,
            Arc::new(PlanConfig {
                case_sensitive: sensitive,
                ..Default::default()
            }),
        );
        let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
        let plan = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        let error = resolver
            .resolve_named_plan(plan)
            .await
            .err()
            .ok_or("ambiguous query accepted")?;
        let error = match error {
            PlanError::Structured(error) => *error,
            PlanError::DataFusionError(DataFusionError::External(error)) => *error
                .downcast::<StructuredSparkError>()
                .map_err(|error| error.to_string())?,
            error => return Err(format!("{sql}: unstructured error: {error}").into()),
        };
        assert_eq!(
            error.class, "org.apache.spark.sql.AnalysisException",
            "{sql}"
        );
        assert_eq!(error.condition, condition, "{sql}");
        assert_eq!(error.sql_state, state, "{sql}");
        assert_eq!(error.parameters, parameters, "{sql}");
    }
    {
        let resolver = PlanResolver::new(
            &context,
            Arc::new(PlanConfig {
                case_sensitive: false,
                ..Default::default()
            }),
        );
        let ast = sail_sql_analyzer::parser::parse_one_statement(
            "SELECT foo FROM (SELECT a.foo,a.foo FROM (SELECT 1 AS foo) a) q",
        )?;
        let plan = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        assert!(resolver.resolve_named_plan(plan).await.is_ok());
    }
    {
        let resolver = PlanResolver::new(
            &context,
            Arc::new(PlanConfig {
                case_sensitive: true,
                ..Default::default()
            }),
        );
        let ast = sail_sql_analyzer::parser::parse_one_statement(
            "SELECT Foo FROM (SELECT 1 AS Foo,2 AS foo) q",
        )?;
        let plan = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        assert!(resolver.resolve_named_plan(plan).await.is_ok());
    }
    Ok(())
}
