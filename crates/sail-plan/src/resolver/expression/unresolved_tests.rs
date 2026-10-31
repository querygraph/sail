use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::plan::PlanService;

use super::*;
use crate::catalog::SparkCatalogObjectDisplay;
use crate::config::PlanConfig;
use crate::formatter::SparkPlanFormatter;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn native_sql_matches_spark_421_missing_column_parameters() -> TestResult {
    let context = SessionContext::new_with_config(SessionConfig::new().with_extension(Arc::new(
        PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        ),
    )));
    let resolver = PlanResolver::new(
        &context,
        Arc::new(PlanConfig {
            case_sensitive: true,
            ..Default::default()
        }),
    );
    // Exact public Spark 4.2.1 observations, including qualifier stripping,
    // stable distance ties, five proposals, quoted names and supplementary Unicode.
    for (sql, object, proposal) in [
        (
            "SELECT Foo FROM (SELECT 1 AS foo) a",
            "`Foo`",
            Some("`foo`"),
        ),
        ("SELECT missing", "`missing`", None),
        (
            "SELECT z1.Foo FROM (SELECT 1 AS foo) z1 CROSS JOIN (SELECT 2 AS bar) a_insensitive CROSS JOIN (SELECT 3 AS foo) z2",
            "`z1`.`Foo`",
            Some("`z1`.`foo`, `z2`.`foo`, `a_insensitive`.`bar`"),
        ),
        (
            "SELECT ext.Foo FROM (SELECT 1 AS foo) z_sensitive CROSS JOIN (SELECT 2 AS foo) ext",
            "`ext`.`Foo`",
            Some("`ext`.`foo`, `z_sensitive`.`foo`"),
        ),
        (
            "SELECT missing FROM (SELECT 1 AS aaa,2 AS bbb,3 AS ccc,4 AS ddd,5 AS eee,6 AS fff,7 AS ggg) q",
            "`missing`",
            Some("`aaa`, `bbb`, `ccc`, `ddd`, `eee`"),
        ),
        (
            "SELECT `A.B` FROM (SELECT 1 AS `a.b`) q",
            "`A.B`",
            Some("`a.b`"),
        ),
        (
            "SELECT `A``B` FROM (SELECT 1 AS `a``b`) q",
            "`A``B`",
            Some("`a``b`"),
        ),
        (
            "SELECT `😀` FROM (SELECT 1 AS `😁`,2 AS a,3 AS `雪`) q",
            "`😀`",
            Some("`😁`, `a`, `雪`"),
        ),
        (
            "SELECT q.Foo FROM (SELECT 1 AS foo,2 AS bar) q",
            "`q`.`Foo`",
            Some("`q`.`foo`, `q`.`bar`"),
        ),
        (
            "SELECT x.foo FROM (SELECT 1 AS foo,2 AS bar) q",
            "`x`.`foo`",
            Some("`q`.`foo`, `q`.`bar`"),
        ),
        (
            "SELECT Foo FROM (SELECT 1 AS foo) q CROSS JOIN (SELECT 2 AS bar) b",
            "`Foo`",
            Some("`q`.`foo`, `b`.`bar`"),
        ),
    ] {
        let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
        let plan = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        let error = resolver
            .resolve_named_plan(plan)
            .await
            .err()
            .ok_or("missing column accepted")?;
        let PlanError::Structured(error) = error else {
            return Err(format!("{sql}: unstructured error: {error}").into());
        };
        assert_eq!(
            error.class, "org.apache.spark.sql.AnalysisException",
            "{sql}"
        );
        assert_eq!(error.sql_state, "42703", "{sql}");
        assert_eq!(error.parameters["objectName"], object, "{sql}");
        match proposal {
            Some(proposal) => {
                assert_eq!(
                    error.condition, "UNRESOLVED_COLUMN.WITH_SUGGESTION",
                    "{sql}"
                );
                assert_eq!(error.parameters["proposal"], proposal, "{sql}");
                assert_eq!(error.parameters.len(), 2);
            }
            None => {
                assert_eq!(
                    error.condition, "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
                    "{sql}"
                );
                assert_eq!(error.parameters.len(), 1);
            }
        }
    }
    let resolver = PlanResolver::new(
        &context,
        Arc::new(PlanConfig {
            case_sensitive: false,
            ..Default::default()
        }),
    );
    let ast =
        sail_sql_analyzer::parser::parse_one_statement("SELECT Foo FROM (SELECT 1 AS foo) q")?;
    let plan = sail_sql_analyzer::statement::from_ast_statement(ast)?;
    assert!(resolver.resolve_named_plan(plan).await.is_ok());
    Ok(())
}

#[test]
fn shared_namespace_stripping_respects_requested_qualification() {
    let candidates = vec![
        vec!["c".into(), "d".into(), "t".into(), "a".into()],
        vec!["c".into(), "d".into(), "u".into(), "b".into()],
    ];
    assert_eq!(
        suggestions(&["a".into()], &candidates),
        ["`t`.`a`", "`u`.`b`"]
    );
    assert_eq!(
        suggestions(&["t".into(), "a".into()], &candidates),
        ["`t`.`a`", "`u`.`b`"]
    );
    assert_eq!(
        suggestions(&["d".into(), "t".into(), "a".into()], &candidates),
        ["`c`.`d`.`t`.`a`", "`c`.`d`.`u`.`b`"]
    );
}

#[test]
fn edit_distance_uses_utf16_units() {
    assert_eq!(distance("😀", "a"), 2);
    assert_eq!(distance("😀", "😁"), 1);
    assert_eq!(distance("", "雪😀"), 3);
}
