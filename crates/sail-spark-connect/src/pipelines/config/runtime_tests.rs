use std::collections::HashMap;

use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::{SparkError, SparkResult};
use crate::pipelines::analysis::{self, AnalyzedGraph};
use crate::pipelines::graph::{GraphDefinition, PipelineGraph};
use crate::pipelines::{catalog_preparation_tests, sql};
use crate::session::SparkSession;
use crate::spark::connect::pipeline_command as pc;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const CASE: &str = "spark.sql.caseSensitive";

#[tokio::test]
async fn unconfigured_views_preserve_native_dynamic_identifier_resolution() -> TestResult {
    let ctx = catalog_preparation_tests::context()?;
    let mut graph = definition("CREATE TEMPORARY VIEW a AS SELECT 1 AS foo; CREATE MATERIALIZED VIEW target AS SELECT foo FROM IDENTIFIER('a')").await?;
    // A table's own settings are not inherited by its consumers.
    set(&mut graph, "target", "pipelines.test.table_only", "value")?;
    let analyzed = analysis::analyze(&ctx, &graph).await?;
    assert_eq!(
        accepted(&graph, &analyzed, "target")?["pipelines.test.table_only"],
        "value"
    );
    assert!(accepted(&graph, &analyzed, "a")?.is_empty());
    Ok(())
}

#[tokio::test]
async fn unconfigured_views_preserve_native_parameter_relation_resolution() -> TestResult {
    use sail_common::spec;

    let ctx = catalog_preparation_tests::context()?;
    let mut graph = definition("CREATE TEMPORARY VIEW a AS SELECT 1 AS foo; CREATE MATERIALIZED VIEW target AS SELECT foo FROM a").await?;
    let root = definition("CREATE MATERIALIZED VIEW target AS SELECT foo FROM param")
        .await?
        .flows
        .into_iter()
        .next()
        .ok_or("root missing")?
        .query;
    let flow = graph
        .flows
        .iter_mut()
        .find(|flow| flow.target.last().is_some_and(|name| name == "target"))
        .ok_or("flow missing")?;
    let mut reference = spec::QueryPlan::new(spec::QueryNode::SubqueryAlias {
        input: Box::new(flow.query.clone()),
        alias: "param".into(),
        qualifier: vec![],
    });
    reference.plan_id = Some(42);
    flow.query = spec::QueryPlan::new(spec::QueryNode::WithRelations {
        root: Box::new(root),
        references: vec![reference],
    });
    let analyzed = analysis::analyze(&ctx, &graph).await?;
    assert!(accepted(&graph, &analyzed, "target")?.is_empty());
    Ok(())
}

async fn definition(query: &str) -> SparkResult<GraphDefinition> {
    let mut graph = PipelineGraph::new("test_catalog".into(), "test_db".into(), Default::default());
    sql::prepare(
        pc::DefineSqlGraphElements {
            sql_text: Some(query.into()),
            ..Default::default()
        },
        |_| async { Err(SparkError::internal("no catalog expressions in fixture")) },
    )
    .await?
    .commit(&mut graph)?;
    Ok(graph.definition)
}
fn set(graph: &mut GraphDefinition, target: &str, key: &str, value: &str) -> TestResult {
    graph
        .flows
        .iter_mut()
        .find(|flow| flow.target.last().is_some_and(|name| name == target))
        .ok_or("flow missing")?
        .sql_conf
        .insert(key.into(), value.into());
    Ok(())
}
fn accepted<'a>(
    graph: &GraphDefinition,
    analyzed: &'a AnalyzedGraph,
    target: &str,
) -> Result<&'a HashMap<String, String>, Box<dyn std::error::Error>> {
    let flow = graph
        .flows
        .iter()
        .find(|flow| flow.target.last().is_some_and(|name| name == target))
        .ok_or("flow missing")?;
    analyzed
        .accepted_confs
        .get(&flow.name)
        .ok_or_else(|| "accepted map missing".into())
}

#[tokio::test]
async fn first_semantic_failure_precedes_conflict_and_reversed_reads_change_the_error() -> TestResult
{
    let ctx = catalog_preparation_tests::context()?;
    for (query, conflict) in [
        ("SELECT Foo FROM z_sensitive CROSS JOIN a_insensitive", true),
        (
            "SELECT Foo FROM a_insensitive CROSS JOIN z_sensitive",
            false,
        ),
        (
            "SELECT z1.Foo FROM z_sensitive z1 CROSS JOIN a_insensitive CROSS JOIN z_sensitive z2",
            false,
        ),
        (
            "SELECT Foo FROM (SELECT Foo FROM z_sensitive) q CROSS JOIN a_insensitive",
            true,
        ),
        (
            "SELECT Bar, (SELECT foo FROM z_sensitive) AS nested FROM a_insensitive",
            true,
        ),
    ] {
        let mut graph = definition(&format!("CREATE TEMPORARY VIEW z_sensitive AS SELECT 1 AS foo; CREATE TEMPORARY VIEW a_insensitive AS SELECT 2 AS bar; CREATE MATERIALIZED VIEW target AS {query}")).await?;
        set(&mut graph, "z_sensitive", CASE, "true")?;
        set(&mut graph, "a_insensitive", CASE, "false")?;
        let error = analysis::analyze(&ctx, &graph)
            .await
            .err()
            .ok_or("invalid graph accepted")?;
        if conflict {
            let SparkError::Structured(error) = error else {
                return Err(format!("wrong conflict error: {error}").into());
            };
            assert_eq!(error.condition, "DUPLICATE_FLOW_SQL_CONF");
            assert_eq!(error.sql_state, "42710");
            assert_eq!(error.parameters["key"], CASE);
            assert_eq!(error.parameters["flowName1"], "a_insensitive");
            assert_eq!(error.parameters["flowName2"], "z_sensitive");
        } else {
            let SparkError::Structured(error) = error else {
                return Err(format!("wrong unresolved-column error: {error}").into());
            };
            assert_eq!(error.condition, "UNRESOLVED_COLUMN.WITH_SUGGESTION");
            assert_eq!(error.sql_state, "42703");
            assert!(error.parameters["objectName"].contains("Foo"));
            assert!(error.parameters["proposal"].contains("foo"));
        }
    }
    Ok(())
}

#[tokio::test]
async fn accepted_maps_are_transitive_through_views_but_stop_at_tables() -> TestResult {
    let ctx = catalog_preparation_tests::context()?;
    let mut graph = definition("CREATE TEMPORARY VIEW a AS SELECT 1 AS foo; CREATE TEMPORARY VIEW b AS SELECT foo FROM a; CREATE MATERIALIZED VIEW barrier AS SELECT foo FROM b; CREATE TEMPORARY VIEW c AS SELECT 2 AS bar; CREATE MATERIALIZED VIEW target AS SELECT foo, bar FROM barrier CROSS JOIN c").await?;
    for target in ["a", "b", "barrier", "c", "target"] {
        set(
            &mut graph,
            target,
            &format!("pipelines.test.{target}"),
            target,
        )?;
    }
    let analyzed = analysis::analyze(&ctx, &graph).await?;
    for (target, expected) in [
        ("b", vec!["a", "b"]),
        ("barrier", vec!["a", "b", "barrier"]),
        ("target", vec!["c", "target"]),
    ] {
        let map = accepted(&graph, &analyzed, target)?;
        assert_eq!(map.len(), expected.len());
        for key in expected {
            assert_eq!(map[&format!("pipelines.test.{key}")], key);
        }
    }
    Ok(())
}

#[tokio::test]
async fn equal_values_and_conditional_reanalysis_preserve_the_run_session() -> TestResult {
    let ctx = catalog_preparation_tests::context()?;
    let mut graph = definition("CREATE TEMPORARY VIEW a AS SELECT 1 AS foo; CREATE MATERIALIZED VIEW target AS SELECT Foo FROM a").await?;
    set(&mut graph, "a", CASE, "false")?;
    let analyzed = analysis::analyze(&ctx, &graph).await?;
    assert_eq!(accepted(&graph, &analyzed, "target")?[CASE], "false");
    assert!(
        graph
            .flows
            .iter()
            .find(|flow| flow.target.last().is_some_and(|name| name == "target"))
            .ok_or("target missing")?
            .sql_conf
            .is_empty()
    );
    assert!(
        !ctx.extension::<SparkSession>()?
            .plan_config()?
            .case_sensitive
    );
    set(&mut graph, "target", CASE, "false")?;
    let same = analysis::analyze(&ctx, &graph).await?;
    assert_eq!(
        accepted(&graph, &same, "target")?,
        accepted(&graph, &analyzed, "target")?
    );
    Ok(())
}

#[tokio::test]
async fn changed_union_repeats_native_analysis_but_an_unchanged_union_does_not() -> TestResult {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::{MemTable, provider_as_source};
    use sail_common_datafusion::datasource::{
        DataSource, DataSourceRegistry, SinkInfo, SourceInfo,
    };
    struct Counter(Arc<AtomicUsize>);
    #[tonic::async_trait]
    impl DataSource for Counter {
        fn name(&self) -> &str {
            "call_probe"
        }
        async fn create_source(
            &self,
            _: &dyn datafusion::catalog::Session,
            _: SourceInfo,
        ) -> datafusion::common::Result<Arc<dyn datafusion::logical_expr::TableSource>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(provider_as_source(Arc::new(MemTable::try_new(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
                vec![vec![]],
            )?)))
        }
        async fn create_writer(
            &self,
            _: &dyn datafusion::catalog::Session,
            _: SinkInfo,
        ) -> datafusion::common::Result<datafusion::logical_expr::LogicalPlan> {
            datafusion::common::internal_err!("read-only counter")
        }
    }
    let ctx = catalog_preparation_tests::context()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let registry = DataSourceRegistry::new();
    registry.register_data_source(Arc::new(Counter(calls.clone())))?;
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(registry));
    let mut graph = definition("CREATE TEMPORARY VIEW a AS SELECT 1 AS foo; CREATE MATERIALIZED VIEW target AS SELECT foo FROM a CROSS JOIN call_probe.input").await?;
    let key = "pipelines.test.inherited";
    set(&mut graph, "a", key, "value")?;
    let analyzed = analysis::analyze(&ctx, &graph).await?;
    assert_eq!(accepted(&graph, &analyzed, "target")?[key], "value");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "changed raw map must perform a new native analysis call"
    );
    calls.store(0, Ordering::SeqCst);
    set(&mut graph, "target", key, "value")?;
    analysis::analyze(&ctx, &graph).await?;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "equal accepted/declared maps need only the first call"
    );
    Ok(())
}
