use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use sail_catalog::manager::{CatalogManager, CatalogManagerOptions};
use sail_catalog::provider::CatalogProvider;
use sail_catalog_memory::MemoryCatalogProvider;
use sail_common::spec;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::rename::logical_plan::rename_logical_plan;
use sail_common_datafusion::session::plan::PlanService;
use sail_plan::catalog::SparkCatalogObjectDisplay;
use sail_plan::formatter::SparkPlanFormatter;
use sail_plan::resolver::PlanResolver;

use super::{PipelineGraph, register, registration_error};
use crate::error::{SparkError, SparkResult};
use crate::session::{SparkSession, SparkSessionOptions};
use crate::spark::connect::{OutputType, pipeline_command as pc};

fn graph() -> PipelineGraph {
    PipelineGraph::new("test_catalog".into(), "test_db".into(), HashMap::new())
}

fn register_sql(graph: &mut PipelineGraph, sql: &str, file: &str) -> SparkResult<()> {
    register(
        graph,
        pc::DefineSqlGraphElements {
            sql_text: Some(sql.into()),
            sql_file_path: Some(file.into()),
            ..Default::default()
        },
    )
}

fn name(table: &str) -> Vec<String> {
    vec!["test_catalog".into(), "test_db".into(), table.into()]
}

#[test]
fn sql_registration_errors_have_origin_parameters_with_optional_paths() {
    let origin = sail_sql_analyzer::source::StatementOrigin {
        text: "SELECT '雪;' -- tail\n",
        line: 3,
    };
    for file in [Some("é;origin.sql"), None] {
        let error = registration_error("unsupported statement".into(), origin, file);
        assert_eq!(error.class, "org.apache.spark.sql.AnalysisException");
        assert_eq!(
            error.condition,
            "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR"
        );
        assert_eq!(error.sql_state, "42000");
        assert_eq!(
            error.parameters,
            HashMap::from([
                ("message".into(), "unsupported statement".into()),
                (
                    "offendingQuery".into(),
                    "\nOffending query:\nSELECT '雪;' -- tail\n\n".into()
                ),
                (
                    "codeLocation".into(),
                    file.map(|file| format!("Query defined at {file}:3"))
                        .unwrap_or_default()
                ),
            ])
        );
    }
}

#[test]
fn identity_partition_columns_keep_declared_order() -> SparkResult<()> {
    let mut graph = graph();
    register_sql(
        &mut graph,
        "CREATE MATERIALIZED VIEW a PARTITIONED BY (id1, id2) \
         AS SELECT id AS id1, id AS id2 FROM range(1, 2)",
        "partition.sql",
    )?;
    let [output] = graph.definition.outputs.as_slice() else {
        return Err(SparkError::internal("expected one partitioned output"));
    };
    assert_eq!(output.name, name("a"));
    assert_eq!(output.kind, OutputType::MaterializedView);
    let Some(pc::define_output::Details::TableDetails(details)) = &output.definition.details else {
        return Err(SparkError::internal("expected table details"));
    };
    assert_eq!(details.partition_cols, vec!["id1", "id2"]);
    Ok(())
}

#[test]
fn streaming_table_properties_are_registered_without_execution() -> SparkResult<()> {
    let mut graph = graph();
    // The original Spark fixture resolves registration without validating streamingness.
    register_sql(
        &mut graph,
        "CREATE STREAMING TABLE st TBLPROPERTIES ('prop1'='foo', 'prop2'='bar') AS SELECT 1",
        "properties.sql",
    )?;
    let [output] = graph.definition.outputs.as_slice() else {
        return Err(SparkError::internal("expected one streaming output"));
    };
    assert_eq!(output.name, name("st"));
    assert_eq!(output.kind, OutputType::Table);
    let Some(pc::define_output::Details::TableDetails(details)) = &output.definition.details else {
        return Err(SparkError::internal("expected table details"));
    };
    assert_eq!(
        details.table_properties,
        HashMap::from([
            ("prop1".into(), "foo".into()),
            ("prop2".into(), "bar".into()),
        ])
    );
    Ok(())
}

#[test]
fn sql_configuration_is_captured_per_flow_and_reset_per_file() -> SparkResult<()> {
    let mut graph = graph();
    register_sql(
        &mut graph,
        "CREATE MATERIALIZED VIEW a AS SELECT id FROM range(1, 2); \
         SET conf.test = a; \
         CREATE VIEW b AS SELECT id FROM range(1, 2); \
         SET conf.test = b; \
         SET conf.test2 = c; \
         CREATE STREAMING TABLE c AS SELECT id FROM range(1, 2);",
        "configuration.sql",
    )?;
    let configurations: Vec<_> = graph
        .definition
        .flows
        .iter()
        .map(|flow| (flow.name.clone(), flow.sql_conf.clone()))
        .collect();
    assert_eq!(
        configurations,
        vec![
            (name("a"), HashMap::new()),
            (name("b"), HashMap::from([("conf.test".into(), "a".into())])),
            (
                name("c"),
                HashMap::from([
                    ("conf.test".into(), "b".into()),
                    ("conf.test2".into(), "c".into()),
                ])
            ),
        ]
    );
    assert!(graph.definition.sql_conf.is_empty());
    register_sql(
        &mut graph,
        "CREATE MATERIALIZED VIEW d AS SELECT 1 AS value",
        "next.sql",
    )?;
    let Some(flow) = graph.definition.flows.last() else {
        return Err(SparkError::internal("expected next-file flow"));
    };
    assert_eq!(flow.name, name("d"));
    assert!(flow.sql_conf.is_empty());
    Ok(())
}

fn resolution_context() -> Result<SessionContext, Box<dyn std::error::Error>> {
    pyo3::Python::initialize();
    let provider =
        MemoryCatalogProvider::new("test_catalog".into(), vec!["test_db"].try_into()?, None);
    let catalog = CatalogManager::try_new(CatalogManagerOptions {
        catalogs: HashMap::from([(
            "test_catalog".into(),
            Arc::new(provider) as Arc<dyn CatalogProvider>,
        )]),
        default_catalog: "test_catalog".into(),
        default_database: vec!["test_db".into()],
        global_temporary_database: vec!["global_temp".into()],
    })?;
    let spark = SparkSession::try_new(
        "schema-test".into(),
        "test-user".into(),
        SparkSessionOptions {
            execution_heartbeat_interval: Duration::from_secs(1),
        },
    )?;
    let plan_service = PlanService::new(
        Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
        Box::new(SparkPlanFormatter),
    );
    let config = SessionConfig::new()
        .with_extension(Arc::new(catalog))
        .with_extension(Arc::new(spark))
        .with_extension(Arc::new(plan_service));
    // Match Sail's session factory: it manages functions instead of loading DF defaults.
    let state = SessionStateBuilder::new().with_config(config).build();
    Ok(SessionContext::new_with_state(state))
}

#[tokio::test]
async fn declared_schema_validation_preserves_resolved_query_nullability()
-> Result<(), Box<dyn std::error::Error>> {
    let mut graph = graph();
    register_sql(
        &mut graph,
        "CREATE MATERIALIZED VIEW a (id LONG COMMENT 'comment') AS SELECT * FROM RANGE(5);",
        "schema.sql",
    )?;
    let [output] = graph.definition.outputs.as_slice() else {
        return Err(SparkError::internal("expected one declared output").into());
    };
    let [flow] = graph.definition.flows.as_slice() else {
        return Err(SparkError::internal("expected one query flow").into());
    };
    let context = resolution_context()?;
    let spark = context.extension::<SparkSession>()?;
    let named = PlanResolver::new(&context, spark.plan_config()?)
        .resolve_named_plan(spec::Plan::Query(flow.query.clone()))
        .await?;
    let fields = named
        .fields
        .ok_or_else(|| SparkError::internal("expected query field names"))?;
    let resolved = rename_logical_plan(named.plan, &fields)?;
    // This is the resolved query schema, before nullable catalog-publication conversion.
    let expected = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    assert_eq!(resolved.schema().as_arrow(), &expected);
    super::super::schema::infer(&context, output, std::slice::from_ref(&resolved))?;
    assert_eq!(resolved.schema().as_arrow(), &expected);
    Ok(())
}

#[test]
fn persisted_view_retains_original_query_text_and_comment() -> SparkResult<()> {
    let mut graph = graph();
    register_sql(
        &mut graph,
        "CREATE VIEW b COMMENT 'my persisted comment' AS SELECT * FROM range(1, 4);",
        "view.sql",
    )?;
    let [output] = graph.definition.outputs.as_slice() else {
        return Err(SparkError::internal("expected one persisted view"));
    };
    assert_eq!(output.name, name("b"));
    assert_ne!(output.kind, OutputType::TemporaryView);
    let Some(view) = &output.view else {
        return Err(SparkError::internal("expected persisted view definition"));
    };
    assert_eq!(view.definition, "SELECT * FROM range(1, 4)");
    assert_eq!(view.comment.as_deref(), Some("my persisted comment"));
    Ok(())
}

#[test]
fn view_source_intervals_preserve_file_atomicity_and_namespace_reset() -> SparkResult<()> {
    let mut graph = graph();
    register_sql(
        &mut graph,
        "USE other_db; CREATE VIEW a AS SELECT 'é;雪' AS x; \
         CREATE VIEW b AS SELECT /* ; */ x FROM a;",
        "views.sql",
    )?;
    assert_eq!(graph.definition.outputs.len(), 2);
    for (output, (table, query)) in graph.definition.outputs.iter().zip([
        ("a", "SELECT 'é;雪' AS x"),
        ("b", "SELECT /* ; */ x FROM a"),
    ]) {
        assert_eq!(output.name, vec!["test_catalog", "other_db", table]);
        let Some(view) = &output.view else {
            return Err(SparkError::internal("expected persisted view"));
        };
        assert_eq!(view.definition, query);
    }
    assert!(
        register_sql(
            &mut graph,
            "USE failed_db; CREATE VIEW rejected AS SELECT 2; SELECT 3;",
            "failed.sql",
        )
        .is_err()
    );
    assert_eq!(graph.definition.outputs.len(), 2);
    register_sql(&mut graph, "CREATE VIEW c AS SELECT 4", "next.sql")?;
    assert_eq!(graph.definition.outputs.len(), 3);
    assert_eq!(graph.definition.outputs[2].name, name("c"));
    Ok(())
}
