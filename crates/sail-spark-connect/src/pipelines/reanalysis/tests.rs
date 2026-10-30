use std::sync::{Mutex, Weak};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{Result, internal_datafusion_err};
use datafusion::datasource::{TableProvider, provider_as_source};
use datafusion::logical_expr::{Expr, TableSource, TableType};
use datafusion::physical_plan::ExecutionPlan;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{
    CreateDatabaseOptions, CreateTableColumnOptions, CreateTableMode, CreateTableOptions,
};
use sail_common_datafusion::datasource::{DataSource, DataSourceRegistry, SinkInfo, SourceInfo};

use super::*;
use crate::pipelines::graph::PipelineGraph;
use crate::pipelines::{analysis, catalog_preparation_tests, sql};
use crate::spark::connect::pipeline_command as pc;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;
#[derive(Clone)]
struct Event {
    path: String,
    sensitive: bool,
    lease: Weak<()>,
}
type Trace = Arc<Mutex<Vec<Event>>>;
fn events(trace: &Trace) -> std::result::Result<Vec<Event>, Box<dyn std::error::Error>> {
    Ok(trace.lock().map_err(|_| "trace poisoned")?.clone())
}
#[derive(Debug)]
struct TrackedTable {
    _lease: Arc<()>,
}
#[tonic::async_trait]
impl TableProvider for TrackedTable {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _: &dyn Session,
        _: Option<&Vec<usize>>,
        _: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(internal_datafusion_err!("input resolution must not scan"))
    }
}
struct Source(Trace);
#[tonic::async_trait]
impl DataSource for Source {
    fn name(&self) -> &str {
        "reanalysis_probe"
    }
    async fn create_source(
        &self,
        _: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        assert_eq!(info.paths.len(), 1);
        let lease = Arc::new(());
        self.0
            .lock()
            .map_err(|_| internal_datafusion_err!("trace poisoned"))?
            .push(Event {
                path: info.paths[0].clone(),
                sensitive: info.read_case_sensitive,
                lease: Arc::downgrade(&lease),
            });
        Ok(provider_as_source(Arc::new(TrackedTable { _lease: lease })))
    }
    async fn create_writer(&self, _: &dyn Session, _: SinkInfo) -> Result<LogicalPlan> {
        Err(internal_datafusion_err!("read-only probe"))
    }
}
fn context() -> std::result::Result<(SessionContext, Trace), Box<dyn std::error::Error>> {
    let ctx = catalog_preparation_tests::context()?;
    let trace = Arc::new(Mutex::new(vec![]));
    let registry = DataSourceRegistry::new();
    registry.register_data_source(Arc::new(Source(trace.clone())))?;
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(registry));
    Ok((ctx, trace))
}
async fn definition(text: &str) -> SparkResult<GraphDefinition> {
    let mut graph = PipelineGraph::new("test_catalog".into(), "test_db".into(), Default::default());
    sql::prepare(
        pc::DefineSqlGraphElements {
            sql_text: Some(text.into()),
            ..Default::default()
        },
        |_| async { Err(SparkError::internal("no catalog expressions in fixture")) },
    )
    .await?
    .commit(&mut graph)?;
    Ok(graph.definition)
}
fn name(table: &str) -> Name {
    vec!["test_catalog".into(), "test_db".into(), table.into()]
}
async fn table(ctx: &SessionContext, name: &[&str], path: &str) -> TestResult {
    ctx.extension::<CatalogManager>()?
        .create_table(
            name,
            CreateTableOptions {
                columns: vec![CreateTableColumnOptions {
                    name: "id".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                    comment: None,
                    default: None,
                    generated_always_as: None,
                    identity: None,
                }],
                comment: None,
                constraints: vec![],
                location: Some(path.into()),
                format: "reanalysis_probe".into(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                mode: CreateTableMode::Create,
                properties: vec![],
                is_external: true,
                is_write_precondition: false,
            },
        )
        .await?;
    Ok(())
}
#[tokio::test]
async fn separate_commands_reresolve_views_and_release_each_provider() -> TestResult {
    let (ctx, trace) = context()?;
    let graph = definition("CREATE TEMPORARY VIEW v AS SELECT * FROM reanalysis_probe.`shared`; CREATE VIEW unrelated AS SELECT * FROM reanalysis_probe.`unrelated`; CREATE MATERIALIZED VIEW a AS SELECT * FROM v; CREATE MATERIALIZED VIEW b AS SELECT * FROM v").await?;
    let analyzed = analysis::analyze(&ctx, &graph).await?;
    let baseline = events(&trace)?.len();
    assert!(
        events(&trace)?
            .iter()
            .all(|event| event.lease.upgrade().is_none())
    );
    for target in [name("a"), name("b")] {
        let resolved = resolve_inputs(
            &ctx,
            &graph,
            &analyzed,
            &target,
            ctx.extension::<SparkSession>()?.plan_config()?,
        )
        .await?;
        assert_eq!(resolved.len(), 1);
        assert!(resolved.contains_key(&vec!["v".into()]));
        assert!(
            events(&trace)?
                .last()
                .ok_or("no provider event")?
                .lease
                .upgrade()
                .is_some()
        );
        drop(resolved);
        assert!(
            events(&trace)?
                .iter()
                .all(|event| event.lease.upgrade().is_none())
        );
    }
    let calls = events(&trace)?;
    assert_eq!(
        calls[baseline..]
            .iter()
            .map(|event| event.path.as_str())
            .collect::<Vec<_>>(),
        ["shared", "shared"]
    );
    Ok(())
}
#[tokio::test]
async fn table_stops_reanalysis_and_uses_the_consuming_command_configuration() -> TestResult {
    let (ctx, trace) = context()?;
    let graph = definition("CREATE TEMPORARY VIEW hidden AS SELECT * FROM reanalysis_probe.`behind`; CREATE MATERIALIZED VIEW barrier AS SELECT * FROM hidden; CREATE MATERIALIZED VIEW target AS SELECT * FROM barrier").await?;
    let analyzed = analysis::analyze(&ctx, &graph).await?;
    table(&ctx, &["barrier"], "committed").await?;
    let baseline = events(&trace)?.len();
    let mut config = ctx
        .extension::<SparkSession>()?
        .plan_config()?
        .as_ref()
        .clone();
    assert!(!config.case_sensitive);
    config.case_sensitive = true;
    let resolved =
        resolve_inputs(&ctx, &graph, &analyzed, &name("target"), Arc::new(config)).await?;
    assert_eq!(resolved.len(), 1);
    assert!(resolved.contains_key(&name("barrier")));
    let calls = events(&trace)?;
    assert_eq!(
        calls[baseline..]
            .iter()
            .map(|event| (event.path.as_str(), event.sensitive))
            .collect::<Vec<_>>(),
        [("committed", true)]
    );
    drop(resolved);
    assert!(
        events(&trace)?
            .iter()
            .all(|event| event.lease.upgrade().is_none())
    );
    Ok(())
}
#[tokio::test]
async fn graph_persisted_view_uses_its_definition_namespace_without_catalog_publication()
-> TestResult {
    let (ctx, trace) = context()?;
    ctx.extension::<CatalogManager>()?
        .create_database(
            &["view_db"],
            CreateDatabaseOptions {
                comment: None,
                location: None,
                if_not_exists: false,
                properties: vec![],
            },
        )
        .await?;
    table(&ctx, &["test_catalog", "view_db", "backing"], "namespace").await?;
    table(&ctx, &["backing"], "wrong-namespace").await?;
    let graph = definition("USE view_db; SET spark.sql.caseSensitive=true; CREATE VIEW p AS SELECT id FROM backing; USE test_db; SET spark.sql.caseSensitive=false; CREATE MATERIALIZED VIEW target AS SELECT id FROM view_db.p").await?;
    let analyzed = analysis::analyze(&ctx, &graph).await?;
    let baseline = events(&trace)?.len();
    let resolved = resolve_inputs(
        &ctx,
        &graph,
        &analyzed,
        &name("target"),
        ctx.extension::<SparkSession>()?.plan_config()?,
    )
    .await?;
    assert!(resolved.contains_key(&vec!["test_catalog".into(), "view_db".into(), "p".into()]));
    let calls = events(&trace)?;
    assert_eq!(
        calls[baseline..]
            .iter()
            .map(|event| (event.path.as_str(), event.sensitive))
            .collect::<Vec<_>>(),
        [("namespace", true)]
    );
    assert!(
        ctx.extension::<CatalogManager>()?
            .get_table_or_view(&["test_catalog", "view_db", "p"])
            .await
            .is_err()
    );
    drop(resolved);
    assert!(
        events(&trace)?
            .iter()
            .all(|event| event.lease.upgrade().is_none())
    );
    Ok(())
}
