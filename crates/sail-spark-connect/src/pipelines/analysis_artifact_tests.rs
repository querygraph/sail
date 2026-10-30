//! Characterize artifacts before introducing finite analysis/attempt owners.
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, internal_datafusion_err};
use datafusion::datasource::{MemTable, TableProvider, provider_as_source};
use datafusion::logical_expr::{Expr, LogicalPlan, TableSource, TableType};
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{DataSource, DataSourceRegistry, SinkInfo, SourceInfo};

use super::*;
use crate::pipelines::graph::PipelineGraph;
use crate::pipelines::sql;
use crate::spark::connect::pipeline_command as pc;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

#[derive(Debug)]
struct ProbeTable {
    _lease: Arc<()>,
    scans: Arc<AtomicUsize>,
}
#[tonic::async_trait]
impl TableProvider for ProbeTable {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        let batch = RecordBatch::try_new(
            self.schema(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        MemTable::try_new(self.schema(), vec![vec![batch]])?
            .scan(session, projection, filters, limit)
            .await
    }
}
struct ProbeSource {
    leases: Arc<Mutex<Vec<Weak<()>>>>,
    scans: Arc<AtomicUsize>,
    reject: bool,
}
#[tonic::async_trait]
impl DataSource for ProbeSource {
    fn name(&self) -> &str {
        "artifact_probe"
    }
    async fn create_source(
        &self,
        _: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        assert_eq!(info.paths, ["input"]);
        if self.reject {
            return Err(internal_datafusion_err!(
                "source unavailable after analysis"
            ));
        }
        let lease = Arc::new(());
        self.leases
            .lock()
            .map_err(|_| internal_datafusion_err!("probe leases poisoned"))?
            .push(Arc::downgrade(&lease));
        Ok(provider_as_source(Arc::new(ProbeTable {
            _lease: lease,
            scans: self.scans.clone(),
        })))
    }
    async fn create_writer(&self, _: &dyn Session, _: SinkInfo) -> Result<LogicalPlan> {
        Err(internal_datafusion_err!(
            "artifact analysis must not create a writer"
        ))
    }
}
async fn definition(
    text: &str,
) -> crate::error::SparkResult<crate::pipelines::graph::GraphDefinition> {
    let mut graph = PipelineGraph::new("test_catalog".into(), "test_db".into(), HashMap::new());
    sql::prepare(
        pc::DefineSqlGraphElements {
            sql_text: Some(text.into()),
            ..Default::default()
        },
        |_| async {
            Err(crate::error::SparkError::internal(
                "no catalog expression in fixture",
            ))
        },
    )
    .await?
    .commit(&mut graph)?;
    Ok(graph.definition)
}
async fn retention(text: &str) -> TestResult {
    let ctx = sql::tests::resolution_context()?;
    let leases = Arc::new(Mutex::new(vec![]));
    let scans = Arc::new(AtomicUsize::new(0));
    let registry = DataSourceRegistry::new();
    registry.register_data_source(Arc::new(ProbeSource {
        leases: leases.clone(),
        scans: scans.clone(),
        reject: false,
    }))?;
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(registry));
    let graph = definition(text).await?;
    let analyzed = analyze(&ctx, &graph).await?;
    let leases = leases.lock().map_err(|_| "probe leases poisoned")?;
    assert_eq!(
        leases.len(),
        1,
        "only the external definition creates a provider"
    );
    assert!(
        leases[0].upgrade().is_none(),
        "exported analysis artifact retained provider"
    );
    assert_eq!(scans.load(Ordering::SeqCst), 0);
    for output in &graph.outputs {
        let schema = analyzed
            .schemas
            .get(&output.name)
            .ok_or("analyzed output missing")?;
        assert_eq!(schema.field(0).name(), "id");
    }
    drop(analyzed);
    assert!(leases[0].upgrade().is_none());
    Ok(())
}
#[tokio::test]
async fn analysis_temporary_view_exports_schema_without_provider() -> TestResult {
    retention("CREATE TEMPORARY VIEW temp AS SELECT * FROM artifact_probe.`input`; CREATE MATERIALIZED VIEW target AS SELECT * FROM temp").await
}
#[tokio::test]
async fn analysis_materialized_table_exports_schema_without_provider() -> TestResult {
    retention("CREATE MATERIALIZED VIEW target AS SELECT * FROM artifact_probe.`input`").await
}
#[tokio::test]
async fn analysis_persisted_view_exports_schema_without_provider() -> TestResult {
    retention("CREATE VIEW target AS SELECT * FROM artifact_probe.`input`").await
}
#[tokio::test]
async fn analysis_temporary_clock_remains_unfolded() -> TestResult {
    let ctx = sql::tests::resolution_context()?;
    let graph = definition("CREATE TEMPORARY VIEW temp AS SELECT current_timestamp() AS clock; CREATE MATERIALIZED VIEW target AS SELECT * FROM temp").await?;
    let spark = ctx.extension::<crate::session::SparkSession>()?;
    let named = sail_plan::resolver::PlanResolver::new(&ctx, spark.plan_config()?)
        .resolve_named_plan(spec::Plan::Query(graph.flows[0].query.clone()))
        .await?;
    let plan = &named.plan;
    let mut clocks = 0;
    plan.apply(|node| {
        for expr in node.expressions() {
            if expr.exists(|expr| Ok(matches!(expr, Expr::ScalarFunction(function) if function.func.name() == "timestamp_now")))? {
                clocks += 1;
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert_eq!(
        clocks, 1,
        "normal graph resolution must not fold the clock at analysis time"
    );
    drop(named);
    let analyzed = analyze(&ctx, &graph).await?;
    let schema = analyzed
        .schemas
        .get(&vec!["temp".into()])
        .ok_or("temporary schema missing")?;
    assert!(matches!(
        schema.field(0).data_type(),
        DataType::Timestamp(..)
    ));
    Ok(())
}

#[tokio::test]
async fn temporary_working_plan_remains_available_for_analysis_helper_execution() -> TestResult {
    let ctx = crate::pipelines::catalog_preparation_tests::context()?;
    let leases = Arc::new(Mutex::new(vec![]));
    let scans = Arc::new(AtomicUsize::new(0));
    let registry = DataSourceRegistry::new();
    registry.register_data_source(Arc::new(ProbeSource {
        leases: leases.clone(),
        scans: scans.clone(),
        reject: false,
    }))?;
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(registry));
    let mut graph = definition("CREATE TEMPORARY VIEW temp AS SELECT * FROM artifact_probe.`input`; CREATE MATERIALIZED VIEW target AS SELECT * FROM temp").await?;
    let read = graph.flows[1].query.clone();
    graph.flows[1].query = spec::QueryPlan::new(spec::QueryNode::Tail {
        input: Box::new(read),
        limit: spec::Expr::Literal(spec::Literal::Int64 { value: Some(1) }),
    });
    let analyzed = analyze(&ctx, &graph).await?;
    assert_eq!(
        scans.load(Ordering::SeqCst),
        1,
        "tail's analysis-time count must see the working provider plan"
    );
    assert_eq!(analyzed.schemas.len(), 2);
    assert!(
        leases
            .lock()
            .map_err(|_| "probe leases poisoned")?
            .iter()
            .all(|lease| lease.upgrade().is_none())
    );
    ctx.extension::<sail_common_datafusion::session::job::JobService>()?
        .runner()
        .stop()
        .await?;
    Ok(())
}

#[tokio::test]
async fn persisted_view_publishes_captured_columns_after_source_is_unavailable() -> TestResult {
    let ctx = crate::pipelines::catalog_preparation_tests::context()?;
    let leases = Arc::new(Mutex::new(vec![]));
    let scans = Arc::new(AtomicUsize::new(0));
    let registry = DataSourceRegistry::new();
    registry.register_data_source(Arc::new(ProbeSource {
        leases: leases.clone(),
        scans: scans.clone(),
        reject: false,
    }))?;
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(registry));
    let graph = definition("CREATE VIEW target (captured COMMENT 'column comment') COMMENT 'view comment' AS SELECT id FROM artifact_probe.`input`").await?;
    let analyzed = analyze(&ctx, &graph).await?;
    let options = analyzed
        .publications
        .get(&graph.outputs[0].name)
        .ok_or("view publication missing")?
        .clone();
    assert_eq!(options.columns[0].name, "captured");
    assert_eq!(options.columns[0].data_type, DataType::Int64);
    assert!(!options.columns[0].nullable);
    assert_eq!(
        options.columns[0].comment.as_deref(),
        Some("column comment")
    );
    assert!(
        leases
            .lock()
            .map_err(|_| "probe leases poisoned")?
            .iter()
            .all(|lease| lease.upgrade().is_none())
    );
    let unavailable = DataSourceRegistry::new();
    unavailable.register_data_source(Arc::new(ProbeSource {
        leases,
        scans: scans.clone(),
        reject: true,
    }))?;
    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(unavailable));
    crate::pipelines::materialize::views(&ctx, &graph, &analyzed).await?;
    let status = ctx
        .extension::<sail_catalog::manager::CatalogManager>()?
        .get_table_or_view(&graph.outputs[0].name)
        .await?;
    let sail_common_datafusion::catalog::TableKind::View {
        definition,
        columns,
        comment,
        properties,
    } = status.kind
    else {
        return Err("published target is not a view".into());
    };
    assert_eq!(definition, options.definition);
    assert_eq!(columns[0].name, options.columns[0].name);
    assert_eq!(columns[0].data_type, options.columns[0].data_type);
    assert_eq!(columns[0].nullable, options.columns[0].nullable);
    assert_eq!(columns[0].comment, options.columns[0].comment);
    assert_eq!(comment, options.comment);
    assert_eq!(properties, options.properties);
    assert_eq!(scans.load(Ordering::SeqCst), 0);
    ctx.extension::<sail_common_datafusion::session::job::JobService>()?
        .runner()
        .stop()
        .await?;
    Ok(())
}
