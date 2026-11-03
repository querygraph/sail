//! Session diagnostics read at execution time, for observable resource lifecycle.
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, plan_err};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use nutmeg_graph::SessionRegistry;
use serde_json::json;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Utf8,
        false,
    )]))
}

#[derive(Debug)]
pub(crate) struct DiagnosticsTable(pub SessionRegistry);

#[async_trait]
impl TableProvider for DiagnosticsTable {
    fn schema(&self) -> SchemaRef {
        schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        _: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(projection) => Arc::new(schema().project(projection)?),
            None => schema(),
        };
        Ok(Arc::new(DiagnosticsExec {
            registry: self.0.clone(),
            projection: projection.cloned(),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }))
    }
}

#[derive(Debug)]
struct DiagnosticsExec {
    registry: SessionRegistry,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for DiagnosticsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt("NutmegDiagnosticsExec", f)
    }
}

impl ExecutionPlan for DiagnosticsExec {
    fn name(&self) -> &str {
        "NutmegDiagnosticsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!("Nutmeg diagnostics have no inputs");
        }
        Ok(self)
    }
    fn execute(&self, partition: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return plan_err!("Nutmeg diagnostics have one output partition");
        }
        let registry = self.registry.clone();
        let projection = self.projection.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(async move {
                let memory = registry.memory()?;
                let graphs = registry.list()?.into_iter().map(|g| json!({
                "name": g.name, "node_count": g.staged_nodes, "edge_count": g.staged_edges,
                "revision": g.revision, "projections": g.projections, "staged_bytes": g.staged_bytes,
            })).collect::<Vec<_>>();
                let reads = registry.reads()?.into_iter().map(|r| json!({
                "id": r.id, "algorithm": r.algorithm, "graph": r.graph, "state": r.state.name(),
                "message": r.message, "batches": r.batches, "rows": r.rows,
                "live_bytes": r.live_bytes, "peak_bytes": r.peak_bytes, "work_units": r.work_units,
                "diagnostics": r.diagnostics,
            })).collect::<Vec<_>>();
                let status = json!({
                    "memory": {"limit_bytes": memory.limit_bytes, "used_bytes": memory.used_bytes,
                        "peak_bytes": memory.peak_bytes, "staged_bytes": memory.staged_bytes},
                    "graphs": graphs, "reads": reads,
                })
                .to_string();
                let batch = RecordBatch::try_new(
                    schema(),
                    vec![Arc::new(StringArray::from(vec![status]))],
                )?;
                let batch = match projection {
                    Some(projection) => batch.project(&projection)?,
                    None => batch,
                };
                registry.retain_output_owner(batch)
            }),
        )))
    }
}
