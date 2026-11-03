//! DataFusion's exported codec keeps a Weak<TaskContextProvider>. Keep a strong
//! owner with both provider and physical plan, beyond Python/capsule lifetimes.
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, internal_err};
use datafusion_execution::{TaskContext, TaskContextProvider};

#[derive(Debug)]
pub(crate) struct ContextProvider(pub Arc<TaskContext>);
impl TaskContextProvider for ContextProvider {
    fn task_ctx(&self) -> Arc<TaskContext> {
        self.0.clone()
    }
}

#[derive(Debug)]
pub(crate) struct OwnedProvider {
    pub inner: Arc<dyn TableProvider>,
    pub context: Arc<ContextProvider>,
}

#[async_trait]
impl TableProvider for OwnedProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }
    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.scan(session, projection, filters, limit).await?;
        Ok(Arc::new(OwnedExec {
            inner,
            context: self.context.clone(),
        }))
    }
}

#[derive(Debug)]
struct OwnedExec {
    inner: Arc<dyn ExecutionPlan>,
    context: Arc<ContextProvider>,
}

impl DisplayAs for OwnedExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NutmegContextOwnerExec")
    }
}

impl ExecutionPlan for OwnedExec {
    fn name(&self) -> &str {
        "NutmegContextOwnerExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("NutmegContextOwnerExec requires one child");
        }
        Ok(Arc::new(Self {
            inner: children[0].clone(),
            context: self.context.clone(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}
