//! Local physical-plan boundary for an already-planned native relation.
//!
//! DataFusion's FFI does not preserve all input requirements, and foreign child
//! replacement can export host-added nodes without a host Tokio runtime. Do not
//! let the outer host optimizer rewrite this mixed-library execution region.

use std::fmt::{Formatter, Result as FmtResult};
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, Statistics, plan_err};
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};

#[derive(Debug)]
pub(super) struct NativeTableProvider {
    inner: Arc<dyn TableProvider>,
}

impl NativeTableProvider {
    pub fn new(inner: Arc<dyn TableProvider>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl TableProvider for NativeTableProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.scan(session, projection, filters, limit).await?;
        let properties = Arc::new(declared_properties(inner.properties()));
        Ok(Arc::new(NativeRelationExec {
            inner,
            properties,
            // Keep provider-owned native resources alive with its execution.
            _provider: Arc::clone(&self.inner),
        }))
    }
}

/// A native plan's declared layout, restated with host column expressions.
///
/// A plan that crosses the FFI boundary keeps its output partitioning and
/// ordering, but every expression in them arrives as a foreign expression
/// the host can only evaluate: it compares equal to nothing and projects
/// through nothing, so a scan that declares `Hash([key], N)` still gets a
/// repartition under every join, and a declared order gets a sort. A column
/// is the one expression whose identity its display gives away, `name@index`;
/// when that display names a column of the plan's own schema at that index,
/// the host column is the same expression. Anything else is kept as it came.
fn declared_properties(inner: &PlanProperties) -> PlanProperties {
    let schema = Arc::clone(inner.eq_properties.schema());
    let partitioning = match inner.output_partitioning() {
        Partitioning::Hash(exprs, count) => Partitioning::Hash(
            exprs.iter().map(|expr| host_column(expr, &schema)).collect(),
            *count,
        ),
        other => other.clone(),
    };
    let ordering = inner.output_ordering().and_then(|ordering| {
        LexOrdering::new(ordering.iter().map(|sort| PhysicalSortExpr {
            expr: host_column(&sort.expr, &schema),
            options: sort.options,
        }))
    });
    let eq_properties = match ordering {
        Some(ordering) => EquivalenceProperties::new_with_orderings(schema, [ordering]),
        None => EquivalenceProperties::new(schema),
    };
    PlanProperties::new(
        eq_properties,
        partitioning,
        inner.emission_type,
        inner.boundedness,
    )
}

/// `expr` as a host [`Column`] when it is one column of `schema`, else itself.
fn host_column(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Arc<dyn PhysicalExpr> {
    if expr.is::<Column>() || !expr.children().is_empty() {
        return Arc::clone(expr);
    }
    let display = expr.to_string();
    let Some((name, index)) = display.rsplit_once('@') else {
        return Arc::clone(expr);
    };
    let Ok(index) = index.parse::<usize>() else {
        return Arc::clone(expr);
    };
    match schema.fields().get(index) {
        Some(field) if field.name() == name => Arc::new(Column::new(name, index)),
        _ => Arc::clone(expr),
    }
}

#[derive(Debug)]
struct NativeRelationExec {
    inner: Arc<dyn ExecutionPlan>,
    /// [`declared_properties`] of `inner`, computed once.
    properties: Arc<PlanProperties>,
    _provider: Arc<dyn TableProvider>,
}

impl DisplayAs for NativeRelationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "NativeRelationExec: local, opaque_region=true, native={}",
            self.inner.name()
        )
    }
}

impl ExecutionPlan for NativeRelationExec {
    fn name(&self) -> &'static str {
        "NativeRelationExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Host inputs were optimized before export. The package owns the
        // complete region below this leaf, including its input distribution.
        vec![]
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
        if !children.is_empty() {
            return plan_err!("NativeRelationExec is an opaque leaf and does not accept children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}

#[cfg(test)]
mod tests;
