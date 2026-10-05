//! A recursive CTE's work table that the recursive term may read more than once.
//!
//! DataFusion executes a recursive CTE with `RecursiveQueryExec`: after each
//! iteration it writes the iteration's rows into a work table, which a single
//! `WorkTableExec` in the recursive term takes. A second reference to the CTE is
//! rejected ("Multiple recursive references to the same CTE are not supported"),
//! because the batches can be taken only once.
//!
//! [`SharedCteWorkTable`] scans as [`SharedWorkTableExec`] nodes that share one
//! cell. When `RecursiveQueryExec` hands out its work table, exactly one node of
//! the recursive term claims it, so DataFusion sees a single reference. At run
//! time, the first node executed in an iteration takes the iteration's batches
//! through a DataFusion `WorkTableExec` bound to that work table, and keeps them;
//! every other node of the same iteration replays them. Record batches share
//! their buffers, so a replay copies no data.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::work_table::WorkTableExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions, SendableRecordBatchStream,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{Expr, TableType};
use futures::TryStreamExt;

/// The work table of one recursive CTE, shared by every reference to it.
#[derive(Debug)]
struct SharedWork {
    name: String,
    schema: SchemaRef,
    state: Mutex<SharedState>,
}

#[derive(Default)]
struct SharedState {
    /// The work table handed out by the current `RecursiveQueryExec`, and a
    /// DataFusion `WorkTableExec` bound to it.
    binding: Option<(Arc<dyn Any + Send + Sync>, Arc<dyn ExecutionPlan>)>,
    /// The rows of the current iteration, once a reference has taken them.
    current: Option<Vec<RecordBatch>>,
}

impl Debug for SharedState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedState")
            .field("bound", &self.binding.is_some())
            .field("current", &self.current.as_ref().map(|b| b.len()))
            .finish()
    }
}

/// A table provider for a recursive CTE's self-reference. Every scan of one
/// provider shares the same work table.
#[derive(Debug, Clone)]
pub struct SharedCteWorkTable {
    work: Arc<SharedWork>,
}

impl SharedCteWorkTable {
    pub fn new(name: impl Into<String>, schema: SchemaRef) -> Self {
        Self {
            work: Arc::new(SharedWork {
                name: name.into(),
                schema,
                state: Mutex::new(SharedState::default()),
            }),
        }
    }
}

#[async_trait]
impl TableProvider for SharedCteWorkTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.work.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SharedWorkTableExec::try_new(
            Arc::clone(&self.work),
            projection.cloned(),
        )?))
    }
}

/// One reference to a recursive CTE inside its recursive term.
#[derive(Debug, Clone)]
pub struct SharedWorkTableExec {
    work: Arc<SharedWork>,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl SharedWorkTableExec {
    fn try_new(work: Arc<SharedWork>, projection: Option<Vec<usize>>) -> Result<Self> {
        let schema = match &projection {
            Some(projection) => Arc::new(work.schema.project(projection)?),
            None => Arc::clone(&work.schema),
        };
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            work,
            projection,
            schema,
            cache,
        })
    }

    pub fn name(&self) -> &str {
        &self.work.name
    }

    /// The rows of the current iteration: taken from the work table by the
    /// first reference executed in the iteration, replayed for the others.
    fn iteration_batches(&self, context: Arc<TaskContext>) -> Result<Vec<RecordBatch>> {
        let mut state = match self.work.state.lock() {
            Ok(state) => state,
            Err(_) => return internal_err!("work table {} is poisoned", self.work.name),
        };
        let Some((_, reader)) = &state.binding else {
            return internal_err!(
                "work table {} is not bound to a recursive query",
                self.work.name
            );
        };
        match reader.execute(0, context) {
            Ok(stream) => {
                // The stream replays batches already in memory; it never waits on I/O.
                let batches = futures::executor::block_on(stream.try_collect::<Vec<_>>())?;
                state.current = Some(batches.clone());
                Ok(batches)
            }
            // The work table was taken earlier in this iteration by another reference.
            Err(e) => match &state.current {
                Some(batches) => Ok(batches.clone()),
                None => Err(e),
            },
        }
    }
}

impl DisplayAs for SharedWorkTableExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SharedWorkTableExec: name={}", self.work.name)
            }
            DisplayFormatType::TreeRender => write!(f, "name={}", self.work.name),
        }
    }
}

impl ExecutionPlan for SharedWorkTableExec {
    fn name(&self) -> &'static str {
        "SharedWorkTableExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "SharedWorkTableExec got an invalid partition {partition} (expected 0)"
            );
        }
        let mut batches = self.iteration_batches(context)?;
        if let Some(projection) = &self.projection {
            batches = batches
                .into_iter()
                .map(|b| b.project(projection))
                .collect::<std::result::Result<Vec<_>, _>>()?;
        }
        Ok(Box::pin(MemoryStream::try_new(
            batches,
            Arc::clone(&self.schema),
            None,
        )?))
    }

    /// Binds the work table that `RecursiveQueryExec` hands out. All references
    /// share one cell, so only the first one asked in a binding pass claims the
    /// table (and is counted by DataFusion as the one reference); the others
    /// decline. A new work table (a rebuilt `RecursiveQueryExec`) is claimed anew.
    fn with_new_state(&self, state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        let reader =
            WorkTableExec::new(self.work.name.clone(), Arc::clone(&self.work.schema), None)
                .ok()?
                .with_new_state(Arc::clone(&state))?;
        let mut shared = self.work.state.lock().ok()?;
        if let Some((bound, _)) = &shared.binding
            && Arc::ptr_eq(bound, &state)
        {
            return None;
        }
        shared.binding = Some((state, reader));
        shared.current = None;
        Some(Arc::new(self.clone()))
    }
}

/// Gives a copy of a logical plan its own recursive CTE work tables.
///
/// A CTE is inlined at every reference, so a query that reads a recursive CTE
/// twice holds two `RecursiveQuery` plans that would share one provider, and
/// therefore one work table cell. Each copy gets fresh providers here; scans of
/// the same provider within the copy keep sharing one.
///
/// Only plans that contain a finished `RecursiveQuery` are copied: inside its own
/// recursive term a CTE is a bare work table scan, and every such reference must
/// keep sharing the one work table.
pub fn refresh_work_tables(
    plan: datafusion_expr::LogicalPlan,
) -> Result<datafusion_expr::LogicalPlan> {
    use std::collections::HashMap;

    use datafusion::datasource::{provider_as_source, source_as_provider};
    use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
    use datafusion_expr::{LogicalPlan, TableScan};

    let has_recursive_query =
        plan.exists(|node| Ok(matches!(node, LogicalPlan::RecursiveQuery(_))))?;
    if !has_recursive_query {
        return Ok(plan);
    }

    let mut fresh: HashMap<*const SharedWork, Arc<dyn TableProvider>> = HashMap::new();
    plan.transform_up(|node| {
        let LogicalPlan::TableScan(scan) = &node else {
            return Ok(Transformed::no(node));
        };
        let Ok(provider) = source_as_provider(&scan.source) else {
            return Ok(Transformed::no(node));
        };
        let Some(table) = provider.downcast_ref::<SharedCteWorkTable>() else {
            return Ok(Transformed::no(node));
        };
        let replacement = fresh
            .entry(Arc::as_ptr(&table.work))
            .or_insert_with(|| {
                Arc::new(SharedCteWorkTable::new(
                    table.work.name.clone(),
                    Arc::clone(&table.work.schema),
                ))
            })
            .clone();
        let LogicalPlan::TableScan(scan) = node else {
            return internal_err!("expected a table scan");
        };
        Ok(Transformed::yes(LogicalPlan::TableScan(TableScan {
            source: provider_as_source(replacement),
            ..scan
        })))
    })
    .data()
}

/// Whether `plan` is a recursive CTE's self-reference: the (aliased) scan of
/// its work table. A CTE that merely reads the work table, defined inside the
/// recursive term, is not: it is shared like any other CTE, and computed again
/// for every iteration. Treating it as a self-reference would inline it at
/// every reference, and a chain of such CTEs grows exponentially.
pub fn is_work_table_reference(plan: &datafusion_expr::LogicalPlan) -> Result<bool> {
    use datafusion::datasource::source_as_provider;
    use datafusion_expr::LogicalPlan;

    let mut node = plan;
    while let LogicalPlan::SubqueryAlias(alias) = node {
        node = alias.input.as_ref();
    }
    Ok(match node {
        LogicalPlan::TableScan(scan) => source_as_provider(&scan.source)
            .map(|p| p.downcast_ref::<SharedCteWorkTable>().is_some())
            .unwrap_or(false),
        _ => false,
    })
}
