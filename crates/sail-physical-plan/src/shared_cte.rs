use std::fmt::Formatter;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions, SendableRecordBatchStream, collect,
};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result, internal_err};
use futures::future::{BoxFuture, FutureExt, Shared};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::Notify;

type SharedBatches =
    Shared<BoxFuture<'static, std::result::Result<Arc<Vec<RecordBatch>>, Arc<DataFusionError>>>>;

/// The result of one shared CTE in one execution of its plan.
///
/// `WithSharedCtesExec` starts the CTE's computation as a task of its own
/// before it runs the plan that reads it, and every `SharedCteRefExec` awaits
/// it. A reference may run before that, as in a scalar subquery that is
/// executed ahead of the main plan, so a reference that finds nothing started
/// starts the computation itself, from the definition `WithSharedCtesExec`
/// registered (the latest one, after the physical optimizer).
/// `RecursiveQueryExec` runs its recursive term again for every iteration,
/// resetting the plan's state first; the reset of the `WithSharedCtesExec`
/// clears the result, so a CTE defined inside a recursive term is computed
/// again for each iteration, and one defined outside it is computed once.
#[derive(Debug, Default)]
pub struct SharedCteResult {
    batches: Mutex<Option<SharedBatches>>,
    definition: Mutex<Option<Arc<dyn ExecutionPlan>>>,
    started: Notify,
}

impl SharedCteResult {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Option<SharedBatches>>> {
        self.batches
            .lock()
            .map_err(|_| DataFusionError::Internal("shared CTE result is poisoned".to_string()))
    }

    /// Starts computing `plan` into this result, unless it has started.
    fn start(&self, plan: &Arc<dyn ExecutionPlan>, context: &Arc<TaskContext>) -> Result<()> {
        let mut batches = self.lock()?;
        if batches.is_some() {
            return Ok(());
        }
        let task = tokio::spawn(collect(Arc::clone(plan), Arc::clone(context)));
        let shared: SharedBatches = async move {
            match task.await {
                Ok(Ok(batches)) => Ok(Arc::new(batches)),
                Ok(Err(e)) => Err(Arc::new(e)),
                Err(e) => Err(Arc::new(DataFusionError::External(Box::new(e)))),
            }
        }
        .boxed()
        .shared();
        *batches = Some(shared);
        drop(batches);
        self.started.notify_waiters();
        Ok(())
    }

    fn set_definition(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
        match self.definition.lock() {
            Ok(mut definition) => {
                *definition = Some(Arc::clone(plan));
                Ok(())
            }
            Err(_) => internal_err!("shared CTE definition is poisoned"),
        }
    }

    fn definition(&self) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        match self.definition.lock() {
            Ok(definition) => Ok(definition.clone()),
            Err(_) => internal_err!("shared CTE definition is poisoned"),
        }
    }

    /// The batches: started here if nothing has started them yet.
    async fn get(self: Arc<Self>, context: Arc<TaskContext>) -> Result<Arc<Vec<RecordBatch>>> {
        loop {
            let notified = self.started.notified();
            let started = self.lock()?.clone();
            if let Some(batches) = started {
                return batches.await.map_err(DataFusionError::Shared);
            }
            if let Some(definition) = self.definition()? {
                self.start(&definition, &context)?;
                continue;
            }
            notified.await;
        }
    }

    fn reset(&self) -> Result<()> {
        *self.lock()? = None;
        Ok(())
    }

    /// The batches, if the computation has finished.
    pub fn finished(&self) -> Result<Option<Arc<Vec<RecordBatch>>>> {
        let started = self.lock()?.clone();
        match started.and_then(|f| f.now_or_never()) {
            Some(Ok(batches)) => Ok(Some(batches)),
            Some(Err(e)) => Err(DataFusionError::Shared(e)),
            None => Ok(None),
        }
    }
}

/// The schema and finished batches of the shared CTE `name` in `plan`: what a
/// query computed for that CTE, once the query has run.
pub fn shared_cte_batches(
    plan: &Arc<dyn ExecutionPlan>,
    name: &str,
) -> Result<Option<(SchemaRef, Arc<Vec<RecordBatch>>)>> {
    let mut found = None;
    plan.apply(|node| {
        if let Some(with) = node.downcast_ref::<WithSharedCtesExec>()
            && let Some(i) = with.names.iter().position(|n| n.eq_ignore_ascii_case(name))
        {
            if let Some(batches) = with.results[i].finished()? {
                found = Some((with.children[i].schema(), batches));
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

fn single_partition(schema: SchemaRef) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    ))
}

/// A reference to a shared CTE: streams the CTE's result.
#[derive(Debug, Clone)]
pub struct SharedCteRefExec {
    name: String,
    result: Arc<SharedCteResult>,
    cache: Arc<PlanProperties>,
}

impl SharedCteRefExec {
    pub fn new(name: String, result: Arc<SharedCteResult>, schema: SchemaRef) -> Self {
        Self {
            name,
            result,
            cache: single_partition(schema),
        }
    }
}

impl DisplayAs for SharedCteRefExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SharedCteRefExec: name={}", self.name)
            }
            DisplayFormatType::TreeRender => write!(f, "name={}", self.name),
        }
    }
}

impl ExecutionPlan for SharedCteRefExec {
    fn name(&self) -> &'static str {
        "SharedCteRefExec"
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
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("SharedCteRefExec takes no children");
        }
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

    /// A reference leaves the result alone: the `WithSharedCtesExec` that owns
    /// the CTE resets it. A recursive query resets the plan of its recursive
    /// term for every iteration, and a CTE defined outside that term is the
    /// same for every iteration; clearing it from a reference inside the term
    /// would run its (already consumed) definition again.
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "SharedCteRefExec got an invalid partition {partition} (expected 0)"
            );
        }
        let result = Arc::clone(&self.result);
        let stream = futures::stream::once(async move {
            let batches = result.get(context).await?;
            Ok::<_, DataFusionError>(futures::stream::iter(
                batches.iter().cloned().map(Ok).collect::<Vec<_>>(),
            ))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream.boxed(),
        )))
    }
}

/// Starts the shared CTEs of one `WITH` scope, then runs the plan that reads
/// them. The children are the CTE definitions followed by the plan.
#[derive(Debug, Clone)]
pub struct WithSharedCtesExec {
    names: Vec<String>,
    results: Vec<Arc<SharedCteResult>>,
    children: Vec<Arc<dyn ExecutionPlan>>,
}

impl WithSharedCtesExec {
    pub fn try_new(
        names: Vec<String>,
        results: Vec<Arc<SharedCteResult>>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Self> {
        if results.len() != names.len() || children.len() != results.len() + 1 {
            return internal_err!("WithSharedCtesExec takes one child per CTE plus one");
        }
        for (result, definition) in results.iter().zip(&children) {
            result.set_definition(definition)?;
        }
        Ok(Self {
            names,
            results,
            children,
        })
    }

    fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.children[self.results.len()]
    }
}

impl DisplayAs for WithSharedCtesExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "WithSharedCtesExec: names={:?}", self.names)
            }
            DisplayFormatType::TreeRender => write!(f, "names={:?}", self.names),
        }
    }
}

impl ExecutionPlan for WithSharedCtesExec {
    fn name(&self) -> &'static str {
        "WithSharedCtesExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input().properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        let mut order = vec![false; self.results.len()];
        order.push(true);
        order
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.children.len()]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            self.names.clone(),
            self.results.clone(),
            children,
        )?))
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

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        for result in &self.results {
            result.reset()?;
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        for (result, definition) in self.results.iter().zip(&self.children) {
            result.start(definition, &context)?;
        }
        self.input().execute(partition, context)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn reset_clears_a_started_computation() {
        let result = SharedCteResult::new();
        result.reset().unwrap();
        assert!(result.batches.lock().unwrap().is_none());
    }
}
