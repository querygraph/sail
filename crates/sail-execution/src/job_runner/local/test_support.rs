use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_datafusion_err, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
};
use futures::Stream;

type Execute = dyn FnOnce(Arc<TaskContext>) -> Result<SendableRecordBatchStream> + Send;

pub(super) fn plan(
    callback: impl FnOnce(Arc<TaskContext>) -> Result<SendableRecordBatchStream> + Send + 'static,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(CallbackExec {
        callback: Mutex::new(Some(Box::new(callback))),
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Bounded,
        )),
    })
}

struct CallbackExec {
    callback: Mutex<Option<Box<Execute>>>,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for CallbackExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CallbackExec")
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CallbackExec {
    fn fmt_as(&self, _: DisplayFormatType, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CallbackExec")
    }
}

impl ExecutionPlan for CallbackExec {
    fn name(&self) -> &str {
        "CallbackExec"
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

    #[expect(deprecated)]
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: datafusion::physical_plan::ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(children)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("CallbackExec does not accept children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("CallbackExec only accepts partition zero");
        }
        let callback = {
            let mut callback = self
                .callback
                .lock()
                .map_err(|_| internal_datafusion_err!("test plan callback lock poisoned"))?;
            callback
                .take()
                .ok_or_else(|| internal_datafusion_err!("test plan executed more than once"))?
        };
        callback(context)
    }
}

#[derive(Default)]
pub(super) struct Counts {
    pub(super) polls: AtomicUsize,
    pub(super) drops: AtomicUsize,
}

type Next = dyn FnMut(&mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> + Send;

pub(super) fn stream(
    poll: impl FnMut(&mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> + Send + 'static,
) -> (SendableRecordBatchStream, Arc<Counts>) {
    let counts = Arc::new(Counts::default());
    (
        Box::pin(CallbackStream {
            poll: Box::new(poll),
            schema: Arc::new(Schema::empty()),
            counts: counts.clone(),
        }),
        counts,
    )
}

struct CallbackStream {
    poll: Box<Next>,
    schema: SchemaRef,
    counts: Arc<Counts>,
}

impl Stream for CallbackStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.counts.polls.fetch_add(1, Ordering::SeqCst);
        (this.poll)(context)
    }
}

impl RecordBatchStream for CallbackStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for CallbackStream {
    fn drop(&mut self) {
        self.counts.drops.fetch_add(1, Ordering::SeqCst);
    }
}
