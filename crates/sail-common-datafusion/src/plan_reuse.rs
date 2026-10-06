//! Running one physical plan many times over changing inputs.
//!
//! A client that runs the same large query again and again, each time over new
//! rows (a game's tic, a simulation step), pays for planning on every run. Two
//! session-level pieces let it plan once:
//!
//! * A **slot** is a temporary view whose rows are held in memory and replaced
//!   in place. Creating or replacing a view named in `spark.sail.slotViews`
//!   runs the view's query once and stores its rows in the slot; the view
//!   itself becomes a scan of the slot ([`SlotTable`], [`SlotExec`]), which
//!   reads whatever the slot holds when the scan is executed.
//! * The **plan cache** keeps the physical plan of each query the session runs
//!   while `spark.sail.planCache` is `true`, keyed by the query as received.
//!   A repeated query skips analysis, optimization and physical planning: the
//!   cached plan's operator state is reset and the plan runs again, reading the
//!   slots' current rows.
//!
//! The cache assumes the query's other inputs (tables and views that are not
//! slots) do not change while it is on; turning it off clears it. A slot whose
//! schema changes is replaced by a new one, which also clears the cache, since
//! cached plans read the old slot.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions, SendableRecordBatchStream,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{Expr, TableType};

use crate::extension::SessionExtension;

/// The session option naming the views that are slots (comma-separated).
pub const SLOT_VIEWS_OPTION: &str = "spark.sail.slotViews";
/// The session option that turns the plan cache on (`true`) or off.
pub const PLAN_CACHE_OPTION: &str = "spark.sail.planCache";
/// The session option for the number of partitions that cached plans are
/// planned for. Queries over a few thousand rows in slots gain nothing from
/// spreading them over every core, and pay for the repartitioning each run.
pub const TARGET_PARTITIONS_OPTION: &str = "spark.sail.targetPartitions";

/// The rows of one slot.
struct Slot {
    name: String,
    schema: SchemaRef,
    batches: RwLock<Arc<Vec<RecordBatch>>>,
}

impl Debug for Slot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slot").field("name", &self.name).finish()
    }
}

impl Slot {
    fn current(&self) -> Result<Arc<Vec<RecordBatch>>> {
        match self.batches.read() {
            Ok(batches) => Ok(Arc::clone(&batches)),
            Err(_) => internal_err!("slot {} is poisoned", self.name),
        }
    }
}

/// A slot as a table: every scan reads the slot's rows at execution time.
#[derive(Debug, Clone)]
pub struct SlotTable {
    slot: Arc<Slot>,
}

#[async_trait]
impl TableProvider for SlotTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.slot.schema)
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
        Ok(Arc::new(SlotExec::try_new(
            Arc::clone(&self.slot),
            projection.cloned(),
        )?))
    }
}

/// A scan of a slot. It reports no statistics, so that a plan made for one
/// set of rows makes no assumption that a later set breaks.
#[derive(Debug, Clone)]
pub struct SlotExec {
    slot: Arc<Slot>,
    projection: Option<Vec<usize>>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl SlotExec {
    fn try_new(slot: Arc<Slot>, projection: Option<Vec<usize>>) -> Result<Self> {
        let schema = match &projection {
            Some(projection) => Arc::new(slot.schema.project(projection)?),
            None => Arc::clone(&slot.schema),
        };
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            slot,
            projection,
            schema,
            cache,
        })
    }
}

impl DisplayAs for SlotExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SlotExec: name={}", self.slot.name)
            }
            DisplayFormatType::TreeRender => write!(f, "name={}", self.slot.name),
        }
    }
}

impl ExecutionPlan for SlotExec {
    fn name(&self) -> &'static str {
        "SlotExec"
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
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("SlotExec got an invalid partition {partition} (expected 0)");
        }
        let batches = self.slot.current()?;
        let batches = match &self.projection {
            Some(projection) => batches
                .iter()
                .map(|b| b.project(projection))
                .collect::<std::result::Result<Vec<_>, _>>()?,
            None => batches.as_ref().clone(),
        };
        Ok(Box::pin(MemoryStream::try_new(
            batches,
            Arc::clone(&self.schema),
            None,
        )?))
    }
}

/// The session's slots and cached plans.
#[derive(Default)]
pub struct PlanReuse {
    slots: Mutex<HashMap<String, Arc<Slot>>>,
    plans: Mutex<HashMap<String, Arc<dyn ExecutionPlan>>>,
}

impl Debug for PlanReuse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanReuse").finish()
    }
}

impl SessionExtension for PlanReuse {
    fn name() -> &'static str {
        "plan reuse"
    }
}

impl PlanReuse {
    /// Stores `batches` in the slot `name` and returns the slot as a table.
    /// A slot with the same schema keeps its identity, so plans that read it
    /// see the new rows; otherwise a new slot replaces it and the plan cache is
    /// cleared.
    pub fn update_slot(
        &self,
        name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<SlotTable> {
        let Ok(mut slots) = self.slots.lock() else {
            return internal_err!("plan reuse slots are poisoned");
        };
        if let Some(slot) = slots.get(name)
            && slot.schema == schema
        {
            let Ok(mut current) = slot.batches.write() else {
                return internal_err!("slot {name} is poisoned");
            };
            *current = Arc::new(batches);
            return Ok(SlotTable {
                slot: Arc::clone(slot),
            });
        }
        let slot = Arc::new(Slot {
            name: name.to_string(),
            schema,
            batches: RwLock::new(Arc::new(batches)),
        });
        slots.insert(name.to_string(), Arc::clone(&slot));
        self.clear_plans()?;
        Ok(SlotTable { slot })
    }

    pub fn cached_plan(&self, key: &str) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Ok(plans) = self.plans.lock() else {
            return internal_err!("plan cache is poisoned");
        };
        Ok(plans.get(key).cloned())
    }

    pub fn cache_plan(&self, key: String, plan: Arc<dyn ExecutionPlan>) -> Result<()> {
        let Ok(mut plans) = self.plans.lock() else {
            return internal_err!("plan cache is poisoned");
        };
        plans.insert(key, plan);
        Ok(())
    }

    pub fn clear_plans(&self) -> Result<()> {
        let Ok(mut plans) = self.plans.lock() else {
            return internal_err!("plan cache is poisoned");
        };
        plans.clear();
        Ok(())
    }
}

/// Whether `name` is one of the comma-separated slot view names.
pub fn is_slot_view(option: Option<&str>, name: &str) -> bool {
    option.is_some_and(|names| {
        names
            .split(',')
            .any(|n| n.trim().eq_ignore_ascii_case(name))
    })
}

/// Resets the operator state of a plan that is about to run again, as
/// DataFusion's `reset_plan_states` does, but keeps every node's computed
/// properties: a node whose children are unchanged is only reset, and one
/// whose children were reset gets them with `ChildrenPropertiesMode::Keep`.
/// The tree's shape is unchanged, so its properties still hold, and
/// recomputing them (equivalences, orderings) costs about what planning does.
pub fn reset_plan_keep_properties(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    let mut new_children = Vec::with_capacity(children.len());
    let mut changed = false;
    for child in children {
        let reset = reset_plan_keep_properties(Arc::clone(child))?;
        changed |= !Arc::ptr_eq(&reset, child);
        new_children.push(reset);
    }
    let plan = if changed {
        plan.replace_children(
            new_children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
        )?
    } else {
        plan
    };
    plan.reset_state()
}
