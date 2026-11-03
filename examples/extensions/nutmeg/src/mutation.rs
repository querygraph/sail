//! Lazy, at-most-once graph writes and drops.
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
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
    SendableRecordBatchStream, execute_stream,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_execution::TaskContext;
use futures::{TryStreamExt, stream};
use nutmeg_graph::{ColumnMapping, SessionRegistry, StageOrder};

use crate::Request;

#[derive(Debug)]
enum Operation {
    Stage {
        nodes: ColumnMapping,
        edges: ColumnMapping,
    },
    Drop,
}

#[derive(Debug)]
enum Attempt {
    Ready,
    Running,
    Complete(RecordBatch),
}

#[derive(Debug)]
struct Mutation {
    registry: SessionRegistry,
    graph: String,
    operation: Operation,
    schema: SchemaRef,
    attempt: Mutex<Attempt>,
}

#[derive(Debug)]
pub(crate) struct MutationTable {
    mutation: Arc<Mutation>,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
}

impl MutationTable {
    pub fn stage(
        registry: SessionRegistry,
        request: Request,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Self> {
        fn mapping(values: std::collections::BTreeMap<String, String>) -> Result<ColumnMapping> {
            let mut mapping = ColumnMapping::default();
            for (key, value) in values {
                if !mapping.set(&key.to_ascii_lowercase(), value) {
                    return plan_err!("nutmeg: unknown column mapping {key}");
                }
            }
            Ok(mapping)
        }
        let nodes = mapping(request.node_mapping)?;
        let edges = mapping(request.edge_mapping)?;
        // Empty streams still have schemas. Reject missing/mistyped structural
        // fields before a lazy provider can replace a graph with empty inputs.
        nutmeg_graph::normalize_nodes(&RecordBatch::new_empty(inputs[0].schema()), &nodes)?;
        nutmeg_graph::normalize_edges(&RecordBatch::new_empty(inputs[1].schema()), &edges)?;
        Ok(Self {
            mutation: Arc::new(Mutation {
                registry,
                graph: request.graph,
                operation: Operation::Stage { nodes, edges },
                schema: Arc::new(Schema::new(vec![
                    Field::new("graph", DataType::Utf8, false),
                    Field::new("nodeCount", DataType::Int64, false),
                    Field::new("edgeCount", DataType::Int64, false),
                    Field::new("revision", DataType::Int64, false),
                ])),
                attempt: Mutex::new(Attempt::Ready),
            }),
            inputs,
        })
    }

    pub fn drop(registry: SessionRegistry, graph: String) -> Self {
        Self {
            mutation: Arc::new(Mutation {
                registry,
                graph,
                operation: Operation::Drop,
                schema: Arc::new(Schema::new(vec![
                    Field::new("graph", DataType::Utf8, false),
                    Field::new("dropped", DataType::Boolean, false),
                ])),
                attempt: Mutex::new(Attempt::Ready),
            }),
            inputs: vec![],
        }
    }
}

#[async_trait]
impl TableProvider for MutationTable {
    fn schema(&self) -> SchemaRef {
        self.mutation.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        _session: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MutationExec::new(
            self.mutation.clone(),
            self.inputs.clone(),
            projection.cloned(),
        )?))
    }
}

#[derive(Debug)]
struct MutationExec {
    mutation: Arc<Mutation>,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl MutationExec {
    fn new(
        mutation: Arc<Mutation>,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let schema = match &projection {
            Some(p) => Arc::new(mutation.schema.project(p)?),
            None => mutation.schema.clone(),
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            mutation,
            inputs,
            projection,
            properties,
        })
    }
}

impl DisplayAs for MutationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "NutmegMutationExec: graph={}, operation={:?}, placement=driver, at_most_once=true",
            self.mutation.graph, self.mutation.operation
        )
    }
}

impl ExecutionPlan for MutationExec {
    fn name(&self) -> &str {
        "NutmegMutationExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
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
        if children.len() != self.inputs.len() {
            return exec_err!("nutmeg: mutation input arity changed");
        }
        Ok(Arc::new(Self::new(
            self.mutation.clone(),
            children,
            self.projection.clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("nutmeg: mutation has one output partition");
        }
        let (mutation, inputs, projection) = (
            self.mutation.clone(),
            self.inputs.clone(),
            self.projection.clone(),
        );
        let future = async move {
            let cached = {
                let mut attempt = mutation.attempt.lock().map_err(|_| {
                    datafusion_common::DataFusionError::Execution(
                        "nutmeg: attempt lock poisoned".into(),
                    )
                })?;
                match &*attempt {
                    Attempt::Complete(batch) => Some(batch.clone()),
                    Attempt::Running => {
                        return exec_err!(
                            "nutmeg: mutation already attempted; concurrent/retried execution is refused and an unacknowledged outcome is indeterminate"
                        );
                    }
                    Attempt::Ready => {
                        *attempt = Attempt::Running;
                        None
                    }
                }
            };
            let batch = match cached {
                Some(batch) => batch,
                None => {
                    let batch = mutation.run(inputs, context).await?;
                    *mutation.attempt.lock().map_err(|_| {
                        datafusion_common::DataFusionError::Execution(
                            "nutmeg: acknowledgement outcome indeterminate".into(),
                        )
                    })? = Attempt::Complete(batch.clone());
                    batch
                }
            };
            match projection {
                Some(p) => Ok(batch.project(&p)?),
                None => Ok(batch),
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream::once(future),
        )))
    }
}

impl Mutation {
    async fn run(
        &self,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        context: Arc<TaskContext>,
    ) -> Result<RecordBatch> {
        let graph: ArrayRef = Arc::new(StringArray::from(vec![self.graph.as_str()]));
        let columns = match &self.operation {
            Operation::Drop => vec![
                graph,
                Arc::new(BooleanArray::from(vec![
                    self.registry.drop_graph(&self.graph)?,
                ])) as ArrayRef,
            ],
            Operation::Stage { nodes, edges } => {
                let mut staging =
                    self.registry
                        .replacing(&self.graph, nodes, edges, StageOrder::Canonical);
                for (index, input) in inputs.into_iter().enumerate() {
                    // A completely empty stream still carries property fields.
                    // Retain its schema before execution yields any row batches.
                    let empty = RecordBatch::new_empty(input.schema());
                    if index == 0 {
                        staging.push_nodes(&empty)?;
                    } else {
                        staging.push_edges(&empty)?;
                    }
                    // execute_stream coalesces every partition, including uneven
                    // and empty partitions. Never assume the child has width 1.
                    let mut stream = execute_stream(input, context.clone())?;
                    while let Some(batch) = stream.try_next().await? {
                        if index == 0 {
                            staging.push_nodes(&batch)?;
                        } else {
                            staging.push_edges(&batch)?;
                        }
                    }
                }
                let info = staging.finish()?;
                vec![
                    graph,
                    Arc::new(Int64Array::from(vec![info.staged_nodes as i64])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![info.staged_edges as i64])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![info.revision as i64])) as ArrayRef,
                ]
            }
        };
        self.registry
            .retain_output_owner(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}
