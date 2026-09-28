use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_ffi::execution_plan::{FFI_ExecutionPlan, ForeignExecutionPlan};
use sail_common_datafusion::connect_extension::HostInputExec;

use super::*;

#[derive(Debug)]
struct FixedProvider(Arc<dyn ExecutionPlan>);

#[async_trait]
impl TableProvider for FixedProvider {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        _session: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::clone(&self.0))
    }
}

#[tokio::test]
async fn native_region_blocks_host_repartition_insertion_inside_ffi_plan() -> Result<()> {
    // Keep the two-row fixture above the optimizer's one-batch threshold;
    // otherwise its exact in-memory statistics suppress repartitioning.
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(1),
    );
    let state = ctx.state();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])?;
    let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
    let input = Arc::new(HostInputExec::new(
        source,
        ctx.task_ctx(),
        tokio::runtime::Handle::current(),
    ));
    // Force the actual foreign adapter even in this one-library fixture. Its
    // default input requirements permit host repartitioning of its local child.
    let ffi = FFI_ExecutionPlan::new(
        Arc::new(CoalescePartitionsExec::new(input)),
        Some(tokio::runtime::Handle::current()),
    );
    let foreign: Arc<dyn ExecutionPlan> = Arc::new(ForeignExecutionPlan::try_from(ffi)?);
    assert!(!foreign.children().is_empty());
    let optimizer = EnsureRequirements::new();
    let rewritten = optimizer.optimize(Arc::clone(&foreign), state.config_options())?;
    assert!(
        displayable(rewritten.as_ref())
            .indent(true)
            .to_string()
            .contains("RepartitionExec"),
        "fixture must reproduce host insertion within an exposed foreign graph"
    );

    let provider = NativeTableProvider::new(Arc::new(FixedProvider(Arc::clone(&foreign))));
    let plan = provider.scan(&state, None, &[], None).await?;
    assert_eq!(plan.schema(), foreign.schema());
    // The wrapper restates the declared layout; nothing here declares one,
    // and an unknown partitioning never compares equal, so compare its shape.
    assert!(matches!(
        plan.properties().output_partitioning(),
        Partitioning::UnknownPartitioning(1)
    ));
    assert_eq!(
        plan.properties().output_partitioning().partition_count(),
        foreign.properties().output_partitioning().partition_count()
    );
    assert!(plan.properties().output_ordering().is_none());
    let plan = optimizer.optimize(plan, state.config_options())?;
    assert_eq!(plan.name(), "NativeRelationExec");
    assert!(plan.children().is_empty());
    #[expect(deprecated)]
    let unchanged = Arc::clone(&plan).with_new_children(vec![])?;
    #[expect(deprecated)]
    let rejected = Arc::clone(&plan).with_new_children(vec![foreign]);
    assert!(rejected.is_err());
    assert!(Arc::ptr_eq(&plan, &unchanged));
    let batches = collect(plan.execute(0, ctx.task_ctx())?).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    Ok(())
}


/// A column as it arrives across the FFI boundary: a leaf whose display is
/// `name@index`, equal only to itself, and nothing the host can downcast.
#[derive(Debug)]
struct OpaqueColumn {
    name: String,
    index: usize,
}

impl std::fmt::Display for OpaqueColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}@{}", self.name, self.index)
    }
}

impl PartialEq for OpaqueColumn {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}
impl Eq for OpaqueColumn {}
impl std::hash::Hash for OpaqueColumn {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.index.hash(state)
    }
}

impl PhysicalExpr for OpaqueColumn {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        Ok(input_schema.field(self.index).data_type().clone())
    }
    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(input_schema.field(self.index).is_nullable())
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<datafusion_expr::ColumnarValue> {
        Ok(datafusion_expr::ColumnarValue::Array(Arc::clone(
            batch.column(self.index),
        )))
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.name)
    }
}

fn opaque(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(OpaqueColumn {
        name: name.to_string(),
        index,
    })
}

#[test]
fn declared_layout_is_restated_with_host_columns_only_when_the_schema_agrees() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("rank", DataType::Float64, false),
    ]));
    let ordering = LexOrdering::new([PhysicalSortExpr::new_default(opaque("id", 0))]).unwrap();
    let inner = PlanProperties::new(
        EquivalenceProperties::new_with_orderings(Arc::clone(&schema), [ordering]),
        Partitioning::Hash(vec![opaque("id", 0)], 4),
        datafusion::physical_plan::execution_plan::EmissionType::Incremental,
        datafusion::physical_plan::execution_plan::Boundedness::Bounded,
    );
    let restated = declared_properties(&inner);
    match restated.output_partitioning() {
        Partitioning::Hash(exprs, 4) => {
            let column = exprs[0].downcast_ref::<Column>().expect("host column");
            assert_eq!((column.name(), column.index()), ("id", 0));
        }
        other => panic!("{other:?}"),
    }
    let sort = &restated.output_ordering().expect("ordering kept")[0];
    assert!(sort.expr.is::<Column>());
    assert_eq!(restated.emission_type, inner.emission_type);

    // A display that names the wrong column, an index past the schema, or a
    // non-column shape is kept as it came rather than guessed at.
    for (name, index) in [("rank", 0), ("id", 1), ("id", 7)] {
        assert!(!host_column(&opaque(name, index), &schema).is::<Column>());
    }
    let already = Arc::new(Column::new("rank", 1)) as Arc<dyn PhysicalExpr>;
    assert!(Arc::ptr_eq(&host_column(&already, &schema), &already));
}

/// A plan whose properties declare a layout the way an FFI plan does.
#[derive(Debug)]
struct DeclaredExec {
    inner: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for DeclaredExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "DeclaredExec")
    }
}

impl ExecutionPlan for DeclaredExec {
    fn name(&self) -> &'static str {
        "DeclaredExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
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
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}

fn declared_source(ctx: &SessionContext, name: &str, parts: usize, foreign: bool) -> Result<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(name, DataType::Int64, false),
    ]));
    // Bucket i holds the ids congruent to i, sorted: a layout the declaration
    // describes truthfully, whatever hash the engine would have used.
    let partitions: Vec<Vec<RecordBatch>> = (0..parts)
        .map(|bucket| {
            let ids: Vec<i64> = (0..40).filter(|v| (*v as usize) % parts == bucket).collect();
            let values: Vec<i64> = ids.iter().map(|v| v * 10).collect();
            vec![RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(ids)), Arc::new(Int64Array::from(values))],
            )
            .unwrap()]
        })
        .collect();
    let inner = MemorySourceConfig::try_new_exec(&partitions, Arc::clone(&schema), None)?;
    let key = if foreign { opaque("id", 0) } else { Arc::new(Column::new("id", 0)) };
    let ordering = LexOrdering::new([PhysicalSortExpr::new_default(Arc::clone(&key))]).unwrap();
    let properties = Arc::new(PlanProperties::new(
        EquivalenceProperties::new_with_orderings(schema, [ordering]),
        Partitioning::Hash(vec![key], parts),
        datafusion::physical_plan::execution_plan::EmissionType::Incremental,
        datafusion::physical_plan::execution_plan::Boundedness::Bounded,
    ));
    let _ = ctx;
    Ok(Arc::new(NativeTableProvider::new(Arc::new(FixedProvider(Arc::new(
        DeclaredExec { inner, properties },
    ))))))
}

#[tokio::test]
async fn foreign_declared_layout_lets_a_join_run_without_repartition() -> Result<()> {
    let parts = 4;
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(parts).with_batch_size(1),
    );
    let left = ctx.read_table(declared_source(&ctx, "a", parts, true)?)?;
    let right = ctx
        .read_table(declared_source(&ctx, "b", parts, true)?)?
        .select(vec![
            datafusion::prelude::col("id").alias("rid"),
            datafusion::prelude::col("b"),
        ])?;
    let joined = left.join(right, datafusion_expr::JoinType::Inner, &["id"], &["rid"], None)?;
    let plan = joined.clone().create_physical_plan().await?;
    let text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(!text.contains("RepartitionExec"), "{text}");
    assert!(!text.contains("SortExec"), "{text}");
    let rows: usize = joined
        .collect()
        .await?
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 40);
    Ok(())
}
