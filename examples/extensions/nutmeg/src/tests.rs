use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;

use super::*;

fn batch(columns: &[(&str, Vec<&str>)]) -> RecordBatch {
    RecordBatch::try_from_iter(columns.iter().map(|(name, values)| {
        (
            *name,
            Arc::new(StringArray::from(values.clone())) as ArrayRef,
        )
    }))
    .unwrap()
}

async fn input(ctx: &SessionContext, batches: Vec<Vec<RecordBatch>>) -> Arc<dyn ExecutionPlan> {
    let schema = batches.iter().flatten().next().unwrap().schema();
    MemTable::try_new(schema, batches)
        .unwrap()
        .scan(&ctx.state(), None, &[], None)
        .await
        .unwrap()
}

async fn execute(ctx: &SessionContext, table: Arc<dyn TableProvider>) -> Result<Vec<RecordBatch>> {
    let plan = table.scan(&ctx.state(), None, &[], None).await?;
    collect(plan, ctx.task_ctx()).await
}

fn payload(verb: &str, graph: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"version": 1, "verb": verb, "graph": graph})).unwrap()
}

#[tokio::test]
async fn two_input_stage_reads_all_four_partitions_and_replays_receipt() {
    nutmeg_graph::prepare_output_schemas().unwrap();
    let registry = SessionRegistry::new(16 << 20);
    let ctx = SessionContext::new();
    let nodes = input(
        &ctx,
        vec![
            vec![batch(&[("node_id", vec!["a", "b"])])],
            vec![],
            vec![batch(&[("node_id", vec!["c"])])],
            vec![batch(&[("node_id", vec!["d", "e"])])],
        ],
    )
    .await;
    let edges = input(
        &ctx,
        vec![
            vec![batch(&[("source", vec!["a"]), ("target", vec!["b"])])],
            vec![batch(&[
                ("source", vec!["b", "c"]),
                ("target", vec!["c", "d"]),
            ])],
            vec![],
            vec![batch(&[("source", vec!["d"]), ("target", vec!["e"])])],
        ],
    )
    .await;
    let provider = plan(
        &registry,
        TYPE_URL,
        &payload("stage", "g"),
        vec![nodes, edges],
    )
    .unwrap();
    assert!(
        registry.list().unwrap().is_empty(),
        "planning must not stage"
    );
    assert_eq!(
        provider
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["graph", "nodeCount", "edgeCount", "revision"]
    );
    let physical = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
    assert!(registry.list().unwrap().is_empty(), "scan must not stage");
    let result = collect(physical.clone(), ctx.task_ctx()).await.unwrap();
    assert_eq!(
        result[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );
    assert_eq!(
        result[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        4
    );
    assert_eq!(registry.list().unwrap()[0].revision, 1);
    let replay = collect(physical, ctx.task_ctx()).await.unwrap();
    assert_eq!(result, replay);
    assert_eq!(
        registry.list().unwrap()[0].revision,
        1,
        "replay must not mutate twice"
    );

    let request = serde_json::to_vec(
        &serde_json::json!({"version":1,"verb":"run","graph":"g","algorithm":"degree"}),
    )
    .unwrap();
    let algorithm = plan(&registry, TYPE_URL, &request, vec![]).unwrap();
    assert!(
        registry.reads().unwrap().is_empty(),
        "planning must not run a kernel"
    );
    let results = execute(&ctx, algorithm).await.unwrap();
    assert_eq!(results.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
    assert_eq!(registry.reads().unwrap().len(), 1);
    let total_degree: f64 = results
        .iter()
        .flat_map(|b| {
            let col = b.column_by_name("degree").unwrap();
            (0..b.num_rows()).map(move |row| {
                datafusion_common::ScalarValue::try_from_array(col, row)
                    .unwrap()
                    .to_string()
                    .parse::<f64>()
                    .unwrap()
            })
        })
        .sum();
    assert_eq!(total_degree, 4.0);
    let dropped = execute(
        &ctx,
        plan(&registry, TYPE_URL, &payload("drop", "g"), vec![]).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(dropped[0].num_rows(), 1);
    assert!(registry.list().unwrap().is_empty());
}

#[tokio::test]
async fn ffi_provider_round_trip_preserves_native_execution() {
    let ctx = SessionContext::new();
    nutmeg_graph::prepare_output_schemas().unwrap();
    let registry = SessionRegistry::new(16 << 20);
    let node_plan = input(&ctx, vec![vec![batch(&[("node_id", vec!["a", "b"])])]]).await;
    let edge_plan = input(
        &ctx,
        vec![vec![batch(&[("source", vec!["a"]), ("target", vec!["b"])])]],
    )
    .await;
    let mut exported = FFI_ExecutionPlan::new(edge_plan, Some(tokio::runtime::Handle::current()));
    extern "C" fn foreign_marker() -> usize {
        0
    }
    exported.library_marker_id = foreign_marker;
    let imported: Arc<dyn ExecutionPlan> = (&exported).try_into().unwrap();
    let provider = plan(
        &registry,
        TYPE_URL,
        &payload("stage", "ffi"),
        vec![node_plan, imported],
    )
    .unwrap();
    let (imported, weak_context): (Arc<dyn TableProvider>, _) = {
        let context = Arc::new(ContextProvider(ctx.task_ctx()));
        let weak_context = Arc::downgrade(&context);
        let provider: Arc<dyn TableProvider> = Arc::new(OwnedProvider {
            inner: provider,
            context: context.clone(),
        });
        let context: Arc<dyn TaskContextProvider> = context;
        let mut exported = FFI_TableProvider::new(
            provider,
            false,
            Some(tokio::runtime::Handle::current()),
            &context,
            None,
        );
        exported.library_marker_id = foreign_marker;
        // The local strong context and capsule disappear before scan begins.
        ((&exported).into(), weak_context)
    };
    let physical = imported.scan(&ctx.state(), None, &[], None).await.unwrap();
    drop(imported);
    assert!(
        weak_context.upgrade().is_some(),
        "the physical plan retains the context after its provider is dropped"
    );
    let rows = collect(physical, ctx.task_ctx()).await.unwrap();
    assert!(
        weak_context.upgrade().is_none(),
        "context ownership is released after the provider and plan are gone"
    );
    assert_eq!(
        rows[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        1
    );
}

#[test]
fn rejects_wrong_verb_version_shape_and_algorithm_before_execution() {
    nutmeg_graph::prepare_output_schemas().unwrap();
    let registry = SessionRegistry::new(1 << 20);
    for request in [
        serde_json::json!({"version":2,"verb":"drop","graph":"g"}),
        serde_json::json!({"version":1,"verb":"unknown","graph":"g"}),
        serde_json::json!({"version":1,"verb":"stage","graph":"g"}),
        serde_json::json!({"version":1,"verb":"run","graph":"g"}),
        serde_json::json!({"version":1,"verb":"drop","graph":"g","ignored":true}),
    ] {
        assert!(
            plan(
                &registry,
                TYPE_URL,
                &serde_json::to_vec(&request).unwrap(),
                vec![]
            )
            .is_err(),
            "{request}"
        );
    }
}

/// Five batches are mathematically too many to complete after one is consumed:
/// the channel holds two, and the producer blocks while handing over the fourth.
/// No timing assumption decides whether the producer is still running.
#[tokio::test(flavor = "multi_thread")]
async fn dropped_blocked_stream_cancels_and_keeps_exported_batch_valid() {
    use futures::StreamExt;
    use nutmeg_graph::{ColumnMapping, READ_CHANNEL_BATCHES, ReadState, StageOrder};
    nutmeg_graph::prepare_output_schemas().unwrap();
    let registry = SessionRegistry::new(256 << 20);
    let ctx = SessionContext::new();
    const N: usize = 5 * 8192;
    let ids: Vec<String> = (0..N).map(|i| format!("n{i:06}")).collect();
    let edges = RecordBatch::try_from_iter([
        (
            "source",
            Arc::new(StringArray::from(ids.clone())) as ArrayRef,
        ),
        ("target", Arc::new(StringArray::from(ids)) as ArrayRef),
    ])
    .unwrap();
    let mapping = ColumnMapping::default();
    let mut tx = registry.replacing("cancel", &mapping, &mapping, StageOrder::Canonical);
    tx.push_edges(&edges).unwrap();
    tx.finish().unwrap();
    drop(edges);
    let table = registry
        .algorithm("degree", "cancel", &Default::default(), ColumnNames::Grust)
        .unwrap();
    let native: Arc<dyn ExecutionPlan> =
        Arc::new(AlgorithmExec::try_new(Arc::new(table), None, None).unwrap());
    let exported = FFI_ExecutionPlan::new(native, Some(tokio::runtime::Handle::current()));
    // Force the foreign adapter so returned Arrow batches really cross the
    // FFI stream, rather than the same-library marker taking its shortcut.
    let exec: Arc<dyn ExecutionPlan> =
        Arc::new(datafusion_ffi::execution_plan::ForeignExecutionPlan::try_from(exported).unwrap());
    let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
    let retained = stream.next().await.unwrap().unwrap();
    assert_eq!(retained.num_rows(), 8192);
    let expected = retained.clone();
    let before = registry.reads().unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].state, ReadState::Running);
    assert!(before[0].batches <= 1 + READ_CHANNEL_BATCHES + 1);
    drop(stream);
    // This is an eventual-completion watchdog, not a raced execution window.
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if registry.reads().unwrap()[0].state != ReadState::Running {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled producer must terminate");
    let ended = registry.reads().unwrap();
    assert_eq!(ended[0].state, ReadState::Cancelled);
    assert!(ended[0].rows < N);
    assert_eq!(
        retained, expected,
        "consumer-owned buffers survive stream drop"
    );
    assert_eq!(
        retained
            .column_by_name("nodeId")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "n000000"
    );
    drop(exec);
    registry.drop_graph("cancel").unwrap();
    assert!(
        registry.memory().unwrap().used_bytes > 0,
        "consumer-held Arrow buffers retain their admission after graph and plan drop"
    );
    assert_eq!(retained, expected);
    drop(expected);
    drop(retained);
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while registry.memory().unwrap().used_bytes != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all graph and read accounting released");
}

#[tokio::test]
async fn empty_inputs_validate_schema_and_mapping_before_mutation() {
    nutmeg_graph::prepare_output_schemas().unwrap();
    let registry = SessionRegistry::new(16 << 20);
    let ctx = SessionContext::new();
    let empty_nodes = input(&ctx, vec![vec![batch(&[("node_id", vec![])])]]).await;
    let empty_edges = input(
        &ctx,
        vec![vec![batch(&[("source", vec![]), ("target", vec![])])]],
    )
    .await;
    let empty_wrong = input(&ctx, vec![vec![batch(&[("missing", vec![])])]]).await;
    let valid = plan(
        &registry,
        TYPE_URL,
        &payload("stage", "empty"),
        vec![empty_nodes.clone(), empty_edges.clone()],
    )
    .unwrap();
    execute(&ctx, valid).await.unwrap();
    let before = registry.list().unwrap();
    assert_eq!(before[0].staged_edges, 0);
    assert!(
        plan(
            &registry,
            TYPE_URL,
            &payload("stage", "empty"),
            vec![empty_nodes.clone(), empty_wrong]
        )
        .is_err()
    );
    let mapping = serde_json::to_vec(&serde_json::json!({"version":1,"verb":"stage","graph":"empty","nodeMapping":{"idColumn":"does_not_exist"}})).unwrap();
    assert!(
        plan(
            &registry,
            TYPE_URL,
            &mapping,
            vec![empty_nodes, empty_edges]
        )
        .is_err()
    );
    assert_eq!(registry.list().unwrap()[0].revision, before[0].revision);
}
