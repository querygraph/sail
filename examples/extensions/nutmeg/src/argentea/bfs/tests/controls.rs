use super::*;
use datafusion::{
    physical_expr::PhysicalExpr,
    physical_plan::{
        DisplayAs, DisplayFormatType, PlanProperties, SendableRecordBatchStream,
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_common::{Result, tree_node::TreeNodeRecursion};
use datafusion_execution::TaskContext;
use futures::TryStreamExt;
use std::{fmt, task::Poll};
use tokio::sync::Notify;

#[derive(Debug)]
struct PendingInput {
    inner: Arc<dyn ExecutionPlan>,
    polled: Arc<Notify>,
}
impl DisplayAs for PendingInput {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PendingBfsInput")
    }
}
impl ExecutionPlan for PendingInput {
    fn name(&self) -> &str {
        "PendingBfsInput"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
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
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let polled = self.polled.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.inner.schema(),
            futures::stream::poll_fn(move |_| {
                polled.notify_one();
                Poll::Pending
            }),
        )))
    }
}

#[tokio::test]
async fn cap_receipts_are_typed_and_source_endpoint_quota_failures_are_distinct() {
    for case in 0..4 {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let state = BfsState::new(worker(
            1,
            if case == 3 { 24 << 10 } else { 8 << 20 },
            &drops,
        ))
        .unwrap();
        let path = std::env::temp_dir().join(format!(
            "argentea-bfs-failure-{}-{case}.jsonl",
            std::process::id()
        ));
        state
            .base
            .test_audit_file(std::fs::File::create(&path).unwrap());
        let mut base = request(Verb::Init, 0, 1, 8, "bfs_direction");
        base.max_levels = if case == 0 { 0 } else { 14 };
        if case == 1 {
            base.source = 100;
        }
        let mut edges = EDGES.to_vec();
        if case == 2 {
            edges.push((20, 999));
        }
        let plan = build(
            &ctx,
            &base,
            graph(&ctx, &IDS, &edges, 1).await,
            std::slice::from_ref(&state),
        )
        .await;
        assert!(collect(plan.clone(), ctx.task_ctx()).await.is_err());
        state.base.close().unwrap();
        state.close().unwrap();
        let rows = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
            .collect::<Vec<_>>();
        let cap = rows
            .iter()
            .filter(|r| r["event"] == "failure" && r["code"] == "bfs_level_cap")
            .collect::<Vec<_>>();
        if case == 0 {
            assert_eq!(cap.len(), 1);
            assert_eq!(cap[0]["levels"], 0);
            assert_eq!(cap[0]["max_levels"], 0);
            assert_eq!(cap[0]["frontier_vertices"], 1);
            assert_eq!(cap[0]["reached"], 1);
        } else {
            assert!(cap.is_empty());
        }
        assert!(!rows.iter().any(|r| r["event"] == "result"));
        drop(plan);
        drop(state);
        std::fs::remove_file(path).unwrap();
    }
}

#[tokio::test]
async fn close_interrupts_both_bfs_barriers_with_initialized_state() {
    for verb in [Verb::Decide, Verb::Apply] {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let state = BfsState::new(worker(1, 8 << 20, &drops)).unwrap();
        let init = stage(
            &ctx,
            request(Verb::Init, 0, 1, 8, "bfs_direction"),
            graph(&ctx, &IDS, &EDGES, 1).await,
            std::slice::from_ref(&state),
        )
        .await;
        let batches = collect(init, ctx.task_ctx()).await.unwrap();
        let mut input = memory(&ctx, vec![batches]).await;
        if verb == Verb::Apply {
            let plan = stage(
                &ctx,
                request(Verb::Decide, 0, 1, 8, "bfs_direction"),
                vec![input],
                std::slice::from_ref(&state),
            )
            .await;
            input = memory(&ctx, vec![collect(plan, ctx.task_ctx()).await.unwrap()]).await;
        }
        let polled = Arc::new(Notify::new());
        let pending = Arc::new(PendingInput {
            inner: input,
            polled: polled.clone(),
        });
        let plan = stage(
            &ctx,
            request(verb, 0, 1, 8, "bfs_direction"),
            vec![pending],
            std::slice::from_ref(&state),
        )
        .await;
        let task = tokio::spawn(collect(plan, ctx.task_ctx()));
        tokio::time::timeout(Duration::from_secs(5), polled.notified())
            .await
            .unwrap();
        assert!(state.partition(0).unwrap().is_some());
        state.base.close().unwrap();
        state.close().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
    }
}
#[tokio::test]
async fn dropped_bfs_statistics_stream_cancels_and_retained_integer_slice_remains_valid() {
    let ctx = SessionContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let state = BfsState::new(worker(1, 8 << 20, &drops)).unwrap();
    let usage = state.base.resources.execution.clone();
    let plan = stage(
        &ctx,
        request(Verb::Init, 0, 1, 8, "bfs_direction"),
        graph(&ctx, &IDS, &EDGES, 1).await,
        std::slice::from_ref(&state),
    )
    .await;
    let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
    let batch = stream.try_next().await.unwrap().unwrap();
    let slice = batch.column_by_name("aux").unwrap().slice(0, 1);
    drop(stream);
    assert!(state.base.check().is_err());
    state.base.close().unwrap();
    state.close().unwrap();
    drop(plan);
    drop(state);
    drop(batch);
    assert_eq!(
        slice
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        8
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(usage.usage().unwrap().live_bytes > 0);
    drop(slice);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(usage.usage().unwrap().live_bytes, 0);
}
#[tokio::test]
async fn late_missing_statistics_and_false_topology_totals_fail_before_publication() {
    for defect in 0..3 {
        let ctx = SessionContext::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let state = BfsState::new(worker(1, 8 << 20, &drops)).unwrap();
        let init = stage(
            &ctx,
            request(Verb::Init, 0, 1, 8, "bfs_direction"),
            graph(&ctx, &IDS, &EDGES, 1).await,
            std::slice::from_ref(&state),
        )
        .await;
        let mut batches = collect(init, ctx.task_ctx()).await.unwrap();
        let verb;
        if defect < 2 {
            if defect == 0 {
                batches.pop();
            } else {
                batches.push(batches[0].clone());
            }
            verb = Verb::Decide;
        } else {
            let plan = stage(
                &ctx,
                request(Verb::Decide, 0, 1, 8, "bfs_direction"),
                vec![memory(&ctx, vec![batches]).await],
                std::slice::from_ref(&state),
            )
            .await;
            batches = collect(plan, ctx.task_ctx()).await.unwrap();
            for batch in &mut batches {
                let kinds = integer(batch, "kind");
                let aux = integer(batch, "aux");
                let replacement = (0..batch.num_rows())
                    .map(|i| {
                        if kinds.value(i) == wire::COMPLETE {
                            aux.value(i) + 1
                        } else {
                            aux.value(i)
                        }
                    })
                    .collect::<Vec<_>>();
                let mut columns = batch.columns().to_vec();
                columns[batch.schema().index_of("aux").unwrap()] =
                    Arc::new(Int64Array::from(replacement));
                *batch = RecordBatch::try_new(batch.schema(), columns).unwrap();
            }
            verb = Verb::Apply;
        }
        let native = state.partition(0).unwrap().unwrap();
        let before = lock(&native).unwrap().state_rows().collect::<Vec<_>>();
        let plan = stage(
            &ctx,
            request(verb, 0, 1, 8, "bfs_direction"),
            vec![memory(&ctx, vec![batches]).await],
            std::slice::from_ref(&state),
        )
        .await;
        assert!(collect(plan, ctx.task_ctx()).await.is_err());
        assert_eq!(
            lock(&native).unwrap().state_rows().collect::<Vec<_>>(),
            before
        );
        assert_eq!(lock(&native).unwrap().incoming_identity(), None);
        state.base.close().unwrap();
        state.close().unwrap();
    }
}

#[tokio::test]
async fn missing_candidates_with_rewritten_wire_completion_fail_before_publication() {
    for algorithm in ["bfs_reference", "bfs_frontier"] {
        for malformed in [false, true] {
            let ctx = SessionContext::new();
            let drops = Arc::new(AtomicUsize::new(0));
            let state = BfsState::new(worker(1, 8 << 20, &drops)).unwrap();
            let mut base = request(Verb::Init, 0, 1, 2, algorithm);
            base.source = 0;
            let init = stage(
                &ctx,
                base.clone(),
                graph(&ctx, &[0, 1], &[(0, 1)], 1).await,
                std::slice::from_ref(&state),
            )
            .await;
            let mut batches = collect(init, ctx.task_ctx()).await.unwrap();
            // Complete real topology validation, then materialize the first
            // candidate stream through its local output EOF.
            for (verb, phase) in [(Verb::Decide, 0), (Verb::Apply, 0), (Verb::Decide, 1)] {
                let mut request = base.clone();
                request.verb = verb;
                request.phase = phase;
                let plan = stage(
                    &ctx,
                    request,
                    vec![memory(&ctx, vec![batches]).await],
                    std::slice::from_ref(&state),
                )
                .await;
                batches = collect(plan, ctx.task_ctx()).await.unwrap();
            }
            if malformed {
                let mut removed = 0;
                for batch in &mut batches {
                    let keep = arrow::array::BooleanArray::from_iter(
                        integer(batch, "kind")
                            .values()
                            .iter()
                            .map(|kind| Some(*kind == wire::COMPLETE)),
                    );
                    let filtered = arrow::compute::filter_record_batch(batch, &keep).unwrap();
                    removed += batch.num_rows() - filtered.num_rows();
                    let mut columns = filtered.columns().to_vec();
                    // Forge both counters to agree with the empty received
                    // stream. The producer's earlier frontier statistic is 1.
                    for name in ["sequence", "aux"] {
                        columns[filtered.schema().index_of(name).unwrap()] =
                            Arc::new(Int64Array::from(vec![0; filtered.num_rows()]));
                    }
                    *batch = RecordBatch::try_new(filtered.schema(), columns).unwrap();
                }
                assert_eq!(removed, 1);
            }
            let native = state.partition(0).unwrap().unwrap();
            let before = lock(&native).unwrap().state_rows().collect::<Vec<_>>();
            let mut request = base;
            request.verb = Verb::Apply;
            request.phase = 1;
            let plan = stage(
                &ctx,
                request,
                vec![memory(&ctx, vec![batches]).await],
                std::slice::from_ref(&state),
            )
            .await;
            let result = collect(plan, ctx.task_ctx()).await;
            if malformed {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("BFS completion count does not match producer statistics")
                );
                assert_eq!(
                    lock(&native).unwrap().state_rows().collect::<Vec<_>>(),
                    before
                );
                assert_eq!(lock(&native).unwrap().next_phase(), 1);
                assert_eq!(lock(&native).unwrap().levels(), 0);
            } else {
                result.unwrap();
                assert_eq!(lock(&native).unwrap().next_phase(), 2);
                assert_eq!(lock(&native).unwrap().reached_count(), 2);
                assert_eq!(lock(&native).unwrap().levels(), 1);
            }
            state.base.close().unwrap();
            state.close().unwrap();
        }
    }
}
