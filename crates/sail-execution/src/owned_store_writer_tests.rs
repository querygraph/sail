use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry, ObjectStoreUrl,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use sail_common_datafusion::task_resources::ScopedTaskContext;
use sail_physical_plan::remote_checkpoint::RemoteCheckpointWriteExec;

use super::*;

async fn checkpoint_writer_loss(parallel: bool) -> TestResult {
    let raw = Arc::new(Provider::default());
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .execution
        .parquet
        .allow_single_file_parallelism = parallel;
    config
        .options_mut()
        .execution
        .objectstore_writer_buffer_size = 256;
    let session = SessionContext::new_with_config(config);
    let parent = session.task_ctx();
    let url = ObjectStoreUrl::parse("memory://owned-checkpoint")?;
    parent
        .runtime_env()
        .register_object_store(url.as_ref(), raw.clone());
    let a = ScopedTaskContext::new(&parent, Handle::current());
    let b = ScopedTaskContext::new(&parent, Handle::current());
    let a_registry = Arc::new(DefaultObjectStoreRegistry::new());
    // Explicit envelope for this two-row fixture, including synchronous chunks.
    // BufWriter's nominal concurrency is not a general burst bound.
    a_registry.register_store(
        url.as_ref(),
        Arc::new(OwnedObjectStore::new(
            raw.clone(),
            a.resources.clone(),
            NonZeroUsize::new(8).ok_or("part bound")?,
        )),
    );
    let a_context = a.with_object_store_registry(a_registry)?.context;
    let b_store = Arc::new(object_store::memory::InMemory::new());
    let b_registry = Arc::new(DefaultObjectStoreRegistry::new());
    b_registry.register_store(url.as_ref(), owned(b_store.clone(), b.resources.clone()));
    let b_context = b.with_object_store_registry(b_registry)?.context;
    let (complete, completing, release_complete) = Gate::new();
    raw.gates.lock().map_err(|_| "gates")?.complete = Some(complete);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])?;
    let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None)?;
    let prefix = Path::from("checkpoint/owned");
    let plan =
        RemoteCheckpointWriteExec::try_new(input, url.clone(), prefix.clone(), schema, None)?;
    let caller =
        tokio::spawn(async move { plan.execute(0, a_context)?.try_collect::<Vec<_>>().await });
    if let Err(error) = timeout(Duration::from_secs(10), completing).await? {
        return Err(format!(
            "completion not reached: {error}; writer outcome: {:?}",
            caller.await?
        )
        .into());
    }
    caller.abort();
    assert!(caller.await.is_err_and(|e| e.is_cancelled()));
    a.resources.cancel()?;
    assert!(
        a.resources.drain().now_or_never().is_none(),
        "owner completed before held multipart completion"
    );
    assert!(
        raw.inner
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await?
            .is_empty()
    );
    let sibling = b_context.runtime_env().object_store(&url)?;
    sibling
        .put(&Path::from("sibling"), "independent".into())
        .await?;
    assert_eq!(
        b_store.get(&Path::from("sibling")).await?.bytes().await?,
        "independent"
    );
    release_complete.send(()).map_err(|_| "completion lost")?;
    let report = timeout(Duration::from_secs(10), a.resources.drain()).await??;
    assert!(report.is_clean());
    assert_eq!(report.registered, 1);
    let files = raw
        .inner
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(files.len(), 1);
    let bytes = raw.inner.get(&files[0].location).await?.bytes().await?;
    assert!(bytes.starts_with(b"PAR1") && bytes.ends_with(b"PAR1"));
    assert_eq!(raw.aborts(), 0);
    let resolved = parent.runtime_env().object_store(&url)?;
    let expected: Arc<dyn ObjectStore> = raw.clone();
    assert!(Arc::ptr_eq(&resolved, &expected));
    assert!(b.resources.drain().await?.is_clean());
    // Explicitly outside the now-sealed operation, after qualified drainage.
    raw.inner.delete(&files[0].location).await?;
    assert!(raw.inner.head(&files[0].location).await.is_err());
    Ok(())
}

#[tokio::test]
async fn sequential_checkpoint_loss_retains_completion_and_sibling_isolation() -> TestResult {
    checkpoint_writer_loss(false).await
}
#[tokio::test]
async fn parallel_checkpoint_loss_retains_completion_and_sibling_isolation() -> TestResult {
    checkpoint_writer_loss(true).await
}

#[tokio::test]
async fn checkpoint_capacity_refusal_joins_abort_without_publication() -> TestResult {
    for parallel in [false, true] {
        let raw = Arc::new(Provider::default());
        let mut config = SessionConfig::new();
        config
            .options_mut()
            .execution
            .parquet
            .allow_single_file_parallelism = parallel;
        config
            .options_mut()
            .execution
            .objectstore_writer_buffer_size = 1;
        let session = SessionContext::new_with_config(config);
        let context = ScopedTaskContext::new(&session.task_ctx(), Handle::current());
        let url = ObjectStoreUrl::parse("memory://capacity-refusal")?;
        let registry = Arc::new(DefaultObjectStoreRegistry::new());
        registry.register_store(url.as_ref(), owned(raw.clone(), context.resources.clone()));
        let derived = context.with_object_store_registry(registry)?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])?;
        let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None)?;
        let plan =
            RemoteCheckpointWriteExec::try_new(input, url, Path::from("capacity"), schema, None)?;
        let error = plan
            .execute(0, derived.context)?
            .try_collect::<Vec<_>>()
            .await
            .err()
            .ok_or("capacity refusal unexpectedly succeeded")?;
        assert!(error.to_string().contains("capacity"), "{error:?}");
        context.resources.cancel()?;
        let report = timeout(Duration::from_secs(10), context.resources.drain()).await??;
        assert!(report.is_clean());
        assert_eq!(report.registered, 1);
        assert_eq!(raw.aborts(), 1);
        assert!(
            raw.inner
                .list(None)
                .try_collect::<Vec<_>>()
                .await?
                .is_empty()
        );
    }
    Ok(())
}
