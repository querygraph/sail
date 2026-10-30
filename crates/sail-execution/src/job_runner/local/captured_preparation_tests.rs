use datafusion::execution::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry, ObjectStoreUrl,
};
use futures::FutureExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use tokio::time::{Duration, timeout};

use super::*;

#[tokio::test]
async fn stop_before_capture_retains_same_cause_and_drains_admission() -> TestResult {
    let runner = LocalJobRunner::default();
    let context = SessionContext::new();
    let preparation = (&runner as &dyn JobRunner).begin_preparation(&context)?;
    let control = preparation.control();
    let mut stop = Box::pin(runner.stop());
    assert_pending(stop.as_mut());
    let error = preparation
        .capture(None)
        .err()
        .ok_or("stopped capture accepted")?;
    let DataFusionError::Shared(cause) = error else {
        return Err("capture did not retain shared cause".into());
    };
    assert!(cause.to_string().contains("canceled before capture"));
    let report = timeout(Duration::from_secs(10), control.completion()).await?;
    let Some(error) = &report.error else {
        return Err("capture failure report has no primary".into());
    };
    let DataFusionError::Shared(reported) = error.as_ref() else {
        return Err("capture report lost shared cause".into());
    };
    assert!(Arc::ptr_eq(&cause, reported));
    assert!(report.resources_drained);
    assert!(report.cleanup_result().is_ok());
    timeout(Duration::from_secs(10), stop).await??;
    assert!(runner.lock().active.is_empty());
    Ok(())
}

#[tokio::test]
async fn captured_stop_refuses_factory_and_unpolled_execute_never_starts_it() -> TestResult {
    for stop_first in [false, true] {
        let runner = LocalJobRunner::default();
        let context = SessionContext::new();
        let preparation = (&runner as &dyn JobRunner)
            .begin_preparation(&context)?
            .capture(None)?;
        let control = preparation.control();
        let calls = Arc::new(AtomicUsize::new(0));
        let invoked = calls.clone();
        let physical = plan(move |_| {
            invoked.fetch_add(1, Ordering::SeqCst);
            Ok(stream(|_| Poll::Pending).0)
        });
        if stop_first {
            let mut stop = Box::pin(runner.stop());
            assert_pending(stop.as_mut());
            assert!(preparation.execute(physical).await.is_err());
            timeout(Duration::from_secs(10), stop).await??;
        } else {
            drop(preparation.execute(physical));
            timeout(Duration::from_secs(10), runner.stop()).await??;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(control.completion().await.resources_drained);
        assert!(runner.lock().active.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn captured_guard_finish_cancel_and_drop_keep_registered_cleanup_owned() -> TestResult {
    for transition in [0, 1, 2] {
        let runner = LocalJobRunner::default();
        let context = SessionContext::new();
        let preparation = (&runner as &dyn JobRunner)
            .begin_preparation(&context)?
            .capture(None)?;
        let control = preparation.control();
        let resources = preparation.resources().clone();
        let (release, released) = oneshot::channel();
        resources.spawn(|_| async move {
            released
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            Ok(())
        })?;
        match transition {
            0 => {
                preparation.finish();
            }
            1 => {
                preparation.cancel();
            }
            _ => drop(preparation),
        }
        let mut completion = Box::pin(control.completion());
        assert_pending(completion.as_mut());
        assert_eq!(runner.lock().active.len(), 1);
        release
            .send(())
            .map_err(|_| "captured cleanup lost supervisor")?;
        let report = timeout(Duration::from_secs(10), completion).await?;
        assert!(report.resources_drained);
        assert!(report.cleanup_result().is_ok());
        assert_eq!(report.result().is_ok(), transition == 0);
        assert_eq!(resources.drain().await?.registered, 1);
        runner.stop().await?;
        assert!(runner.lock().active.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn captured_registry_survives_raw_stream_disposal_until_registered_io_settles() -> TestResult
{
    let runner = LocalJobRunner::default();
    let context = SessionContext::new();
    let registry = Arc::new(DefaultObjectStoreRegistry::new());
    let weak = Arc::downgrade(&registry);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let url = ObjectStoreUrl::parse("memory://captured-lifetime")?;
    registry.register_store(url.as_ref(), store.clone());
    let preparation = (&runner as &dyn JobRunner)
        .begin_preparation(&context)?
        .capture(Some(registry))?;
    let control = preparation.control();
    let resources = preparation.resources().clone();
    let (observed, observation) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let (source, counts) = stream(|_| Poll::Pending);
    let physical = plan(move |task| {
        task.extension::<TaskResourceScope>()?
            .spawn(move |mut canceled| async move {
                canceled
                    .wait_for(|value| *value)
                    .await
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                let _ = observed.send(());
                released
                    .await
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                let actual = task.runtime_env().object_store(url)?;
                assert!(Arc::ptr_eq(&actual, &store));
                Ok(())
            })?;
        Ok(source)
    });
    let output = preparation.execute(physical).await?;
    assert!(weak.upgrade().is_some());
    drop(output);
    timeout(Duration::from_secs(10), observation).await??;
    assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    assert!(weak.upgrade().is_some());
    assert!(resources.drain().now_or_never().is_none());
    release
        .send(())
        .map_err(|_| "captured I/O lost supervisor")?;
    let report = timeout(Duration::from_secs(10), control.completion()).await?;
    assert!(report.resources_drained);
    assert!(report.cleanup_result().is_ok());
    assert_eq!(resources.drain().await?.registered, 1);
    assert!(weak.upgrade().is_none());
    runner.stop().await?;
    assert!(runner.lock().active.is_empty());
    Ok(())
}
