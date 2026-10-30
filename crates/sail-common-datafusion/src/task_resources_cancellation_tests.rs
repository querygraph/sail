use std::cell::Cell;
use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::oneshot;
use tokio::time::timeout;

use super::*;
use crate::managed_stream::StreamPreparation;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn cancellation_observation_is_latched_but_does_not_certify_drainage() -> TestResult {
    let scope = TaskResourceScope::new(Handle::current());
    let (release, wait) = oneshot::channel();
    scope.spawn_mutation(|_| async move {
        wait.await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(())
    })?;
    let mut before = Box::pin(scope.cancelled());
    assert!(futures::poll!(before.as_mut()).is_pending());
    scope.seal()?;
    assert!(futures::poll!(before.as_mut()).is_pending());
    scope.cancel()?;
    timeout(Duration::from_secs(10), before).await?;
    // This observer is created after cancellation and need not see a change.
    assert!(scope.cancelled().now_or_never().is_some());
    assert!(scope.drain().now_or_never().is_none());
    release.send(()).map_err(|_| "retained mutation lost")?;
    let report = timeout(Duration::from_secs(10), scope.drain()).await??;
    assert!(report.is_clean());
    assert_eq!(report.registered, 1);
    Ok(())
}

#[tokio::test]
async fn abandoned_and_multiple_cancellation_observers_preserve_sibling_admission() -> TestResult {
    let scope = Arc::new(TaskResourceScope::new(Handle::current()));
    let sibling = TaskResourceScope::new(Handle::current());
    assert!(scope.cancelled().now_or_never().is_none()); // abandoned observer
    scope.spawn(|_| async { Ok(()) })?; // observation did not seal admission
    let mut observers = Vec::new();
    for _ in 0..3 {
        let scope = scope.clone();
        observers.push(tokio::spawn(async move { scope.cancelled().await }));
    }
    scope.cancel()?;
    for observer in observers {
        timeout(Duration::from_secs(10), observer).await??;
    }
    assert!(scope.spawn(|_| async { Ok(()) }).is_err());
    assert!(sibling.cancelled().now_or_never().is_none());
    sibling.spawn(|_| async { Ok(()) })?;
    assert!(scope.drain().await?.is_clean());
    assert!(sibling.drain().await?.is_clean());
    Ok(())
}

struct PlanningLease<'a>(&'a Cell<bool>);
impl Drop for PlanningLease<'_> {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[tokio::test]
async fn borrowed_preparation_is_dropped_before_stop_observes_completion() -> TestResult {
    let scope = Arc::new(TaskResourceScope::new(Handle::current()));
    let (preparation, control) = StreamPreparation::new(scope.clone(), Handle::current());
    let (entered, entering) = oneshot::channel();
    let dropped = Cell::new(false);
    // The borrow is deliberately local and !Sync: this future stays on its
    // caller, rather than being moved into a detached 'static planning task.
    let mut planning = Box::pin(async {
        let _lease = PlanningLease(&dropped);
        let _ = entered.send(());
        pending::<()>().await;
    });
    let stopper = tokio::spawn(async move {
        entering
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        control.cancel()?;
        Ok::<_, DataFusionError>(control.completion().await)
    });
    tokio::select! {
        _ = &mut planning => return Err("held planning unexpectedly finished".into()),
        _ = scope.cancelled() => {}
    }
    drop(planning);
    assert!(dropped.get());
    preparation.fail(DataFusionError::Execution("planning cancelled".into()));
    let report = timeout(Duration::from_secs(10), stopper).await???;
    assert!(report.resources_drained);
    assert!(report.cleanup_result().is_ok());
    assert!(
        report
            .result()
            .is_err_and(|error| error.to_string().contains("planning cancelled"))
    );
    Ok(())
}

#[test]
fn cancelled_borrowed_waiter_keeps_registered_io_owned_until_settlement() -> TestResult {
    let io = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    caller.block_on(async {
        let scope = Arc::new(TaskResourceScope::new(io.handle().clone()));
        let (preparation, control) = StreamPreparation::new(scope.clone(), Handle::current());
        let (entered, entering) = oneshot::channel();
        let (release, response) = oneshot::channel();
        let published = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dropped = Cell::new(false);
        let effect = published.clone();
        let mut planning = Box::pin(async {
            let _lease = PlanningLease(&dropped);
            scope.run_mutation(move |_| async move {
                let _ = entered.send(Handle::current().id());
                // This registered I/O task cannot complete merely because its
                // result receiver or caller's planning future disappears.
                response.await.map_err(|error| DataFusionError::External(Box::new(error)))?;
                effect.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }).await
        });
        let stop_control = control.clone();
        let io_id = io.handle().id();
        let stopper = tokio::spawn(async move {
            let actual = entering.await.map_err(|error| DataFusionError::External(Box::new(error)))?;
            assert_eq!(actual, io_id);
            stop_control.cancel()
        });
        tokio::select! {
            result = &mut planning => return Err(format!("held I/O finished early: {result:?}").into()),
            _ = scope.cancelled() => {}
        }
        drop(planning);
        assert!(dropped.get());
        preparation.fail(DataFusionError::Execution("planning cancelled with I/O pending".into()));
        timeout(Duration::from_secs(10), stopper).await???;
        // Poll the actual drain as well: a pending completion notification
        // alone could merely mean its cleanup task has not been scheduled yet.
        assert!(scope.drain().now_or_never().is_none());
        assert!(control.completion().now_or_never().is_none());
        assert!(!published.load(std::sync::atomic::Ordering::SeqCst));
        release.send(()).map_err(|_| "registered I/O task lost")?;
        let report = timeout(Duration::from_secs(10), control.completion()).await?;
        assert!(published.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(scope.drain().await?.registered, 1);
        assert!(report.resources_drained);assert!(report.cleanup_result().is_ok());
        assert!(report.result().is_err_and(|error| error.to_string().contains("planning cancelled")));
        assert_ne!(io_id, Handle::current().id());
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
