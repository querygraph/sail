use std::time::Duration;

use futures::FutureExt;
use tokio::sync::oneshot;
use tokio::time::timeout;

use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn no_stream_finish_seals_without_cancel_and_joins_admitted_work() -> TestResult {
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    let (release, held) = oneshot::channel();
    resources.spawn_mutation(|canceled| async move {
        assert!(!*canceled.borrow());
        held.await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        assert!(!*canceled.borrow());
        Ok(())
    })?;
    let (preparation, _) = StreamPreparation::new(resources.clone(), Handle::current());
    let control = preparation.finish();
    assert!(resources.spawn(|_| async { Ok(()) }).is_err());
    assert!(resources.cancelled().now_or_never().is_none());
    assert!(resources.drain().now_or_never().is_none());
    release.send(()).map_err(|_| "admitted work lost")?;
    let report = timeout(Duration::from_secs(10), control.completion()).await?;
    assert!(report.resources_drained);
    assert!(report.cleanup_result().is_ok());
    assert!(report.result().is_ok());
    Ok(())
}

#[tokio::test]
async fn no_stream_cancel_retains_cleanup_after_observation_is_abandoned() -> TestResult {
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    let (entered, entering) = oneshot::channel();
    let (release, held) = oneshot::channel();
    resources.spawn_mutation(|canceled| async move {
        let _ = entered.send(());
        held.await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        assert!(*canceled.borrow());
        Ok(())
    })?;
    timeout(Duration::from_secs(10), entering).await??;
    let (preparation, _) = StreamPreparation::new(resources.clone(), Handle::current());
    let control = preparation.cancel();
    assert!(resources.cancelled().now_or_never().is_some());
    assert!(resources.drain().now_or_never().is_none());
    assert!(control.completion().now_or_never().is_none()); // abandon a waiter
    release.send(()).map_err(|_| "admitted work lost")?;
    let report = timeout(Duration::from_secs(10), control.completion()).await?;
    assert!(report.resources_drained);
    assert!(report.cleanup_result().is_ok());
    assert!(
        report
            .result()
            .is_err_and(|error| error.to_string().contains("canceled"))
    );
    Ok(())
}

#[tokio::test]
async fn successful_finish_cannot_erase_an_earlier_cancel_request() -> TestResult {
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    let (preparation, control) = StreamPreparation::new(resources.clone(), Handle::current());
    control.cancel()?;
    let finished = preparation.finish();
    assert!(resources.cancelled().now_or_never().is_some());
    let before = control.completion().await;
    let after = finished.completion().await;
    assert!(Arc::ptr_eq(&before, &after));
    assert!(before.resources_drained);
    assert!(before.cleanup_result().is_ok());
    assert!(
        before
            .result()
            .is_err_and(|error| error.to_string().contains("canceled"))
    );
    Ok(())
}
