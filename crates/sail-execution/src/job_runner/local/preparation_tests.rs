use std::cell::Cell;

use futures::FutureExt;
use tokio::time::{Duration, timeout};

use super::*;

#[tokio::test]
async fn bound_object_reuses_admitted_scope_and_late_authoritative_config() -> TestResult {
    let runner = LocalJobRunner::default();
    let context = SessionContext::new();
    let dynamic: &dyn JobRunner = &runner;
    let preparation = dynamic.begin_preparation(&context)?;
    let resources = preparation.resources().clone();
    assert_eq!(runner.lock().active.len(), 1);
    // Same authoritative mutation underlying native SET, after admission.
    context
        .state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = 9;
    let expected = resources.clone();
    let output = preparation
        .execute(plan(move |ctx| {
            assert!(Arc::ptr_eq(
                &expected,
                &ctx.extension::<TaskResourceScope>()?
            ));
            assert_eq!(
                ctx.session_config().options().execution.target_partitions,
                9
            );
            Ok(stream(|_| Poll::Ready(None)).0)
        }))
        .await?;
    let batches = output.collect::<Vec<_>>().await;
    assert!(batches.is_empty());
    assert_eq!(resources.drain().await?.registered, 0);
    runner.stop().await?;
    assert!(runner.lock().active.is_empty());
    Ok(())
}

#[tokio::test]
async fn stop_before_begin_refuses_preparation() -> TestResult {
    let runner = LocalJobRunner::default();
    let context = SessionContext::new();
    runner.stop().await?;
    assert!(runner.begin_preparation(&context).is_err());
    assert!(
        (&runner as &dyn JobRunner)
            .begin_preparation(&context)
            .is_err()
    );
    assert!(runner.lock().active.is_empty());
    Ok(())
}

#[tokio::test]
async fn stop_before_execute_refuses_physical_factory() -> TestResult {
    let runner = LocalJobRunner::default();
    let context = SessionContext::new();
    let preparation = runner.begin_preparation(&context)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let mut stop = Box::pin(runner.stop());
    assert_pending(stop.as_mut());
    assert!(preparation.resources().cancelled().now_or_never().is_some());
    let result = preparation
        .execute(plan(move |_| {
            invoked.fetch_add(1, Ordering::SeqCst);
            Ok(stream(|_| Poll::Pending).0)
        }))
        .await;
    assert!(result.is_err_and(|error| error.to_string().contains("canceled")));
    timeout(Duration::from_secs(10), stop).await??;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}

struct BorrowedPlan<'a>(&'a Cell<bool>);
impl Drop for BorrowedPlan<'_> {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[tokio::test]
async fn stop_owns_borrowed_planning_and_its_registered_io() -> TestResult {
    let runner = Arc::new(LocalJobRunner::default());
    let context = SessionContext::new();
    let preparation = runner.begin_preparation(&context)?;
    let resources = preparation.resources().clone();
    let (entered, entering) = oneshot::channel();
    let (release, response) = oneshot::channel();
    let published = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let effect = published.clone();
    let dropped = Cell::new(false);
    let mut planning = Box::pin(async {
        let _lease = BorrowedPlan(&dropped);
        resources
            .run_mutation(move |_| async move {
                let _ = entered.send(());
                response
                    .await
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                effect.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
    });
    let owner = runner.clone();
    let stop = tokio::spawn(async move {
        entering
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        owner.stop().await
    });
    tokio::select! {
        result=&mut planning => return Err(format!("held planning finished: {result:?}").into()),
        _=resources.cancelled() => {}
    }
    drop(planning);
    assert!(dropped.get());
    let report = preparation.cancel();
    assert!(resources.drain().now_or_never().is_none());
    assert_eq!(runner.lock().active.len(), 1);
    assert!(!published.load(Ordering::SeqCst));
    release.send(()).map_err(|_| "admitted I/O lost")?;
    timeout(Duration::from_secs(10), stop).await???;
    let completion = report.completion().await;
    assert!(completion.resources_drained);
    assert!(completion.cleanup_result().is_ok());
    assert!(published.load(Ordering::SeqCst));
    assert_eq!(resources.drain().await?.registered, 1);
    assert!(runner.lock().active.is_empty());
    Ok(())
}

#[tokio::test]
async fn dropped_preparation_stays_visible_until_cleanup_and_retains_failure() -> TestResult {
    for failure in [false, true] {
        let runner = LocalJobRunner::default();
        let context = SessionContext::new();
        let preparation = runner.begin_preparation(&context)?;
        let report = preparation.control();
        let resources = preparation.resources().clone();
        let (observed, observation) = oneshot::channel();
        let (release, released) = oneshot::channel();
        delayed_cleanup(
            &resources,
            observed,
            released,
            if failure { Err(unknown()) } else { Ok(()) },
        )?;
        drop(preparation);
        timeout(Duration::from_secs(10), observation).await??;
        let mut stop = Box::pin(runner.stop());
        assert_pending(stop.as_mut());
        assert!(resources.drain().now_or_never().is_none());
        assert_eq!(runner.lock().active.len(), 1);
        release.send(()).map_err(|_| "cleanup disappeared")?;
        let result = timeout(Duration::from_secs(10), stop).await?;
        if failure {
            let error = result.err().ok_or("unknown cleanup was ignored")?;
            assert!(!CommonErrorCause::new::<NoPython>(&error).is_retryable());
        } else {
            result?;
        }
        assert!(report.completion().await.resources_drained);
        assert!(runner.lock().active.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn finish_without_execution_retires_and_next_preparation_has_fresh_owner() -> TestResult {
    let runner = LocalJobRunner::default();
    let context = SessionContext::new();
    let preparation = (&runner as &dyn JobRunner).begin_preparation(&context)?;
    let before = preparation.resources().clone();
    let report = preparation.finish().completion().await;
    assert!(report.resources_drained);
    assert!(report.result().is_ok());
    let next = runner.begin_preparation(&context)?;
    assert!(!Arc::ptr_eq(&before, next.resources()));
    assert_eq!(runner.lock().active.len(), 1);
    assert!(next.cancel().completion().await.cleanup_result().is_ok());
    runner.stop().await?;
    assert!(runner.lock().active.is_empty());
    Ok(())
}

#[tokio::test]
async fn dropping_unpolled_execute_releases_preparation_without_invoking_plan() -> TestResult {
    let runner = LocalJobRunner::default();
    let context = SessionContext::new();
    let preparation = (&runner as &dyn JobRunner).begin_preparation(&context)?;
    let control = preparation.control();
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    let execution = preparation.execute(plan(move |_| {
        invoked.fetch_add(1, Ordering::SeqCst);
        Ok(stream(|_| Poll::Pending).0)
    }));
    assert_eq!(runner.lock().active.len(), 1);
    drop(execution);
    let report = timeout(Duration::from_secs(10), control.completion()).await?;
    assert!(report.resources_drained);
    assert!(report.cleanup_result().is_ok());
    assert!(
        report
            .result()
            .is_err_and(|error| error.to_string().contains("abandoned"))
    );
    runner.stop().await?;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(runner.lock().active.is_empty());
    Ok(())
}
