use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::task::{Context, Poll, Waker};

use datafusion::prelude::SessionConfig;
use futures::StreamExt;
use sail_common_datafusion::error::{
    CommitStateUnknownError, PythonErrorCause, PythonErrorCauseExtractor, StructuredSparkError,
};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use tokio::sync::oneshot;

use super::*;

#[path = "test_support.rs"]
mod support;
use support::{plan, stream};

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

fn assert_pending<T>(future: Pin<&mut impl Future<Output = T>>) {
    assert!(
        future
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

struct NoPython;

impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn std::error::Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}

fn query_error() -> DataFusionError {
    DataFusionError::External(Box::new(StructuredSparkError {
        class: "java.lang.NumberFormatException".into(),
        condition: "CAST_INVALID_INPUT".into(),
        sql_state: "22018".into(),
        message: "invalid local query value".into(),
        parameters: HashMap::from([("expression".into(), "'bad'".into())]),
    }))
}

fn unknown() -> DataFusionError {
    DataFusionError::External(Box::new(CommitStateUnknownError(
        "local commit outcome unknown".into(),
    )))
}

fn delayed_cleanup(
    resources: &TaskResourceScope,
    observed: oneshot::Sender<()>,
    released: oneshot::Receiver<()>,
    result: Result<()>,
) -> Result<()> {
    resources.spawn(|mut canceled| async move {
        canceled
            .wait_for(|value| *value)
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let _ = observed.send(());
        released
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        result
    })?;
    Ok(())
}

#[tokio::test]
async fn stopped_admission_never_invokes_execution() -> TestResult {
    let runner = LocalJobRunner::default();
    let context = SessionContext::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    runner.stop().await?;
    let output = runner
        .execute(
            &context,
            plan(move |_| {
                invoked.fetch_add(1, Ordering::SeqCst);
                Ok(stream(|_| Poll::Pending).0)
            }),
        )
        .await;
    assert!(output.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(runner.lock().active.is_empty());
    Ok(())
}

#[test]
fn admitted_construction_and_its_cleanup_both_delay_stop() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let runner = Arc::new(LocalJobRunner::default());
    let context = SessionContext::new();
    let (entered, entry) = mpsc::sync_channel(0);
    let (construct, construction) = mpsc::sync_channel(0);
    let (observed, observation) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let (source, counts) = stream(|_| Poll::Pending);
    let physical = plan(move |context| {
        delayed_cleanup(
            context.extension::<TaskResourceScope>()?.as_ref(),
            observed,
            released,
            Ok(()),
        )?;
        entered
            .send(())
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        construction
            .recv()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(source)
    });
    let worker = runner.clone();
    let handle = runtime.handle().clone();
    let execution = std::thread::spawn(move || handle.block_on(worker.execute(&context, physical)));
    entry.recv()?;
    assert_eq!(runner.lock().active.len(), 1);
    runtime.block_on(async {
        let mut stop = Box::pin(runner.stop());
        assert_pending(stop.as_mut());
        observation.await?;
        assert_eq!(counts.drops.load(Ordering::SeqCst), 0);
        construct.send(())?;
        let _retained = execution
            .join()
            .map_err(|_| "execution thread panicked")??;
        assert_pending(stop.as_mut());
        assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
        release.send(()).map_err(|_| "cleanup supervisor lost")?;
        stop.await?;
        assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
        assert!(runner.lock().active.is_empty());
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[tokio::test]
async fn execution_receives_fresh_scopes_isolated_from_sibling_and_parent() -> TestResult {
    let parent = Arc::new(TaskResourceScope::new(Handle::current()));
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(7)
            .with_extension(parent.clone()),
    );
    let runner = LocalJobRunner::default();
    let mut sources = vec![];
    let mut contexts = vec![];
    for _ in 0..2 {
        let (sent, received) = oneshot::channel();
        sources.push(
            runner
                .execute(
                    &context,
                    plan(move |context| {
                        sent.send(context)
                            .map_err(|_| internal_datafusion_err!("context receiver lost"))?;
                        Ok(stream(|_| Poll::Pending).0)
                    }),
                )
                .await?,
        );
        contexts.push(received.await?);
    }
    let first = contexts[0].extension::<TaskResourceScope>()?;
    let second = contexts[1].extension::<TaskResourceScope>()?;
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(!Arc::ptr_eq(&first, &parent));
    assert!(!Arc::ptr_eq(&second, &parent));
    for child in &contexts {
        assert_eq!(
            child.session_config().options().execution.target_partitions,
            7
        );
        assert!(Arc::ptr_eq(&child.runtime_env(), &context.runtime_env()));
    }
    let first_control = runner.snapshot()[0].1.control.clone();
    drop(sources.remove(0));
    first_control.completion().await.cleanup_result()?;
    assert!(first.spawn(|_| async { Ok(()) }).is_err());
    let (sent, received) = oneshot::channel();
    second.spawn(|canceled| async move {
        let _ = sent.send(*canceled.borrow());
        Ok(())
    })?;
    assert!(!received.await?);
    runner.stop().await?;
    parent.spawn(|canceled| async move {
        assert!(!*canceled.borrow());
        Ok(())
    })?;
    assert!(parent.drain().await?.is_clean());
    Ok(())
}

#[tokio::test]
async fn construction_errors_and_panics_wait_for_admitted_resource_cleanup() -> TestResult {
    for panics in [false, true] {
        let runner = LocalJobRunner::default();
        let context = SessionContext::new();
        let (observed, observation) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let physical = plan(move |context| {
            delayed_cleanup(
                context.extension::<TaskResourceScope>()?.as_ref(),
                observed,
                released,
                Ok(()),
            )?;
            if panics {
                std::panic::resume_unwind(Box::new("construction fixture panic"));
            }
            Err(query_error())
        });
        let mut execution = Box::pin(runner.execute(&context, physical));
        assert_pending(execution.as_mut());
        observation.await?;
        assert_eq!(runner.lock().active.len(), 1);
        assert_pending(execution.as_mut());
        release.send(()).map_err(|_| "construction cleanup lost")?;
        let error = execution
            .await
            .err()
            .ok_or("construction failure was successful")?;
        let cause = CommonErrorCause::new::<NoPython>(&error);
        if panics {
            assert!(
                matches!(cause, CommonErrorCause::Internal(message) if message.contains("construction fixture panic"))
            );
        } else {
            assert!(
                matches!(cause, CommonErrorCause::Structured(error) if error.condition == "CAST_INVALID_INPUT")
            );
        }
        assert!(runner.lock().active.is_empty());
        runner.stop().await?;
    }
    Ok(())
}

#[tokio::test]
async fn query_failure_is_separate_from_cleanup_uncertainty_at_stop() -> TestResult {
    for uncertain in [false, true] {
        let runner = LocalJobRunner::default();
        let context = SessionContext::new();
        let physical = plan(move |context| {
            context
                .extension::<TaskResourceScope>()?
                .spawn(move |_| async move { if uncertain { Err(unknown()) } else { Ok(()) } })?;
            let mut error = Some(query_error());
            Ok(stream(move |_| Poll::Ready(error.take().map(Err))).0)
        });
        let mut output = runner.execute(&context, physical).await?;
        let error = output
            .next()
            .await
            .ok_or("query error missing")?
            .err()
            .ok_or("query succeeded")?;
        if uncertain {
            assert!(matches!(
                CommonErrorCause::new::<NoPython>(&error),
                CommonErrorCause::CommitStateUnknown(_)
            ));
            let cleanup = runner
                .stop()
                .await
                .err()
                .ok_or("uncertain cleanup reported success")?;
            let cause = CommonErrorCause::new::<NoPython>(&cleanup);
            assert!(!cause.is_retryable());
            assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
        } else {
            assert!(matches!(
                CommonErrorCause::new::<NoPython>(&error),
                CommonErrorCause::Structured(_)
            ));
            runner.stop().await?;
        }
        assert!(runner.lock().active.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn reaping_preserves_later_nonretryable_cleanup_after_an_earlier_failure() -> TestResult {
    let runner = LocalJobRunner::default();
    let context = SessionContext::new();
    for uncertain in [false, true] {
        let physical = plan(move |context| {
            context
                .extension::<TaskResourceScope>()?
                .spawn(move |_| async move {
                    Err(if uncertain {
                        unknown()
                    } else {
                        DataFusionError::Execution("earlier cleanup failed".into())
                    })
                })?;
            Ok(stream(|_| Poll::Ready(None)).0)
        });
        let mut output = runner.execute(&context, physical).await?;
        assert_eq!(runner.lock().active.len(), 1);
        assert!(matches!(output.next().await, Some(Err(_))));
    }
    let _pending = runner
        .execute(&context, plan(|_| Ok(stream(|_| Poll::Pending).0)))
        .await?;
    assert_eq!(runner.lock().active.len(), 1);
    assert!(runner.lock().failures.nonretryable.is_some());
    let error = runner
        .stop()
        .await
        .err()
        .ok_or("reaping lost cleanup failures")?;
    // Collection's Display only renders its first member. Inspect both retained
    // errors directly, then verify the classifier still finds the later cause.
    let DataFusionError::Collection(errors) = &error else {
        return Err("stop did not retain both cleanup failures".into());
    };
    assert_eq!(errors.len(), 2);
    assert!(errors[0].to_string().contains("earlier cleanup failed"));
    assert!(
        errors[1]
            .to_string()
            .contains("local commit outcome unknown")
    );
    let cause = CommonErrorCause::new::<NoPython>(&error);
    assert!(!cause.is_retryable());
    assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
    assert!(runner.lock().active.is_empty());
    Ok(())
}

#[tokio::test]
async fn completed_history_is_reaped_on_later_admission_and_stop() -> TestResult {
    let runner = LocalJobRunner::default();
    let context = SessionContext::new();
    let mut retained = vec![];
    for _ in 0..4 {
        let mut output = runner
            .execute(&context, plan(|_| Ok(stream(|_| Poll::Ready(None)).0)))
            .await?;
        assert_eq!(runner.lock().active.len(), 1);
        assert!(output.next().await.is_none());
        retained.push(output);
    }
    runner.stop().await?;
    assert!(runner.lock().active.is_empty());
    assert_eq!(retained.len(), 4);
    Ok(())
}

#[tokio::test]
async fn abandoning_stop_keeps_admission_sealed_and_cleanup_owned() -> TestResult {
    let runner = LocalJobRunner::default();
    let context = SessionContext::new();
    let (observed, observation) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let (source, counts) = stream(|_| Poll::Pending);
    let _retained = runner
        .execute(
            &context,
            plan(move |context| {
                delayed_cleanup(
                    context.extension::<TaskResourceScope>()?.as_ref(),
                    observed,
                    released,
                    Ok(()),
                )?;
                Ok(source)
            }),
        )
        .await?;
    let mut abandoned = Box::pin(runner.stop());
    assert_pending(abandoned.as_mut());
    drop(abandoned);
    observation.await?;
    assert!(runner.lock().stopped);
    let calls = Arc::new(AtomicUsize::new(0));
    let invoked = calls.clone();
    assert!(
        runner
            .execute(
                &context,
                plan(move |_| {
                    invoked.fetch_add(1, Ordering::SeqCst);
                    Ok(stream(|_| Poll::Pending).0)
                })
            )
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut resumed = Box::pin(runner.stop());
    assert_pending(resumed.as_mut());
    release
        .send(())
        .map_err(|_| "abandoned stop lost cleanup")?;
    resumed.await?;
    assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
    assert!(runner.lock().active.is_empty());
    Ok(())
}

#[test]
fn interrupted_completion_is_retained_until_stop_joins_the_resource_scope() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let cleanup_runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let resources = Arc::new(TaskResourceScope::new(runtime.handle().clone()));
    let (observed, observation) = oneshot::channel();
    let (release, released) = oneshot::channel();
    delayed_cleanup(&resources, observed, released, Err(unknown()))?;
    let (preparation, control) =
        StreamPreparation::new(resources.clone(), cleanup_runtime.handle().clone());
    let runner = LocalJobRunner::default();
    runner.admit(Job {
        control: control.clone(),
        resources,
    })?;
    preparation.fail(DataFusionError::Execution("prepared query failed".into()));
    drop(cleanup_runtime);
    let report = control
        .completed()
        .ok_or("interrupted cleanup report missing")?;
    assert!(!report.resources_drained);
    runner.reap();
    assert_eq!(runner.lock().active.len(), 1);
    runtime.block_on(async {
        let mut stop = Box::pin(runner.stop());
        assert_pending(stop.as_mut());
        observation.await?;
        assert_eq!(runner.lock().active.len(), 1);
        release
            .send(())
            .map_err(|_| "interrupted cleanup lost supervisor")?;
        let error = stop
            .await
            .err()
            .ok_or("interrupted cleanup reported success")?;
        assert!(error.to_string().contains("cleanup interrupted"));
        let cause = CommonErrorCause::new::<NoPython>(&error);
        assert!(!cause.is_retryable());
        assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
        assert!(runner.lock().active.is_empty());
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[path = "preparation_tests.rs"]
mod preparation;

#[path = "capture_boundary_tests.rs"]
mod capture_boundary;

#[path = "captured_preparation_tests.rs"]
mod captured_preparation;
