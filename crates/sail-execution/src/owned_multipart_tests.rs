use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::FutureExt;
use object_store::ObjectStoreExt;
use sail_common_datafusion::error::{
    CommonErrorCause, PythonErrorCause, PythonErrorCauseExtractor,
};
use sail_object_store::owned_multipart::{Disposition, MultipartError};
use tokio::runtime::Handle;
use tokio::time::timeout;

use super::*;
#[path = "owned_multipart_test_upload.rs"]
mod fixture;
use fixture::{Gate, Injected, State, Upload};
type TestResult = std::result::Result<(), Box<dyn Error>>;

struct NoPython;
impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}
fn capacity() -> NonZeroUsize {
    NonZeroUsize::MIN
}

fn multipart(error: &DataFusionError) -> Option<&MultipartError> {
    match error {
        DataFusionError::External(error) => error.downcast_ref(),
        DataFusionError::Shared(error) => multipart(error),
        DataFusionError::Context(_, error) => multipart(error),
        DataFusionError::Collection(errors) => errors.iter().find_map(multipart),
        _ => None,
    }
}

#[tokio::test]
async fn sealed_and_unpolled_creation_never_invoke_provider() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    for sealed in [false, true] {
        let scope = TaskResourceScope::new(Handle::current());
        let count = calls.clone();
        let factory = move || async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err(object_store::Error::NotImplemented {
                operation: "fixture creation".into(),
                implementer: "OwnedMultipartFixture".into(),
            })
        };
        if sealed {
            scope.seal()?;
            assert!(create(&scope, factory, capacity()).await.is_err());
        } else {
            drop(create(&scope, factory, capacity()));
        }
        let report = scope.drain().await?;
        assert!(report.is_clean());
        assert_eq!(report.registered, 0);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn abandoned_creation_keeps_scope_pending_until_actual_abort() -> TestResult {
    let scope = Arc::new(TaskResourceScope::new(Handle::current()));
    let state = State::new();
    let (creation_gate, entered, release_creation) = Gate::new();
    let (abort_gate, aborting, release_abort) = Gate::new();
    let caller = {
        let scope = scope.clone();
        let state = state.clone();
        tokio::spawn(async move {
            create(
                &scope,
                move || async move {
                    creation_gate.wait().await;
                    let mut upload = Upload::new(state).await?;
                    upload.abort = Some(abort_gate);
                    Ok(Box::new(upload) as Box<dyn MultipartUpload>)
                },
                capacity(),
            )
            .await
        })
    };
    timeout(Duration::from_secs(5), entered).await??;
    caller.abort();
    assert!(caller.await.is_err_and(|error| error.is_cancelled()));
    scope.cancel()?;
    assert!(scope.drain().now_or_never().is_none());
    release_creation
        .send(())
        .map_err(|_| "creation disappeared")?;
    timeout(Duration::from_secs(5), aborting).await??;
    assert!(scope.drain().now_or_never().is_none());
    release_abort.send(()).map_err(|_| "abort disappeared")?;
    let report = timeout(Duration::from_secs(5), scope.drain()).await??;
    assert!(report.is_clean());
    assert_eq!(report.registered, 1);
    assert_eq!(state.aborts.load(Ordering::SeqCst), 1);
    assert!(matches!(
        state.store.head(&state.path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_observer_and_drain_waiter_cannot_release_pending_commit() -> TestResult {
    let scope = Arc::new(TaskResourceScope::new(Handle::current()));
    let state = State::new();
    let (complete_gate, entered, release) = Gate::new();
    let provider_state = state.clone();
    let mut upload = create(
        &scope,
        move || async move {
            let mut upload = Upload::new(provider_state).await?;
            upload.complete = Some(complete_gate);
            Ok(Box::new(upload) as Box<dyn MultipartUpload>)
        },
        capacity(),
    )
    .await?;
    upload.put_part("payload".into())?.await?;
    let outcome = upload.complete()?;
    let caller = tokio::spawn(outcome);
    timeout(Duration::from_secs(5), entered).await??;
    caller.abort();
    assert!(caller.await.is_err_and(|error| error.is_cancelled()));
    drop(upload);
    scope.cancel()?;
    // now_or_never polls and drops this pending drain observer. The scope
    // must retain the same shared join state for the later caller.
    assert!(scope.drain().now_or_never().is_none());
    assert!(matches!(
        state.store.head(&state.path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    let sibling = TaskResourceScope::new(Handle::current());
    let sibling_state = State::at(
        state.store.clone(),
        object_store::path::Path::from("sibling/part"),
    );
    assert!(Arc::ptr_eq(&sibling_state.store, &state.store));
    let provider_sibling = sibling_state.clone();
    let mut other = create(
        &sibling,
        move || async move {
            Ok(Box::new(Upload::new(provider_sibling).await?) as Box<dyn MultipartUpload>)
        },
        capacity(),
    )
    .await?;
    other.put_part("sibling".into())?.await?;
    assert!(matches!(
        other.complete()?.await?,
        Disposition::Committed(_)
    ));
    assert!(sibling.drain().await?.is_clean());
    assert_eq!(
        sibling_state
            .store
            .get(&sibling_state.path)
            .await?
            .bytes()
            .await?,
        "sibling"
    );
    assert!(scope.drain().now_or_never().is_none());
    release.send(()).map_err(|_| "complete disappeared")?;
    let report = timeout(Duration::from_secs(5), scope.drain()).await??;
    assert!(report.is_clean());
    assert_eq!(report.registered, 1);
    assert_eq!(
        state.store.get(&state.path).await?.bytes().await?,
        "payload"
    );
    assert_eq!(state.aborts.load(Ordering::SeqCst), 0);
    // Only confirmed drainage permits this fixture's compensation/deletion.
    state.store.delete(&state.path).await?;
    assert!(matches!(
        state.store.head(&state.path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn failed_completion_is_nonretryable_with_original_provider_cause_in_scope() -> TestResult {
    for abandoned in [false, true] {
        let scope = TaskResourceScope::new(Handle::current());
        let state = State::new();
        let provider = state.clone();
        let (gate, entered, release) = Gate::new();
        let mut upload = create(
            &scope,
            move || async move {
                let mut upload = Upload::new(provider).await?;
                upload.lose_complete_response = true;
                upload.complete = Some(gate);
                Ok(Box::new(upload) as Box<dyn MultipartUpload>)
            },
            capacity(),
        )
        .await?;
        upload.put_part("committed".into())?.await?;
        let observer = upload.complete()?;
        timeout(Duration::from_secs(5), entered).await??;
        let observer = if abandoned {
            drop(observer);
            drop(upload);
            None
        } else {
            Some(observer)
        };
        scope.cancel()?;
        assert!(scope.drain().now_or_never().is_none());
        release.send(()).map_err(|_| "complete disappeared")?;
        let observed = if let Some(observer) = observer {
            Some(observer.await.err().ok_or("expected response loss")?)
        } else {
            None
        };
        let report = scope.drain().await?;
        assert!(!report.is_clean());
        assert_eq!(report.failures.len(), 1);
        let retained = multipart(&report.failures[0].error).ok_or("typed multipart error lost")?;
        assert_eq!(retained.causes()[0].stage, Stage::Complete);
        assert!(
            matches!(retained.causes()[0].error.as_ref(),object_store::Error::Generic{source,..} if source.downcast_ref::<Injected>().is_some())
        );
        for error in report.errors() {
            let cause = CommonErrorCause::new::<NoPython>(&error);
            assert!(!cause.is_retryable());
            assert!(matches!(cause, CommonErrorCause::CommitStateUnknown(_)));
        }
        if let Some(observed) = observed {
            assert!(Arc::ptr_eq(
                &observed.causes()[0].error,
                &retained.causes()[0].error
            ));
            let mapped = execution_error(observed);
            assert!(!CommonErrorCause::new::<NoPython>(&mapped).is_retryable());
        }
        // The provider really committed before losing the response. Neither
        // observed failure nor cancellation made abort or namespace deletion safe.
        assert_eq!(
            state.store.get(&state.path).await?.bytes().await?,
            "committed"
        );
        assert_eq!(state.aborts.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[test]
fn provider_and_supervisor_run_on_existing_scope_runtime() -> TestResult {
    let caller = tokio::runtime::Runtime::new()?;
    let io = tokio::runtime::Runtime::new()?;
    let scope = TaskResourceScope::new(io.handle().clone());
    let state = State::new();
    let provider = state.clone();
    caller.block_on(async {
        let mut upload = create(
            &scope,
            move || async move {
                Ok(Box::new(Upload::new(provider).await?) as Box<dyn MultipartUpload>)
            },
            capacity(),
        )
        .await?;
        upload.put_part("payload".into())?.await?;
        upload.complete()?.await?;
        let report = scope.drain().await?;
        assert!(report.is_clean());
        assert_eq!(report.registered, 1);
        Ok::<_, Box<dyn Error>>(())
    })?;
    let runtimes = state.runtimes.lock().map_err(|_| "fixture lock")?;
    assert_eq!(runtimes.len(), 4);
    assert!(runtimes.iter().all(|&id| id == io.handle().id()));
    assert_ne!(io.handle().id(), caller.handle().id());
    Ok(())
}
