use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::FutureExt;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use tokio::sync::oneshot;
use tokio::time::timeout;

use super::*;

#[path = "test_upload.rs"]
mod fixture;
use fixture::{Gate, TestUpload, failure};
type TestResult = std::result::Result<(), Box<dyn Error>>;

fn capacity() -> NonZeroUsize {
    NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN)
}

#[tokio::test]
async fn abandoned_creation_waits_for_provider_then_confirmed_abort() -> TestResult {
    let (mut upload, state) = TestUpload::new().await?;
    let (abort, aborted, release_abort) = Gate::new();
    upload.abort_gate = Some(abort);
    let (create, entered, release_create) = Gate::new();
    let (supervisor, creation) = prepare(
        move || async move {
            create.wait().await;
            Ok(Box::new(upload) as Box<dyn MultipartUpload>)
        },
        capacity(),
    );
    let (cancel, canceled) = watch::channel(false);
    let mut owner = tokio::spawn(supervisor.run(canceled));
    timeout(Duration::from_secs(5), entered).await??;
    drop(creation);
    cancel.send_replace(true);
    assert!((&mut owner).now_or_never().is_none());
    release_create
        .send(())
        .map_err(|_| "creation disappeared")?;
    timeout(Duration::from_secs(5), aborted).await??;
    assert!((&mut owner).now_or_never().is_none());
    assert_eq!(state.abort_count.load(Ordering::SeqCst), 1);
    release_abort.send(()).map_err(|_| "abort disappeared")?;
    timeout(Duration::from_secs(5), owner).await???;
    assert_eq!(state.complete_count.load(Ordering::SeqCst), 0);
    assert!(state.store.head(&state.path).await.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_part_observers_remain_owned_and_complete_after_all_parts() -> TestResult {
    let (mut upload, state) = TestUpload::new().await?;
    let (first, first_entered, release_first) = Gate::new();
    let (second, second_entered, release_second) = Gate::new();
    upload.part_gates = vec![first, second];
    let (supervisor, creation) = prepare(
        move || async move { Ok(Box::new(upload) as Box<dyn MultipartUpload>) },
        capacity(),
    );
    let (cancel, canceled) = watch::channel(false);
    let mut owner = tokio::spawn(supervisor.run(canceled));
    let mut handle = creation.wait().await?;
    let first = handle.put_part("first".into())?;
    let second = handle.put_part("second".into())?;
    let mut result = handle.complete()?;
    assert!(matches!(
        handle.put_part("late".into()),
        Err(MultipartError::Closed)
    ));
    drop((first, second, handle));
    cancel.send_replace(true);
    timeout(Duration::from_secs(5), first_entered).await??;
    timeout(Duration::from_secs(5), second_entered).await??;
    assert_eq!(*state.order.lock().map_err(|_| "fixture lock")?, vec![0, 1]);
    assert_eq!(state.complete_count.load(Ordering::SeqCst), 0);
    assert!((&mut owner).now_or_never().is_none());
    assert!((&mut result).now_or_never().is_none());
    release_second.send(()).map_err(|_| "second disappeared")?;
    assert_eq!(state.complete_count.load(Ordering::SeqCst), 0);
    release_first.send(()).map_err(|_| "first disappeared")?;
    assert!(matches!(
        timeout(Duration::from_secs(5), result).await??,
        Disposition::Committed(_)
    ));
    timeout(Duration::from_secs(5), owner).await???;
    assert_eq!(state.abort_count.load(Ordering::SeqCst), 0);
    assert_eq!(
        state.store.get(&state.path).await?.bytes().await?,
        "firstsecond"
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_joins_active_parts_before_abort_and_closes_data_admission() -> TestResult {
    let (mut upload, state) = TestUpload::new().await?;
    let (part, entered, release) = Gate::new();
    upload.part_gates = vec![part];
    let (supervisor, creation) = prepare(
        move || async move { Ok(Box::new(upload) as Box<dyn MultipartUpload>) },
        capacity(),
    );
    let (cancel, canceled) = watch::channel(false);
    let mut owner = tokio::spawn(supervisor.run(canceled));
    let mut handle = creation.wait().await?;
    let observer = handle.put_part("part".into())?;
    let result = handle.outcome();
    timeout(Duration::from_secs(5), entered).await??;
    cancel.send_replace(true);
    assert!(matches!(
        handle.put_part("late".into()),
        Err(MultipartError::Closed)
    ));
    assert!(matches!(handle.complete(), Err(MultipartError::Closed)));
    drop((observer, handle));
    assert!((&mut owner).now_or_never().is_none());
    assert_eq!(state.abort_count.load(Ordering::SeqCst), 0);
    release.send(()).map_err(|_| "part disappeared")?;
    assert_eq!(
        timeout(Duration::from_secs(5), result).await??,
        Disposition::Aborted
    );
    timeout(Duration::from_secs(5), owner).await???;
    assert_eq!(state.abort_count.load(Ordering::SeqCst), 1);
    assert!(state.store.head(&state.path).await.is_err());
    Ok(())
}

#[tokio::test]
async fn completion_is_retained_through_observer_loss_and_scope_cancel() -> TestResult {
    let (mut upload, state) = TestUpload::new().await?;
    let (complete, entered, release) = Gate::new();
    upload.complete_gate = Some(complete);
    let (supervisor, creation) = prepare(
        move || async move { Ok(Box::new(upload) as Box<dyn MultipartUpload>) },
        capacity(),
    );
    let (cancel, canceled) = watch::channel(false);
    let mut owner = tokio::spawn(supervisor.run(canceled));
    let mut handle = creation.wait().await?;
    handle.put_part("payload".into())?.await?;
    let result = handle.complete()?;
    timeout(Duration::from_secs(5), entered).await??;
    drop((result, handle));
    cancel.send_replace(true);
    assert!((&mut owner).now_or_never().is_none());
    assert!(state.store.head(&state.path).await.is_err());
    release.send(()).map_err(|_| "complete disappeared")?;
    timeout(Duration::from_secs(5), owner).await???;
    assert_eq!(
        state.store.get(&state.path).await?.bytes().await?,
        "payload"
    );
    assert_eq!(state.abort_count.load(Ordering::SeqCst), 0);
    state.store.delete(&state.path).await?;
    Ok(())
}

#[tokio::test]
async fn part_and_abort_errors_keep_both_original_typed_causes() -> TestResult {
    let (mut upload, _) = TestUpload::new().await?;
    upload.fail_part = true;
    upload.fail_abort = true;
    let (supervisor, creation) = prepare(
        move || async move { Ok(Box::new(upload) as Box<dyn MultipartUpload>) },
        capacity(),
    );
    let (_cancel, canceled) = watch::channel(false);
    let owner = tokio::spawn(supervisor.run(canceled));
    let mut handle = creation.wait().await?;
    let result = handle.outcome();
    let observed = handle
        .put_part("payload".into())?
        .await
        .err()
        .ok_or("expected part failure")?;
    let terminal = result.await.err().ok_or("expected settlement failure")?;
    let retained = owner.await?.err().ok_or("expected retained failure")?;
    assert_eq!(
        retained
            .causes()
            .iter()
            .map(|c| c.stage)
            .collect::<Vec<_>>(),
        vec![Stage::Part, Stage::Abort]
    );
    assert!(Arc::ptr_eq(
        &observed.causes()[0].error,
        &retained.causes()[0].error
    ));
    let (MultipartError::Provider(a), MultipartError::Provider(b)) = (terminal, retained) else {
        return Err("aggregate lost".into());
    };
    assert!(Arc::ptr_eq(&a, &b));
    assert!(a.iter().all(|cause| matches!(cause.error.as_ref(), object_store::Error::Generic { source, .. } if source.downcast_ref::<fixture::Injected>().is_some())));
    Ok(())
}

#[tokio::test]
async fn failed_completion_is_uncertain_and_not_blindly_aborted() -> TestResult {
    let (mut upload, state) = TestUpload::new().await?;
    upload.fail_complete = true;
    let (supervisor, creation) = prepare(
        move || async move { Ok(Box::new(upload) as Box<dyn MultipartUpload>) },
        capacity(),
    );
    let (_cancel, canceled) = watch::channel(false);
    let owner = tokio::spawn(supervisor.run(canceled));
    let mut handle = creation.wait().await?;
    let observed = handle
        .complete()?
        .await
        .err()
        .ok_or("expected complete failure")?;
    let retained = owner
        .await?
        .err()
        .ok_or("expected retained complete failure")?;
    assert_eq!(retained.causes()[0].stage, Stage::Complete);
    assert!(Arc::ptr_eq(
        &observed.causes()[0].error,
        &retained.causes()[0].error
    ));
    assert_eq!(state.abort_count.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn no_provider_call_before_registered_supervisor_poll() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    for abandoned in [false, true] {
        let count = calls.clone();
        let (supervisor, creation) = prepare(
            move || async move {
                count.fetch_add(1, Ordering::SeqCst);
                Err(failure())
            },
            capacity(),
        );
        if abandoned {
            drop(creation);
            let (_cancel, canceled) = watch::channel(false);
            supervisor.run(canceled).await?;
        } else {
            drop((supervisor, creation));
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_bounds_inflight_parts_without_reordering() -> TestResult {
    let (mut upload, _) = TestUpload::new().await?;
    let (part, entered, release) = Gate::new();
    upload.part_gates = vec![part];
    let (supervisor, creation) = prepare(
        move || async move { Ok(Box::new(upload) as Box<dyn MultipartUpload>) },
        NonZeroUsize::MIN,
    );
    let (_cancel, canceled) = watch::channel(false);
    let owner = tokio::spawn(supervisor.run(canceled));
    let mut handle = creation.wait().await?;
    let first = handle.put_part("first".into())?;
    timeout(Duration::from_secs(5), entered).await??;
    assert!(matches!(
        handle.put_part("not admitted".into()),
        Err(MultipartError::AtCapacity)
    ));
    release.send(()).map_err(|_| "part disappeared")?;
    first.await?;
    for _ in 0..100 {
        handle.put_part("next".into())?.await?;
    }
    handle.complete()?.await?;
    owner.await??;
    Ok(())
}

#[tokio::test]
async fn empty_upload_and_explicit_abort_have_one_disposition() -> TestResult {
    for abort_first in [false, true] {
        let (upload, state) = TestUpload::new().await?;
        let (supervisor, creation) = prepare(
            move || async move { Ok(Box::new(upload) as Box<dyn MultipartUpload>) },
            capacity(),
        );
        let (_cancel, canceled) = watch::channel(false);
        let owner = tokio::spawn(supervisor.run(canceled));
        let mut handle = creation.wait().await?;
        if abort_first {
            let result = handle.abort()?;
            assert!(matches!(handle.complete(), Err(MultipartError::Closed)));
            assert_eq!(result.await?, Disposition::Aborted);
            assert_eq!(state.complete_count.load(Ordering::SeqCst), 0);
            assert_eq!(state.abort_count.load(Ordering::SeqCst), 1);
        } else {
            let result = handle.complete()?;
            let abort = handle.abort()?;
            assert!(matches!(result.await?, Disposition::Committed(_)));
            assert!(matches!(abort.await?, Disposition::Committed(_)));
            assert_eq!(state.store.get(&state.path).await?.bytes().await?.len(), 0);
            assert_eq!(state.complete_count.load(Ordering::SeqCst), 1);
            assert_eq!(state.abort_count.load(Ordering::SeqCst), 0);
        }
        owner.await??;
    }
    Ok(())
}

#[tokio::test]
async fn failed_creation_is_shared_with_owner_and_live_observer() -> TestResult {
    let (supervisor, creation) = prepare(|| async { Err(failure()) }, capacity());
    let (_cancel, canceled) = watch::channel(false);
    let owner = tokio::spawn(supervisor.run(canceled));
    let observed = creation
        .wait()
        .await
        .err()
        .ok_or("expected creation failure")?;
    let retained = owner
        .await?
        .err()
        .ok_or("expected retained creation failure")?;
    assert_eq!(retained.causes()[0].stage, Stage::Create);
    let (MultipartError::Provider(a), MultipartError::Provider(b)) = (observed, retained) else {
        return Err("creation cause lost".into());
    };
    assert!(Arc::ptr_eq(&a, &b));
    Ok(())
}

#[tokio::test]
async fn interrupted_owner_is_not_reported_as_a_clean_abort() -> TestResult {
    let (upload, _) = TestUpload::new().await?;
    let (supervisor, creation) = prepare(
        move || async move { Ok(Box::new(upload) as Box<dyn MultipartUpload>) },
        capacity(),
    );
    let (_cancel, canceled) = watch::channel(false);
    let owner = tokio::spawn(supervisor.run(canceled));
    let handle = creation.wait().await?;
    let result = handle.outcome();
    owner.abort();
    assert!(owner.await.is_err_and(|error| error.is_cancelled()));
    assert!(matches!(result.await, Err(MultipartError::Interrupted)));
    Ok(())
}

#[test]
fn cancellation_rechecks_completion_admission_under_the_same_lock() -> TestResult {
    let state = Mutex::new(Admission::Completing);
    assert!(!super::supervisor::close_if_open(&state)?);
    assert_eq!(
        *state.lock().map_err(|_| "state lock")?,
        Admission::Completing
    );
    Ok(())
}

#[tokio::test]
async fn explicit_abort_and_scope_cancellation_settle_the_same_upload() -> TestResult {
    let (upload, state) = TestUpload::new().await?;
    let (supervisor, creation) = prepare(
        move || async move { Ok(Box::new(upload) as Box<dyn MultipartUpload>) },
        capacity(),
    );
    let (cancel, canceled) = watch::channel(false);
    let owner = tokio::spawn(supervisor.run(canceled));
    let mut handle = creation.wait().await?;
    // Current-thread executor: neither synchronous request yields to the owner.
    let result = handle.abort()?;
    cancel.send_replace(true);
    assert_eq!(
        timeout(Duration::from_secs(5), result).await??,
        Disposition::Aborted
    );
    timeout(Duration::from_secs(5), owner).await???;
    assert_eq!(state.abort_count.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn oversized_capacity_is_rejected_before_provider_invocation() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let capacity = NonZeroUsize::new(Semaphore::MAX_PERMITS + 1).ok_or("capacity")?;
    let (supervisor, creation) = prepare(
        move || async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err(failure())
        },
        capacity,
    );
    let (_cancel, canceled) = watch::channel(false);
    let owner = tokio::spawn(supervisor.run(canceled));
    assert!(matches!(
        creation.wait().await,
        Err(MultipartError::InvalidCapacity)
    ));
    assert!(matches!(owner.await?, Err(MultipartError::InvalidCapacity)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn provider_creation_part_invocation_and_settlement_use_owner_runtime() -> TestResult {
    let caller = tokio::runtime::Runtime::new()?;
    let io = tokio::runtime::Runtime::new()?;
    for abort in [false, true] {
        let (state_tx, state_rx) = oneshot::channel();
        let (supervisor, creation) = prepare(
            move || async move {
                let (upload, state) = TestUpload::new().await?;
                let _ = state_tx.send(state);
                Ok(Box::new(upload) as Box<dyn MultipartUpload>)
            },
            capacity(),
        );
        let (_cancel, canceled) = watch::channel(false);
        let owner = io.spawn(supervisor.run(canceled));
        let state = caller.block_on(async move {
            let mut handle = creation.wait().await?;
            let state = state_rx.await?;
            handle.put_part("payload".into())?.await?;
            if abort {
                handle.abort()?.await?;
            } else {
                handle.complete()?.await?;
            }
            owner.await??;
            Ok::<_, Box<dyn Error>>(state)
        })?;
        let runtimes = state.runtimes.lock().map_err(|_| "fixture runtime lock")?;
        // creation, synchronous put_part, part future, complete/abort
        assert_eq!(runtimes.len(), 4);
        assert!(runtimes.iter().all(|&id| id == io.handle().id()));
        assert_ne!(caller.handle().id(), io.handle().id());
    }
    Ok(())
}

#[path = "tests_notification.rs"]
mod notification;
