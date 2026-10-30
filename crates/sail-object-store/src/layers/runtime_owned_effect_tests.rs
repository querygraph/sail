use std::num::NonZeroUsize;

use super::*;
use crate::owned_multipart::{self, Stage};

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn retained_supervisor_does_not_abort_after_uncertain_nested_completion() -> TestResult {
    for layers in [1, 2] {
        let store = Arc::new(InMemory::new());
        let path = Path::from("checkpoint/uncertain");
        let (disposed, disposed_rx) = oneshot::channel();
        let aborts = Arc::new(AtomicUsize::new(0));
        let raw = HeldPublication {
            store: store.clone(),
            path: path.clone(),
            started: None,
            release: None,
            disposed: Some(disposed),
            aborts: aborts.clone(),
            fail_after_publish: true,
        };
        let handle = Handle::current();
        let (supervisor, creation) = owned_multipart::prepare(
            move || async move {
                let mut upload: Box<dyn MultipartUpload> = Box::new(raw);
                for _ in 0..layers {
                    upload = Box::new(RuntimeAwareMultipartUpload::new(upload, handle.clone()));
                }
                Ok(upload)
            },
            NonZeroUsize::MIN,
        );
        let (_cancel, canceled) = tokio::sync::watch::channel(false);
        let owner = tokio::spawn(supervisor.run(canceled));
        let mut upload = creation.wait().await?;
        let observed = upload
            .complete()?
            .await
            .err()
            .ok_or("expected uncertain completion")?;
        assert_eq!(observed.causes()[0].stage, Stage::Complete);
        assert!(owner.await?.is_err());
        // The raw provider's destructor proves the nested wrapper ownership
        // chain and any Drop cleanup completed. No absence-by-sleep assertion.
        timeout(Duration::from_secs(5), disposed_rx).await??;
        assert_eq!(store.get(&path).await?.bytes().await?, "late publication");
        assert_eq!(
            aborts.load(Ordering::SeqCst),
            0,
            "nested Drop aborted an uncertain publication"
        );
    }
    Ok(())
}

#[tokio::test]
async fn explicit_abort_after_uncertain_complete_remains_a_caller_decision() -> TestResult {
    let store = Arc::new(InMemory::new());
    let path = Path::from("explicit-abort");
    let (disposed, disposed_rx) = oneshot::channel();
    let aborts = Arc::new(AtomicUsize::new(0));
    let raw = HeldPublication {
        store: store.clone(),
        path: path.clone(),
        started: None,
        release: None,
        disposed: Some(disposed),
        aborts: aborts.clone(),
        fail_after_publish: true,
    };
    let mut upload = RuntimeAwareMultipartUpload::new(Box::new(raw), Handle::current());
    assert!(upload.complete().await.is_err());
    upload.abort().await?;
    drop(upload);
    timeout(Duration::from_secs(5), disposed_rx).await??;
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
    assert_eq!(store.get(&path).await?.bytes().await?, "late publication");
    Ok(())
}

#[tokio::test]
async fn unpolled_complete_keeps_abandoned_upload_cleanup() -> TestResult {
    let store = Arc::new(InMemory::new());
    let path = Path::from("unpolled-complete");
    let (disposed, disposed_rx) = oneshot::channel();
    let aborts = Arc::new(AtomicUsize::new(0));
    let raw = HeldPublication {
        store: store.clone(),
        path: path.clone(),
        started: None,
        release: None,
        disposed: Some(disposed),
        aborts: aborts.clone(),
        fail_after_publish: false,
    };
    let mut upload = RuntimeAwareMultipartUpload::new(Box::new(raw), Handle::current());
    drop(upload.complete());
    drop(upload);
    timeout(Duration::from_secs(5), disposed_rx).await??;
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
    assert!(matches!(
        store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    Ok(())
}

#[path = "runtime_owned_test_store.rs"]
mod store;

#[test]
fn nested_store_creation_on_distinct_runtimes_preserves_uncertain_publication() -> TestResult {
    use object_store::{ObjectStore, PutMultipartOptions};

    use crate::layers::runtime::RuntimeAwareObjectStore;

    let caller = tokio::runtime::Runtime::new()?;
    let inner_io = tokio::runtime::Runtime::new()?;
    let outer_io = tokio::runtime::Runtime::new()?;
    let (disposed, disposed_rx) = oneshot::channel();
    let raw = Arc::new(store::PublicationStore {
        memory: Arc::new(InMemory::new()),
        disposed: std::sync::Mutex::new(Some(disposed)),
        aborts: Arc::new(AtomicUsize::new(0)),
        created_on: std::sync::Mutex::new(None),
    });
    let native = RuntimeAwareObjectStore::try_new(|| Ok(raw.clone()), inner_io.handle().clone())?;
    let native: Arc<dyn ObjectStore> = Arc::new(native);
    let nested = RuntimeAwareObjectStore::try_new(|| Ok(native), outer_io.handle().clone())?;
    let path = Path::from("nested-runtimes");
    let location = path.clone();
    caller.block_on(async {
        let (supervisor, creation) = owned_multipart::prepare(
            move || async move {
                nested
                    .put_multipart_opts(&location, PutMultipartOptions::default())
                    .await
            },
            NonZeroUsize::MIN,
        );
        let (_cancel, canceled) = tokio::sync::watch::channel(false);
        let owner = tokio::spawn(supervisor.run(canceled));
        let mut upload = creation.wait().await?;
        let failure = upload
            .complete()?
            .await
            .err()
            .ok_or("expected uncertain complete")?;
        assert_eq!(failure.causes()[0].stage, Stage::Complete);
        assert!(owner.await?.is_err());
        timeout(Duration::from_secs(5), disposed_rx).await??;
        assert_eq!(
            raw.memory.get(&path).await?.bytes().await?,
            "late publication"
        );
        assert_eq!(raw.aborts.load(Ordering::SeqCst), 0);
        assert_eq!(
            *raw.created_on.lock().map_err(|_| "fixture lock")?,
            Some(inner_io.handle().id())
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    assert_ne!(inner_io.handle().id(), outer_io.handle().id());
    assert_ne!(caller.handle().id(), inner_io.handle().id());
    Ok(())
}

#[test]
fn dropping_first_polled_complete_before_io_runs_cannot_start_abort() -> TestResult {
    use futures::FutureExt;

    let caller = tokio::runtime::Runtime::new()?;
    let io = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let store = Arc::new(InMemory::new());
    let path = Path::from("before-io-poll");
    let (disposed, disposed_rx) = oneshot::channel();
    let aborts = Arc::new(AtomicUsize::new(0));
    let raw = HeldPublication {
        store: store.clone(),
        path: path.clone(),
        started: None,
        release: None,
        disposed: Some(disposed),
        aborts: aborts.clone(),
        fail_after_publish: true,
    };
    let mut upload = RuntimeAwareMultipartUpload::new(Box::new(raw), io.handle().clone());
    caller.block_on(async {
        let mut complete = Box::pin(upload.complete());
        // The separate current-thread I/O runtime has never been driven.
        assert!(complete.as_mut().now_or_never().is_none());
        drop(complete);
        drop(upload);
        assert!(matches!(
            store.head(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    io.block_on(async {
        timeout(Duration::from_secs(5), disposed_rx).await??;
        assert_eq!(store.get(&path).await?.bytes().await?, "late publication");
        assert_eq!(aborts.load(Ordering::SeqCst), 0);
        Ok::<_, Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
