use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::time::timeout;

use super::*;
#[path = "owned_put_test_store.rs"]
mod store;
use store::{HeldStore, PutFailure};
type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

fn held(fail: bool) -> (Arc<HeldStore>, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let (entered, wait) = oneshot::channel();
    let (release, pending) = oneshot::channel();
    (
        Arc::new(HeldStore {
            inner: Arc::new(InMemory::new()),
            calls: AtomicUsize::new(0),
            fail,
            entered: Mutex::new(Some(entered)),
            release: Mutex::new(Some(pending)),
        }),
        wait,
        release,
    )
}
fn original(error: &DataFusionError) -> Option<&object_store::Error> {
    match error {
        DataFusionError::Shared(error) => original(error),
        DataFusionError::ObjectStore(error) => Some(error),
        _ => None,
    }
}
#[tokio::test]
async fn lost_caller_does_not_release_put_ownership_before_drain() -> TestResult {
    let scope = Arc::new(TaskResourceScope::new(Handle::current()));
    let (store, entered, release) = held(false);
    let path = Path::from("checkpoint/session/part");
    let task = {
        let scope = scope.clone();
        let store = store.clone();
        let path = path.clone();
        tokio::spawn(async move {
            put(&scope, store, path, "payload".into(), PutOptions::default()).await
        })
    };
    timeout(Duration::from_secs(5), entered).await??;
    task.abort();
    assert!(task.await.is_err_and(|e| e.is_cancelled()));
    scope.cancel()?;
    assert!(scope.drain().now_or_never().is_none());
    assert!(matches!(
        store.inner.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    release.send(()).map_err(|_| "provider disappeared")?;
    let report = timeout(Duration::from_secs(5), scope.drain()).await??;
    assert!(report.is_clean());
    assert_eq!(report.registered, 1);
    assert_eq!(store.inner.get(&path).await?.bytes().await?, "payload");
    store.inner.delete(&path).await?;
    assert!(matches!(
        store.inner.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));
    Ok(())
}
#[tokio::test]
async fn provider_error_keeps_the_same_typed_cause_in_observer_and_drain() -> TestResult {
    for abandoned in [false, true] {
        let scope = Arc::new(TaskResourceScope::new(Handle::current()));
        let (store, entered, release) = held(true);
        let task = {
            let scope = scope.clone();
            tokio::spawn(async move {
                put(
                    &scope,
                    store,
                    Path::from("checkpoint/part"),
                    "data".into(),
                    PutOptions::default(),
                )
                .await
            })
        };
        timeout(Duration::from_secs(5), entered).await??;
        let observer = if abandoned {
            task.abort();
            assert!(task.await.is_err_and(|e| e.is_cancelled()));
            None
        } else {
            Some(task)
        };
        release.send(()).map_err(|_| "provider disappeared")?;
        let observed = if let Some(task) = observer {
            Some(task.await?.err().ok_or("expected provider failure")?)
        } else {
            None
        };
        let report = scope.drain().await?;
        assert_eq!(report.registered, 1);
        assert_eq!(report.failures.len(), 1);
        let Some(object_store::Error::PermissionDenied { source, .. }) =
            original(&report.failures[0].error)
        else {
            return Err("typed provider failure lost".into());
        };
        assert!(source.downcast_ref::<PutFailure>().is_some());
        if let Some(observed) = observed {
            let DataFusionError::Shared(observed) = observed else {
                return Err("observer shared cause lost".into());
            };
            let DataFusionError::Shared(retained) = &report.failures[0].error else {
                return Err("shared cause lost".into());
            };
            assert!(Arc::ptr_eq(&observed, retained));
        }
    }
    Ok(())
}
#[tokio::test]
async fn sealed_scope_rejects_put_before_provider_invocation() -> TestResult {
    let scope = TaskResourceScope::new(Handle::current());
    let (store, _, _) = held(false);
    scope.seal()?;
    assert!(
        put(
            &scope,
            store.clone(),
            Path::from("part"),
            "data".into(),
            PutOptions::default()
        )
        .await
        .is_err()
    );
    assert_eq!(store.calls.load(Ordering::SeqCst), 0);
    assert_eq!(scope.drain().await?.registered, 0);
    Ok(())
}
#[tokio::test]
async fn an_unpolled_put_admits_no_work() -> TestResult {
    let scope = TaskResourceScope::new(Handle::current());
    let (store, _, _) = held(false);
    drop(put(
        &scope,
        store.clone(),
        Path::from("part"),
        "data".into(),
        PutOptions::default(),
    ));
    assert_eq!(store.calls.load(Ordering::SeqCst), 0);
    assert_eq!(scope.drain().await?.registered, 0);
    Ok(())
}
