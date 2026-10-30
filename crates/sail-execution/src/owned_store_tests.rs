use std::error::Error;
use std::sync::atomic::Ordering;
use std::time::Duration;

use datafusion::common::DataFusionError;
use futures::FutureExt;
use object_store::ObjectStoreExt;
use sail_common_datafusion::error::{
    CommonErrorCause, PythonErrorCause, PythonErrorCauseExtractor,
};
use tokio::runtime::Handle;
use tokio::time::timeout;

use super::*;
#[path = "owned_store_test_provider.rs"]
mod provider;
use provider::Provider;

use crate::owned_multipart_test_upload::Gate;

type TestResult = Result<(), Box<dyn Error>>;
struct NoPython;
impl PythonErrorCauseExtractor for NoPython {
    fn extract(_: &(dyn Error + 'static)) -> Option<PythonErrorCause> {
        None
    }
}
fn scope() -> Arc<TaskResourceScope> {
    Arc::new(TaskResourceScope::new(Handle::current()))
}
fn owned(inner: Arc<dyn ObjectStore>, resources: Arc<TaskResourceScope>) -> Arc<OwnedObjectStore> {
    Arc::new(OwnedObjectStore::new(inner, resources, NonZeroUsize::MIN))
}

#[tokio::test]
async fn sealed_and_unpolled_mutations_never_invoke_provider() -> TestResult {
    let raw = Arc::new(Provider::default());
    let resources = scope();
    let store = owned(raw.clone(), resources.clone());
    let path = Path::from("checkpoint/part");
    drop(store.put(&path, "unpolled".into()));
    drop(store.put_multipart(&path));
    resources.seal()?;
    assert!(store.put(&path, "sealed".into()).await.is_err());
    assert!(store.put_multipart(&path).await.is_err());
    assert!(store.copy(&path, &Path::from("copy")).await.is_err());
    assert!(store.rename(&path, &Path::from("rename")).await.is_err());
    assert!(store.delete(&path).await.is_err());
    assert_eq!(raw.calls.load(Ordering::SeqCst), 0);
    assert_eq!(resources.drain().await?.registered, 0);
    Ok(())
}

#[tokio::test]
async fn lost_put_caller_stays_owned_until_actual_publication() -> TestResult {
    let raw = Arc::new(Provider::default());
    let resources = scope();
    let store = owned(raw.clone(), resources.clone());
    let (gate, entered, release) = Gate::new();
    raw.gates.lock().map_err(|_| "gates")?.put = Some(gate);
    let path = Path::from("checkpoint/part");
    let caller = {
        let store = store.clone();
        let path = path.clone();
        tokio::spawn(async move { store.put(&path, "retained".into()).await })
    };
    timeout(Duration::from_secs(10), entered).await??;
    caller.abort();
    assert!(caller.await.is_err_and(|e| e.is_cancelled()));
    resources.cancel()?;
    assert!(resources.drain().now_or_never().is_none());
    assert!(raw.inner.head(&path).await.is_err());
    release.send(()).map_err(|_| "provider lost")?;
    let report = timeout(Duration::from_secs(10), resources.drain()).await??;
    assert!(report.is_clean());
    assert_eq!(report.registered, 1);
    assert_eq!(raw.inner.get(&path).await?.bytes().await?, "retained");
    Ok(())
}

#[tokio::test]
async fn abandoned_creation_and_handle_join_actual_abort() -> TestResult {
    for during_creation in [false, true] {
        let raw = Arc::new(Provider::default());
        let resources = scope();
        let store = owned(raw.clone(), resources.clone());
        let (abort, aborting, release_abort) = Gate::new();
        raw.gates.lock().map_err(|_| "gates")?.abort = Some(abort);
        if during_creation {
            let (create, creating, release_create) = Gate::new();
            raw.gates.lock().map_err(|_| "gates")?.create = Some(create);
            let caller =
                tokio::spawn(async move { store.put_multipart(&Path::from("part")).await });
            timeout(Duration::from_secs(10), creating).await??;
            caller.abort();
            assert!(caller.await.is_err_and(|e| e.is_cancelled()));
            resources.cancel()?;
            release_create.send(()).map_err(|_| "create lost")?;
        } else {
            drop(store.put_multipart(&Path::from("part")).await?);
        }
        timeout(Duration::from_secs(10), aborting).await??;
        assert!(resources.drain().now_or_never().is_none());
        release_abort.send(()).map_err(|_| "abort lost")?;
        let report = timeout(Duration::from_secs(10), resources.drain()).await??;
        assert!(report.is_clean());
        assert_eq!(report.registered, 1);
        assert_eq!(raw.aborts(), 1);
    }
    Ok(())
}

#[tokio::test]
async fn facade_keeps_part_order_capacity_and_typed_completion_uncertainty() -> TestResult {
    let raw = Arc::new(Provider {
        lose_complete_response: true,
        ..Default::default()
    });
    let resources = scope();
    let store = owned(raw.clone(), resources.clone());
    let path = Path::from("checkpoint/part");
    let mut upload = store.put_multipart(&path).await?;
    let part = upload.put_part("one".into());
    let rejected = upload.put_part("excess".into());
    let error = rejected.await.err().ok_or("capacity was not enforced")?;
    assert!(error.to_string().contains("capacity"));
    part.await?;
    upload.put_part("two".into()).await?;
    let error = upload
        .complete()
        .await
        .err()
        .ok_or("lost response succeeded")?;
    assert!(
        !CommonErrorCause::new::<NoPython>(&DataFusionError::ObjectStore(Box::new(error)))
            .is_retryable()
    );
    drop(upload);
    let report = resources.drain().await?;
    assert!(!report.is_clean());
    assert_eq!(raw.aborts(), 0);
    assert_eq!(raw.inner.get(&path).await?.bytes().await?, "onetwo");
    Ok(())
}

#[path = "owned_store_writer_tests.rs"]
mod writer;

#[tokio::test]
async fn lost_copy_and_rename_callers_join_provider_completion() -> TestResult {
    for rename in [false, true] {
        let raw = Arc::new(Provider::default());
        let resources = scope();
        let store = owned(raw.clone(), resources.clone());
        let from = Path::from("from");
        let to = Path::from("to");
        raw.inner.put(&from, "original".into()).await?;
        let (gate, entered, release) = Gate::new();
        raw.gates.lock().map_err(|_| "gates")?.copy = Some(gate);
        let caller = {
            let from = from.clone();
            let to = to.clone();
            tokio::spawn(async move {
                if rename {
                    store.rename(&from, &to).await
                } else {
                    store.copy(&from, &to).await
                }
            })
        };
        timeout(Duration::from_secs(10), entered).await??;
        caller.abort();
        assert!(caller.await.is_err_and(|e| e.is_cancelled()));
        resources.cancel()?;
        assert!(resources.drain().now_or_never().is_none());
        assert!(raw.inner.head(&to).await.is_err());
        release.send(()).map_err(|_| "copy disappeared")?;
        assert!(
            timeout(Duration::from_secs(10), resources.drain())
                .await??
                .is_clean()
        );
        assert_eq!(raw.inner.get(&to).await?.bytes().await?, "original");
        assert_eq!(raw.inner.head(&from).await.is_err(), rename);
    }
    Ok(())
}

#[tokio::test]
async fn lost_delete_consumer_joins_one_location_without_prefetch() -> TestResult {
    use std::sync::atomic::AtomicUsize;
    let raw = Arc::new(Provider::default());
    let resources = scope();
    let store = owned(raw.clone(), resources.clone());
    let a = Path::from("a");
    let b = Path::from("b");
    raw.inner.put(&a, "a".into()).await?;
    raw.inner.put(&b, "b".into()).await?;
    let (gate, entered, release) = Gate::new();
    raw.gates.lock().map_err(|_| "gates")?.delete = Some(gate);
    let polled = Arc::new(AtomicUsize::new(0));
    let count = polled.clone();
    let paths = futures::stream::iter(vec![Ok(a.clone()), Ok(b.clone())])
        .inspect(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
        })
        .boxed();
    let caller = tokio::spawn(async move { store.delete_stream(paths).next().await });
    timeout(Duration::from_secs(10), entered).await??;
    caller.abort();
    assert!(caller.await.is_err_and(|e| e.is_cancelled()));
    resources.cancel()?;
    assert!(resources.drain().now_or_never().is_none());
    assert_eq!(polled.load(Ordering::SeqCst), 1);
    release.send(()).map_err(|_| "delete disappeared")?;
    assert!(
        timeout(Duration::from_secs(10), resources.drain())
            .await??
            .is_clean()
    );
    assert!(raw.inner.head(&a).await.is_err());
    assert!(raw.inner.head(&b).await.is_ok());
    assert_eq!(polled.load(Ordering::SeqCst), 1);
    Ok(())
}

#[path = "owned_store_provider_tests.rs"]
mod providers;

#[tokio::test]
async fn conditional_provider_errors_keep_their_variants_and_typed_source() -> TestResult {
    use object_store::memory::InMemory;
    use object_store::{PutMode, PutOptions, UpdateVersion};
    for operation in 0..4 {
        let raw = Arc::new(InMemory::new());
        let resources = scope();
        let store = owned(raw.clone(), resources.clone());
        let from = Path::from("source");
        let to = Path::from("existing");
        raw.put(&from, "source".into()).await?;
        raw.put(&to, "original".into()).await?;
        let result = match operation {
            0 => store
                .put_opts(&to, "replacement".into(), PutMode::Create.into())
                .await
                .map(|_| ()),
            1 => store
                .put_opts(
                    &to,
                    "replacement".into(),
                    PutOptions {
                        mode: PutMode::Update(UpdateVersion {
                            e_tag: Some("wrong-version".into()),
                            version: None,
                        }),
                        ..Default::default()
                    },
                )
                .await
                .map(|_| ()),
            2 => store.copy_if_not_exists(&from, &to).await,
            _ => store.rename_if_not_exists(&from, &to).await,
        };
        let error = result
            .err()
            .ok_or("conditional mutation unexpectedly succeeded")?;
        let (path, source) = match &error {
            object_store::Error::AlreadyExists { path, source } if operation != 1 => (path, source),
            object_store::Error::Precondition { path, source } if operation == 1 => (path, source),
            _ => return Err(format!("provider condition variant lost: {error:?}").into()),
        };
        assert_eq!(path, "existing");
        let retained = source
            .downcast_ref::<DataFusionError>()
            .ok_or("typed resource cause lost")?;
        let DataFusionError::Shared(original) = retained else {
            return Err("unshared original error".into());
        };
        assert!(matches!(original.as_ref(), DataFusionError::ObjectStore(_)));
        let report = resources.drain().await?;
        assert!(!report.is_clean());
        assert_eq!(report.registered, 1);
        assert_eq!(raw.get(&to).await?.bytes().await?, "original");
        assert_eq!(raw.get(&from).await?.bytes().await?, "source");
    }
    Ok(())
}
