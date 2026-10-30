use datafusion::execution::object_store::{ObjectStoreRegistry, ObjectStoreUrl};
use object_store::local::LocalFileSystem;
use sail_common::runtime::RuntimeHandle;
use sail_object_store::DynamicObjectStoreRegistry;
use tokio::runtime::Builder;

use super::*;

struct Directory(std::path::PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn local_provider_round_trip_preserves_read_copy_rename_and_delete() -> TestResult {
    let directory = Directory(std::env::temp_dir().join(format!(
        "sail-owned-store-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    )));
    std::fs::create_dir(&directory.0)?;
    let raw = Arc::new(LocalFileSystem::new_with_prefix(&directory.0)?);
    let resources = scope();
    let store = owned(raw.clone(), resources.clone());
    let a = Path::from("a");
    let b = Path::from("b");
    let c = Path::from("c");
    let m = Path::from("multipart");
    store.put(&a, "hello".into()).await?;
    let mut upload = store.put_multipart(&m).await?;
    upload.put_part("multipart".into()).await?;
    upload.complete().await?;
    drop(upload);
    let ranges = store.get_ranges(&a, &[0..2, 3..5]).await?;
    assert_eq!(
        ranges,
        vec![Bytes::from_static(b"he"), Bytes::from_static(b"lo")]
    );
    assert_eq!(store.head(&a).await?.size, 5);
    store.copy(&a, &b).await?;
    store.rename(&b, &c).await?;
    assert!(raw.head(&b).await.is_err());
    assert_eq!(store.get(&c).await?.bytes().await?, "hello");
    let listed = store.list_with_offset(None, &a).collect::<Vec<_>>().await;
    assert_eq!(listed.len(), 2);
    assert_eq!(store.list_with_delimiter(None).await?.objects.len(), 3);
    let removed = store
        .delete_stream(futures::stream::iter(vec![Ok(a), Ok(c), Ok(m)]).boxed())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(removed.len(), 3);
    assert!(removed.iter().all(Result::is_ok));
    let report = resources.drain().await?;
    assert!(report.is_clean());
    assert_eq!(report.registered, 7);
    assert!(raw.list(None).next().await.is_none());
    Ok(())
}

#[test]
fn dynamic_registry_runtime_store_keeps_its_cached_provider() -> TestResult {
    let caller = Builder::new_current_thread().enable_all().build()?;
    let io = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let registry = DynamicObjectStoreRegistry::new(RuntimeHandle::new(
        caller.handle().clone(),
        io.handle().clone(),
    ));
    caller.block_on(async {
        let url = ObjectStoreUrl::parse("memory:///")?;
        let provider = registry.get_store(url.as_ref())?;
        let resources = scope();
        let store = owned(provider.clone(), resources.clone());
        let path = Path::from("dynamic/part");
        let mut upload = store.put_multipart(&path).await?;
        upload.put_part("native-runtime".into()).await?;
        upload.complete().await?;
        drop(upload);
        assert!(resources.drain().await?.is_clean());
        assert_eq!(provider.get(&path).await?.bytes().await?, "native-runtime");
        assert!(Arc::ptr_eq(&provider, &registry.get_store(url.as_ref())?));
        assert_ne!(io.handle().id(), Handle::current().id());
        Ok::<_, Box<dyn Error>>(())
    })?;
    Ok(())
}
