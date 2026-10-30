use datafusion::execution::cache::TableScopedPath;
use datafusion::execution::cache::cache_manager::CachedFileList;
use datafusion::execution::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry, ObjectStoreUrl,
};
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;

use super::*;

#[tokio::test]
async fn derived_registry_preserves_owner_functions_config_and_runtime_resources() -> TestResult {
    let parent = parent();
    let owned = ScopedTaskContext::new(&parent, Handle::current());
    let parent_runtime = owned.context.runtime_env();
    let cache = parent_runtime
        .cache_manager
        .get_list_files_cache()
        .ok_or("cache fixture must be enabled")?;
    let memory = InMemory::new();
    let cached_path = Path::from("cached-list/file");
    memory.put(&cached_path, "cached".into()).await?;
    let key = TableScopedPath {
        table: None,
        path: Path::from("cached-list"),
    };
    cache.put(
        &key,
        CachedFileList::new(vec![memory.head(&cached_path).await?]),
    );
    assert!(cache.get(&key).is_some());
    let registry: Arc<dyn ObjectStoreRegistry> = Arc::new(DefaultObjectStoreRegistry::new());
    let child = owned.with_object_store_registry(registry.clone())?;
    let runtime = child.context.runtime_env();
    assert!(!Arc::ptr_eq(&parent_runtime, &runtime));
    assert!(Arc::ptr_eq(&registry, &runtime.object_store_registry));
    assert!(!Arc::ptr_eq(
        &parent_runtime.object_store_registry,
        &runtime.object_store_registry
    ));
    assert!(Arc::ptr_eq(
        &parent_runtime.memory_pool,
        &runtime.memory_pool
    ));
    assert!(Arc::ptr_eq(
        &parent_runtime.disk_manager,
        &runtime.disk_manager
    ));
    assert!(!Arc::ptr_eq(
        &parent_runtime.cache_manager,
        &runtime.cache_manager
    ));
    assert!(Arc::ptr_eq(
        &parent_runtime.cache_manager.get_file_metadata_cache(),
        &runtime.cache_manager.get_file_metadata_cache()
    ));
    assert!(Arc::ptr_eq(
        &parent_runtime
            .cache_manager
            .get_file_statistic_cache()
            .ok_or("statistics cache")?,
        &runtime
            .cache_manager
            .get_file_statistic_cache()
            .ok_or("child statistics cache")?
    ));
    let child_cache = runtime
        .cache_manager
        .get_list_files_cache()
        .ok_or("child list cache")?;
    assert!(Arc::ptr_eq(&cache, &child_cache));
    assert_eq!(
        child_cache
            .get(&key)
            .ok_or("lost cache entry")?
            .files_matching_prefix(&None)[0]
            .location,
        cached_path
    );
    child_cache.remove(&key);
    assert!(cache.get(&key).is_none());
    assert!(Arc::ptr_eq(&owned.resources, &child.resources));
    assert!(Arc::ptr_eq(
        &owned.resources,
        &child.context.extension::<TaskResourceScope>()?
    ));
    assert_eq!(owned.context.task_id(), child.context.task_id());
    assert_eq!(owned.context.session_id(), child.context.session_id());
    assert_eq!(
        child
            .context
            .session_config()
            .options()
            .execution
            .target_partitions,
        7
    );
    assert!(Arc::ptr_eq(
        &owned.context.extension::<Sentinel>()?,
        &child.context.extension::<Sentinel>()?
    ));
    same_registry(
        owned.context.scalar_functions(),
        child.context.scalar_functions(),
    );
    same_registry(
        owned.context.higher_order_functions(),
        child.context.higher_order_functions(),
    );
    same_registry(
        owned.context.aggregate_functions(),
        child.context.aggregate_functions(),
    );
    same_registry(
        owned.context.window_functions(),
        child.context.window_functions(),
    );
    child.resources.cancel()?;
    assert!(owned.resources.spawn(|_| async { Ok(()) }).is_err());
    assert!(child.resources.drain().await?.is_clean());
    Ok(())
}

#[tokio::test]
async fn parent_and_sibling_store_lookups_remain_isolated() -> TestResult {
    let parent = parent();
    let a = ScopedTaskContext::new(&parent, Handle::current());
    let b = ScopedTaskContext::new(&parent, Handle::current());
    let url = ObjectStoreUrl::parse("memory://isolated/")?;
    let key = Path::from("value");
    let parent_store = Arc::new(InMemory::new());
    let a_store = Arc::new(InMemory::new());
    let b_store = Arc::new(InMemory::new());
    parent_store.put(&key, "parent".into()).await?;
    a_store.put(&key, "a".into()).await?;
    b_store.put(&key, "b".into()).await?;
    parent
        .runtime_env()
        .register_object_store(url.as_ref(), parent_store);
    let a_registry = Arc::new(DefaultObjectStoreRegistry::new());
    let b_registry = Arc::new(DefaultObjectStoreRegistry::new());
    a_registry.register_store(url.as_ref(), a_store);
    b_registry.register_store(url.as_ref(), b_store);
    let a_child = a.with_object_store_registry(a_registry)?;
    let b_child = b.with_object_store_registry(b_registry)?;
    for (context, expected) in [
        (&parent, "parent"),
        (a_child.context.as_ref(), "a"),
        (b_child.context.as_ref(), "b"),
        (a.context.as_ref(), "parent"),
    ] {
        assert_eq!(
            context
                .runtime_env()
                .object_store(&url)?
                .get(&key)
                .await?
                .bytes()
                .await?,
            expected
        );
    }
    a_child.resources.cancel()?;
    assert!(a.resources.spawn(|_| async { Ok(()) }).is_err());
    b_child.resources.spawn(|canceled| async move {
        assert!(!*canceled.borrow());
        Ok(())
    })?;
    assert_eq!(b.resources.drain().await?.registered, 1);
    assert_eq!(a.resources.drain().await?.registered, 0);
    Ok(())
}

#[tokio::test]
async fn mismatched_or_missing_owner_is_rejected() -> TestResult {
    let parent = parent();
    let a = ScopedTaskContext::new(&parent, Handle::current());
    let registry = Arc::new(DefaultObjectStoreRegistry::new());
    let invalid = ScopedTaskContext {
        context: a.context.clone(),
        resources: Arc::new(TaskResourceScope::new(Handle::current())),
    };
    assert!(
        invalid
            .with_object_store_registry(registry.clone())
            .is_err_and(|error| error.to_string().contains("does not match"))
    );
    let no_owner = ScopedTaskContext {
        context: SessionContext::new().task_ctx(),
        resources: a.resources.clone(),
    };
    assert!(
        no_owner
            .with_object_store_registry(registry)
            .is_err_and(|error| error.to_string().contains("extension not found"))
    );
    assert!(a.resources.drain().await?.is_clean());
    Ok(())
}
