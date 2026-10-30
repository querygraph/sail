use std::collections::HashMap;

use arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::ScalarValue;

use super::*;
use crate::extension::{SessionExtension, SessionExtensionAccessor};

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

struct Sentinel;
impl SessionExtension for Sentinel {
    fn name() -> &'static str {
        "resource context sentinel"
    }
}

fn parent() -> TaskContext {
    let session = SessionContext::new();
    session.register_udf(create_udf(
        "resource_context_scalar",
        vec![],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(7))))),
    ));
    let base = session.task_ctx();
    let config = SessionConfig::new()
        .with_target_partitions(7)
        .with_extension(Arc::new(Sentinel))
        .with_extension(Arc::new(TaskResourceScope::new(Handle::current())));
    TaskContext::new(
        Some("attempt-context".into()),
        "session-context".into(),
        config,
        base.scalar_functions().clone(),
        datafusion::functions_nested::all_default_higher_order_functions()
            .into_iter()
            .map(|f| (f.name().to_string(), f))
            .collect(),
        datafusion::functions_aggregate::all_default_aggregate_functions()
            .into_iter()
            .map(|f| (f.name().to_string(), f))
            .collect(),
        datafusion::functions_window::all_default_window_functions()
            .into_iter()
            .map(|f| (f.name().to_string(), f))
            .collect(),
        base.runtime_env(),
    )
}

fn same_registry<T>(parent: &HashMap<String, Arc<T>>, child: &HashMap<String, Arc<T>>) {
    assert!(
        !parent.is_empty(),
        "registry fixture must exercise preservation"
    );
    assert_eq!(parent.len(), child.len());
    for (name, function) in parent {
        assert!(
            child
                .get(name)
                .is_some_and(|value| Arc::ptr_eq(function, value))
        );
    }
}

#[tokio::test]
async fn preserves_all_registries_runtime_configuration_and_ids() -> TestResult {
    let parent = parent();
    let child = ScopedTaskContext::new(&parent, Handle::current());
    let ctx = child.context.as_ref();
    assert_eq!(ctx.task_id(), parent.task_id());
    assert_eq!(ctx.session_id(), parent.session_id());
    assert_eq!(
        ctx.session_config().options().execution.target_partitions,
        7
    );
    assert!(Arc::ptr_eq(&parent.runtime_env(), &ctx.runtime_env()));
    assert!(Arc::ptr_eq(parent.memory_pool(), ctx.memory_pool()));
    assert!(Arc::ptr_eq(
        &parent.extension::<Sentinel>()?,
        &ctx.extension::<Sentinel>()?
    ));
    same_registry(parent.scalar_functions(), ctx.scalar_functions());
    same_registry(
        parent.higher_order_functions(),
        ctx.higher_order_functions(),
    );
    same_registry(parent.aggregate_functions(), ctx.aggregate_functions());
    same_registry(parent.window_functions(), ctx.window_functions());
    assert!(Arc::ptr_eq(
        &ctx.extension::<TaskResourceScope>()?,
        &child.resources
    ));
    assert!(!Arc::ptr_eq(
        &parent.extension::<TaskResourceScope>()?,
        &child.resources
    ));
    assert!(child.resources.drain().await?.is_clean());
    Ok(())
}

#[tokio::test]
async fn cancellation_isolated_from_sibling_and_parent_contexts() -> TestResult {
    let parent = parent();
    let a = ScopedTaskContext::new(&parent, Handle::current());
    let b = ScopedTaskContext::new(&parent, Handle::current());
    a.resources.cancel()?;
    assert!(
        a.context
            .extension::<TaskResourceScope>()?
            .spawn(|_| async { Ok(()) })
            .is_err()
    );
    b.context
        .extension::<TaskResourceScope>()?
        .spawn(|canceled| async move {
            assert!(!*canceled.borrow());
            Ok(())
        })?;
    let parent_scope = parent.extension::<TaskResourceScope>()?;
    parent_scope.spawn(|canceled| async move {
        assert!(!*canceled.borrow());
        Ok(())
    })?;
    assert_eq!(a.resources.drain().await?.registered, 0);
    assert_eq!(b.resources.drain().await?.registered, 1);
    assert_eq!(parent_scope.drain().await?.registered, 1);
    Ok(())
}
#[path = "task_resources_registry_context_tests.rs"]
mod registry;

#[tokio::test]
async fn externally_admitted_owner_is_reused_without_affecting_parent() -> TestResult {
    let parent = parent();
    let original = parent.extension::<TaskResourceScope>()?;
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    let child = ScopedTaskContext::from_resources(&parent, resources.clone());
    assert!(Arc::ptr_eq(&resources, &child.resources));
    assert!(Arc::ptr_eq(
        &resources,
        &child.context.extension::<TaskResourceScope>()?
    ));
    resources.cancel()?;
    assert!(child.resources.spawn(|_| async { Ok(()) }).is_err());
    original.spawn(|canceled| async move {
        assert!(!*canceled.borrow());
        Ok(())
    })?;
    assert_eq!(original.drain().await?.registered, 1);
    assert_eq!(resources.drain().await?.registered, 0);
    Ok(())
}
