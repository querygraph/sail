//! Characterize the current planning/execution boundary before adding transfer.

use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

use datafusion::catalog::Session;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry, ObjectStoreUrl,
};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use sail_common_datafusion::session::scoped::ScopedSession;

use super::*;

#[derive(Debug)]
struct SnapshotPlanner {
    store: Arc<dyn ObjectStore>,
    resources: Mutex<Option<Arc<TaskResourceScope>>>,
    physical: Mutex<Option<Arc<dyn ExecutionPlan>>>,
    calls: AtomicUsize,
}

#[tonic::async_trait]
impl QueryPlanner for SnapshotPlanner {
    async fn create_physical_plan(
        &self,
        logical: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(logical.schema().fields().is_empty());
        assert_eq!(session.config().target_partitions(), 7);
        let expected = self
            .resources
            .lock()
            .map_err(|_| internal_datafusion_err!("snapshot owner lock poisoned"))?
            .clone()
            .ok_or_else(|| internal_datafusion_err!("snapshot owner not installed"))?;
        assert!(Arc::ptr_eq(
            &session.extension::<TaskResourceScope>()?,
            &expected
        ));
        let store = session
            .runtime_env()
            .object_store(ObjectStoreUrl::parse("memory://capture")?)?;
        assert!(Arc::ptr_eq(&store, &self.store));
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.physical
            .lock()
            .map_err(|_| internal_datafusion_err!("physical plan lock poisoned"))?
            .take()
            .ok_or_else(|| internal_datafusion_err!("physical plan not installed"))
    }
}

#[tokio::test]
async fn legacy_bound_execution_recaptures_authority_after_scoped_planning() -> TestResult {
    let parent: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let child: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let planner = Arc::new(SnapshotPlanner {
        store: child.clone(),
        resources: Mutex::new(None),
        physical: Mutex::new(None),
        calls: AtomicUsize::new(0),
    });
    let context = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(7))
            .with_query_planner(planner.clone())
            .build(),
    );
    let url = ObjectStoreUrl::parse("memory://capture")?;
    context
        .runtime_env()
        .register_object_store(url.as_ref(), parent.clone());
    let runner = LocalJobRunner::default();
    let preparation = (&runner as &dyn JobRunner).begin_preparation(&context)?;
    let resources = preparation.resources().clone();
    *planner
        .resources
        .lock()
        .map_err(|_| "owner lock poisoned")? = Some(resources.clone());
    let registry = Arc::new(DefaultObjectStoreRegistry::new());
    registry.register_store(url.as_ref(), child);
    let snapshot = ScopedSession::new(context.state(), resources.clone())
        .with_object_store_registry(registry)?;
    context
        .state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = 9;

    let expected = resources.clone();
    let executed = Arc::new(AtomicBool::new(false));
    let observed = executed.clone();
    *planner.physical.lock().map_err(|_| "plan lock poisoned")? = Some(plan(move |ctx| {
        assert!(Arc::ptr_eq(
            &ctx.extension::<TaskResourceScope>()?,
            &expected
        ));
        // Legacy execute intentionally captures authority at this late phase;
        // it has no argument through which the selected snapshot could transfer.
        assert_eq!(ctx.session_config().target_partitions(), 9);
        let store = ctx
            .runtime_env()
            .object_store(ObjectStoreUrl::parse("memory://capture")?)?;
        assert!(Arc::ptr_eq(&store, &parent));
        observed.store(true, Ordering::SeqCst);
        Ok(stream(|_| Poll::Ready(None)).0)
    }));
    let logical = LogicalPlanBuilder::empty(false).build()?;
    let physical = snapshot.create_physical_plan(&logical).await?;
    assert_eq!(planner.calls.load(Ordering::SeqCst), 1);
    assert!(!executed.load(Ordering::SeqCst));
    assert!(
        preparation
            .execute(physical)
            .await?
            .collect::<Vec<_>>()
            .await
            .is_empty()
    );
    assert!(executed.load(Ordering::SeqCst));
    assert_eq!(resources.drain().await?.registered, 0);
    runner.stop().await?;
    assert!(runner.lock().active.is_empty());
    assert!(context.extension::<TaskResourceScope>().is_err());
    Ok(())
}

#[tokio::test]
async fn captured_bound_execution_preserves_planning_snapshot() -> TestResult {
    let parent: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let child: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let planner = Arc::new(SnapshotPlanner {
        store: child.clone(),
        resources: Mutex::new(None),
        physical: Mutex::new(None),
        calls: AtomicUsize::new(0),
    });
    let context = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(7))
            .with_query_planner(planner.clone())
            .build(),
    );
    let url = ObjectStoreUrl::parse("memory://capture")?;
    context
        .runtime_env()
        .register_object_store(url.as_ref(), parent.clone());
    let runner = LocalJobRunner::default();
    let preparation = (&runner as &dyn JobRunner).begin_preparation(&context)?;
    let resources = preparation.resources().clone();
    *planner
        .resources
        .lock()
        .map_err(|_| "owner lock poisoned")? = Some(resources.clone());
    let registry = Arc::new(DefaultObjectStoreRegistry::new());
    registry.register_store(url.as_ref(), child.clone());
    let preparation = preparation.capture(Some(registry))?;
    context
        .state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = 9;

    let expected = resources.clone();
    let executed = Arc::new(AtomicBool::new(false));
    let observed = executed.clone();
    *planner.physical.lock().map_err(|_| "plan lock poisoned")? = Some(plan(move |ctx| {
        assert!(Arc::ptr_eq(
            &ctx.extension::<TaskResourceScope>()?,
            &expected
        ));
        assert_eq!(ctx.session_config().target_partitions(), 7);
        let store = ctx
            .runtime_env()
            .object_store(ObjectStoreUrl::parse("memory://capture")?)?;
        assert!(Arc::ptr_eq(&store, &child));
        observed.store(true, Ordering::SeqCst);
        Ok(stream(|_| Poll::Ready(None)).0)
    }));
    let logical = LogicalPlanBuilder::empty(false).build()?;
    let physical = preparation.session().create_physical_plan(&logical).await?;
    assert_eq!(planner.calls.load(Ordering::SeqCst), 1);
    assert!(!executed.load(Ordering::SeqCst));
    assert!(
        preparation
            .execute(physical)
            .await?
            .collect::<Vec<_>>()
            .await
            .is_empty()
    );
    assert!(executed.load(Ordering::SeqCst));
    assert_eq!(resources.drain().await?.registered, 0);
    runner.stop().await?;
    assert!(runner.lock().active.is_empty());
    assert!(context.extension::<TaskResourceScope>().is_err());
    Ok(())
}
