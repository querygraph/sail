//! Keep the storage effects of a local checkpoint job in the job's resource scope.
//!
//! A checkpoint is written below a prefix that session cleanup deletes. A provider operation
//! that is still running when its caller goes away could publish an object after that deletion.
//! Routing the store through [`OwnedObjectStore`] makes such an operation a registered resource,
//! so stopping the job waits for it.

use std::collections::HashSet;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::object_store::{ObjectStoreRegistry, ObjectStoreUrl};
use datafusion::physical_plan::ExecutionPlan;
use object_store::ObjectStore;
use sail_common_datafusion::task_resources::TaskResourceScope;
use sail_physical_plan::remote_checkpoint::RemoteCheckpointWriteExec;
use url::Url;

use crate::owned_store::OwnedObjectStore;

/// The bound on parts in flight for one upload. A part over the bound fails rather than waits,
/// and a writer can submit many parts at once, so this must not limit an upload that a provider
/// would accept. It is the part count limit of S3. It is not a memory budget.
const MAX_IN_FLIGHT_PARTS: NonZeroUsize = NonZeroUsize::new(10_000).unwrap();

/// The stores that the checkpoint writers in a plan write to.
pub(super) fn checkpoint_store_urls(plan: &Arc<dyn ExecutionPlan>) -> Vec<ObjectStoreUrl> {
    let mut urls = vec![];
    let mut plans = vec![plan];
    while let Some(plan) = plans.pop() {
        if let Some(writer) = plan.downcast_ref::<RemoteCheckpointWriteExec>() {
            urls.push(writer.object_store_url().clone());
        }
        plans.extend(plan.children());
    }
    urls
}

fn store_key(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort]
    )
}

/// An object store registry that hands out owned facades for the given stores.
/// Everything else, including reads and registration, is left to the parent.
pub(super) struct OwnedCheckpointStoreRegistry {
    parent: Arc<dyn ObjectStoreRegistry>,
    resources: Arc<TaskResourceScope>,
    stores: HashSet<String>,
}

impl OwnedCheckpointStoreRegistry {
    pub(super) fn new(
        parent: Arc<dyn ObjectStoreRegistry>,
        resources: Arc<TaskResourceScope>,
        urls: &[ObjectStoreUrl],
    ) -> Self {
        let stores = urls
            .iter()
            .map(|url| {
                let url: &Url = url.as_ref();
                store_key(url)
            })
            .collect();
        Self {
            parent,
            resources,
            stores,
        }
    }
}

impl fmt::Debug for OwnedCheckpointStoreRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedCheckpointStoreRegistry")
            .field("stores", &self.stores)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreRegistry for OwnedCheckpointStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.parent.register_store(url, store)
    }

    fn deregister_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        self.parent.deregister_store(url)
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let store = self.parent.get_store(url)?;
        if !self.stores.contains(&store_key(url)) {
            return Ok(store);
        }
        Ok(Arc::new(OwnedObjectStore::new(
            store,
            self.resources.clone(),
            MAX_IN_FLIGHT_PARTS,
        )))
    }
}
