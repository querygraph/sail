//! Retained ownership of one completed object-store PUT, before caller loss.
//!
//! This component does not qualify multipart cleanup, provider error visibility,
//! retries, or a session barrier. Its execution owner must retain/drain the scope.

use std::sync::Arc;

use datafusion_common::{DataFusionError, Result};
use object_store::path::Path;
use object_store::{ObjectStore, PutOptions, PutPayload, PutResult};

use crate::task_resources::TaskResourceScope;

/// Invoke the provider only after scope admission, then retain its actual future
/// until completion even if the caller is canceled. Scope cancellation must not
/// abandon an already-admitted PUT: its completion is joined before clean drain.
/// Original provider errors are preserved; their commit/replay classification
/// remains the responsibility of the enclosing effect protocol.
pub async fn put(
    resources: &TaskResourceScope,
    store: Arc<dyn ObjectStore>,
    location: Path,
    payload: PutPayload,
    options: PutOptions,
) -> Result<PutResult> {
    resources
        .run_mutation(move |_| async move {
            store
                .put_opts(&location, payload, options)
                .await
                .map_err(|error| DataFusionError::ObjectStore(Box::new(error)))
        })
        .await
}

#[cfg(test)]
#[path = "owned_put_tests.rs"]
mod tests;
