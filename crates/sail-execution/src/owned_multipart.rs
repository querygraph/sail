//! Register a multipart lifetime with the existing execution resource scope.
//!
//! This is an ownership adapter, not checkpoint routing or a session barrier.
//! The provider factory and its futures run on the scope's retained runtime;
//! callers must qualify provider-internal work and runtime affinity separately.

use std::future::Future;
use std::num::NonZeroUsize;

use datafusion::common::{DataFusionError, Result};
use object_store::MultipartUpload;
use sail_common_datafusion::error::CommitStateUnknownError;
use sail_common_datafusion::task_resources::TaskResourceScope;
use sail_object_store::owned_multipart::{self, MultipartError, MultipartHandle, Stage};

/// Admit the entire supervisor before provider creation. Losing this future or
/// its returned handle cannot release the raw upload before settlement. A sealed
/// scope refuses the registration without invoking the factory. Never use
/// TaskResourceScope::run to return an asynchronously disposable raw upload.
pub async fn create<F, Fut>(
    resources: &TaskResourceScope,
    factory: F,
    capacity: NonZeroUsize,
) -> Result<MultipartHandle>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = object_store::Result<Box<dyn MultipartUpload>>> + Send + 'static,
{
    let (supervisor, creation) = owned_multipart::prepare(factory, capacity);
    resources.spawn_mutation(move |canceled| async move {
        supervisor.run(canceled).await.map_err(execution_error)
    })?;
    creation.wait().await.map_err(execution_error)
}

/// Preserve provider causes and classify failed completion conservatively.
/// Consumers mapping command/result errors into DataFusion must use this too.
/// The scope retains the same classification even if every observer disappears.
/// Interrupted tasks and provider-internal detached work still require separate
/// runtime-loss qualification; this function does not establish their outcome.
pub fn execution_error(error: MultipartError) -> DataFusionError {
    if error
        .causes()
        .iter()
        .any(|cause| cause.stage == Stage::Complete)
    {
        DataFusionError::Collection(vec![
            DataFusionError::External(Box::new(CommitStateUnknownError(error.to_string()))),
            DataFusionError::External(Box::new(error)),
        ])
    } else {
        DataFusionError::External(Box::new(error))
    }
}

#[cfg(test)]
#[path = "owned_multipart_tests.rs"]
mod tests;
