use std::sync::Arc;

use datafusion_common::Result;

use crate::extension::SessionExtension;

/// The operations of a session that a protocol layer owns outside of the job runner.
///
/// A session can have work that is not a job: requests being planned, tasks that deliver
/// results, and long-running drivers that start jobs one after another. Closing a session
/// must stop these as well, so this lets the session manager do it without knowing the protocol.
#[tonic::async_trait]
pub trait SessionOperations: Send + Sync {
    /// Refuse new operations and request that the admitted ones stop.
    /// This must not wait for them. Later calls have no effect.
    fn begin_close(&self) -> Result<()>;

    /// Wait until every admitted operation has finished.
    ///
    /// An operation can be waiting for a job, so the caller must stop the job runner
    /// concurrently with this, rather than before or after it.
    async fn wait_closed(&self) -> Result<()>;
}

/// The session extension that provides [`SessionOperations`].
pub struct SessionOperationsService {
    operations: Arc<dyn SessionOperations>,
}

impl SessionOperationsService {
    pub fn new(operations: Arc<dyn SessionOperations>) -> Self {
        Self { operations }
    }

    pub fn operations(&self) -> &dyn SessionOperations {
        self.operations.as_ref()
    }
}

impl SessionExtension for SessionOperationsService {
    fn name() -> &'static str {
        "SessionOperationsService"
    }
}
