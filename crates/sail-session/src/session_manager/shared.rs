use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::common::{DataFusionError, internal_datafusion_err};
use tokio::sync::watch;

use crate::session_manager::actor::{CleanupOutcome, SessionCleanup};

/// The state that outlives the session manager actor, shared by every session manager handle.
pub struct ManagerShared {
    /// Every cleanup that has been started. The resources of a successful cleanup have been
    /// released. A failed cleanup stays reachable here after the actor has stopped.
    cleanups: Mutex<HashMap<String, Arc<SessionCleanup>>>,
    /// The final result of the shutdown. This is not in the mailbox, so that every caller can
    /// observe the same result, including after the mailbox has closed.
    shutdown: watch::Sender<Option<CleanupOutcome>>,
}

impl ManagerShared {
    pub(crate) fn new() -> Self {
        Self {
            cleanups: Mutex::new(HashMap::new()),
            shutdown: watch::channel(None).0,
        }
    }

    pub(crate) fn is_shut_down(&self) -> bool {
        self.shutdown.borrow().is_some()
    }

    /// Publish the final result. Only the first result counts.
    pub(crate) fn complete_shutdown(&self, outcome: CleanupOutcome) {
        self.shutdown.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(outcome);
            true
        });
    }

    /// Report that the actor went away without completing a shutdown.
    pub(crate) fn interrupt_shutdown(&self) {
        self.complete_shutdown(Err(Arc::new(interrupted())));
    }

    /// Wait for the final result. Dropping this future has no effect on the shutdown.
    pub(crate) async fn wait_for_shutdown(&self) -> CleanupOutcome {
        let mut shutdown = self.shutdown.subscribe();
        match shutdown.wait_for(Option::is_some).await {
            Ok(outcome) => outcome
                .clone()
                .unwrap_or_else(|| Err(Arc::new(interrupted()))),
            // The sender lives in `self`, so this cannot be reached.
            Err(_) => Err(Arc::new(interrupted())),
        }
    }

    fn lock_cleanups(&self) -> MutexGuard<'_, HashMap<String, Arc<SessionCleanup>>> {
        // No foreign code runs under this mutex.
        self.cleanups
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn retain_cleanup(&self, session_id: &str, cleanup: Arc<SessionCleanup>) {
        self.lock_cleanups().insert(session_id.to_string(), cleanup);
    }

    pub(crate) fn cleanup(&self, session_id: &str) -> Option<Arc<SessionCleanup>> {
        self.lock_cleanups().get(session_id).cloned()
    }
}

fn interrupted() -> DataFusionError {
    internal_datafusion_err!("session manager shutdown was interrupted; its outcome is unknown")
}
