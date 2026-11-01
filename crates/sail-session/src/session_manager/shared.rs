use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::session_manager::actor::SessionCleanup;

/// The state that outlives the session manager actor, shared by every session manager handle.
#[derive(Default)]
pub struct ManagerShared {
    /// Every cleanup that has been started. The resources of a successful cleanup have been
    /// released. A failed cleanup stays reachable here after the actor has stopped.
    cleanups: Mutex<HashMap<String, Arc<SessionCleanup>>>,
}

impl ManagerShared {
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

    pub(crate) fn cleanups(&self) -> Vec<Arc<SessionCleanup>> {
        self.lock_cleanups().values().cloned().collect()
    }
}
