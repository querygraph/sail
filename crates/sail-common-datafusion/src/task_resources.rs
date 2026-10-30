//! Ownership of asynchronous resource supervisors for one execution scope.
//!
//! Supervisors return success only after resource completion or cleanup is
//! confirmed. The execution owner must retain the scope and await `drain` before
//! reporting cleanup-qualified termination. Drop requests cancellation but cannot
//! await cleanup. This primitive does not supply upload or memory guarantees.

use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::{Arc, Mutex};

use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use futures::FutureExt;
use futures::future::{BoxFuture, Shared, join_all};
use tokio::runtime::Handle;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::extension::SessionExtension;

/// Identity within a single scope, not a global task or object identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceId(pub u64);

#[derive(Debug)]
pub struct ResourceFailure {
    pub id: ResourceId,
    pub error: DataFusionError,
}

#[derive(Debug)]
pub struct ResourceDrainReport {
    pub registered: usize,
    pub failures: Vec<ResourceFailure>,
}

impl ResourceDrainReport {
    pub fn is_clean(&self) -> bool {
        self.failures.is_empty()
    }

    /// Retain each original typed cause while adding its resource identity.
    /// The returned errors keep this shared report alive without cloning or
    /// flattening the non-Clone supervisor errors.
    pub fn errors(self: &Arc<Self>) -> Vec<DataFusionError> {
        self.failures
            .iter()
            .enumerate()
            .map(|(index, failure)| {
                DataFusionError::Context(
                    format!("resource {} completion or cleanup", failure.id.0),
                    Box::new(DataFusionError::External(Box::new(ResourceError {
                        report: self.clone(),
                        index,
                    }))),
                )
            })
            .collect()
    }
}

#[derive(Debug)]
struct ResourceError {
    report: Arc<ResourceDrainReport>,
    index: usize,
}

impl std::fmt::Display for ResourceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.report.failures[self.index].error, formatter)
    }
}

impl std::error::Error for ResourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.report.failures[self.index].error)
    }
}

type Drain = Shared<BoxFuture<'static, Arc<ResourceDrainReport>>>;

struct Resource {
    id: ResourceId,
    handle: JoinHandle<Result<()>>,
}

enum State {
    Open {
        next_id: u64,
        resources: Vec<Resource>,
    },
    Closing(Drain),
}

pub struct TaskResourceScope {
    runtime: Handle,
    cancellation: watch::Sender<bool>,
    state: Mutex<State>,
}

impl Debug for TaskResourceScope {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskResourceScope")
            .finish_non_exhaustive()
    }
}

impl SessionExtension for TaskResourceScope {
    fn name() -> &'static str {
        "task resource scope"
    }
}

impl TaskResourceScope {
    /// Use an execution-owned runtime that remains alive through drainage.
    pub fn new(runtime: Handle) -> Self {
        Self {
            runtime,
            cancellation: watch::channel(false).0,
            state: Mutex::new(State::Open {
                next_id: 0,
                resources: vec![],
            }),
        }
    }

    /// Register ownership before the factory is invoked or any resource starts.
    /// The factory runs inside the owned task; a sealed scope never invokes it.
    pub fn spawn<F, Fut>(&self, factory: F) -> Result<ResourceId>
    where
        F: FnOnce(watch::Receiver<bool>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let mut state = self
            .state
            .lock()
            .map_err(|_| internal_datafusion_err!("task resource scope lock poisoned"))?;
        let State::Open { next_id, resources } = &mut *state else {
            return Err(DataFusionError::Execution(
                "task resource scope is sealed".into(),
            ));
        };
        let id = ResourceId(*next_id);
        let following = next_id
            .checked_add(1)
            .ok_or_else(|| internal_datafusion_err!("task resource identity exhausted"))?;
        resources
            .try_reserve(1)
            .map_err(|error| DataFusionError::ResourcesExhausted(error.to_string()))?;
        let canceled = self.cancellation.subscribe();
        let handle = self.runtime.spawn(async move { factory(canceled).await });
        resources.push(Resource { id, handle });
        *next_id = following;
        Ok(id)
    }

    /// Synchronously seal admission and latch cooperative cancellation.
    /// Later calls cannot reopen admission or undo cancellation.
    pub fn cancel(&self) -> Result<()> {
        self.begin_drain(true).map(|_| ())
    }

    /// Synchronously seal admission without canceling successful supervisors.
    /// The owner must still await `drain` to qualify their completion.
    pub fn seal(&self) -> Result<()> {
        self.begin_drain(false).map(|_| ())
    }

    /// Seal admission and join every supervisor. Multiple callers share one
    /// report. Dropping a pending caller does not drop the owned join handles:
    /// the shared drain remains stored in the scope until another caller resumes.
    pub async fn drain(&self) -> Result<Arc<ResourceDrainReport>> {
        Ok(self.begin_drain(false)?.await)
    }

    fn begin_drain(&self, cancel: bool) -> Result<Drain> {
        let drain = self.sealed_drain()?;
        // Waking a listener can run arbitrary code, including reentering this
        // scope. Admission is sealed before notification, outside our mutex.
        if cancel {
            self.cancellation.send_replace(true);
        }
        Ok(drain)
    }

    fn sealed_drain(&self) -> Result<Drain> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| internal_datafusion_err!("task resource scope lock poisoned"))?;
        let resources = match &mut *state {
            State::Closing(drain) => return Ok(drain.clone()),
            State::Open { resources, .. } => std::mem::take(resources),
        };
        let drain = async move {
            let registered = resources.len();
            let results = join_all(resources.into_iter().map(|resource| async move {
                let result = resource
                    .handle
                    .await
                    .unwrap_or_else(|error| Err(DataFusionError::External(Box::new(error))));
                (resource.id, result)
            }))
            .await;
            let failures = results
                .into_iter()
                .filter_map(|(id, result)| result.err().map(|error| ResourceFailure { id, error }))
                .collect();
            Arc::new(ResourceDrainReport {
                registered,
                failures,
            })
        }
        .boxed()
        .shared();
        *state = State::Closing(drain.clone());
        Ok(drain)
    }
}

impl Drop for TaskResourceScope {
    fn drop(&mut self) {
        // Best effort only. Runtime/actor owners must explicitly await drain.
        self.cancellation.send_replace(true);
    }
}

#[cfg(test)]
#[path = "task_resources_tests.rs"]
mod tests;

#[path = "task_resources_context.rs"]
mod context;
pub use context::ScopedTaskContext;
