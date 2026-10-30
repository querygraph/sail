use datafusion::common::DataFusionError;
use datafusion::execution::object_store::ObjectStoreRegistry;
use sail_common_datafusion::session::job::CapturedJobPreparation;

use super::*;

/// The minting runner and its admission remain bound to one query snapshot.
pub(super) struct CapturedLocalJobPreparation<'a> {
    preparation: LocalJobPreparation<'a>,
    session: ScopedSession,
}

impl<'a> LocalJobPreparation<'a> {
    pub(super) fn capture_snapshot(
        self,
        registry: Option<Arc<dyn ObjectStoreRegistry>>,
    ) -> Result<CapturedLocalJobPreparation<'a>> {
        let captured = catch_unwind(AssertUnwindSafe(|| {
            if self.resources.cancelled().now_or_never().is_some() {
                return Err(DataFusionError::Execution(
                    "managed stream preparation canceled before capture".into(),
                ));
            }
            let session = ScopedSession::new(self.context.state(), self.resources.clone());
            match registry {
                Some(registry) => session.with_object_store_registry(registry),
                None => Ok(session),
            }
        }))
        .unwrap_or_else(|payload| Err(panic_error("capture", payload)));
        match captured {
            Ok(session) => Ok(CapturedLocalJobPreparation {
                preparation: self,
                session,
            }),
            Err(error) => {
                // Capture is synchronous. The caller saves control before this
                // transition and can await independently owned cleanup. Keep
                // the same typed cause reachable from both observations.
                let error = Arc::new(error);
                self.preparation
                    .fail(DataFusionError::Shared(error.clone()));
                Err(DataFusionError::Shared(error))
            }
        }
    }
}

impl CapturedJobPreparation for CapturedLocalJobPreparation<'_> {
    fn session(&self) -> &ScopedSession {
        &self.session
    }
}

#[tonic::async_trait]
impl JobPreparation for CapturedLocalJobPreparation<'_> {
    fn resources(&self) -> &Arc<TaskResourceScope> {
        self.preparation.resources()
    }

    fn control(&self) -> ManagedStreamControl {
        self.preparation.control()
    }

    fn finish(self: Box<Self>) -> ManagedStreamControl {
        self.preparation.finish()
    }

    fn cancel(self: Box<Self>) -> ManagedStreamControl {
        self.preparation.cancel()
    }

    async fn execute(
        self: Box<Self>,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        // The owned task context copies the captured config/extensions,
        // functions and runtime. Streams and registered tasks own the further
        // dependencies they use; no execution borrows this SessionState.
        self.preparation
            .execute_with_snapshot(plan, Some(self.session))
            .await
    }
}
