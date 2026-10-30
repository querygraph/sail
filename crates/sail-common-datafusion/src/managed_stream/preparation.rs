use super::*;

/// Owns resource cleanup while a caller synchronously prepares a stream.
///
/// Publish the control before invoking the factory. Cancellation seals resource
/// admission immediately, but completion waits until this guard relinquishes
/// preparation. The caller runs its factory and handles panics outside our lock.
pub struct StreamPreparation {
    control: ManagedStreamControl,
    finished: bool,
}

impl StreamPreparation {
    pub fn new(resources: Arc<TaskResourceScope>, runtime: Handle) -> (Self, ManagedStreamControl) {
        let control = ManagedStreamControl(Arc::new(Inner {
            state: Mutex::new(State {
                phase: Phase::Preparing,
                canceled: false,
                consumer: None,
            }),
            resources,
            runtime,
            completion: watch::channel(None).0,
        }));
        (
            Self {
                control: control.clone(),
                finished: false,
            },
            control,
        )
    }

    /// Transfer the prepared stream without calling any upstream methods.
    /// If cancellation won the handoff, disposal begins without polling it.
    pub fn complete(
        mut self,
        schema: SchemaRef,
        stream: SendableRecordBatchStream,
    ) -> ManagedRecordBatchStream {
        let wrapper = ManagedRecordBatchStream::from_control(schema, self.control.clone());
        let canceled_stream = {
            let mut state = self.control.0.lock();
            if state.canceled {
                state.phase = Phase::Draining;
                Some(stream)
            } else {
                state.phase = Phase::Ready(stream);
                None
            }
        };
        self.finished = true;
        if let Some(stream) = canceled_stream {
            // drain constructs its cleanup guard before invoking cancellation
            // listeners or spawning a task on the caller's runtime.
            let _ = self.control.0.drain(Some(stream), None);
        }
        wrapper
    }

    /// Finish preparation without producing a stream. This seals admission
    /// without requesting cancellation, unless a control already canceled it.
    /// The caller retains its own typed result; the returned control reports
    /// resource cleanup separately. No artificial stream or schema is needed.
    pub fn finish(self) -> ManagedStreamControl {
        self.finish_without_stream(false)
    }

    /// Cancel preparation without taking ownership of the caller's typed error.
    /// Cleanup remains independently owned even if its observer disappears.
    /// The control's cancellation outcome is distinct from that caller result.
    pub fn cancel(self) -> ManagedStreamControl {
        self.finish_without_stream(true)
    }

    fn finish_without_stream(mut self, cancel: bool) -> ManagedStreamControl {
        {
            let mut state = self.control.0.lock();
            state.canceled |= cancel;
            state.phase = Phase::Draining;
        }
        self.finished = true;
        // drain installs its cleanup guard before cancellation notifications
        // or spawning. Admission/cleanup failures remain in the shared report.
        let _ = self.control.0.drain(None, None);
        self.control.clone()
    }

    /// Preserve the preparation error while disposing registered resources.
    pub fn fail(mut self, error: DataFusionError) {
        self.finish_error(error);
    }

    fn finish_error(&mut self, error: DataFusionError) {
        self.control.0.lock().phase = Phase::Draining;
        self.finished = true;
        let _ = self.control.0.drain(None, Some(error));
    }
}

impl Drop for StreamPreparation {
    fn drop(&mut self) {
        if !self.finished {
            self.finish_error(DataFusionError::Internal(
                "managed stream preparation abandoned".into(),
            ));
        }
    }
}

#[cfg(test)]
#[path = "preparation_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "preparation_finish_tests.rs"]
mod finish_tests;
