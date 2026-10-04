//! Optional C2 observations of existing calls, exported through fastrace.
//! Durations are local monotonic intervals and may overlap. They are not
//! an additive execution breakdown or proof that resources were reclaimed.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Instant;

use fastrace::Span;
use fastrace::collector::SpanContext;
use serde::Serialize;

const ID_BYTES: usize = 256;
const PHASE_COUNT: usize = 43;
static STATE: State = State::new();

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(usize)]
pub enum Phase {
    OperationBinding,
    JobAccepted,
    TaskAssigned,
    Resolve,
    LogicalExecute,
    Optimize,
    PhysicalPlan,
    JobGraph,
    JobTopology,
    Schedule,
    Definition,
    PhysicalEncode,
    PreparationQueue,
    Preparation,
    DefinitionCacheHit,
    BatchDispatch,
    TaskDispatched,
    TaskDefinitionEncode,
    TaskDefinitionDecode,
    BatchPhysicalDecode,
    PhysicalDecode,
    BatchAdmission,
    TaskAdmitted,
    TaskTerminal,
    ReaderDeclaration,
    WriterDeclaration,
    ReaderOpen,
    WriterOpen,
    StreamConsumption,
    StreamOwner,
    FirstPoll,
    FirstBatch,
    SinkCommit,
    SinkAbort,
    OutputForward,
    CleanupDispatch,
    CleanupQueuedAck,
    CloseJobExecuted,
    LocalStreamsRemoved,
    ReleaseResponseBuffer,
    ReleaseOperation,
    ExecutorOwner,
    ExecutorBufferOwner,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::OperationBinding => "c2.operation.binding",
            Self::JobAccepted => "c2.job.accepted",
            Self::TaskAssigned => "c2.task.assigned",
            Self::Resolve => "c2.plan.resolve",
            Self::LogicalExecute => "c2.plan.logical_execute",
            Self::Optimize => "c2.plan.optimize",
            Self::PhysicalPlan => "c2.plan.physical",
            Self::JobGraph => "c2.job.graph",
            Self::JobTopology => "c2.job.topology",
            Self::Schedule => "c2.scheduler.select_assign",
            Self::Definition => "c2.stage.definition",
            Self::PhysicalEncode => "c2.stage.physical_encode",
            Self::PreparationQueue => "c2.task.preparation_queue",
            Self::Preparation => "c2.task.preparation",
            Self::DefinitionCacheHit => "c2.stage.definition_cache_hit",
            Self::BatchDispatch => "c2.batch.dispatch",
            Self::TaskDispatched => "c2.task.dispatched",
            Self::TaskDefinitionEncode => "c2.batch.definition_encode",
            Self::TaskDefinitionDecode => "c2.batch.definition_decode",
            Self::BatchPhysicalDecode => "c2.batch.physical_decode",
            Self::PhysicalDecode => "c2.task.physical_decode",
            Self::BatchAdmission => "c2.batch.admission",
            Self::TaskAdmitted => "c2.task.admitted",
            Self::TaskTerminal => "c2.task.terminal",
            Self::ReaderDeclaration => "c2.reader.declaration",
            Self::WriterDeclaration => "c2.writer.declaration",
            Self::ReaderOpen => "c2.reader.open",
            Self::WriterOpen => "c2.writer.open",
            Self::StreamConsumption => "c2.stream.consumption",
            Self::StreamOwner => "c2.stream.owner",
            Self::FirstPoll => "c2.stream.first_poll",
            Self::FirstBatch => "c2.stream.first_batch",
            Self::SinkCommit => "c2.sink.commit",
            Self::SinkAbort => "c2.sink.abort",
            Self::OutputForward => "c2.output.forward",
            Self::CleanupDispatch => "c2.cleanup.dispatch",
            Self::CleanupQueuedAck => "c2.cleanup.queued_ack",
            Self::CloseJobExecuted => "c2.cleanup.close_job_executed",
            Self::LocalStreamsRemoved => "c2.cleanup.local_streams_removed",
            Self::ReleaseResponseBuffer => "c2.release.response_buffer",
            Self::ReleaseOperation => "c2.release.operation",
            Self::ExecutorOwner => "c2.executor.owner",
            Self::ExecutorBufferOwner => "c2.executor.buffer_owner",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Placement {
    Driver,
    Worker(u64),
}

/// Borrowed identity is evaluated only in enabled mode and is not retained.
/// Native trace IDs bind pre-job planning to the actual operation root.
pub enum Identity<'a> {
    CausalContext,
    Operation {
        session: &'a str,
        operation: &'a str,
    },
    Session(&'a str),
    CausalJob {
        job: u64,
    },
    CausalStage {
        job: u64,
        stage: u64,
    },
    PlacementJob {
        session: &'a str,
        job: u64,
        placement: Placement,
    },
    Job {
        session: &'a str,
        job: u64,
    },
    Stage {
        session: &'a str,
        job: u64,
        stage: usize,
    },
    Task {
        session: &'a str,
        job: u64,
        stage: usize,
        partition: usize,
        attempt: usize,
        placement: Placement,
    },
}

impl Identity<'_> {
    fn properties(self) -> (Vec<(&'static str, String)>, bool) {
        let mut values = Vec::with_capacity(10);
        let mut complete = true;
        let mut session = None;
        let mut job = None;
        match self {
            Self::CausalContext => values.push(("c2.identity_kind", "causal_context".into())),
            Self::Operation {
                session: id,
                operation,
            } => {
                values.push(("c2.identity_kind", "operation".into()));
                session = Some(id);
                let (value, intact) = bounded(operation);
                values.push(("c2.operation_id", value));
                complete &= intact;
            }
            Self::Session(id) => {
                values.push(("c2.identity_kind", "session".into()));
                session = Some(id);
            }
            Self::CausalJob { job: number } => {
                values.push(("c2.identity_kind", "causal_job".into()));
                job = Some(number);
            }
            Self::CausalStage { job: number, stage } => {
                values.push(("c2.identity_kind", "causal_stage".into()));
                values.push(("execution.stage", stage.to_string()));
                job = Some(number);
            }
            Self::PlacementJob {
                session: id,
                job: number,
                placement,
            } => {
                values.push(("c2.identity_kind", "placement_job".into()));
                match placement {
                    Placement::Driver => values.push(("c2.placement", "driver".into())),
                    Placement::Worker(worker) => {
                        values.push(("c2.placement", "worker".into()));
                        values.push(("cluster.worker.id", worker.to_string()));
                    }
                }
                session = Some(id);
                job = Some(number);
            }
            Self::Job {
                session: id,
                job: number,
            } => {
                values.push(("c2.identity_kind", "job".into()));
                session = Some(id);
                job = Some(number);
            }
            Self::Stage {
                session: id,
                job: number,
                stage,
            } => {
                values.push(("c2.identity_kind", "stage".into()));
                values.push(("execution.stage", stage.to_string()));
                session = Some(id);
                job = Some(number);
            }
            Self::Task {
                session: id,
                job: number,
                stage,
                partition,
                attempt,
                placement,
            } => {
                values.push(("c2.identity_kind", "task".into()));
                values.push(("execution.stage", stage.to_string()));
                values.push(("execution.partition", partition.to_string()));
                values.push(("execution.attempt", attempt.to_string()));
                match placement {
                    Placement::Driver => values.push(("c2.placement", "driver".into())),
                    Placement::Worker(worker) => {
                        values.push(("c2.placement", "worker".into()));
                        values.push(("cluster.worker.id", worker.to_string()));
                    }
                }
                session = Some(id);
                job = Some(number);
            }
        }
        if let Some(id) = session {
            let (value, intact) = bounded(id);
            values.push(("session.id", value));
            complete &= intact;
        }
        if let Some(number) = job {
            values.push(("execution.job.id", number.to_string()));
        }
        (values, complete)
    }
}

fn bounded(value: &str) -> (String, bool) {
    let mut end = value.len().min(ID_BYTES);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (
        value[..end].to_owned(),
        end == value.len() && !value.is_empty(),
    )
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Succeeded,
    Failed,
    Cancelled,
    Abandoned,
}

impl Outcome {
    fn name(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Abandoned => "abandoned",
        }
    }
}

struct State {
    enabled: AtomicBool,
    sequence: AtomicU64,
    ended: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    abandoned: AtomicU64,
    incomplete_identity: AtomicU64,
    saturated: AtomicBool,
    phases: [AtomicU64; PHASE_COUNT],
}

impl State {
    const fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            sequence: AtomicU64::new(0),
            ended: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            abandoned: AtomicU64::new(0),
            incomplete_identity: AtomicU64::new(0),
            saturated: AtomicBool::new(false),
            phases: [const { AtomicU64::new(0) }; PHASE_COUNT],
        }
    }

    fn increment(&self, counter: &AtomicU64) -> u64 {
        match counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1)) {
            Ok(n) => n + 1,
            Err(_) => {
                self.saturated.store(true, Ordering::Relaxed);
                u64::MAX
            }
        }
    }

    fn summary(&self) -> Summary {
        let created = self.sequence.load(Ordering::Relaxed);
        let ended = self.ended.load(Ordering::Relaxed);
        Summary {
            schema_version: 1,
            pid: std::process::id(),
            created,
            ended,
            failed: self.failed.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
            abandoned: self.abandoned.load(Ordering::Relaxed),
            incomplete_identity: self.incomplete_identity.load(Ordering::Relaxed),
            outstanding: created.saturating_sub(ended),
            counters_complete: !self.saturated.load(Ordering::Relaxed) && ended <= created,
            phase_created: self
                .phases
                .iter()
                .map(|phase| phase.load(Ordering::Relaxed))
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Summary {
    pub schema_version: u8,
    pub pid: u32,
    pub created: u64,
    pub ended: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub abandoned: u64,
    pub incomplete_identity: u64,
    pub outstanding: u64,
    pub counters_complete: bool,
    /// Array positions are the explicitly ordered Phase discriminants.
    pub phase_created: Vec<u64>,
}

pub fn enabled() -> bool {
    STATE.enabled.load(Ordering::Relaxed)
}

/// Unmetered RPC context for guards whose callers previously had no parent.
/// These names are outside the `c2.*` observation namespace and do not alter
/// its sequence or phase counters.
#[derive(Clone, Copy)]
pub enum Rpc {
    ReleaseExecute,
    RegisterWorker,
}

impl Rpc {
    fn name(self) -> &'static str {
        match self {
            Self::ReleaseExecute => "ReleaseExecute",
            Self::RegisterWorker => "RegisterWorker",
        }
    }
}

pub fn rpc_span(rpc: Rpc, properties: impl FnOnce() -> Vec<(&'static str, String)>) -> Span {
    rpc_span_in(&STATE, rpc, properties)
}

fn rpc_span_in(
    state: &State,
    rpc: Rpc,
    properties: impl FnOnce() -> Vec<(&'static str, String)>,
) -> Span {
    if !state.enabled.load(Ordering::Relaxed) {
        return Span::noop();
    }
    Span::root(rpc.name(), SpanContext::random()).with_properties(properties)
}

/// Called once during telemetry initialization, after its exporter is ready.
pub fn configure(enabled: bool) {
    STATE.enabled.store(enabled, Ordering::Relaxed);
}

/// A shutdown snapshot, not a delivery acknowledgement from the collector.
pub fn summary() -> Option<Summary> {
    enabled().then(|| STATE.summary())
}

struct Active<'a> {
    state: &'a State,
    span: Span,
    details: Mutex<Vec<(&'static str, String)>>,
    clock: Option<Instant>,
    sequence: u64,
}

/// Dropping a guard without a terminal result records Abandoned, never success.
pub struct Guard<'a> {
    active: Option<Active<'a>>,
}

impl Guard<'static> {
    pub fn point<'id>(phase: Phase, identity: impl FnOnce() -> Identity<'id>) -> Self {
        Self::make(&STATE, phase, identity, false, || {
            let parent = SpanContext::current_local_parent();
            let has_parent = parent.is_some();
            let span = match parent {
                Some(parent) => Span::root(phase.name(), parent),
                None => Span::root(phase.name(), SpanContext::random()),
            };
            (span, has_parent)
        })
    }

    pub fn start<'id>(phase: Phase, identity: impl FnOnce() -> Identity<'id>) -> Self {
        Self::make(&STATE, phase, identity, true, || {
            let parent = SpanContext::current_local_parent();
            let has_parent = parent.is_some();
            let span = match parent {
                Some(parent) => Span::root(phase.name(), parent),
                None => Span::root(phase.name(), SpanContext::random()),
            };
            (span, has_parent)
        })
    }
}

impl<'a> Guard<'a> {
    fn make<'id>(
        state: &'a State,
        phase: Phase,
        identity: impl FnOnce() -> Identity<'id>,
        timed: bool,
        span: impl FnOnce() -> (Span, bool),
    ) -> Self {
        if !state.enabled.load(Ordering::Relaxed) {
            return Self { active: None };
        }
        let clock = timed.then(Instant::now);
        let sequence = state.increment(&state.sequence);
        state.increment(&state.phases[phase as usize]);
        let (span, has_parent) = span();
        let (mut properties, complete) = identity().properties();
        let complete = complete && has_parent;
        // Keep identity on the owned raw span. Separate add_property commands
        // can arrive after this span is exported when it moves across threads.
        let span = span.with_properties(|| {
            properties.extend([
                ("c2.schema_version", "1".into()),
                ("c2.observation_id", sequence.to_string()),
                ("c2.pid", std::process::id().to_string()),
                ("c2.causal_parent_present", has_parent.to_string()),
                ("c2.identity_complete", complete.to_string()),
                ("c2.kind", if timed { "interval" } else { "event" }.into()),
            ]);
            properties
        });
        if !complete {
            state.increment(&state.incomplete_identity);
        }
        Self {
            active: Some(Active {
                state,
                span,
                details: Mutex::new(Vec::new()),
                clock,
                sequence,
            }),
        }
    }

    pub fn observation_id(&self) -> Option<u64> {
        self.active.as_ref().map(|active| active.sequence)
    }

    /// Enabled-only causal scope for an existing async call; no new data action.
    pub fn child_span(&self, name: &'static str) -> Option<Span> {
        self.active
            .as_ref()
            .map(|active| Span::enter_with_parent(name, &active.span))
    }

    pub fn detail(&self, key: &'static str, value: impl FnOnce() -> String) {
        if let Some(active) = &self.active
            && SpanContext::from_span(&active.span).is_some()
        {
            // Preserve noop callback laziness and run user code before locking.
            let value = value();
            active
                .details
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((key, value));
        }
    }

    pub fn finish_result<T, E>(self, result: &Result<T, E>) {
        self.finish(if result.is_ok() {
            Outcome::Succeeded
        } else {
            Outcome::Failed
        });
    }

    pub fn finish(mut self, outcome: Outcome) {
        self.end(outcome);
    }

    fn end(&mut self, outcome: Outcome) {
        if let Some(active) = self.active.take() {
            let elapsed = active.clock.map(|clock| {
                let elapsed = u64::try_from(clock.elapsed().as_nanos());
                if elapsed.is_err() {
                    active.state.saturated.store(true, Ordering::Relaxed);
                }
                elapsed.unwrap_or(u64::MAX)
            });
            let mut properties = active
                .details
                .into_inner()
                .unwrap_or_else(PoisonError::into_inner);
            let span = active.span.with_properties(|| {
                if let Some(elapsed) = elapsed {
                    properties.push(("c2.elapsed_ns", elapsed.to_string()));
                }
                properties.push(("c2.outcome", outcome.name().into()));
                properties
            });
            active.state.increment(&active.state.ended);
            match outcome {
                Outcome::Succeeded => {}
                Outcome::Failed => {
                    active.state.increment(&active.state.failed);
                }
                Outcome::Cancelled => {
                    active.state.increment(&active.state.cancelled);
                }
                Outcome::Abandoned => {
                    active.state.increment(&active.state.abandoned);
                }
            }
            drop(span);
        }
    }
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        self.end(Outcome::Abandoned);
    }
}

/// Lifetime of this named owner, not proof that its allocator returned memory.
/// The enclosing value's ordinary destruction remains unchanged.
pub struct NamedOwner<'a> {
    guard: Option<Guard<'a>>,
}

impl NamedOwner<'static> {
    pub fn optional<'id>(
        phase: Phase,
        identity: impl FnOnce() -> Identity<'id>,
        role: &'static str,
    ) -> Option<Self> {
        if !enabled() {
            return None;
        }
        let guard = Guard::start(phase, identity);
        guard.detail("c2.owner_role", || role.into());
        Some(Self { guard: Some(guard) })
    }
}

impl Drop for NamedOwner<'_> {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            guard.detail("c2.terminal_reason", || {
                "named_owner_drop_not_allocator_reclaim".into()
            });
            guard.finish(Outcome::Succeeded);
        }
    }
}

pub fn event<'id>(phase: Phase, identity: impl FnOnce() -> Identity<'id>) {
    Guard::point(phase, identity).finish(Outcome::Succeeded);
}

/// One sequenced binding under the existing operation root, without altering it.
pub fn bind_operation(parent: &Span, session: &str, operation: &str) {
    Guard::make(
        &STATE,
        Phase::OperationBinding,
        || Identity::Operation { session, operation },
        false,
        || {
            (
                Span::enter_with_parent(Phase::OperationBinding.name(), parent),
                SpanContext::from_span(parent).is_some(),
            )
        },
    )
    .finish(Outcome::Succeeded);
}

#[cfg(test)]
#[path = "c2_tests.rs"]
mod tests;
