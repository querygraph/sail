//! The durable records (design section "Identities and durable records").

use serde::{Deserialize, Serialize};

use crate::identity::{
    AuthorityIncarnation, EffectStep, FlowKey, IntentId, SinkIdentity, TargetKey,
};

/// Authority time in milliseconds; only the authority's clock decides lease
/// eligibility.
pub type AuthorityMillis = i64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TargetState {
    Enrolled,
    Running,
    Resetting,
}

/// The target guard row: every conflicting operation mutates it, so two
/// transactions on the same target serialise on it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetGuard {
    pub target: TargetKey,
    /// Unique across targets, so catalog aliases cannot enrol one table twice.
    pub physical_key: String,
    pub authority: AuthorityIncarnation,
    pub sink: SinkIdentity,
    pub auxiliary: SinkIdentity,
    /// Monotone per target; bumped by every mutation of this row.
    pub revision: u64,
    pub epoch: u64,
    pub generation: u64,
    pub owner: Option<String>,
    pub lease_deadline: Option<AuthorityMillis>,
    pub state: TargetState,
    /// The next checkpoint directory index, reserved by `BeginReset`; the
    /// filesystem is never the allocator.
    pub checkpoint_index: u64,
    /// The one unfinished intent or reset of the target, if any.
    pub active_intent: Option<IntentId>,
    /// The unfinished intent the last reset retired; retained with its
    /// receipts, never advanced and never acknowledged.
    pub superseded_intent: Option<IntentId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowCheckpoint {
    pub target: TargetKey,
    pub generation: u64,
    pub flow: FlowKey,
    pub definition_digest: String,
    pub next_batch_sequence: u64,
    /// Opaque, versioned progress payload and its digest.
    pub progress: serde_json::Value,
    pub progress_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntentKind {
    Batch,
    Initialization,
    Reset,
}

/// A sealed intent: the original request, never overwritten on takeover.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Intent {
    pub id: IntentId,
    pub kind: IntentKind,
    pub target: TargetKey,
    pub generation: u64,
    pub flow: FlowKey,
    pub batch_sequence: u64,
    /// Original range vector, frontier reference, config and schema digests.
    pub request: serde_json::Value,
    pub request_digest: String,
    /// The progress this batch starts from; completion rechecks it.
    pub start_progress_digest: String,
    /// The validated end vector. Completion sets progress to exactly this:
    /// it cannot accept a caller-supplied cursor.
    pub end_progress: serde_json::Value,
    pub end_progress_digest: String,
    /// Set once by the sealed-input unit; immutable afterwards.
    pub prepared_digest: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectPrepare {
    pub intent: IntentId,
    pub step: EffectStep,
    pub sink: SinkIdentity,
    /// The head the effect was prepared against; ratification rechecks it.
    pub expected_head: u64,
    pub action_digest: String,
    pub request_digest: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceiptKind {
    Data { ratified_version: u64 },
    Noop { checked_head: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectReceipt {
    pub intent: IntentId,
    pub step: EffectStep,
    pub kind: ReceiptKind,
    pub prepared_digest: String,
    pub action_digest: String,
    /// The guard revision the ratification committed under.
    pub authority_commit: u64,
}

/// One ledger row per request id; a lost response is resolved by lookup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    pub request_id: String,
    pub request_digest: String,
    pub response: serde_json::Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AckState {
    Pending,
    /// A response was lost; committed progress and receipts stay authoritative.
    Unknown,
    Confirmed,
}

/// Generation-scoped source acknowledgment work queued by batch completion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub id: String,
    pub intent: IntentId,
    pub generation: u64,
    /// The sealed end vector of the completed batch.
    pub cursor: serde_json::Value,
    pub state: AckState,
    pub attempts: u64,
}

/// What batch completion commits in one transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Completion {
    pub checkpoint: FlowCheckpoint,
    pub ack: Ack,
}

/// A ratified log entry; recovery publishes it from here if the crash came
/// between ratification and publication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RatifiedCommit {
    pub sink: SinkIdentity,
    pub version: u64,
    pub staged_path: String,
    pub action_digest: String,
    pub intent: IntentId,
    pub step: EffectStep,
}

/// A sink's catalog-ratified head.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkHead {
    pub sink: SinkIdentity,
    pub version: u64,
}
