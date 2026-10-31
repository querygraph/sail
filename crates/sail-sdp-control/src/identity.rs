//! Identities: a name or a checkpoint path is never a sink identity.

use serde::{Deserialize, Serialize};

/// The authority domain and its incarnation; a restored or forked database
/// must rotate the incarnation before serving.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AuthorityIncarnation(pub String);

/// The enrolled physical target group: one opaque enrollment key bound to
/// the target table's immutable identity and its auxiliary table's identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TargetKey(pub String);

/// An immutable table identity (the Delta `Metadata` id, or the catalog's
/// table id where the log is catalog-managed); a recreated table is a new one.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SinkIdentity(pub String);

/// The stable identity of a flow under a target: pipeline and flow names
/// resolved at enrollment, never a path.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FlowKey(pub String);

/// A stable intent identity: `(target, generation, flow, batch sequence)`
/// rendered canonically. Ownership changes never change it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IntentId(pub String);

impl IntentId {
    pub fn new(target: &TargetKey, generation: u64, flow: &FlowKey, batch_sequence: u64) -> Self {
        Self(format!(
            "{}/{generation}/{}/{batch_sequence}",
            escape(&target.0),
            escape(&flow.0)
        ))
    }

    /// A reset's identity; one reset per generation.
    pub fn reset(target: &TargetKey, generation: u64) -> Self {
        Self(format!("{}/{generation}/reset", escape(&target.0)))
    }
}

/// Escape a name for a composite key, so that names containing separators
/// cannot alias.
pub fn escape(name: &str) -> String {
    name.replace('%', "%25").replace('/', "%2F")
}

/// Which of the batch's effects a preparation or receipt belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EffectStep {
    /// Materializing the target under an Initialization intent.
    InitializeTarget,
    /// Creating the auxiliary table under an Initialization intent.
    InitializeAuxiliary,
    Auxiliary,
    Target,
    /// A reset's target truncate/replacement.
    ResetTarget,
    /// A reset's auxiliary drop/recreation.
    ResetAuxiliary,
}

/// A fence token: everything a ratification is checked against. Carried in
/// the write context to the physical commit; checked against current
/// authority state, never trusted on its own.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FenceToken {
    pub authority: AuthorityIncarnation,
    pub target: TargetKey,
    pub generation: u64,
    pub epoch: u64,
    pub owner: String,
}
