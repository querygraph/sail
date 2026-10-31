//! The seam between a Delta commit and the authority that owns its table.
//! The commit stages its log entry, asks the authority to ratify it, and
//! publishes only a new ratification. The authority lives outside this crate.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion_common::Result;
use sail_common::spec::FencedWrite;

use crate::extension::SessionExtension;

/// What a fenced commit asks the authority to ratify.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FencedEffect {
    /// A staged log entry for `version`.
    Data {
        version: i64,
        action_digest: String,
        staged_path: String,
    },
    /// No actions, checked against the exact head.
    Noop { checked_head: i64 },
}

/// The authority's answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ratification {
    /// This attempt's effect is ratified; the commit publishes it.
    Ratified,
    /// An earlier attempt of the same request is the ratified effect. This
    /// attempt's staged entry must not be published.
    Replayed,
}

#[async_trait]
pub trait FencedCommitAuthority: Debug + Send + Sync {
    /// Whether an authority owns the table at `table_url`.
    async fn is_enrolled(&self, table_url: &str) -> Result<bool>;

    async fn ratify(&self, fence: &FencedWrite, effect: &FencedEffect) -> Result<Ratification>;
}

/// The session's commit authority; sessions without one have no fenced writes.
#[derive(Clone, Debug)]
pub struct FencedCommits(pub Arc<dyn FencedCommitAuthority>);

impl SessionExtension for FencedCommits {
    fn name() -> &'static str {
        "FencedCommits"
    }
}
