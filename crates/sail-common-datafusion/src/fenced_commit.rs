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
    /// Whether an authority owns the table at `table_url`. With `table_id`,
    /// the live table's identity, only that table counts: a table dropped and
    /// created again at the same location is another table, not enrolled.
    async fn is_enrolled(&self, table_url: &str, table_id: Option<&str>) -> Result<bool>;

    async fn ratify(&self, fence: &FencedWrite, effect: &FencedEffect) -> Result<Ratification>;

    /// The fence of a commit whose plan does not carry one. Only a merge plan
    /// can carry a fence, and a schema-replacing overwrite is not a merge.
    fn ambient_fence(&self) -> Option<FencedWrite> {
        None
    }
}

/// The session's commit authority; sessions without one have no fenced writes.
#[derive(Clone, Debug)]
pub struct FencedCommits(pub Arc<dyn FencedCommitAuthority>);

impl SessionExtension for FencedCommits {
    fn name() -> &'static str {
        "FencedCommits"
    }
}

/// The Delta table property an enrolled table carries: the incarnation of the
/// authority that fences it. A writer in any session, on any server, refuses
/// to commit over it without that authority's fence.
pub const ENROLLED_AUTHORITY_PROPERTY: &str = "sail.autocdc.authority";

/// The commit authorities installed on this server, one per control store,
/// so a session that holds none still refuses an unfenced write to a table
/// another session enrolled. Sessions share it through the session factory.
#[derive(Debug, Default)]
pub struct FencedCommitRegistry {
    authorities:
        std::sync::RwLock<std::collections::BTreeMap<String, Arc<dyn FencedCommitAuthority>>>,
}

impl FencedCommitRegistry {
    /// Register the authority for `key`, replacing one registered before.
    pub fn register(&self, key: &str, authority: Arc<dyn FencedCommitAuthority>) {
        if let Ok(mut authorities) = self.authorities.write() {
            authorities.insert(key.to_string(), authority);
        }
    }

    /// Every registered authority.
    pub fn authorities(&self) -> Vec<Arc<dyn FencedCommitAuthority>> {
        self.authorities
            .read()
            .map(|authorities| authorities.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Whether any registered authority has the table at `table_url`, with
    /// identity `table_id` when known, enrolled.
    pub async fn is_enrolled(&self, table_url: &str, table_id: Option<&str>) -> Result<bool> {
        for authority in self.authorities() {
            if authority.is_enrolled(table_url, table_id).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl SessionExtension for FencedCommitRegistry {
    fn name() -> &'static str {
        "FencedCommitRegistry"
    }
}
