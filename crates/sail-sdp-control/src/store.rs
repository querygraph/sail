//! The transactional seam. A backend provides one serialisable-enough
//! transaction over named record tables; [`crate::control`] holds the only
//! copy of the operations. Records are canonical JSON bodies under a key,
//! with an optional link for listing an intent's records.

use async_trait::async_trait;

use crate::identity::AuthorityIncarnation;
use crate::records::AuthorityMillis;
use crate::transitions::Outcome;

/// The record tables, named as in the design's SQL table list.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Table {
    Target,
    Physical,
    Flow,
    Intent,
    Prepare,
    Receipt,
    Head,
    RatifiedCommit,
    Operation,
    Ack,
}

impl Table {
    pub const ALL: [Table; 10] = [
        Table::Target,
        Table::Physical,
        Table::Flow,
        Table::Intent,
        Table::Prepare,
        Table::Receipt,
        Table::Head,
        Table::RatifiedCommit,
        Table::Operation,
        Table::Ack,
    ];

    pub fn sql_name(self) -> &'static str {
        match self {
            Table::Target => "sdp_target",
            Table::Physical => "sdp_physical",
            Table::Flow => "sdp_flow",
            Table::Intent => "sdp_intent",
            Table::Prepare => "sdp_effect_prepare",
            Table::Receipt => "sdp_effect_receipt",
            Table::Head => "delta_head",
            Table::RatifiedCommit => "delta_ratified_commit",
            Table::Operation => "sdp_operation",
            Table::Ack => "sdp_ack",
        }
    }
}

/// How a transaction step failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fail {
    /// Lost a write race or hit transient contention; rerun on a fresh
    /// snapshot. The retried body rereads the guard and re-applies every
    /// check, and never changes the sealed request.
    Retry,
    /// A final outcome; the transaction rolls back.
    Final(Outcome),
}

impl From<Outcome> for Fail {
    fn from(outcome: Outcome) -> Self {
        Fail::Final(outcome)
    }
}

/// One open transaction.
#[async_trait]
pub trait Tx: Send + Sync {
    async fn get(&self, table: Table, key: &str) -> Result<Option<String>, Fail>;

    /// The bodies linked to `link`.
    async fn list(&self, table: Table, link: &str) -> Result<Vec<String>, Fail>;

    /// Insert or replace. Every write to a guard row goes through here, so
    /// conflicting operations collide on that row.
    async fn put(
        &self,
        table: Table,
        key: &str,
        link: Option<&str>,
        body: &str,
    ) -> Result<(), Fail>;

    async fn commit(&self) -> Result<(), Fail>;

    async fn rollback(&self);
}

#[async_trait]
pub trait Store: Send + Sync {
    type Tx: Tx;

    /// The authority domain this store serves.
    fn authority(&self) -> AuthorityIncarnation;

    async fn begin(&self) -> Result<Self::Tx, Fail>;
}

/// Authority time. Only this decides lease eligibility.
pub trait Clock: Send + Sync {
    fn now(&self) -> AuthorityMillis;
}

/// The authority process's wall clock. A jump can affect availability or
/// cause an early takeover, never safety: the serialised epoch check supplies
/// that.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> AuthorityMillis {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
            })
    }
}
