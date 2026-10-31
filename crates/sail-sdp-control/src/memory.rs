//! An in-process store for model tests. One transaction at a time, with
//! rollback by snapshot, which gives the single-process serialisation the
//! design requires of a real adapter. It says what it is: unfenced across
//! processes and not an authority.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::OwnedMutexGuard;

use crate::identity::AuthorityIncarnation;
use crate::store::{Fail, Store, Table, Tx};

/// The store's own statement of its boundary.
pub const AUTHORITY_STATEMENT: &str =
    "in-process memory store: single process, not fenced across processes, not an authority";

type Rows = BTreeMap<(Table, String), (Option<String>, String)>;

#[derive(Debug, Default)]
pub struct UnfencedMemoryStore {
    rows: Arc<tokio::sync::Mutex<Rows>>,
}

impl UnfencedMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct MemoryTx {
    open: Mutex<OpenTx>,
}

struct OpenTx {
    rows: OwnedMutexGuard<Rows>,
    snapshot: Rows,
}

impl MemoryTx {
    fn with<T>(&self, f: impl FnOnce(&mut OpenTx) -> T) -> T {
        let mut open = self
            .open
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        f(&mut open)
    }
}

#[async_trait]
impl Tx for MemoryTx {
    async fn get(&self, table: Table, key: &str) -> Result<Option<String>, Fail> {
        Ok(self.with(|open| {
            open.rows
                .get(&(table, key.to_string()))
                .map(|(_, body)| body.clone())
        }))
    }

    async fn list(&self, table: Table, link: &str) -> Result<Vec<String>, Fail> {
        Ok(self.with(|open| {
            open.rows
                .iter()
                .filter(|((t, _), (l, _))| *t == table && l.as_deref() == Some(link))
                .map(|(_, (_, body))| body.clone())
                .collect()
        }))
    }

    async fn put(
        &self,
        table: Table,
        key: &str,
        link: Option<&str>,
        body: &str,
    ) -> Result<(), Fail> {
        self.with(|open| {
            open.rows.insert(
                (table, key.to_string()),
                (link.map(str::to_string), body.to_string()),
            );
        });
        Ok(())
    }

    async fn commit(&self) -> Result<(), Fail> {
        Ok(())
    }

    async fn rollback(&self) {
        self.with(|open| *open.rows = open.snapshot.clone());
    }
}

#[async_trait]
impl Store for UnfencedMemoryStore {
    type Tx = MemoryTx;

    fn authority(&self) -> AuthorityIncarnation {
        AuthorityIncarnation("in-process".into())
    }

    async fn begin(&self) -> Result<MemoryTx, Fail> {
        let rows = self.rows.clone().lock_owned().await;
        let snapshot = rows.clone();
        Ok(MemoryTx {
            open: Mutex::new(OpenTx { rows, snapshot }),
        })
    }
}
