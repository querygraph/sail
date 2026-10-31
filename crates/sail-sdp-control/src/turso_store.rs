//! The Turso authority adapter: one `BEGIN CONCURRENT` transaction per
//! operation on a database in MVCC mode. Qualified by
//! `evidence/turso-control-probe`, which also lists what is not qualified:
//! power loss, a file shared by live processes, and restore or fork. One
//! authority process owns a database file.

use async_trait::async_trait;
use tokio::sync::Mutex;
use turso::{Builder, Connection, Database, Value};

use crate::identity::AuthorityIncarnation;
use crate::store::{Fail, Store, Table, Tx};
use crate::transitions::Outcome;

/// The record schema; checked before the store serves.
pub const SCHEMA_VERSION: i64 = 1;

fn is_retryable(error: &turso::Error) -> bool {
    matches!(error, turso::Error::Busy(_) | turso::Error::BusySnapshot(_))
        || matches!(
            error,
            turso::Error::Error(message)
                if message.contains("database is locked")
                    || message.contains("Write-write conflict")
                    || message.contains("Commit dependency aborted")
        )
}

fn fail(error: turso::Error) -> Fail {
    if is_retryable(&error) {
        Fail::Retry
    } else {
        Fail::Final(Outcome::StateUnknown {
            reason: error.to_string(),
        })
    }
}

fn unavailable(error: turso::Error) -> Outcome {
    Outcome::StateUnknown {
        reason: error.to_string(),
    }
}

pub struct TursoStore {
    database: Database,
    authority: AuthorityIncarnation,
}

async fn connect(database: &Database) -> Result<Connection, turso::Error> {
    let connection = database.connect()?;
    // `journal_mode` returns a row, which `execute_batch` reports as an
    // error although the mode is applied; `open` verifies the mode.
    let _ = connection.execute_batch("PRAGMA journal_mode=mvcc;").await;
    connection.busy_timeout(std::time::Duration::from_secs(10))?;
    Ok(connection)
}

async fn single_text(connection: &Connection, sql: &str) -> Result<Option<String>, turso::Error> {
    let mut rows = connection.query(sql, ()).await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(match row.get_value(0)? {
        Value::Text(text) => Some(text),
        Value::Integer(value) => Some(value.to_string()),
        _ => None,
    })
}

impl TursoStore {
    /// Open or create the authority database. A fresh database records
    /// `incarnation`; an existing one must carry the expected schema version
    /// and keeps its recorded incarnation.
    pub async fn open(path: &str, incarnation: &str) -> Result<Self, Outcome> {
        let database = Builder::new_local(path)
            .build()
            .await
            .map_err(unavailable)?;
        let connection = connect(&database).await.map_err(unavailable)?;
        let mode = single_text(&connection, "PRAGMA journal_mode")
            .await
            .map_err(unavailable)?;
        if mode.as_deref() != Some("mvcc") {
            return Err(Outcome::UnsupportedCapability {
                reason: format!("authority needs MVCC, found {mode:?}"),
            });
        }
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS sdp_authority (\
                 id INTEGER PRIMARY KEY, schema_version INTEGER NOT NULL, \
                 incarnation TEXT NOT NULL)",
                (),
            )
            .await
            .map_err(unavailable)?;
        for table in Table::ALL {
            let name = table.sql_name();
            connection
                .execute(
                    format!(
                        "CREATE TABLE IF NOT EXISTS {name} (\
                         key TEXT PRIMARY KEY, link TEXT, body TEXT NOT NULL)"
                    ),
                    (),
                )
                .await
                .map_err(unavailable)?;
            connection
                .execute(
                    format!("CREATE INDEX IF NOT EXISTS {name}_link ON {name} (link)"),
                    (),
                )
                .await
                .map_err(unavailable)?;
        }
        connection
            .execute(
                "INSERT OR IGNORE INTO sdp_authority VALUES (1, ?1, ?2)",
                (SCHEMA_VERSION, incarnation.to_string()),
            )
            .await
            .map_err(unavailable)?;
        let version = single_text(&connection, "SELECT schema_version FROM sdp_authority")
            .await
            .map_err(unavailable)?;
        if version.as_deref() != Some(SCHEMA_VERSION.to_string().as_str()) {
            return Err(Outcome::UnsupportedCapability {
                reason: format!("authority schema {version:?} is not {SCHEMA_VERSION}"),
            });
        }
        let authority = single_text(&connection, "SELECT incarnation FROM sdp_authority")
            .await
            .map_err(unavailable)?
            .ok_or_else(|| Outcome::StateUnknown {
                reason: "authority row is missing".into(),
            })?;
        Ok(Self {
            database,
            authority: AuthorityIncarnation(authority),
        })
    }
}

/// One transaction on its own connection.
pub struct TursoTx {
    connection: Mutex<Connection>,
}

#[async_trait]
impl Tx for TursoTx {
    async fn get(&self, table: Table, key: &str) -> Result<Option<String>, Fail> {
        let connection = self.connection.lock().await;
        let sql = format!("SELECT body FROM {} WHERE key = ?1", table.sql_name());
        let mut rows = connection
            .query(sql, (key.to_string(),))
            .await
            .map_err(fail)?;
        let Some(row) = rows.next().await.map_err(fail)? else {
            return Ok(None);
        };
        Ok(Some(row.get(0).map_err(fail)?))
    }

    async fn list(&self, table: Table, link: &str) -> Result<Vec<String>, Fail> {
        let connection = self.connection.lock().await;
        let sql = format!(
            "SELECT body FROM {} WHERE link = ?1 ORDER BY key",
            table.sql_name()
        );
        let mut rows = connection
            .query(sql, (link.to_string(),))
            .await
            .map_err(fail)?;
        let mut bodies = Vec::new();
        while let Some(row) = rows.next().await.map_err(fail)? {
            bodies.push(row.get(0).map_err(fail)?);
        }
        Ok(bodies)
    }

    async fn put(
        &self,
        table: Table,
        key: &str,
        link: Option<&str>,
        body: &str,
    ) -> Result<(), Fail> {
        let connection = self.connection.lock().await;
        let name = table.sql_name();
        let changed = connection
            .execute(
                format!("UPDATE {name} SET link = ?1, body = ?2 WHERE key = ?3"),
                (link.map(str::to_string), body.to_string(), key.to_string()),
            )
            .await
            .map_err(fail)?;
        if changed == 0 {
            connection
                .execute(
                    format!("INSERT INTO {name} (key, link, body) VALUES (?1, ?2, ?3)"),
                    (key.to_string(), link.map(str::to_string), body.to_string()),
                )
                .await
                .map_err(fail)?;
        }
        Ok(())
    }

    async fn commit(&self) -> Result<(), Fail> {
        let connection = self.connection.lock().await;
        connection.execute_batch("COMMIT").await.map_err(fail)
    }

    async fn rollback(&self) {
        let connection = self.connection.lock().await;
        let _ = connection.execute_batch("ROLLBACK").await;
    }
}

#[async_trait]
impl Store for TursoStore {
    type Tx = TursoTx;

    fn authority(&self) -> AuthorityIncarnation {
        self.authority.clone()
    }

    async fn begin(&self) -> Result<TursoTx, Fail> {
        let connection = connect(&self.database).await.map_err(fail)?;
        connection
            .execute_batch("BEGIN CONCURRENT")
            .await
            .map_err(fail)?;
        Ok(TursoTx {
            connection: Mutex::new(connection),
        })
    }
}
