//! What a commit authority's orchestration needs from a Delta table outside
//! a commit: its identity and head, and publishing a ratified entry again
//! after a crash between ratification and publication.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::execution::TaskContext;
use datafusion_common::{DataFusionError, Result, plan_err};
use futures::StreamExt;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use sail_common::spec::FencedWrite;
use sail_common_datafusion::fenced_commit::ENROLLED_AUTHORITY_PROPERTY;
use sha2::{Digest, Sha256};
use url::Url;

use crate::delta_log::{StorageConfig, get_object_store_from_context};
use crate::spec::log::commit_path;
use crate::table::{create_logstore_with_object_store, open_table_with_object_store};

fn external<E: std::error::Error + Send + Sync + 'static>(error: E) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn object_store_of(context: &TaskContext, table_url: &Url) -> Result<Arc<dyn ObjectStore>> {
    context
        .runtime_env()
        .object_store_registry
        .get_store(table_url)
        .map_err(external)
}

/// What ratification binds a staged entry by: its exact bytes.
pub(crate) fn action_digest(staged: &[u8]) -> String {
    let hash: String = Sha256::digest(staged)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("sha256:{hash}")
}

/// The table's immutable identity and published head, or `None` for a table
/// with no commit yet, which has no identity to bind.
pub async fn table_state(
    context: &Arc<TaskContext>,
    table_url: &Url,
) -> Result<Option<(String, i64)>> {
    table_state_in(context.as_ref(), table_url).await
}

/// `table_state` for a context held by reference.
pub async fn table_state_in(
    context: &TaskContext,
    table_url: &Url,
) -> Result<Option<(String, i64)>> {
    let store = object_store_of(context, table_url)?;
    let log = Path::from_url_path(table_url.path())
        .map_err(external)?
        .join("_delta_log");
    let mut listing = store.list(Some(&log));
    let mut committed = false;
    while let Some(entry) = listing.next().await {
        if entry
            .map_err(external)?
            .location
            .as_ref()
            .ends_with(".json")
        {
            committed = true;
            break;
        }
    }
    drop(listing);
    if !committed {
        return Ok(None);
    }
    let table = open_table_with_object_store(table_url.clone(), store, StorageConfig)
        .await
        .map_err(external)?;
    let snapshot = table.snapshot().map_err(external)?;
    Ok(Some((
        snapshot.metadata().id().to_string(),
        snapshot.version(),
    )))
}

/// The authority an enrolled table names in its metadata, if any.
pub fn enrolled_authority_of(configuration: &HashMap<String, String>) -> Option<&str> {
    configuration
        .get(ENROLLED_AUTHORITY_PROPERTY)
        .map(String::as_str)
}

/// A table that names an authority admits only that authority's fence. The
/// property travels with the table, so this holds in every session and on
/// every server, whether or not one holds the authority.
pub fn check_enrolled_authority(
    table_url: &Url,
    configuration: &HashMap<String, String>,
    fenced_write: Option<&FencedWrite>,
) -> Result<()> {
    let Some(authority) = enrolled_authority_of(configuration) else {
        return Ok(());
    };
    match fenced_write {
        Some(fence) if fence.authority == authority => Ok(()),
        Some(fence) => plan_err!(
            "table {table_url} is enrolled with AutoCDC authority {authority} and refuses a \
             fence from authority {}",
            fence.authority
        ),
        None => plan_err!(
            "table {table_url} is enrolled with AutoCDC authority {authority} and refuses a \
             write without its fence"
        ),
    }
}

/// The authority a table at `table_url` names, read from its log. `None` for
/// a table with no commit or no such property.
pub async fn enrolled_authority(context: &TaskContext, table_url: &Url) -> Result<Option<String>> {
    if table_state_in(context, table_url).await?.is_none() {
        return Ok(None);
    }
    let store = object_store_of(context, table_url)?;
    let table = open_table_with_object_store(table_url.clone(), store, StorageConfig)
        .await
        .map_err(external)?;
    let snapshot = table.snapshot().map_err(external)?;
    Ok(enrolled_authority_of(snapshot.metadata().configuration()).map(str::to_string))
}

/// Publish a ratified staged entry, which must still be the bytes that were
/// ratified. Publishing it twice is a no-op; a different entry already at
/// that version means something wrote the log outside the authority, and is
/// refused.
pub async fn publish_ratified(
    context: &Arc<TaskContext>,
    table_url: &Url,
    version: i64,
    staged_path: &str,
    ratified_digest: &str,
) -> Result<()> {
    let store = get_object_store_from_context(context, table_url)?;
    // Log paths are relative to the table root.
    let store = create_logstore_with_object_store(store, table_url.clone(), StorageConfig)
        .map_err(external)?
        .object_store(None);
    let bytes = store
        .get(&Path::from(staged_path))
        .await
        .map_err(external)?
        .bytes()
        .await
        .map_err(external)?;
    if action_digest(&bytes) != ratified_digest {
        return plan_err!(
            "the staged entry for log version {version} is not the one the authority ratified"
        );
    }
    publish_entry(store.as_ref(), version, bytes).await
}

/// Publish a log entry at `version` with create-only semantics. Publishing
/// the same bytes twice is a no-op, as after a lost response; different
/// bytes already at that version mean something wrote the log outside the
/// commit protocol, and are refused.
pub async fn publish_entry(store: &dyn ObjectStore, version: i64, bytes: Bytes) -> Result<()> {
    let published = commit_path(version);
    let options = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    match store
        .put_opts(&published, bytes.clone().into(), options)
        .await
    {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::AlreadyExists { .. }) => {
            let existing = store
                .get(&published)
                .await
                .map_err(external)?
                .bytes()
                .await
                .map_err(external)?;
            if existing == bytes {
                Ok(())
            } else {
                plan_err!(
                    "log version {version} holds an entry that was not committed through this protocol"
                )
            }
        }
        Err(error) => Err(external(error)),
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn the_digest_is_of_the_bytes_and_names_its_algorithm() {
        assert_eq!(
            action_digest(b""),
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_ne!(action_digest(b"{\"add\":1}"), action_digest(b"{\"add\":2}"));
    }

    #[tokio::test]
    async fn publishing_an_entry_again_is_a_no_op_but_another_entry_is_refused() {
        let store = object_store::memory::InMemory::new();
        let entry = Bytes::from_static(b"{\"add\":1}");
        publish_entry(&store, 7, entry.clone()).await.unwrap();
        // A lost response is retried with the same bytes.
        publish_entry(&store, 7, entry).await.unwrap();
        let error = publish_entry(&store, 7, Bytes::from_static(b"{\"add\":2}"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("log version 7"), "{error}");
        let published = store
            .get(&commit_path(7))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(published, Bytes::from_static(b"{\"add\":1}"));
    }
}
