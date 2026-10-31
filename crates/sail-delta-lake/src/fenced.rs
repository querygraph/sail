//! What a commit authority's orchestration needs from a Delta table outside
//! a commit: its identity and head, and publishing a ratified entry again
//! after a crash between ratification and publication.

use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion_common::{DataFusionError, Result, plan_err};
use futures::StreamExt;
use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStoreExt, PutMode, PutOptions};
use url::Url;

use crate::delta_log::{StorageConfig, get_object_store_from_context};
use crate::spec::log::commit_path;
use crate::table::{create_logstore_with_object_store, open_table_with_object_store};

fn external<E: std::error::Error + Send + Sync + 'static>(error: E) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// The table's immutable identity and published head, or `None` for a table
/// with no commit yet, which has no identity to bind.
pub async fn table_state(
    context: &Arc<TaskContext>,
    table_url: &Url,
) -> Result<Option<(String, i64)>> {
    let store = get_object_store_from_context(context, table_url)?;
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

/// Publish a ratified staged entry. Publishing it twice is a no-op; a
/// different entry already at that version means something wrote the log
/// outside the authority, and is refused.
pub async fn publish_ratified(
    context: &Arc<TaskContext>,
    table_url: &Url,
    version: i64,
    staged_path: &str,
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
                plan_err!("log version {version} holds an entry the authority did not ratify")
            }
        }
        Err(error) => Err(external(error)),
    }
}
