use std::collections::BTreeMap;

use datafusion::prelude::SessionContext;

use super::{Checkpoint, FlowState};
use crate::error::{SparkError, SparkResult};

async fn open(ctx: &SessionContext, storage: &str, reset: bool) -> SparkResult<Checkpoint> {
    let name = vec!["t".to_string()];
    Checkpoint::open(ctx, storage, &name, &name, reset).await
}

#[tokio::test]
async fn a_retried_full_refresh_continues_the_version_it_opened() -> SparkResult<()> {
    let directory = tempfile::tempdir().map_err(|e| SparkError::internal(e.to_string()))?;
    let storage = format!("file://{}/", directory.path().display());
    let ctx = SessionContext::new();

    // One committed batch under version 0.
    let first = open(&ctx, &storage, false).await?;
    first
        .store(&FlowState {
            batches: 1,
            ..Default::default()
        })
        .await?;

    // A full refresh opens version 1 and gets as far as a pending batch.
    let refresh = open(&ctx, &storage, true).await?;
    assert_ne!(refresh.file, first.file);
    refresh
        .store(&FlowState {
            reset: true,
            pending: Some(BTreeMap::new()),
            ..Default::default()
        })
        .await?;

    // Its retry continues version 1 rather than opening version 2.
    let retried = open(&ctx, &storage, true).await?;
    assert_eq!(retried.file, refresh.file);

    // A refresh that committed a batch is not continued by the next one.
    retried
        .store(&FlowState {
            reset: true,
            batches: 1,
            ..Default::default()
        })
        .await?;
    let later = open(&ctx, &storage, true).await?;
    assert_ne!(later.file, refresh.file);

    // An ordinary run always opens the latest version.
    let ordinary = open(&ctx, &storage, false).await?;
    assert_eq!(ordinary.file, refresh.file);
    Ok(())
}
