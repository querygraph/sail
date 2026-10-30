use datafusion::common::Result;
use datafusion::execution::runtime_env::RuntimeEnv;
use sail_cache::remote_checkpoint::RemoteCheckpointRegistry;
use sail_common_datafusion::session::job::JobRunner;

/// Do not invoke checkpoint cleanup when runner shutdown reports an error.
pub(super) async fn stop_and_clean_checkpoints(
    runner: &dyn JobRunner,
    checkpoints: Option<&RemoteCheckpointRegistry>,
    runtime: &RuntimeEnv,
) -> Result<()> {
    runner.stop().await?;
    if let Some(checkpoints) = checkpoints {
        checkpoints.cleanup_session(runtime).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "cleanup_tests.rs"]
mod tests;
