//! Closing a Spark session, including through a context obtained before it was closed.

use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, StreamExt, stream};
use tokio::time::timeout;
use tonic::{Code, Status};

use super::{SparkSession, SparkSessionOptions};
use crate::error::{SparkError, SparkResult, session_closed_error};
use crate::executor::{Executor, ExecutorMetadata, ExecutorOutput};
use crate::pipelines::PipelineGraph;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const BOUND: Duration = Duration::from_secs(10);

fn session() -> SparkResult<Arc<SparkSession>> {
    pyo3::Python::initialize();
    Ok(Arc::new(SparkSession::try_new(
        "s".to_string(),
        "u".to_string(),
        SparkSessionOptions {
            execution_heartbeat_interval: Duration::from_secs(3600),
        },
    )?))
}

fn pending_executor(reattachable: bool) -> Executor {
    let outputs = stream::pending::<SparkResult<ExecutorOutput>>();
    Executor::with_outputs(
        ExecutorMetadata {
            operation_id: "op".to_string(),
            tags: vec![],
            reattachable,
        },
        Box::pin(outputs),
        Duration::from_secs(3600),
    )
}

fn assert_closed<T>(result: SparkResult<T>) {
    assert!(matches!(
        result,
        Err(SparkError::Structured(error)) if error.condition == "INVALID_HANDLE.SESSION_CLOSED"
    ));
}

#[test]
fn the_error_of_a_closed_session_is_the_one_spark_reports() {
    // Measured on Spark 4.2.1: an INTERNAL status with this exact text, for every RPC.
    let status = Status::from(session_closed_error("abc"));
    assert_eq!(status.code(), Code::Internal);
    assert_eq!(
        status.message(),
        "[INVALID_HANDLE.SESSION_CLOSED] The handle abc is invalid. Session was closed. \
         SQLSTATE: HY000"
    );
}

#[tokio::test]
async fn closing_refuses_new_work_on_a_stale_context() -> TestResult {
    let spark = session()?;
    spark.begin_close_operations()?;
    assert_closed(spark.begin_operation());
    assert_closed(spark.add_executor(pending_executor(false)));
    let graph = PipelineGraph::new("c".to_string(), "d".to_string(), Default::default());
    assert_closed(spark.add_pipeline(graph));
    // Closing twice is harmless.
    spark.begin_close_operations()?;
    timeout(BOUND, spark.wait_operations_closed()).await??;
    Ok(())
}

#[tokio::test]
async fn closing_waits_for_a_request_that_was_admitted_earlier() -> TestResult {
    let spark = session()?;
    let lease = spark.begin_operation()?;
    spark.begin_close_operations()?;
    let mut closed = Box::pin(spark.wait_operations_closed());
    assert!(futures::poll!(closed.as_mut()).is_pending());
    drop(lease);
    timeout(BOUND, closed).await??;
    Ok(())
}

#[tokio::test]
async fn closing_stops_a_running_executor_and_tells_its_client() -> TestResult {
    let spark = session()?;
    let mut output = spark.add_executor(pending_executor(true))?.start()?;
    spark.begin_close_operations()?;
    timeout(BOUND, spark.wait_operations_closed()).await??;
    assert_closed(output.next().await.ok_or("no output")?);
    assert!(spark.get_executor("op")?.is_none());
    Ok(())
}

#[tokio::test]
async fn a_client_behind_a_full_channel_still_learns_that_the_session_closed() -> TestResult {
    let spark = session()?;
    // The first output fills the channel, and nothing reads it until after the close.
    let outputs = stream::once(async { Ok(ExecutorOutput::complete()) }).chain(stream::pending());
    let executor = Executor::with_outputs(
        ExecutorMetadata {
            operation_id: "op".to_string(),
            tags: vec![],
            reattachable: false,
        },
        Box::pin(outputs),
        Duration::from_secs(3600),
    );
    let mut output = spark.add_executor(executor)?.start()?;
    tokio::task::yield_now().await;
    spark.begin_close_operations()?;
    timeout(BOUND, spark.wait_operations_closed()).await??;
    assert!(output.next().await.ok_or("no output")?.is_ok());
    assert_closed(output.next().await.ok_or("the stream ended silently")?);
    assert!(output.next().await.is_none());
    Ok(())
}

#[tokio::test]
async fn an_operation_that_finished_is_not_reported_as_closed() -> TestResult {
    let spark = session()?;
    let executor = Executor::with_outputs(
        ExecutorMetadata {
            operation_id: "op".to_string(),
            tags: vec![],
            reattachable: false,
        },
        Box::pin(stream::once(async { Ok(ExecutorOutput::complete()) })),
        Duration::from_secs(3600),
    );
    let mut output = spark.add_executor(executor)?.start()?;
    assert!(output.next().await.ok_or("no output")?.is_ok());
    assert!(output.next().await.is_none());
    spark.begin_close_operations()?;
    timeout(BOUND, spark.wait_operations_closed()).await??;
    Ok(())
}

#[tokio::test]
async fn closing_does_not_depend_on_a_client_reading_the_output() -> TestResult {
    let spark = session()?;
    // The output of this executor can never be sent, since nothing reads it.
    let _output = spark.add_executor(pending_executor(true))?.start()?;
    spark.begin_close_operations()?;
    timeout(BOUND, spark.wait_operations_closed()).await??;
    Ok(())
}

#[tokio::test]
async fn a_closed_executor_cannot_be_reattached() -> TestResult {
    let executor = pending_executor(true);
    executor.request_close("s")?;
    assert!(executor.start().is_err());
    Ok(())
}

#[tokio::test]
async fn closing_finishes_after_a_canceled_pause_lost_the_task_handle() -> TestResult {
    let spark = session()?;
    let executor = spark.add_executor(pending_executor(true))?;
    let _output = executor.start()?;
    // The pause takes the task out of the executor. Dropping it loses the task handle.
    assert!(executor.pause_if_running().now_or_never().is_none());
    spark.begin_close_operations()?;
    timeout(BOUND, spark.wait_operations_closed()).await??;
    Ok(())
}

#[tokio::test]
async fn a_pause_finishing_after_close_does_not_bring_the_executor_back() -> TestResult {
    let spark = session()?;
    let executor = spark.add_executor(pending_executor(true))?;
    let _output = executor.start()?;
    let mut pause = Box::pin(executor.pause_if_running());
    assert!(futures::poll!(pause.as_mut()).is_pending());
    spark.begin_close_operations()?;
    timeout(BOUND, pause).await??;
    timeout(BOUND, spark.wait_operations_closed()).await??;
    assert!(executor.start().is_err());
    Ok(())
}

#[tokio::test]
async fn closing_cancels_and_forgets_the_pipeline_graphs() -> TestResult {
    let spark = session()?;
    let graph = PipelineGraph::new("c".to_string(), "d".to_string(), Default::default());
    let id = spark.add_pipeline(graph)?;
    spark.begin_close_operations()?;
    assert!(spark.pipeline(&id).is_err());
    Ok(())
}
