use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::compute::concat_batches;
use datafusion::prelude::SessionContext;
use fastrace::Span;
use fastrace::collector::SpanContext;
use fastrace::future::FutureExt;
use futures::stream;
use log::{debug, warn};
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_plan::resolve_and_execute_plan;
use tonic::Status;
use tonic::codegen::tokio_stream::Stream;

use crate::error::{ProtoFieldExt, SparkError, SparkResult};
use crate::executor::{
    Executor, ExecutorBatch, ExecutorMetadata, ExecutorMode, ExecutorOutput, ExecutorOutputStream,
    to_arrow_batch,
};
use crate::session::SparkSession;
use crate::spark::connect::execute_plan_response::{
    ResponseType, ResultComplete, SqlCommandResult,
};
use crate::spark::connect::{
    CachedRemoteRelation, CheckpointCommand, CheckpointCommandResult,
    CommonInlineUserDefinedDataSource, CommonInlineUserDefinedFunction,
    CommonInlineUserDefinedTableFunction, CreateDataFrameViewCommand, ExecutePlanResponse,
    GetResourcesCommand, LocalRelation, MergeIntoTableCommand, Relation,
    RemoveCachedRemoteRelationCommand, SqlCommand, StreamingQueryCommand,
    StreamingQueryCommandResult, StreamingQueryListenerBusCommand, StreamingQueryManagerCommand,
    StreamingQueryManagerCommandResult, WriteOperation, WriteOperationV2,
    WriteStreamOperationStart, WriteStreamOperationStartResult, relation,
};
use crate::streaming::timeout_millis;

pub struct ExecutePlanResponseStream {
    session_id: String,
    operation_id: String,
    inner: ExecutorOutputStream,
}

impl ExecutePlanResponseStream {
    pub fn new(session_id: String, operation_id: String, inner: ExecutorOutputStream) -> Self {
        Self {
            session_id,
            operation_id,
            inner,
        }
    }
}

impl Stream for ExecutePlanResponseStream {
    type Item = Result<ExecutePlanResponse, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<ExecutePlanResponse, Status>>> {
        self.inner.as_mut().poll_next(cx).map(|poll| {
            poll.map(|item| {
                let item = item.map_err(Status::from)?;
                let mut response = ExecutePlanResponse::default();
                response.session_id.clone_from(&self.session_id);
                response.server_side_session_id.clone_from(&self.session_id);
                response.operation_id.clone_from(&self.operation_id.clone());
                response.response_id = item.id;
                match item.batch {
                    ExecutorBatch::Heartbeat => {}
                    ExecutorBatch::ArrowBatch(batch) => {
                        response.response_type = Some(ResponseType::ArrowBatch(batch));
                    }
                    ExecutorBatch::SqlCommandResult(result) => {
                        response.response_type = Some(ResponseType::SqlCommandResult(*result));
                    }
                    ExecutorBatch::WriteStreamOperationStartResult(result) => {
                        response.response_type =
                            Some(ResponseType::WriteStreamOperationStartResult(*result));
                    }
                    ExecutorBatch::StreamingQueryCommandResult(result) => {
                        response.response_type =
                            Some(ResponseType::StreamingQueryCommandResult(*result));
                    }
                    ExecutorBatch::StreamingQueryManagerCommandResult(result) => {
                        response.response_type =
                            Some(ResponseType::StreamingQueryManagerCommandResult(*result));
                    }
                    ExecutorBatch::CheckpointCommandResult(result) => {
                        response.response_type =
                            Some(ResponseType::CheckpointCommandResult(*result));
                    }
                    ExecutorBatch::Schema(schema) => {
                        response.schema = Some(*schema);
                    }
                    ExecutorBatch::Complete => {
                        response.response_type =
                            Some(ResponseType::ResultComplete(ResultComplete::default()));
                    }
                }
                debug!("{response:?}");
                Ok(response)
            })
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

async fn handle_execute_plan(
    ctx: &SessionContext,
    plan: spec::Plan,
    metadata: ExecutorMetadata,
    mode: ExecutorMode,
) -> SparkResult<ExecutePlanResponseStream> {
    let span = Span::root("handle_execute_plan", SpanContext::random());
    let spark = ctx.extension::<SparkSession>()?;
    let service = ctx.extension::<JobService>()?;
    let operation_id = metadata.operation_id.clone();
    let (plan, _) = resolve_and_execute_plan(ctx, spark.plan_config()?, plan).await?;
    let stream = {
        let span = Span::enter_with_parent("JobRunner::execute", &span);
        service.runner().execute(ctx, plan).in_span(span).await?
    };
    let _guard = span.set_local_parent();
    let executor = Executor::new(
        metadata,
        stream,
        spark.options().execution_heartbeat_interval,
        mode,
    );
    let rx = executor.start()?;
    spark.add_executor(executor)?;
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        operation_id,
        rx,
    ))
}

pub(crate) async fn handle_execute_relation(
    ctx: &SessionContext,
    relation: Relation,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = relation.try_into()?;
    handle_execute_plan(ctx, plan, metadata, ExecutorMode::Query).await
}

pub(crate) async fn handle_execute_register_function(
    ctx: &SessionContext,
    udf: CommonInlineUserDefinedFunction,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::RegisterFunction(
        udf.try_into()?,
    )));
    let mode = ExecutorMode::command();
    handle_execute_plan(ctx, plan, metadata, mode).await
}

pub(crate) async fn handle_execute_write_operation(
    ctx: &SessionContext,
    write: WriteOperation,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::Write(
        write.try_into()?,
    )));
    let mode = ExecutorMode::command();
    handle_execute_plan(ctx, plan, metadata, mode).await
}

pub(crate) async fn handle_execute_create_dataframe_view(
    ctx: &SessionContext,
    view: CreateDataFrameViewCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(view.try_into()?));
    let mode = ExecutorMode::command();
    handle_execute_plan(ctx, plan, metadata, mode).await
}

pub(crate) async fn handle_execute_write_operation_v2(
    ctx: &SessionContext,
    write: WriteOperationV2,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::WriteTo(
        write.try_into()?,
    )));
    let mode = ExecutorMode::command();
    handle_execute_plan(ctx, plan, metadata, mode).await
}

pub(crate) async fn handle_execute_merge_into_table_command(
    ctx: &SessionContext,
    command: MergeIntoTableCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(command.try_into()?));
    let mode = ExecutorMode::command();
    handle_execute_plan(ctx, plan, metadata, mode).await
}

/// Handles execution of a SQL command.
/// If a string is sent over we convert it to a relation then convert it to a plan, then execute it.
pub(crate) async fn handle_execute_sql_command(
    ctx: &SessionContext,
    sql: SqlCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = ctx.extension::<SparkSession>()?;
    let relation = if let Some(input) = sql.input {
        input
    } else {
        Relation {
            common: None,
            #[expect(deprecated)]
            rel_type: Some(relation::RelType::Sql(crate::spark::connect::Sql {
                query: sql.sql,
                args: sql.args,
                pos_args: sql.pos_args,
                named_arguments: sql.named_arguments,
                pos_arguments: sql.pos_arguments,
            })),
        }
    };
    let plan: spec::Plan = relation.clone().try_into()?;
    match plan {
        spec::Plan::Command(command) => {
            let mode = ExecutorMode::command_with_completion(move |schema, data| {
                let data = concat_batches(&schema, data.iter())?;
                let relation = Relation {
                    common: None,
                    rel_type: Some(relation::RelType::LocalRelation(LocalRelation {
                        data: Some(to_arrow_batch(&data)?.data),
                        schema: None,
                    })),
                };
                Ok(Some(ExecutorOutput::new(ExecutorBatch::SqlCommandResult(
                    Box::new(SqlCommandResult {
                        relation: Some(relation),
                    }),
                ))))
            });
            handle_execute_plan(ctx, spec::Plan::Command(command), metadata, mode).await
        }
        spec::Plan::Query(_) => {
            let result = ExecutorBatch::SqlCommandResult(Box::new(SqlCommandResult {
                relation: Some(relation),
            }));
            let mut output = vec![ExecutorOutput::new(result)];
            if metadata.reattachable {
                output.push(ExecutorOutput::complete());
            }
            Ok(ExecutePlanResponseStream::new(
                spark.session_id().to_string(),
                metadata.operation_id,
                Box::pin(stream::iter(output.into_iter().map(Ok))),
            ))
        }
    }
}

pub(crate) async fn handle_execute_write_stream_operation_start(
    ctx: &SessionContext,
    start: WriteStreamOperationStart,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = ctx.extension::<SparkSession>()?;
    let service = ctx.extension::<JobService>()?;
    let operation_id = metadata.operation_id.clone();
    let reattachable = metadata.reattachable;
    let query_name = start.query_name.clone();
    let plan = spec::Plan::Command(spec::CommandPlan::new(start.try_into()?));
    let (plan, info) = resolve_and_execute_plan(ctx, spark.plan_config()?, plan).await?;
    let stream = service.runner().execute(ctx, plan).await?;
    let id = spark.start_streaming_query(query_name.clone(), info, stream)?;
    let result = WriteStreamOperationStartResult {
        query_id: Some(id.into()),
        name: query_name,
        // The event is for the client-side listener, which is not supported yet.
        query_started_event_json: None,
    };
    let mut output = vec![ExecutorOutput::new(
        ExecutorBatch::WriteStreamOperationStartResult(Box::new(result)),
    )];
    if reattachable {
        output.push(ExecutorOutput::complete());
    }
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        operation_id,
        Box::pin(stream::iter(output.into_iter().map(Ok))),
    ))
}

pub(crate) async fn handle_execute_streaming_query_command(
    ctx: &SessionContext,
    stream: StreamingQueryCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    use crate::spark::connect::streaming_query_command::{
        AwaitTerminationCommand, Command, ExplainCommand,
    };
    use crate::spark::connect::streaming_query_command_result::{
        AwaitTerminationResult, ExceptionResult, ExplainResult, RecentProgressResult, ResultType,
        StatusResult,
    };

    let spark = ctx.extension::<SparkSession>()?;
    let StreamingQueryCommand { query_id, command } = stream;
    let query_id = query_id.required("streaming query ID")?;
    let command = command.required("streaming query command")?;
    let result_type = match command {
        Command::Status(true) => {
            let status = spark.get_streaming_query_status(&query_id.clone().into())?;
            Some(ResultType::Status(StatusResult {
                status_message: status.message,
                is_data_available: true,
                is_trigger_active: true,
                is_active: status.is_active,
            }))
        }
        Command::LastProgress(true) | Command::RecentProgress(true) => {
            Some(ResultType::RecentProgress(RecentProgressResult {
                recent_progress_json: vec![],
            }))
        }
        Command::Stop(true) => {
            spark.stop_streaming_query(&query_id.clone().into())?;
            None
        }
        Command::ProcessAllAvailable(true) => None,
        Command::Explain(ExplainCommand { extended }) => {
            let mut result = spark.explain_streaming_query(&query_id.clone().into(), extended)?;
            while result.ends_with('\n') {
                result.pop();
            }
            Some(ResultType::Explain(ExplainResult { result }))
        }
        Command::Exception(true) => {
            let (message, class) = if let Some(throwable) =
                spark.get_streaming_query_exception(&query_id.clone().into())?
            {
                (
                    Some(throwable.message().to_string()),
                    Some(throwable.class_name().to_string()),
                )
            } else {
                (None, None)
            };
            Some(ResultType::Exception(ExceptionResult {
                exception_message: message,
                error_class: class,
                stack_trace: None,
            }))
        }
        Command::AwaitTermination(AwaitTerminationCommand { timeout_ms }) => {
            let timeout = timeout_ms.map(timeout_millis).transpose()?;
            let handle = spark.await_streaming_query(&query_id.clone().into())?;
            let terminated = if let Some(handle) = handle {
                handle.terminated(timeout).await?
            } else {
                true
            };
            Some(ResultType::AwaitTermination(AwaitTerminationResult {
                terminated,
            }))
        }
        Command::Status(false)
        | Command::LastProgress(false)
        | Command::RecentProgress(false)
        | Command::Stop(false)
        | Command::ProcessAllAvailable(false)
        | Command::Exception(false) => {
            return Err(SparkError::invalid(format!(
                "invalid streaming query command: {command:?}"
            )));
        }
    };
    let result = StreamingQueryCommandResult {
        query_id: Some(query_id),
        result_type,
    };
    let mut output = vec![ExecutorOutput::new(
        ExecutorBatch::StreamingQueryCommandResult(Box::new(result)),
    )];
    if metadata.reattachable {
        output.push(ExecutorOutput::complete());
    }
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        metadata.operation_id,
        Box::pin(stream::iter(output.into_iter().map(Ok))),
    ))
}

pub(crate) async fn handle_execute_get_resources_command(
    _ctx: &SessionContext,
    _resource: GetResourcesCommand,
    _metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    Err(SparkError::todo("get resources command"))
}

pub(crate) async fn handle_execute_streaming_query_manager_command(
    ctx: &SessionContext,
    command: StreamingQueryManagerCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    use crate::spark::connect::streaming_query_manager_command::{
        AwaitAnyTerminationCommand, Command,
    };
    use crate::spark::connect::streaming_query_manager_command_result::{
        ActiveResult, AwaitAnyTerminationResult, ResultType, StreamingQueryInstance,
    };

    let spark = ctx.extension::<SparkSession>()?;
    let StreamingQueryManagerCommand { command } = command;
    let command = command.required("streaming query manager command")?;
    let result_type = match command {
        Command::Active(true) => {
            let active_queries = spark
                .list_active_streaming_queries()?
                .into_iter()
                .map(|(id, status)| StreamingQueryInstance {
                    id: Some(id.into()),
                    name: Some(status.name),
                })
                .collect();
            Some(ResultType::Active(ActiveResult { active_queries }))
        }
        Command::GetQuery(id) => {
            let (id, status) = spark.find_streaming_query_by_query_id(&id)?;
            Some(ResultType::Query(StreamingQueryInstance {
                id: Some(id.into()),
                name: Some(status.name),
            }))
        }
        Command::AwaitAnyTermination(AwaitAnyTerminationCommand { timeout_ms }) => {
            let timeout = timeout_ms.map(timeout_millis).transpose()?;
            let handles = spark.await_streaming_queries()?;
            let terminated = handles.any_terminated(timeout).await?;
            Some(ResultType::AwaitAnyTermination(AwaitAnyTerminationResult {
                terminated,
            }))
        }
        Command::ResetTerminated(true) => {
            spark.reset_terminated_streaming_queries()?;
            Some(ResultType::ResetTerminated(true))
        }
        Command::AddListener(_) => {
            return Err(SparkError::NotImplemented("add listener".to_string()));
        }
        Command::RemoveListener(_) => {
            return Err(SparkError::NotImplemented("remove listener".to_string()));
        }
        Command::ListListeners(_) => {
            return Err(SparkError::NotImplemented("list listeners".to_string()));
        }
        Command::Active(false) | Command::ResetTerminated(false) => {
            return Err(SparkError::invalid(format!(
                "invalid streaming query manager command: {command:?}"
            )));
        }
    };
    let result = StreamingQueryManagerCommandResult { result_type };
    let mut output = vec![ExecutorOutput::new(
        ExecutorBatch::StreamingQueryManagerCommandResult(Box::new(result)),
    )];
    if metadata.reattachable {
        output.push(ExecutorOutput::complete());
    }
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        metadata.operation_id,
        Box::pin(stream::iter(output.into_iter().map(Ok))),
    ))
}

pub(crate) async fn handle_execute_register_table_function(
    ctx: &SessionContext,
    udtf: CommonInlineUserDefinedTableFunction,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let plan = spec::Plan::Command(spec::CommandPlan::new(
        spec::CommandNode::RegisterTableFunction(udtf.try_into()?),
    ));
    let mode = ExecutorMode::command();
    handle_execute_plan(ctx, plan, metadata, mode).await
}

pub(crate) async fn handle_execute_streaming_query_listener_bus_command(
    _ctx: &SessionContext,
    _command: StreamingQueryListenerBusCommand,
    _metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    Err(SparkError::NotImplemented(
        "streaming query listener bus".to_string(),
    ))
}

pub(crate) async fn handle_execute_checkpoint_command(
    ctx: &SessionContext,
    checkpoint: CheckpointCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let CheckpointCommand {
        relation,
        local: _,
        eager,
        storage_level,
    } = checkpoint;
    if !eager {
        return Err(SparkError::unsupported("lazy DataFrame checkpoint"));
    }
    if storage_level.is_some() {
        return Err(SparkError::unsupported(
            "checkpoint StorageLevel; Sail checkpoints are object-store backed",
        ));
    }
    let relation = relation.required("checkpoint relation")?;
    let query: spec::QueryPlan = relation.try_into()?;
    let relation_id = uuid::Uuid::new_v4().to_string();
    let plan = spec::Plan::Command(spec::CommandPlan::new(
        spec::CommandNode::RemoteCheckpoint {
            relation_id: relation_id.clone(),
            input: Box::new(query),
        },
    ));
    let mode = ExecutorMode::command_with_completion(move |_, _| {
        Ok(Some(ExecutorOutput::new(
            ExecutorBatch::CheckpointCommandResult(Box::new(CheckpointCommandResult {
                relation: Some(CachedRemoteRelation { relation_id }),
            })),
        )))
    });
    handle_execute_plan(ctx, plan, metadata, mode).await
}

pub(crate) async fn handle_execute_remove_cached_remote_relation_command(
    ctx: &SessionContext,
    _command: RemoveCachedRemoteRelationCommand,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = ctx.extension::<SparkSession>()?;
    // TODO: Remove checkpoint data on a best-effort basis when the client releases the relation.
    // Checkpoints have session lifetime so plans can safely share their immutable relation ID.
    // The registry and object-store namespace are cleared together after the job runner stops.
    let output = metadata
        .reattachable
        .then(ExecutorOutput::complete)
        .into_iter();
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        metadata.operation_id,
        Box::pin(stream::iter(output.into_iter().map(Ok))),
    ))
}

pub(crate) async fn handle_interrupt_all(ctx: &SessionContext) -> SparkResult<Vec<String>> {
    let spark = ctx.extension::<SparkSession>()?;
    let mut results = vec![];
    for executor in spark.remove_all_executors()? {
        executor.pause_if_running().await?;
        results.push(executor.metadata.operation_id.clone());
    }
    Ok(results)
}

pub(crate) async fn handle_interrupt_tag(
    ctx: &SessionContext,
    tag: String,
) -> SparkResult<Vec<String>> {
    let spark = ctx.extension::<SparkSession>()?;
    let mut results = vec![];
    for executor in spark.remove_executors_by_tag(tag.as_str())? {
        executor.pause_if_running().await?;
        results.push(executor.metadata.operation_id.clone());
    }
    Ok(results)
}

pub(crate) async fn handle_interrupt_operation_id(
    ctx: &SessionContext,
    operation_id: String,
) -> SparkResult<Vec<String>> {
    let spark = ctx.extension::<SparkSession>()?;
    match spark.remove_executor(operation_id.as_str())? {
        Some(executor) => {
            executor.pause_if_running().await?;
            Ok(vec![executor.metadata.operation_id.clone()])
        }
        None => Ok(vec![]),
    }
}

pub(crate) async fn handle_reattach_execute(
    ctx: &SessionContext,
    operation_id: String,
    response_id: Option<String>,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = ctx.extension::<SparkSession>()?;
    let executor = spark
        .get_executor(operation_id.as_str())?
        .ok_or_else(|| SparkError::invalid(format!("operation not found: {operation_id}")))?;
    if !executor.metadata.reattachable {
        return Err(SparkError::invalid(format!(
            "operation not reattachable: {operation_id}"
        )));
    }
    executor.pause_if_running().await?;
    if let Some(response_id) = response_id {
        executor.release(response_id)?;
    }
    let rx = executor.start()?;
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        operation_id,
        rx,
    ))
}

pub(crate) async fn handle_release_execute(
    ctx: &SessionContext,
    operation_id: String,
    response_id: Option<String>,
) -> SparkResult<()> {
    let spark = ctx.extension::<SparkSession>()?;
    let executor = spark.get_executor(operation_id.as_str())?;
    // TODO: clean up non-reattachable executors which the client does not release explicitly
    let Some(executor) = executor.filter(|executor| executor.metadata.reattachable) else {
        return Ok(());
    };
    if let Some(response_id) = response_id {
        executor.release(response_id)?;
    } else if let Some(executor) = spark.remove_executor(operation_id.as_str())? {
        executor.pause_if_running().await?;
    }
    Ok(())
}

pub(crate) async fn handle_execute_register_datasource(
    ctx: &SessionContext,
    datasource: CommonInlineUserDefinedDataSource,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    use crate::spark::connect::common_inline_user_defined_data_source::DataSource;

    log::info!(
        "RegisterDataSource handler called for datasource: {}",
        datasource.name
    );

    let spark = ctx.extension::<SparkSession>()?;
    let name = datasource.name.clone();

    // Extract the pickled Python datasource class
    let command = match datasource.data_source {
        Some(DataSource::PythonDataSource(pds)) => pds.command,
        None => {
            return Err(SparkError::invalid(
                "RegisterDataSource requires a python_data_source",
            ));
        }
    };

    // Register in the session-scoped TableFormatRegistry with embedded pickled bytes
    {
        use std::sync::Arc;

        use sail_common_datafusion::datasource::TableFormatRegistry;
        use sail_data_source::formats::python::PythonTableFormat;

        // Register format in session's TableFormatRegistry with embedded pickled class
        // This provides session isolation - the format is only visible to this session
        match ctx.extension::<TableFormatRegistry>() {
            Ok(registry) => {
                let format = Arc::new(PythonTableFormat::with_pickled_class(name.clone(), command));
                // Ignore error if already registered (allows re-registration to update)
                if let Err(e) = registry.register(format) {
                    warn!("Failed to register python datasource {}: {}", name, e);
                }
                log::info!("Registered session-scoped datasource: {}", name);
            }
            _ => {
                return Err(SparkError::internal(
                    "TableFormatRegistry not found in session context",
                ));
            }
        }
    }

    // Return empty success response
    let mut output = vec![];
    if metadata.reattachable {
        output.push(ExecutorOutput::complete());
    }
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        metadata.operation_id,
        Box::pin(stream::iter(output.into_iter().map(Ok))),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::Arc;

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::error::ArrowError;
    use datafusion::arrow::ipc::reader::StreamReader;
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
    use pyo3::Python;
    use sail_common::config::AppConfig;
    use sail_common::runtime::RuntimeManager;
    use tonic::codegen::tokio_stream::StreamExt;
    use uuid::Uuid;

    use super::*;
    use crate::session_manager::create_spark_session_manager;
    use crate::spark::connect::execute_plan_response::ResponseType;
    use crate::spark::connect::relation::RelType;
    use crate::spark::connect::{Relation, SqlCommand as SparkSqlCommand, execute_plan_response};

    fn executor_metadata() -> ExecutorMetadata {
        ExecutorMetadata {
            operation_id: Uuid::new_v4().to_string(),
            tags: vec![],
            reattachable: false,
        }
    }

    fn sql_command(sql: &str) -> SparkSqlCommand {
        #[expect(deprecated)]
        SparkSqlCommand {
            sql: sql.to_string(),
            args: HashMap::new(),
            pos_args: vec![],
            named_arguments: HashMap::new(),
            pos_arguments: vec![],
            input: None,
        }
    }

    async fn execute_sql_command(ctx: &SessionContext, sql: &str) -> SparkResult<Option<Relation>> {
        let mut stream =
            handle_execute_sql_command(ctx, sql_command(sql), executor_metadata()).await?;
        while let Some(response) = stream.next().await {
            let response = response.map_err(|e| SparkError::internal(e.to_string()))?;
            if let Some(ResponseType::SqlCommandResult(result)) = response.response_type {
                return Ok(result.relation);
            }
        }
        Ok(None)
    }

    async fn execute_relation(
        ctx: &SessionContext,
        relation: Relation,
    ) -> SparkResult<Vec<String>> {
        let mut stream = handle_execute_relation(ctx, relation, executor_metadata()).await?;
        let mut batches = vec![];
        while let Some(response) = stream.next().await {
            let response = response.map_err(|e| SparkError::internal(e.to_string()))?;
            if let Some(ResponseType::ArrowBatch(execute_plan_response::ArrowBatch {
                data, ..
            })) = response.response_type
            {
                batches.extend(decode_arrow_batches(data)?);
            }
        }
        format_record_batches(batches)
    }

    fn decode_arrow_batches(data: Vec<u8>) -> SparkResult<Vec<RecordBatch>> {
        let reader = StreamReader::try_new(Cursor::new(data), None)?;
        reader
            .collect::<Result<Vec<_>, ArrowError>>()
            .map_err(Into::into)
    }

    fn format_record_batches(batches: Vec<RecordBatch>) -> SparkResult<Vec<String>> {
        let options = FormatOptions::default();
        let mut output = vec![];
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let formatters = batch
                .columns()
                .iter()
                .map(|column| ArrayFormatter::try_new(column, &options))
                .collect::<Result<Vec<_>, ArrowError>>()?;
            for row in 0..batch.num_rows() {
                let line = formatters
                    .iter()
                    .map(|formatter| formatter.value(row).try_to_string())
                    .collect::<Result<Vec<_>, ArrowError>>()?
                    .join("\t");
                output.push(line);
            }
        }
        Ok(output)
    }

    #[test]
    fn test_execute_cypher_sql_command_relation() -> Result<(), Box<dyn std::error::Error>> {
        Python::initialize();
        let config = Arc::new(AppConfig::load()?);
        let runtime = RuntimeManager::try_new(&config.runtime)?;
        let handle = runtime.handle();
        let manager = handle
            .primary()
            .block_on(async { create_spark_session_manager(config, handle.clone()) })?;
        let ctx = handle
            .primary()
            .block_on(manager.get_or_create_session_context("graph".to_string(), "".to_string()))?;

        handle.primary().block_on(async {
            execute_sql_command(
                &ctx,
                "CREATE OR REPLACE TEMPORARY VIEW grust_nodes AS \
                 SELECT * FROM VALUES \
                 ('1', 'Person', '{\"age\":\"42\",\"name\":\"Alice\"}'), \
                 ('2', 'Person', '{\"age\":\"31\",\"name\":\"Bob\"}'), \
                 ('3', 'Document', '{\"age\":\"42\",\"name\":\"Paper\"}') \
                 AS tab(id, label, props)",
            )
            .await?;
            execute_sql_command(
                &ctx,
                "CREATE OR REPLACE TEMPORARY VIEW grust_edges AS \
                 SELECT * FROM VALUES \
                 ('edge-key-1', 'edge-1', '1', 'Person', '2', 'Person', 'KNOWS', '{\"since\":\"2020\"}'), \
                 ('edge-key-2', 'edge-2', '2', 'Person', '1', 'Person', 'LIKES', '{\"since\":\"2021\"}'), \
                 ('edge-key-3', 'edge-3', '3', 'Document', '2', 'Person', 'KNOWS', '{\"since\":\"2022\"}') \
                 AS tab(edge_key, id, src_id, src_label, dst_id, dst_label, edge_type, props)",
            )
            .await?;

            async fn assert_cypher_rows(
                ctx: &SessionContext,
                sql: &str,
                expected: Vec<&str>,
            ) -> SparkResult<()> {
                let relation = execute_sql_command(ctx, sql)
                    .await?
                    .ok_or_else(|| SparkError::internal("missing SQL command relation"))?;
                assert!(matches!(relation.rel_type, Some(RelType::Sql(_))));

                let rows = execute_relation(ctx, relation).await?;
                assert_eq!(rows, expected);
                Ok(())
            }

            assert_cypher_rows(
                &ctx,
                "MATCH (a:Person)-[e:KNOWS]->(b:Person) \
                 WHERE a.age = '42' \
                 RETURN a.id, e.id, e.edge_key, e.label, b.name \
                 ORDER BY b.name \
                 LIMIT 10",
                vec!["1\tedge-1\tedge-key-1\tKNOWS\tBob"],
            )
            .await?;
            assert_cypher_rows(
                &ctx,
                "MATCH (a:Person {age: '42'})-[e:KNOWS {since: '2020'}]->(b:Person {name: 'Bob'}) \
                 RETURN a.id, e.id, b.name",
                vec!["1\tedge-1\tBob"],
            )
            .await?;
            assert_cypher_rows(
                &ctx,
                "MATCH (a:Person)-->(b:Person) \
                 WHERE a.age = '42' \
                 RETURN b.name \
                 ORDER BY b.name \
                 LIMIT 10",
                vec!["Bob"],
            )
            .await?;
            assert_cypher_rows(
                &ctx,
                "MATCH (a:Person)-->(b:Person) \
                 RETURN b.name \
                 ORDER BY b.name \
                 SKIP 1 \
                 LIMIT ALL",
                vec!["Bob"],
            )
            .await?;
            assert_cypher_rows(
                &ctx,
                "MATCH (a)<-[e]-(b) \
                 RETURN a.id, e.id, b.id \
                 ORDER BY e.id",
                vec!["2\tedge-1\t1", "1\tedge-2\t2", "2\tedge-3\t3"],
            )
            .await?;
            assert_cypher_rows(
                &ctx,
                "MATCH (a)-[e]-(b) \
                 RETURN a.id, e.id, b.id \
                 ORDER BY e.id, a.id",
                vec![
                    "1\tedge-1\t2",
                    "2\tedge-1\t1",
                    "1\tedge-2\t2",
                    "2\tedge-2\t1",
                    "2\tedge-3\t3",
                    "3\tedge-3\t2",
                ],
            )
            .await?;
            assert_cypher_rows(
                &ctx,
                "MATCH (a:Person)-[e:KNOWS]->(b:Person), (b)<-[f:KNOWS]-(d:Document) \
                 WHERE a.age = '42' \
                 RETURN a.id, b.name, d.name, e.id, f.id \
                 ORDER BY d.name \
                 LIMIT 10",
                vec!["1\tBob\tPaper\tedge-1\tedge-3"],
            )
            .await?;
            Ok::<_, SparkError>(())
        })?;

        Ok(())
    }
}
