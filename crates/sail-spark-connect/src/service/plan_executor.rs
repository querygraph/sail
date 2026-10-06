use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::compute::concat_batches;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use fastrace::Span;
use fastrace::collector::SpanContext;
use fastrace::future::FutureExt;
use futures::stream;
use log::debug;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::plan_reuse::{
    PLAN_CACHE_OPTION, PlanReuse, TARGET_PARTITIONS_OPTION, reset_plan_keep_properties,
};
use sail_common_datafusion::session::job::JobService;
use sail_plan::{resolve_and_execute_plan, resolve_and_plan_physical};
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
    let spark = ctx.extension::<SparkSession>()?;
    let plan = resolve_and_plan_physical(ctx, spark.plan_config()?, plan).await?;
    execute_physical_plan(ctx, &spark, plan, metadata, mode).await
}

async fn execute_physical_plan(
    ctx: &SessionContext,
    spark: &SparkSession,
    plan: Arc<dyn ExecutionPlan>,
    metadata: ExecutorMetadata,
    mode: ExecutorMode,
) -> SparkResult<ExecutePlanResponseStream> {
    let span = Span::root("handle_execute_plan", SpanContext::random());
    let service = ctx.extension::<JobService>()?;
    let operation_id = metadata.operation_id.clone();
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

fn plan_cache_enabled(spark: &SparkSession) -> SparkResult<bool> {
    Ok(config_value(spark, PLAN_CACHE_OPTION)?
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("true")))
}

pub(crate) async fn handle_execute_relation(
    ctx: &SessionContext,
    relation: Relation,
    metadata: ExecutorMetadata,
) -> SparkResult<ExecutePlanResponseStream> {
    let spark = ctx.extension::<SparkSession>()?;
    let reuse = ctx.extension::<PlanReuse>()?;
    if !plan_cache_enabled(&spark)? {
        reuse.clear_plans()?;
        return handle_execute_plan(ctx, relation.try_into()?, metadata, ExecutorMode::Query).await;
    }
    // The plan cache (`sail_common_datafusion::plan_reuse`) is keyed by the
    // relation as received, so that a cached query is not even parsed again.
    // The client numbers every DataFrame it builds (`plan_id`, in the
    // relation's common fields), so the key leaves that out.
    let key = relation_key(&relation);
    let plan = match reuse.cached_plan(&key)? {
        Some(plan) => plan,
        None => {
            if let Some(partitions) = config_value(&spark, TARGET_PARTITIONS_OPTION)?
                .and_then(|v| v.trim().parse::<usize>().ok())
                .filter(|n| *n > 0)
            {
                ctx.state_ref()
                    .write()
                    .config_mut()
                    .options_mut()
                    .execution
                    .target_partitions = partitions;
            }
            let plan =
                resolve_and_plan_physical(ctx, spark.plan_config()?, relation.try_into()?).await?;
            reuse.cache_plan(key, Arc::clone(&plan))?;
            plan
        }
    };
    let plan = reset_plan_keep_properties(plan)?;
    // A cached plan runs here, in the server process, not through the job
    // runner: the slots it reads live in this process, and the runner's
    // per-operator tracing costs more than the work of a small plan.
    let span = Span::root("handle_execute_plan", SpanContext::random());
    let _guard = span.set_local_parent();
    let stream = execute_stream(plan, ctx.task_ctx())?;
    let operation_id = metadata.operation_id.clone();
    let executor = Executor::new(
        metadata,
        stream,
        spark.options().execution_heartbeat_interval,
        ExecutorMode::Query,
    );
    let rx = executor.start()?;
    spark.add_executor(executor)?;
    Ok(ExecutePlanResponseStream::new(
        spark.session_id().to_string(),
        operation_id,
        rx,
    ))
}

/// The plan cache key of a relation: a 128-bit hash of its protobuf
/// encoding without the common fields. Formatting a large query's relation
/// for the key cost as much as running its cached plan.
fn relation_key(relation: &Relation) -> String {
    use std::hash::{BuildHasher, Hasher};

    use prost::Message;

    let bytes = relation.rel_type.as_ref().map(|r| {
        Relation {
            common: None,
            rel_type: Some(r.clone()),
        }
        .encode_to_vec()
    });
    let bytes = bytes.unwrap_or_default();
    let hash = |seed: u64| {
        let mut hasher =
            std::hash::BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default()
                .build_hasher();
        hasher.write_u64(seed);
        hasher.write(&bytes);
        hasher.finish()
    };
    format!("{:016x}{:016x}{}", hash(0), hash(1), bytes.len())
}

fn config_value(spark: &SparkSession, key: &str) -> SparkResult<Option<String>> {
    Ok(spark
        .get_config_option(vec![key.to_string()])?
        .into_iter()
        .next()
        .and_then(|kv| kv.value))
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

    // Register in the session-scoped DataSourceRegistry with embedded pickled bytes.
    {
        use std::sync::Arc;

        use sail_common_datafusion::datasource::DataSourceRegistry;
        use sail_data_source::formats::python::PythonDataSourceAdapter;

        // The embedded class keeps the source isolated to this session.
        match ctx.extension::<DataSourceRegistry>() {
            Ok(registry) => {
                let source = Arc::new(PythonDataSourceAdapter::with_pickled_class(
                    name.clone(),
                    command,
                ));
                registry.register_data_source(source)?;
                log::info!("Registered session-scoped datasource: {}", name);
            }
            _ => {
                return Err(SparkError::internal(
                    "DataSourceRegistry not found in session context",
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
