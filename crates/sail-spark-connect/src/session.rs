use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::StringifiedPlan;
use futures::future::join_all;
use sail_common::utils::datetime::get_system_timezone;
use sail_common_datafusion::extension::SessionExtension;
use sail_common_datafusion::session::lifecycle::SessionOperations;
#[cfg(feature = "orc")]
use sail_orc::{OrcListingSessionDefaults, OrcReadSessionModes, OrcWriteSessionDefaults};
use sail_plan::config::PlanConfig;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::{ConfigKeyValue, EncoderSchemaSettings, SparkRuntimeConfig};
use crate::error::{
    SparkError, SparkResult, SparkThrowable, StructuredSparkError, session_closed_error,
};
use crate::executor::Executor;
use crate::pipelines::PipelineGraph;
use crate::spark::config::SparkConfigKey;
use crate::streaming::{
    StreamingQuery, StreamingQueryAwaitHandle, StreamingQueryAwaitHandleSet, StreamingQueryId,
    StreamingQueryManager, StreamingQueryStatus,
};

#[derive(Debug, Clone)]
pub(crate) struct SparkSessionOptions {
    pub execution_heartbeat_interval: Duration,
}

/// A Spark session extension to the DataFusion [`SessionContext`].
///
/// [`SessionContext`]: datafusion::prelude::SessionContext
pub(crate) struct SparkSession {
    session_id: String,
    user_id: String,
    options: SparkSessionOptions,
    state: Mutex<SparkSessionState>,
    /// The number of requests that hold a [`SessionOperationLease`].
    requests: watch::Sender<usize>,
    closing: Mutex<ClosingOperations>,
}

/// The right of a request to use the session, from the time that the request obtains the
/// session context until it returns. Closing the session waits for it, since a context that was
/// obtained earlier can still be used. Whatever outlives the request is registered in the
/// session, and registration is refused once the session is closing.
pub(crate) struct SessionOperationLease {
    session: Arc<SparkSession>,
}

impl Drop for SessionOperationLease {
    fn drop(&mut self) {
        // The count is only used to wait for requests, so it cannot be affected by poison.
        let _state = self
            .session
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.session.requests.send_modify(|x| *x -= 1);
    }
}

/// The operations that were taken out of a closing session, to be waited for.
#[derive(Default)]
struct ClosingOperations {
    executors: Vec<Arc<Executor>>,
    streaming_queries: Vec<JoinHandle<()>>,
    pipelines: Vec<Arc<Mutex<PipelineGraph>>>,
}

impl Debug for SparkSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparkSession")
            .field("session_id", &self.session_id)
            .field("user_id", &self.user_id)
            .field("options", &self.options)
            .finish()
    }
}

impl SessionExtension for SparkSession {
    fn name() -> &'static str {
        "spark session"
    }
}

impl SparkSession {
    pub(crate) fn try_new(
        session_id: String,
        user_id: String,
        options: SparkSessionOptions,
    ) -> SparkResult<Self> {
        let extension = Self {
            session_id,
            user_id,
            options,
            state: Mutex::new(SparkSessionState::try_new()?),
            requests: watch::channel(0).0,
            closing: Mutex::new(ClosingOperations::default()),
        };
        extension.set_config(vec![ConfigKeyValue {
            key: SparkConfigKey::SPARK_SQL_SESSION_TIME_ZONE.to_string(),
            value: Some(get_system_timezone()?),
        }])?;
        Ok(extension)
    }

    /// Admit a request to the session, or refuse it if the session is closing.
    pub(crate) fn begin_operation(self: &Arc<Self>) -> SparkResult<SessionOperationLease> {
        let state = self.state.lock()?;
        state.check_open(&self.session_id)?;
        self.requests.send_modify(|x| *x += 1);
        Ok(SessionOperationLease {
            session: self.clone(),
        })
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn user_id(&self) -> &str {
        &self.user_id
    }

    pub(crate) fn options(&self) -> &SparkSessionOptions {
        &self.options
    }

    pub(crate) fn plan_config(&self) -> SparkResult<Arc<PlanConfig>> {
        let state = self.state.lock()?;
        let mut config = PlanConfig::try_from(&state.config)?;
        config.session_user_id = self.user_id().to_string();
        Ok(Arc::new(config))
    }

    #[cfg(feature = "orc")]
    pub(crate) fn orc_config(
        &self,
    ) -> SparkResult<(
        OrcListingSessionDefaults,
        OrcReadSessionModes,
        OrcWriteSessionDefaults,
    )> {
        let state = self.state.lock()?;
        Ok((
            state.config.orc_listing_defaults()?,
            state.config.orc_read_modes(),
            state.config.orc_write_defaults()?,
        ))
    }

    pub(crate) fn plan_config_with_overrides(
        &self,
        overrides: impl IntoIterator<Item = (String, String)>,
    ) -> SparkResult<Arc<PlanConfig>> {
        let mut runtime = self.state.lock()?.config.clone();
        for (key, value) in overrides {
            runtime.set(key, value)?;
        }
        let mut config = PlanConfig::try_from(&runtime)?;
        config.session_user_id = self.user_id().to_string();
        Ok(Arc::new(config))
    }

    /// One snapshot of a pipeline flow call's effective settings. The shared
    /// session configuration is not mutated.
    pub(crate) fn flow_call_config(
        &self,
        overrides: impl IntoIterator<Item = (String, String)>,
    ) -> SparkResult<(Arc<PlanConfig>, EncoderSchemaSettings)> {
        let mut runtime = self.state.lock()?.config.clone();
        for (key, value) in overrides {
            runtime.set(key, value)?;
        }
        let settings = runtime.encoder_schema_settings()?;
        let mut config = PlanConfig::try_from(&runtime)?;
        config.session_user_id = self.user_id().to_string();
        Ok((Arc::new(config), settings))
    }

    /// The session's raw view read check setting, for callers holding a flow overlay.
    pub(crate) fn incompatible_view_check(&self) -> SparkResult<Option<String>> {
        Ok(self.state.lock()?.config.incompatible_view_check())
    }

    pub(crate) fn add_pipeline(&self, graph: PipelineGraph) -> SparkResult<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let mut state = self.state.lock()?;
        state.check_open(&self.session_id)?;
        state
            .pipelines
            .insert(id.clone(), Arc::new(Mutex::new(graph)));
        Ok(id)
    }

    pub(crate) fn pipeline(&self, id: &str) -> SparkResult<Arc<Mutex<PipelineGraph>>> {
        self.state
            .lock()?
            .pipelines
            .get(id)
            .cloned()
            .ok_or_else(|| {
                Box::new(StructuredSparkError {
                    class: "org.apache.spark.SparkException".into(),
                    condition: "DATAFLOW_GRAPH_NOT_FOUND".into(),
                    sql_state: "KD011".into(),
                    message: format!("Dataflow graph with id {id} could not be found"),
                    parameters: HashMap::from([("graphId".into(), id.into())]),
                })
                .into()
            })
    }

    pub(crate) fn drop_pipeline(&self, id: &str) -> SparkResult<()> {
        if let Some(graph) = self.state.lock()?.pipelines.remove(id) {
            graph.lock()?.cancel();
        }
        Ok(())
    }

    pub(crate) fn get_config(&self, keys: Vec<String>) -> SparkResult<Vec<ConfigKeyValue>> {
        let state = self.state.lock()?;
        keys.into_iter()
            .map(|key| {
                let value = state.config.get(&key)?.map(|v| v.to_string());
                Ok(ConfigKeyValue { key, value })
            })
            .collect::<SparkResult<Vec<_>>>()
    }

    pub(crate) fn get_config_option(&self, keys: Vec<String>) -> SparkResult<Vec<ConfigKeyValue>> {
        let state = self.state.lock()?;
        let kv = keys
            .into_iter()
            .map(|key| {
                let value = state.config.get_option(&key).map(|x| x.to_string());
                ConfigKeyValue { key, value }
            })
            .collect();
        Ok(kv)
    }

    pub(crate) fn get_config_with_default(
        &self,
        kv: Vec<ConfigKeyValue>,
    ) -> SparkResult<Vec<ConfigKeyValue>> {
        let state = self.state.lock()?;
        let kv = kv
            .into_iter()
            .map(|ConfigKeyValue { key, value }| {
                let value = state
                    .config
                    .get_with_default(&key, value.as_deref())
                    .map(|x| x.to_string());
                ConfigKeyValue { key, value }
            })
            .collect();
        Ok(kv)
    }

    pub(crate) fn set_config(&self, kv: Vec<ConfigKeyValue>) -> SparkResult<()> {
        let mut state = self.state.lock()?;
        for ConfigKeyValue { key, value } in kv {
            if let Some(value) = value {
                state.config.set(key, value)?;
            } else {
                return Err(SparkError::invalid(format!(
                    "value is required for configuration: {key}"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn unset_config(&self, keys: Vec<String>) -> SparkResult<()> {
        let mut state = self.state.lock()?;
        for key in keys {
            state.config.unset(&key)?
        }
        Ok(())
    }

    pub(crate) fn get_all_config(&self, prefix: Option<&str>) -> SparkResult<Vec<ConfigKeyValue>> {
        let state = self.state.lock()?;
        state.config.get_all(prefix)
    }

    pub(crate) fn add_native_config_key(&self, key: String) -> SparkResult<()> {
        self.state.lock()?.native_config_keys.insert(key);
        Ok(())
    }

    pub(crate) fn remove_native_config_key(&self, key: &str) -> SparkResult<()> {
        self.state.lock()?.native_config_keys.remove(key);
        Ok(())
    }

    pub(crate) fn native_config_keys(&self) -> SparkResult<Vec<String>> {
        Ok(self
            .state
            .lock()?
            .native_config_keys
            .iter()
            .cloned()
            .collect())
    }

    pub(crate) fn get_config_warnings(&self, kv: &[ConfigKeyValue]) -> SparkResult<Vec<String>> {
        let state = self.state.lock()?;
        Ok(state.config.get_warnings(kv))
    }

    pub(crate) fn get_config_warnings_by_keys(&self, keys: &[String]) -> SparkResult<Vec<String>> {
        let state = self.state.lock()?;
        Ok(state.config.get_warnings_by_keys(keys))
    }

    pub(crate) fn is_config_modifiable(&self, key: &str) -> SparkResult<bool> {
        let state = self.state.lock()?;
        Ok(state.config.is_modifiable(key))
    }

    /// Register an executor, which must happen before it is started,
    /// so that closing the session cannot miss a task that has been started.
    pub(crate) fn add_executor(&self, executor: Executor) -> SparkResult<Arc<Executor>> {
        let mut state = self.state.lock()?;
        state.check_open(&self.session_id)?;
        let id = executor.metadata.operation_id.clone();
        let executor = Arc::new(executor);
        state.executors.insert(id, executor.clone());
        Ok(executor)
    }

    pub(crate) fn get_executor(&self, id: &str) -> SparkResult<Option<Arc<Executor>>> {
        let state = self.state.lock()?;
        Ok(state.executors.get(id).cloned())
    }

    pub(crate) fn remove_executor(&self, id: &str) -> SparkResult<Option<Arc<Executor>>> {
        let mut state = self.state.lock()?;
        Ok(state
            .executors
            .remove_entry(id)
            .map(|(_, executor)| executor))
    }

    pub(crate) fn all_executors(&self) -> SparkResult<Vec<Arc<Executor>>> {
        let state = self.state.lock()?;
        Ok(state.executors.values().cloned().collect())
    }

    pub(crate) fn executors_by_tag(&self, tag: &str) -> SparkResult<Vec<Arc<Executor>>> {
        let state = self.state.lock()?;
        Ok(state
            .executors
            .values()
            .filter(|executor| executor.metadata.tags.iter().any(|value| value == tag))
            .cloned()
            .collect())
    }

    pub(crate) fn start_streaming_query(
        &self,
        name: String,
        info: Vec<StringifiedPlan>,
        stream: SendableRecordBatchStream,
    ) -> SparkResult<StreamingQueryId> {
        if !stream.schema().fields().is_empty() {
            return Err(SparkError::invalid(
                "streaming query must write data to a sink",
            ));
        }
        // Here we always generate new query ID and run ID regardless of whether the query
        // is started from a checkpoint. This may be different from the Spark behavior.
        let id = StreamingQueryId {
            query_id: uuid::Uuid::new_v4().to_string(),
            run_id: uuid::Uuid::new_v4().to_string(),
        };
        let mut state = self.state.lock()?;
        state.check_open(&self.session_id)?;
        let query = StreamingQuery::new(name, info, stream);
        state.streaming_queries.add_query(id.clone(), query);
        Ok(id)
    }

    pub(crate) fn stop_streaming_query(&self, id: &StreamingQueryId) -> SparkResult<()> {
        let mut state = self.state.lock()?;
        state.streaming_queries.stop_query(id)?;
        Ok(())
    }

    pub(crate) fn explain_streaming_query(
        &self,
        id: &StreamingQueryId,
        extended: bool,
    ) -> SparkResult<String> {
        let state = self.state.lock()?;
        state.streaming_queries.explain_query(id, extended)
    }

    pub(crate) fn get_streaming_query_status(
        &self,
        id: &StreamingQueryId,
    ) -> SparkResult<StreamingQueryStatus> {
        let state = self.state.lock()?;
        state.streaming_queries.get_query_status(id)
    }

    pub(crate) fn get_streaming_query_exception(
        &self,
        id: &StreamingQueryId,
    ) -> SparkResult<Option<SparkThrowable>> {
        let state = self.state.lock()?;
        state.streaming_queries.get_query_error(id)
    }

    pub(crate) fn await_streaming_query(
        &self,
        id: &StreamingQueryId,
    ) -> SparkResult<Option<StreamingQueryAwaitHandle>> {
        let state = self.state.lock()?;
        state.streaming_queries.await_query(id)
    }

    pub(crate) fn await_streaming_queries(&self) -> SparkResult<StreamingQueryAwaitHandleSet> {
        let state = self.state.lock()?;
        state.streaming_queries.await_queries()
    }

    pub(crate) fn list_active_streaming_queries(
        &self,
    ) -> SparkResult<Vec<(StreamingQueryId, StreamingQueryStatus)>> {
        let state = self.state.lock()?;
        Ok(state.streaming_queries.list_active_queries())
    }

    pub(crate) fn find_streaming_query_by_query_id(
        &self,
        query_id: &str,
    ) -> SparkResult<(StreamingQueryId, StreamingQueryStatus)> {
        let state = self.state.lock()?;
        state.streaming_queries.find_query_by_query_id(query_id)
    }

    pub(crate) fn reset_terminated_streaming_queries(&self) -> SparkResult<()> {
        let mut state = self.state.lock()?;
        state.streaming_queries.reset_stopped_queries();
        Ok(())
    }

    fn begin_close_operations(&self) -> SparkResult<()> {
        let (executors, streaming_queries, pipelines) = {
            let mut state = self.state.lock()?;
            if state.closed {
                return Ok(());
            }
            state.closed = true;
            let executors = state.executors.drain().map(|(_, x)| x).collect::<Vec<_>>();
            let streaming_queries = state.streaming_queries.close_queries();
            let pipelines = mem::take(&mut state.pipelines);
            (executors, streaming_queries, pipelines)
        };
        // Every executor is signaled before anything is awaited, since each may be waiting for
        // a job that the job runner is about to stop.
        let mut first_error = None;
        for executor in &executors {
            if let Err(error) = executor.request_close(&self.session_id) {
                first_error.get_or_insert(error);
            }
        }
        *self.closing.lock()? = ClosingOperations {
            executors,
            streaming_queries,
            pipelines: pipelines.into_values().collect(),
        };
        first_error.map_or(Ok(()), Err)
    }

    async fn wait_operations_closed(&self) -> SparkResult<()> {
        let ClosingOperations {
            executors,
            streaming_queries,
            pipelines,
        } = mem::take(&mut *self.closing.lock()?);
        let closed = join_all(executors.iter().map(|x| x.closed())).await;
        // Closing an executor drops the pipeline run that it delivers. Cancelling the graph is
        // for a run that no executor holds any more, and doing this first would replace the
        // error that the client of a run should see.
        for graph in pipelines {
            graph.lock()?.cancel();
        }
        let joined = join_all(streaming_queries).await;
        let mut requests = self.requests.subscribe();
        // The sender is in `self`, so this cannot fail while `self` is alive.
        let _ = requests.wait_for(|x| *x == 0).await;
        closed.into_iter().collect::<SparkResult<Vec<_>>>()?;
        for result in joined {
            result?;
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl SessionOperations for SparkSession {
    fn begin_close(&self) -> DataFusionResult<()> {
        self.begin_close_operations()
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    async fn wait_closed(&self) -> DataFusionResult<()> {
        self.wait_operations_closed()
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }
}

struct SparkSessionState {
    /// Whether the session is closing. No operation can be admitted or registered after this.
    closed: bool,
    config: SparkRuntimeConfig,
    native_config_keys: BTreeSet<String>,
    executors: HashMap<String, Arc<Executor>>,
    streaming_queries: StreamingQueryManager,
    pipelines: HashMap<String, Arc<Mutex<PipelineGraph>>>,
}

impl SparkSessionState {
    fn try_new() -> SparkResult<Self> {
        Ok(Self {
            closed: false,
            config: SparkRuntimeConfig::try_new()?,
            native_config_keys: BTreeSet::new(),
            executors: HashMap::new(),
            streaming_queries: StreamingQueryManager::new(),
            pipelines: HashMap::new(),
        })
    }

    fn check_open(&self, session_id: &str) -> SparkResult<()> {
        if self.closed {
            return Err(session_closed_error(session_id));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "session_lifecycle_tests.rs"]
mod lifecycle_tests;
