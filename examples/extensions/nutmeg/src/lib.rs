//! Independently compiled Nutmeg Connect extension. No Sail engine dependency.
mod argentea;
mod checkpoint;
mod context;
mod diagnostics;
mod mutation;
use std::sync::{Arc, LazyLock};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use context::{ContextProvider, OwnedProvider};
use datafusion::catalog::{Session, TableProvider};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{Result, plan_err};
use datafusion_execution::{TaskContext, TaskContextProvider};
use datafusion_ffi::execution_plan::FFI_ExecutionPlan;
use datafusion_ffi::table_provider::FFI_TableProvider;
use nutmeg_graph::{AlgorithmExec, AlgorithmTable, ColumnNames, SessionRegistry};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use sail_native_resource_ffi::{MEMORY_LEASE_CAPSULE, MemoryLease};
use serde::Deserialize;

pub const TYPE_URL: &str = "type.googleapis.com/nutmeg.v1.NutmegApi";
static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("Nutmeg runtime")
});

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub version: u32,
    pub verb: String,
    pub graph: String,
    #[serde(default)]
    pub algorithm: Option<String>,
    #[serde(default)]
    pub options: serde_json::Map<String, serde_json::Value>,
    #[serde(default, rename = "columnNames")]
    pub column_names: Option<String>,
    #[serde(default, rename = "nodeMapping")]
    pub node_mapping: std::collections::BTreeMap<String, String>,
    #[serde(default, rename = "edgeMapping")]
    pub edge_mapping: std::collections::BTreeMap<String, String>,
}

fn plan(
    registry: &SessionRegistry,
    type_url: &str,
    payload: &[u8],
    inputs: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn TableProvider>> {
    if type_url != TYPE_URL {
        return plan_err!("nutmeg: unclaimed type URL {type_url}");
    }
    let request: Request = serde_json::from_slice(payload).map_err(|e| {
        datafusion_common::DataFusionError::Plan(format!("nutmeg: invalid JSON payload: {e}"))
    })?;
    if request.version != 1 {
        return plan_err!("nutmeg: unsupported payload version {}", request.version);
    }
    if request.graph.is_empty() || request.graph.len() > 1024 {
        return plan_err!("nutmeg: graph name must contain 1..1024 bytes");
    }
    match request.verb.as_str() {
        "stage" => {
            if inputs.len() != 2 {
                return plan_err!(
                    "nutmeg: stage requires a Sail envelope with two inputs [nodes, edges]"
                );
            }
            if request.algorithm.is_some()
                || !request.options.is_empty()
                || request.column_names.is_some()
            {
                return plan_err!("nutmeg: algorithm/options/columnNames apply only to run");
            }
            Ok(Arc::new(mutation::MutationTable::stage(
                registry.clone(),
                request,
                inputs,
            )?))
        }
        "run" => {
            if !inputs.is_empty() {
                return plan_err!("nutmeg: run accepts no inputs");
            }
            if !request.node_mapping.is_empty() || !request.edge_mapping.is_empty() {
                return plan_err!("nutmeg: column mappings apply only to stage");
            }
            let Some(algorithm) = request.algorithm else {
                return plan_err!("nutmeg: run requires algorithm");
            };
            let names = ColumnNames::parse(request.column_names.as_deref().unwrap_or("grust"))?;
            Ok(Arc::new(StreamingTable(registry.algorithm(
                &algorithm,
                &request.graph,
                &request.options,
                names,
            )?)))
        }
        "nodes" | "edges" | "diagnostics" => {
            if !inputs.is_empty()
                || request.algorithm.is_some()
                || !request.options.is_empty()
                || request.column_names.is_some()
                || !request.node_mapping.is_empty()
                || !request.edge_mapping.is_empty()
            {
                return plan_err!(
                    "nutmeg: nodes/edges/diagnostics accept only version, verb and graph"
                );
            }
            if request.verb == "nodes" {
                registry.nodes(&request.graph)
            } else if request.verb == "edges" {
                registry.edges(&request.graph)
            } else {
                Ok(Arc::new(diagnostics::DiagnosticsTable(registry.clone())))
            }
        }
        "drop" => {
            if !inputs.is_empty() {
                return plan_err!("nutmeg: drop accepts no inputs");
            }
            if request.algorithm.is_some()
                || !request.options.is_empty()
                || request.column_names.is_some()
                || !request.node_mapping.is_empty()
                || !request.edge_mapping.is_empty()
            {
                return plan_err!("nutmeg: drop accepts only version, verb and graph");
            }
            Ok(Arc::new(mutation::MutationTable::drop(
                registry.clone(),
                request.graph,
            )))
        }
        "checkpoint" | "checkpointed" => {
            let writing = request.verb == "checkpoint";
            if writing && inputs.len() != 1 {
                return plan_err!("nutmeg: checkpoint requires a Sail envelope with one input");
            }
            if !writing && !inputs.is_empty() {
                return plan_err!("nutmeg: checkpointed accepts no inputs");
            }
            if request.algorithm.is_some()
                || request.column_names.is_some()
                || !request.node_mapping.is_empty()
                || !request.edge_mapping.is_empty()
            {
                return plan_err!("nutmeg: checkpoint/checkpointed accept only version, verb, graph and options");
            }
            let mut options = request.options;
            let path = match options.remove("path") {
                Some(serde_json::Value::String(path)) => path,
                _ => return plan_err!("nutmeg: checkpointed requires options.path (string)"),
            };
            let key = match options.remove("key") {
                Some(serde_json::Value::String(key)) => key,
                _ => return plan_err!("nutmeg: checkpointed requires options.key (string)"),
            };
            let partitions = match options.remove("partitions").and_then(|v| v.as_u64()) {
                Some(n) if n >= 1 && n <= 65536 => n as usize,
                _ => return plan_err!("nutmeg: checkpointed requires options.partitions in 1..=65536"),
            };
            if !options.is_empty() {
                return plan_err!("nutmeg: checkpoint/checkpointed accept only path, key and partitions options");
            }
            if writing {
                Ok(Arc::new(checkpoint::CheckpointWriteTable::new(
                    Arc::clone(&inputs[0]),
                    &path,
                    &key,
                    partitions,
                )?))
            } else {
                Ok(Arc::new(checkpoint::CheckpointedTable::open(&path, &key, partitions)?))
            }
        }
        other => plan_err!(
            "nutmeg: unknown verb {other}; registered verbs: stage, run, nodes, edges, diagnostics, drop, checkpoint, checkpointed"
        ),
    }
}

/// Always stream: a process environment variable cannot move work into planning.
#[derive(Debug)]
struct StreamingTable(AlgorithmTable);
#[async_trait]
impl TableProvider for StreamingTable {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        _session: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AlgorithmExec::try_new(
            Arc::new(self.0.clone()),
            projection.cloned(),
            limit,
        )?))
    }
}

#[pyclass]
struct BoundExtension {
    registry: SessionRegistry,
}

#[pymethods]
impl BoundExtension {
    #[new]
    #[pyo3(signature = (memory_bytes = 268435456, host_resource = None))]
    fn new(memory_bytes: usize, host_resource: Option<Bound<'_, PyCapsule>>) -> PyResult<Self> {
        nutmeg_graph::prepare_output_schemas()
            .map_err(|e| PyValueError::new_err(format!("nutmeg: schema setup failed: {e}")))?;
        let registry = match host_resource {
            Some(capsule) => {
                let pointer = capsule.pointer_checked(Some(MEMORY_LEASE_CAPSULE))?;
                // SAFETY: Sail supplies a named, live lease capsule. The importer
                // checks the fixed version/size header and exact admitted quota
                // before cloning through host-owned callbacks. No Arc layout is
                // accessed across the native-library boundary.
                let lease = unsafe { MemoryLease::import(pointer, memory_bytes as u64) }
                    .map_err(PyValueError::new_err)?;
                SessionRegistry::new_with_owner(memory_bytes, Arc::new(lease))
            }
            None => SessionRegistry::new(memory_bytes),
        };
        Ok(Self { registry })
    }

    fn scalar_udfs(&self) -> Vec<Py<PyAny>> {
        vec![]
    }

    fn plan_relation<'py>(
        &self,
        py: Python<'py>,
        type_url: &str,
        payload: &[u8],
        inputs: Vec<Bound<'py, PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let plans = inputs
            .into_iter()
            .map(|capsule| {
                let ptr = capsule.pointer_checked(Some(c"datafusion_execution_plan"))?;
                // The loader validates the exact DataFusion/Arrow build before calling
                // us. The capsule name identifies the stable datafusion-ffi layout.
                let ffi = unsafe { ptr.cast::<FFI_ExecutionPlan>().as_ref() };
                Arc::<dyn ExecutionPlan>::try_from(ffi)
                    .map_err(|e| PyValueError::new_err(e.to_string()))
            })
            .collect::<PyResult<Vec<_>>>()?;
        let provider = plan(&self.registry, type_url, payload, plans)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let context = Arc::new(ContextProvider(Arc::new(TaskContext::default())));
        let provider: Arc<dyn TableProvider> = Arc::new(OwnedProvider {
            inner: provider,
            context: context.clone(),
        });
        let context: Arc<dyn TaskContextProvider> = context;
        let ffi = FFI_TableProvider::new(
            provider,
            false,
            Some(RUNTIME.handle().clone()),
            &context,
            None,
        );
        PyCapsule::new_with_value(py, ffi, c"datafusion_table_provider")
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<BoundExtension>()?;
    module.add_class::<argentea::BoundArgentea>()?;
    module.add_function(wrap_pyfunction!(argentea::plan_worker_relation, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
