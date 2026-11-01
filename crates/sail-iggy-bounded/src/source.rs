use std::fmt;
use std::future::Future;
use std::sync::{Arc, mpsc};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::datasource::{MemTable, TableProvider};
use sail_common_datafusion::streaming::bounded::{
    BoundedSource, Cursor, Partition, PartitionId, Range, SourceDescriptor, SourceError,
};
use tokio::runtime::{Builder, Handle, Runtime, RuntimeFlavor};

use crate::broker::{Broker, SdkBroker};
use crate::config::IggyBoundedConfig;
use crate::decode::{decode_messages, validate_schema};
use crate::range::{
    cursor, earliest, incarnation, offset_of, partition_id, partition_number, partition_state,
    read_range,
};

/// The descriptor's `kind`.
pub const KIND: &str = "iggy";

/// One Iggy stream and topic as a bounded source.
///
/// The trait is synchronous and the client is not, so the source owns a small
/// runtime for its client and blocks the caller on each call. Owning one
/// keeps the client's background tasks alive across the caller's runtimes
/// and works from either kind of caller runtime.
pub struct IggyBoundedSource {
    config: IggyBoundedConfig,
    schema: SchemaRef,
    broker: Arc<dyn Broker>,
    runtime: Option<Runtime>,
}

impl IggyBoundedSource {
    /// A source for `config`. This does not reach the broker, which is
    /// contacted on the first source call.
    pub fn try_new(config: IggyBoundedConfig, schema: SchemaRef) -> DataFusionResult<Self> {
        let broker = Arc::new(SdkBroker::new(config.clone()));
        Self::with_broker(config, schema, broker)
    }

    pub(crate) fn with_broker(
        config: IggyBoundedConfig,
        schema: SchemaRef,
        broker: Arc<dyn Broker>,
    ) -> DataFusionResult<Self> {
        validate_schema(&config, &schema)?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("sail-iggy-bounded")
            .enable_all()
            .build()
            .map_err(DataFusionError::from)?;
        Ok(Self {
            config,
            schema,
            broker,
            runtime: Some(runtime),
        })
    }

    /// Run `operation` on the source's runtime under the configured deadline.
    fn call<T: Send + 'static>(
        &self,
        operation: impl Future<Output = Result<T, SourceError>> + Send + 'static,
    ) -> Result<T, SourceError> {
        let runtime = self.runtime.as_ref().ok_or_else(|| SourceError::Internal {
            cause: "the source's runtime is gone".into(),
        })?;
        let deadline = self.config.timeout;
        let (sender, receiver) = mpsc::channel();
        runtime.spawn(async move {
            let result = match tokio::time::timeout(deadline, operation).await {
                Ok(result) => result,
                Err(_) => Err(SourceError::Unavailable {
                    partition: None,
                    cause: format!("no answer within {deadline:?}").into(),
                }),
            };
            // The receiver only goes away if the caller's thread died.
            let _ = sender.send(result);
        });
        let received = match Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| receiver.recv())
            }
            _ => receiver.recv(),
        };
        received.map_err(|e| SourceError::Internal { cause: Box::new(e) })?
    }
}

impl Drop for IggyBoundedSource {
    fn drop(&mut self) {
        // Dropping a runtime waits for its tasks, which panics on a thread
        // that is itself driving async work.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

impl fmt::Debug for IggyBoundedSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IggyBoundedSource")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl BoundedSource for IggyBoundedSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn describe(&self) -> Result<SourceDescriptor, SourceError> {
        let broker = self.broker.clone();
        let mut states = self.call(async move { broker.partitions().await })?;
        states.sort_by_key(|state| state.id);
        Ok(SourceDescriptor {
            kind: KIND.to_string(),
            descriptor: self.config.descriptor(),
            partitions: states
                .into_iter()
                .map(|state| Partition {
                    id: partition_id(state.id),
                    incarnation: incarnation(state.incarnation),
                })
                .collect(),
        })
    }

    fn earliest(&self, partition: &PartitionId) -> Result<Cursor, SourceError> {
        let broker = self.broker.clone();
        let partition = partition.clone();
        self.call(async move { Ok(cursor(earliest(broker.as_ref(), &partition).await?)) })
    }

    fn retains_origin(&self, partition: &PartitionId) -> Result<bool, SourceError> {
        let broker = self.broker.clone();
        let partition = partition.clone();
        self.call(async move { Ok(earliest(broker.as_ref(), &partition).await? == 0) })
    }

    fn frontier(&self, partition: &PartitionId) -> Result<Cursor, SourceError> {
        let broker = self.broker.clone();
        let partition = partition.clone();
        self.call(async move {
            let state = partition_state(broker.as_ref(), &partition).await?;
            Ok(cursor(state.next_offset()))
        })
    }

    fn plan_end(
        &self,
        partition: &PartitionId,
        start: &Cursor,
        frontier: &Cursor,
        max_rows: usize,
    ) -> Result<Cursor, SourceError> {
        let start = offset_of(partition, start)?;
        let frontier = offset_of(partition, frontier)?;
        let step = u64::try_from(max_rows).unwrap_or(u64::MAX);
        Ok(cursor(start.saturating_add(step).min(frontier).max(start)))
    }

    fn scan(&self, range: &Range) -> Result<Arc<dyn TableProvider>, SourceError> {
        let id = partition_number(&range.partition)?;
        let broker = self.broker.clone();
        let owned = range.clone();
        let page_size = self.config.page_size;
        let messages =
            self.call(async move { read_range(broker.as_ref(), &owned, page_size).await })?;
        let batch = decode_messages(&self.config, id, &self.schema, &messages).map_err(|e| {
            SourceError::Corrupt {
                partition: range.partition.clone(),
                cause: Box::new(e),
            }
        })?;
        let table = MemTable::try_new(self.schema.clone(), vec![vec![batch]])
            .map_err(|e| SourceError::Internal { cause: Box::new(e) })?;
        Ok(Arc::new(table))
    }
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
