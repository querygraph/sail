//! The ORC file sink.
//!
//! One native writer owns each output file. Its footer is written only on an
//! explicit finish, and its upload completes only once the whole input has been
//! seen to succeed, so a lost or failed input aborts the file instead of
//! publishing a well-formed truncated one. A failure after one file has
//! committed may leave that file; this is not table-wide atomicity.
//!
//! Each file is supervised by the execution's resource scope, which keeps the
//! upload owned until it settles even if this sink's future is dropped. The
//! bytes between the encoder and the upload are reserved from the memory pool.
//! The native encoder's own buffers are not, so this is not a heap bound.
use std::fmt;
use std::future::Future;
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion_common::{DataFusionError, Result, exec_datafusion_err, exec_err};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::DataSink;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use futures::stream::{FuturesUnordered, StreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use orc_rust::ArrowWriterBuilder;
use sail_common_datafusion::owned_multipart::execution_error;
use sail_common_datafusion::task_resources::TaskResourceScope;
use sail_object_store::owned_multipart::{self, Disposition, MultipartHandle};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

use super::write_options::OrcWriteCompression;
use super::write_schema::OrcWriteSchema;

/// Spark's default `orc.stripe.size`.
const STRIPE_BYTES: usize = 64 * 1024 * 1024;
/// Above the smallest part object stores accept.
const PART_BYTES: usize = 8 * 1024 * 1024;
const PARTS_IN_FLIGHT: usize = 2;
const PARTS_QUEUED: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrcWriterOptions {
    pub compression: OrcWriteCompression,
}

pub struct OrcSink {
    config: FileSinkConfig,
    options: OrcWriterOptions,
    file_schema: SchemaRef,
}

impl OrcSink {
    /// Admit the schema here, so a sink that cannot write never opens output.
    pub fn try_new(config: FileSinkConfig, options: OrcWriterOptions) -> Result<Self> {
        options.compression.native_compression()?;
        let file_schema = file_schema(&config)?;
        Ok(Self {
            config,
            options,
            file_schema,
        })
    }

    pub fn options(&self) -> OrcWriterOptions {
        self.options
    }

    fn empty_file_path(&self) -> Result<Path> {
        let table = self
            .config
            .table_paths
            .first()
            .ok_or_else(|| exec_datafusion_err!("ORC sink has no table path"))?;
        let name = format!("part-{}.{}", Uuid::new_v4(), self.config.file_extension);
        Ok(table.prefix().clone().join(name))
    }

    fn start_file(
        &self,
        path: Path,
        batches: mpsc::Receiver<RecordBatch>,
        context: &Arc<TaskContext>,
        store: &Arc<dyn ObjectStore>,
        resources: &Arc<TaskResourceScope>,
        decided: &watch::Receiver<Option<bool>>,
    ) -> impl Future<Output = Result<FileOutcome>> + use<> {
        let reservation =
            MemoryConsumer::new(format!("OrcSink[{path}]")).register(context.memory_pool());
        let job = FileJob {
            path,
            batches,
            store: store.clone(),
            schema: self.file_schema.clone(),
            options: self.options,
            reservation: Arc::new(reservation),
            decided: decided.clone(),
            resources: resources.clone(),
        };
        let resources = resources.clone();
        async move { resources.run_mutation(|canceled| job.run(canceled)).await }
    }
}

/// The schema of each file: partition values live in the path, and view
/// arrays are stored as their plain equivalents.
fn file_schema(config: &FileSinkConfig) -> Result<SchemaRef> {
    let is_partition = |field: &Field| {
        !config.keep_partition_by_columns
            && config
                .table_partition_cols
                .iter()
                .any(|(name, _)| name == field.name())
    };
    let fields = config
        .output_schema()
        .fields()
        .iter()
        .filter(|field| !is_partition(field))
        .map(|field| field.as_ref().clone().with_data_type(stored_type(field)))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        config.output_schema().metadata().clone(),
    ));
    Ok(OrcWriteSchema::try_new(schema)?.schema().clone())
}

fn stored_type(field: &Field) -> DataType {
    match field.data_type() {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        data_type => data_type.clone(),
    }
}

impl fmt::Debug for OrcSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrcSink")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for OrcSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "OrcSink(file_groups=")?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => write!(f, "format: orc"),
        }
    }
}

#[async_trait]
impl DataSink for OrcSink {
    fn schema(&self) -> &SchemaRef {
        self.config.output_schema()
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        FileSink::write_all(self, data, context).await
    }
}

/// How one file left the write. A cleanup failure is kept apart from a cause,
/// because it must never replace the failure that made the cleanup necessary.
enum FileOutcome {
    Committed(u64),
    Aborted,
    CleanupFailed(DataFusionError),
}

#[async_trait]
impl FileSink for OrcSink {
    fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn spawn_writer_tasks_and_join(
        &self,
        context: &Arc<TaskContext>,
        demux_task: SpawnedTask<Result<()>>,
        mut file_stream_rx: DemuxedStreamReceiver,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<u64> {
        let resources = context
            .session_config()
            .get_extension::<TaskResourceScope>()
            .ok_or_else(|| exec_datafusion_err!("ORC writing needs an execution resource scope"))?;
        // `None` until the input's outcome is known. Dropping the sender, which
        // happens if this future is dropped, reads as an abort.
        let (decision, decided) = watch::channel(None);
        let mut files = FuturesUnordered::new();
        let mut demux = pin!(demux_task.join_unwind());
        let mut demuxing = true;
        let mut receiving = true;
        let mut started = false;
        let mut failures = vec![];
        let mut demux_failure = None;
        while (demuxing || receiving) && failures.is_empty() && demux_failure.is_none() {
            tokio::select! {
                biased;
                Some(file) = files.next(), if !files.is_empty() => {
                    if let Err(error) = file {
                        failures.push(error);
                    }
                }
                file = file_stream_rx.recv(), if receiving => {
                    let Some((path, batches)) = file else {
                        receiving = false;
                        continue;
                    };
                    started = true;
                    files.push(self.start_file(path, batches, context, &object_store, &resources, &decided));
                }
                result = &mut demux, if demuxing => {
                    demuxing = false;
                    demux_failure = match result {
                        Ok(Ok(())) => None,
                        Ok(Err(error)) => Some(error),
                        Err(error) => Some(DataFusionError::External(Box::new(error))),
                    };
                }
            }
        }
        let commit = failures.is_empty() && demux_failure.is_none();
        // Spark leaves one schema-only file, unless the write is partitioned.
        if commit && !started && self.config.table_partition_cols.is_empty() {
            let (_, batches) = mpsc::channel(1);
            let path = self.empty_file_path()?;
            files.push(self.start_file(
                path,
                batches,
                context,
                &object_store,
                &resources,
                &decided,
            ));
        }
        drop(file_stream_rx);
        let _ = decision.send(Some(commit));
        let (mut rows, mut cleanup) = (0, vec![]);
        while let Some(file) = files.next().await {
            match file {
                Ok(FileOutcome::Committed(count)) => rows += count,
                Ok(FileOutcome::Aborted) => {}
                Ok(FileOutcome::CleanupFailed(error)) => cleanup.push(error),
                Err(error) => failures.push(error),
            }
        }
        // A file's own failure closes the demuxer's channels, so it comes first.
        failures.extend(demux_failure);
        failures.extend(cleanup);
        match (failures.len(), failures.pop()) {
            (_, None) => Ok(rows),
            (1, Some(error)) => Err(error),
            (_, Some(error)) => {
                failures.push(error);
                Err(DataFusionError::Collection(failures))
            }
        }
    }
}

enum Input {
    Batch(RecordBatch),
    Finish,
}

struct FileJob {
    path: Path,
    batches: mpsc::Receiver<RecordBatch>,
    store: Arc<dyn ObjectStore>,
    schema: SchemaRef,
    options: OrcWriterOptions,
    reservation: Arc<MemoryReservation>,
    decided: watch::Receiver<Option<bool>>,
    resources: Arc<TaskResourceScope>,
}

impl FileJob {
    async fn run(self, mut canceled: watch::Receiver<bool>) -> Result<FileOutcome> {
        let Self {
            path,
            batches,
            store,
            schema,
            options,
            reservation,
            mut decided,
            resources,
        } = self;
        let mut upload = {
            let (store, path) = (store.clone(), path.clone());
            let (supervisor, creation) = owned_multipart::prepare(
                move || async move { store.put_multipart(&path).await },
                NonZeroUsize::MIN.saturating_add(PARTS_IN_FLIGHT - 1),
            );
            resources.spawn_mutation(move |canceled| async move {
                supervisor.run(canceled).await.map_err(execution_error)
            })?;
            creation.wait().await.map_err(execution_error)?
        };
        let (inputs, inputs_rx) = mpsc::channel(1);
        let (parts, parts_rx) = mpsc::channel(PARTS_QUEUED);
        let writer = PartWriter::new(parts, reservation.clone());
        let encoder =
            tokio::task::spawn_blocking(move || encode(inputs_rx, writer, schema, options));
        // Whichever side stops first closes a channel, which stops the others.
        // All three are always joined, so nothing outlives this file.
        let (forwarded, uploaded, encoded) = tokio::join!(
            forward(batches, inputs, canceled.clone()),
            send_parts(&mut upload, parts_rx, &reservation, canceled.clone()),
            encoder,
        );
        let encoded = encoded
            .map_err(|error| DataFusionError::External(Box::new(error)))
            .and_then(|result| result);
        let written = uploaded.and(encoded).and(forwarded);
        let cause = match written {
            Ok(rows) => match await_decision(&mut decided, &mut canceled).await {
                Decision::Commit => match upload.complete() {
                    Ok(completion) => {
                        return match completion.await.map_err(execution_error)? {
                            Disposition::Committed(_) => Ok(FileOutcome::Committed(rows)),
                            Disposition::Aborted => {
                                exec_err!("ORC upload of {path} was aborted before it completed")
                            }
                        };
                    }
                    Err(error) => Some(execution_error(error)),
                },
                Decision::Abort => None,
                Decision::Canceled => Some(canceled_error()),
            },
            Err(error) => Some(error),
        };
        let aborted = abort(&mut upload).await;
        match (cause, aborted) {
            (None, Ok(())) => Ok(FileOutcome::Aborted),
            (None, Err(cleanup)) => Ok(FileOutcome::CleanupFailed(cleanup)),
            (Some(cause), Ok(())) => Err(cause),
            (Some(cause), Err(cleanup)) => Err(DataFusionError::Collection(vec![cause, cleanup])),
        }
    }
}

async fn abort(upload: &mut MultipartHandle) -> Result<()> {
    let outcome = upload.abort().map_err(execution_error)?;
    outcome.await.map(|_| ()).map_err(execution_error)
}

enum Decision {
    Commit,
    Abort,
    Canceled,
}

/// Wait for the input's outcome. A dropped sink reads as an abort.
async fn await_decision(
    decided: &mut watch::Receiver<Option<bool>>,
    canceled: &mut watch::Receiver<bool>,
) -> Decision {
    tokio::select! {
        biased;
        () = until_canceled(canceled) => Decision::Canceled,
        commit = until_decided(decided) => {
            if commit { Decision::Commit } else { Decision::Abort }
        }
    }
}

async fn until_canceled(canceled: &mut watch::Receiver<bool>) {
    let _ = canceled.wait_for(|canceled| *canceled).await;
}

async fn until_decided(decided: &mut watch::Receiver<Option<bool>>) -> bool {
    let decision = decided.wait_for(Option::is_some).await;
    decision.is_ok_and(|decision| *decision == Some(true))
}

fn canceled_error() -> DataFusionError {
    DataFusionError::Execution("ORC write canceled".into())
}

/// Feed the encoder, and finish it only at the end of this file's input. An
/// encoder that stopped reports its own error, so this ends quietly.
async fn forward(
    mut batches: mpsc::Receiver<RecordBatch>,
    inputs: mpsc::Sender<Input>,
    mut canceled: watch::Receiver<bool>,
) -> Result<u64> {
    let mut rows = 0;
    loop {
        tokio::select! {
            biased;
            () = until_canceled(&mut canceled) => return Err(canceled_error()),
            () = inputs.closed() => return Ok(rows),
            batch = batches.recv() => {
                let Some(batch) = batch else { break };
                let count = batch.num_rows() as u64;
                if inputs.send(Input::Batch(batch)).await.is_err() {
                    return Ok(rows);
                }
                rows += count;
            }
        }
    }
    let _ = inputs.send(Input::Finish).await;
    Ok(rows)
}

/// Upload parts in order. Each part's reservation is released once uploaded.
async fn send_parts(
    upload: &mut MultipartHandle,
    mut parts: mpsc::Receiver<Bytes>,
    reservation: &MemoryReservation,
    mut canceled: watch::Receiver<bool>,
) -> Result<()> {
    let mut pending = FuturesUnordered::new();
    let mut open = true;
    while open || !pending.is_empty() {
        tokio::select! {
            biased;
            () = until_canceled(&mut canceled) => return Err(canceled_error()),
            Some(part) = pending.next(), if !pending.is_empty() => {
                let (result, size): (std::result::Result<(), _>, usize) = part;
                result.map_err(execution_error)?;
                reservation.shrink(size);
            }
            part = parts.recv(), if open && pending.len() < PARTS_IN_FLIGHT => {
                let Some(part) = part else {
                    open = false;
                    continue;
                };
                let size = part.len();
                let sent = upload
                    .put_part(PutPayload::from_bytes(part))
                    .map_err(execution_error)?;
                pending.push(async move { (sent.await, size) });
            }
        }
    }
    Ok(())
}

fn encode(
    mut inputs: mpsc::Receiver<Input>,
    mut output: PartWriter,
    schema: SchemaRef,
    options: OrcWriterOptions,
) -> Result<()> {
    let result = encode_file(&mut inputs, &mut output, &schema, options);
    let stopped = output.failure.take();
    match (result, stopped) {
        // The native error only says that a write failed; this says why.
        (Err(_), Some(cause)) => Err(cause),
        (result, _) => result,
    }
}

fn encode_file(
    inputs: &mut mpsc::Receiver<Input>,
    output: &mut PartWriter,
    schema: &SchemaRef,
    options: OrcWriterOptions,
) -> Result<()> {
    let mut builder =
        ArrowWriterBuilder::new(&mut *output, schema.clone()).with_stripe_byte_size(STRIPE_BYTES);
    if let Some(compression) = options.compression.native_compression()? {
        builder = builder.with_compression(compression);
    }
    let mut writer = builder.try_build().map_err(native_error)?;
    loop {
        match inputs.blocking_recv() {
            Some(Input::Batch(batch)) => {
                writer
                    .write(&conform(&batch, schema)?)
                    .map_err(native_error)?;
            }
            Some(Input::Finish) => break,
            // Without an explicit finish the input is incomplete: no footer.
            None => return Err(canceled_error()),
        }
    }
    writer.close().map_err(native_error)?;
    output.finish()
}

fn native_error(error: orc_rust::error::OrcError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// The batch under the file schema. Only view arrays change representation.
fn conform(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(
            |(column, field)| match (column.data_type(), field.data_type()) {
                (from, to) if from == to => Ok(column.clone()),
                (DataType::Utf8View, DataType::Utf8) | (DataType::BinaryView, DataType::Binary) => {
                    Ok(cast(column, field.data_type())?)
                }
                (from, to) => {
                    exec_err!("ORC input type {from:?} differs from the file type {to:?}")
                }
            },
        )
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &options,
    )?)
}

/// The encoder's synchronous output: fills a part, then hands it to the upload,
/// blocking while the upload is behind. Bytes are reserved before they are copied.
struct PartWriter {
    buffer: Vec<u8>,
    parts: mpsc::Sender<Bytes>,
    reservation: Arc<MemoryReservation>,
    failure: Option<DataFusionError>,
}

impl PartWriter {
    fn new(parts: mpsc::Sender<Bytes>, reservation: Arc<MemoryReservation>) -> Self {
        Self {
            buffer: vec![],
            parts,
            reservation,
            failure: None,
        }
    }

    fn send(&mut self) -> Result<()> {
        let part = Bytes::from(std::mem::take(&mut self.buffer));
        self.parts
            .blocking_send(part)
            .map_err(|_| exec_datafusion_err!("ORC upload stopped"))
    }

    fn finish(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.send()
    }

    fn try_write(&mut self, data: &[u8]) -> Result<()> {
        self.reservation.try_grow(data.len())?;
        self.buffer.extend_from_slice(data);
        if self.buffer.len() >= PART_BYTES {
            self.send()?;
        }
        Ok(())
    }
}

impl Write for PartWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        match self.try_write(data) {
            Ok(()) => Ok(data.len()),
            Err(error) => {
                let message = error.to_string();
                self.failure = Some(error);
                Err(io::Error::other(message))
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "sink_tests.rs"]
mod tests;
