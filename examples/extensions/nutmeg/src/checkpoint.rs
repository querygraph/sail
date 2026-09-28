//! `checkpointed`: scan a bucketed, sorted Parquet checkpoint and declare its
//! layout, so a join or aggregate on the bucket key needs no shuffle and no
//! sort.
//!
//! A checkpoint is a directory of `partitions` Parquet files, one per bucket,
//! written by `frame.repartition(partitions, key).sortWithinPartitions(key)`
//! on the client; Sail names them `{token}_{i}.zst.parquet` and graphframes-rs
//! `part-{i}.parquet`. The bucket index is the trailing integer of the file
//! name, so either naming is read. File `i` is served as partition `i`, and
//! the scan declares `Partitioning::Hash([key], partitions)` and the ascending
//! order of `key` within each partition through DataFusion's own
//! `FileScanConfigBuilder::with_output_partitioning`; a scan that declares its
//! partitioning is not re-split by the optimizer.
//!
//! The declaration is trusted, not checked: it is only correct when every
//! side of a join was bucketed by the same function into the same number of
//! buckets. Two checkpoints written by the same Sail session's shuffle are.
//! The reader refuses a directory whose files do not form exactly one bucket
//! per index in `0..partitions`, and a key that is not a column of the file.
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::{TryStreamExt, stream};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};

/// One bucketed, sorted checkpoint directory on the local filesystem.
#[derive(Debug)]
pub struct CheckpointedTable {
    directory: PathBuf,
    key: String,
    key_index: usize,
    /// Absolute path and size of bucket `i`, at index `i`.
    files: Vec<(PathBuf, u64)>,
    schema: SchemaRef,
}

/// The trailing integer of a Parquet file name, before its extensions:
/// `abc_7.zst.parquet` and `part-7.parquet` both give 7.
pub(crate) fn bucket_index(name: &str) -> Option<usize> {
    let stem = name.split('.').next()?;
    let digits: String = stem
        .chars()
        .rev()
        .take_while(|c| c.is_ascii_digit())
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    if digits.is_empty() || digits.len() == stem.len() {
        return None;
    }
    let separator = stem.as_bytes()[stem.len() - digits.len() - 1];
    if separator != b'_' && separator != b'-' {
        return None;
    }
    digits.parse().ok()
}

impl CheckpointedTable {
    /// Open `path` (a `file://` URL or an absolute directory) as a checkpoint
    /// bucketed by `key` into `partitions` files.
    pub fn open(path: &str, key: &str, partitions: usize) -> Result<Self> {
        if partitions == 0 {
            return plan_err!("nutmeg: checkpointed requires partitions >= 1");
        }
        let directory = match path.strip_prefix("file://") {
            Some(rest) => PathBuf::from(rest),
            None => PathBuf::from(path),
        };
        if !directory.is_absolute() {
            return plan_err!("nutmeg: checkpointed path must be a file:// URL or an absolute directory");
        }
        let entries = std::fs::read_dir(&directory).map_err(|e| {
            datafusion_common::DataFusionError::Plan(format!(
                "nutmeg: checkpointed cannot list {}: {e}",
                directory.display()
            ))
        })?;
        let mut files: Vec<Option<(PathBuf, u64)>> = vec![None; partitions];
        for entry in entries {
            let entry = entry.map_err(|e| datafusion_common::DataFusionError::Plan(e.to_string()))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".parquet") || name.starts_with('.') || name.starts_with('_') {
                continue;
            }
            let Some(index) = bucket_index(&name) else {
                return plan_err!("nutmeg: checkpointed file {name} has no bucket index");
            };
            if index >= partitions {
                return plan_err!(
                    "nutmeg: checkpointed file {name} names bucket {index}, beyond partitions={partitions}"
                );
            }
            if files[index].is_some() {
                return plan_err!("nutmeg: checkpointed bucket {index} has more than one file");
            }
            let size = entry
                .metadata()
                .map_err(|e| datafusion_common::DataFusionError::Plan(e.to_string()))?
                .len();
            files[index] = Some((entry.path(), size));
        }
        let files: Vec<(PathBuf, u64)> = files
            .into_iter()
            .enumerate()
            .map(|(index, file)| {
                file.ok_or_else(|| {
                    datafusion_common::DataFusionError::Plan(format!(
                        "nutmeg: checkpointed bucket {index} of {partitions} is missing in {}",
                        directory.display()
                    ))
                })
            })
            .collect::<Result<_>>()?;
        let schema = read_schema(&files[0].0)?;
        let key_index = schema.index_of(key).map_err(|_| {
            datafusion_common::DataFusionError::Plan(format!(
                "nutmeg: checkpointed key `{key}` is not a column of {}",
                files[0].0.display()
            ))
        })?;
        Ok(Self {
            directory,
            key: key.to_string(),
            key_index,
            files,
            schema,
        })
    }

    /// The bucket count, which is the partition count the scan declares.
    pub fn partitions(&self) -> usize {
        self.files.len()
    }

    /// The directory the buckets were listed from.
    pub fn directory(&self) -> &Path {
        &self.directory
    }
}

impl std::fmt::Display for CheckpointedTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "checkpointed({}, key={}, partitions={})",
            self.directory().display(),
            self.key,
            self.partitions()
        )
    }
}

fn read_schema(path: &Path) -> Result<SchemaRef> {
    let file = File::open(path)
        .map_err(|e| datafusion_common::DataFusionError::Plan(format!("{}: {e}", path.display())))?;
    let metadata = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new())
        .map_err(|e| datafusion_common::DataFusionError::Plan(format!("{}: {e}", path.display())))?;
    Ok(Arc::clone(metadata.schema()))
}

#[async_trait]
impl TableProvider for CheckpointedTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&self.key, self.key_index));
        let ordering = LexOrdering::new([PhysicalSortExpr::new(Arc::clone(&key), key_order())])
        .ok_or_else(|| datafusion_common::DataFusionError::Internal("empty ordering".into()))?;
        let groups = self
            .files
            .iter()
            .map(|(path, size)| {
                FileGroup::new(vec![PartitionedFile::new(
                    path.to_string_lossy().into_owned(),
                    *size,
                )])
            })
            .collect();
        let source = Arc::new(ParquetSource::new(Arc::clone(&self.schema)));
        let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file_groups(groups)
            .with_output_ordering(vec![ordering])
            .with_output_partitioning(Some(Partitioning::Hash(vec![key], self.files.len())))
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}


/// The sort order every checkpoint is written in and declared with.
fn key_order() -> SortOptions {
    SortOptions {
        descending: false,
        nulls_first: false,
    }
}

/// `checkpoint`: write one input, hash-bucketed by `key` into `partitions`
/// files with DataFusion's own repartition hash, each bucket sorted by `key`,
/// so that [`CheckpointedTable`]'s declaration is true by construction.
///
/// The bucket is exactly what `RepartitionExec` computes for
/// `Partitioning::Hash([key], partitions)`, so a checkpoint is co-partitioned
/// not only with other checkpoints but with anything DataFusion itself hash
/// partitions on the same key and count. Files are `part-{i}.parquet`. The
/// write is attempted once; a retried or concurrent execution is refused,
/// because a half-written directory is not a checkpoint.
#[derive(Debug)]
pub struct CheckpointWriteTable {
    input: Arc<dyn ExecutionPlan>,
    directory: PathBuf,
    key: String,
    partitions: usize,
    schema: SchemaRef,
    attempt: Mutex<Option<arrow::array::RecordBatch>>,
}

impl CheckpointWriteTable {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        path: &str,
        key: &str,
        partitions: usize,
    ) -> Result<Self> {
        if partitions == 0 || partitions > 65536 {
            return plan_err!("nutmeg: checkpoint requires partitions in 1..=65536");
        }
        let directory = match path.strip_prefix("file://") {
            Some(rest) => PathBuf::from(rest),
            None => PathBuf::from(path),
        };
        if !directory.is_absolute() {
            return plan_err!("nutmeg: checkpoint path must be a file:// URL or an absolute directory");
        }
        input.schema().index_of(key).map_err(|_| {
            datafusion_common::DataFusionError::Plan(format!(
                "nutmeg: checkpoint key `{key}` is not a column of the input"
            ))
        })?;
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("path", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("key", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("partitions", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("rows", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("bytes", arrow::datatypes::DataType::Int64, false),
        ]));
        Ok(Self {
            input,
            directory,
            key: key.to_string(),
            partitions,
            schema,
            attempt: Mutex::new(None),
        })
    }
}

#[async_trait]
impl TableProvider for CheckpointWriteTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::Temporary
    }
    async fn scan(
        &self,
        _session: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CheckpointWriteExec::new(
            Arc::new(Self {
                input: Arc::clone(&self.input),
                directory: self.directory.clone(),
                key: self.key.clone(),
                partitions: self.partitions,
                schema: Arc::clone(&self.schema),
                attempt: Mutex::new(self.attempt.lock().map_err(|_| poisoned())?.clone()),
            }),
            Arc::clone(&self.input),
            projection.cloned(),
        )?))
    }
}

fn poisoned() -> datafusion_common::DataFusionError {
    datafusion_common::DataFusionError::Execution("nutmeg: checkpoint lock poisoned".into())
}

#[derive(Debug)]
struct CheckpointWriteExec {
    table: Arc<CheckpointWriteTable>,
    input: Arc<dyn ExecutionPlan>,
    projection: Option<Vec<usize>>,
    properties: Arc<PlanProperties>,
}

impl CheckpointWriteExec {
    fn new(
        table: Arc<CheckpointWriteTable>,
        input: Arc<dyn ExecutionPlan>,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let schema = match &projection {
            Some(p) => Arc::new(table.schema.project(p)?),
            None => Arc::clone(&table.schema),
        };
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            table,
            input,
            projection,
            properties,
        })
    }
}

impl DisplayAs for CheckpointWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "NutmegCheckpointWriteExec: path={}, key={}, partitions={}, placement=driver, at_most_once=true",
            self.table.directory.display(),
            self.table.key,
            self.table.partitions
        )
    }
}

impl ExecutionPlan for CheckpointWriteExec {
    fn name(&self) -> &str {
        "NutmegCheckpointWriteExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return exec_err!("nutmeg: checkpoint takes one input");
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&self.table),
            Arc::clone(&children[0]),
            self.projection.clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("nutmeg: checkpoint has one output partition");
        }
        let (table, input, projection) = (
            Arc::clone(&self.table),
            Arc::clone(&self.input),
            self.projection.clone(),
        );
        let future = async move {
            let done = table.attempt.lock().map_err(|_| poisoned())?.clone();
            let batch = match done {
                Some(batch) => batch,
                None => {
                    let batch = write_checkpoint(&table, input, context).await?;
                    *table.attempt.lock().map_err(|_| poisoned())? = Some(batch.clone());
                    batch
                }
            };
            match projection {
                Some(p) => Ok(batch.project(&p)?),
                None => Ok(batch),
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream::once(future),
        )))
    }
}

/// Repartition by DataFusion's hash of the key, sort each bucket, write one
/// file per bucket, all buckets consumed concurrently (a repartition's
/// distributor blocks when any output partition is left unread).
async fn write_checkpoint(
    table: &CheckpointWriteTable,
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<arrow::array::RecordBatch> {
    let directory = &table.directory;
    if directory.exists() {
        let mut entries = std::fs::read_dir(directory)
            .map_err(|e| datafusion_common::DataFusionError::Execution(format!("{}: {e}", directory.display())))?;
        if entries.next().is_some() {
            return exec_err!(
                "nutmeg: checkpoint refuses the non-empty directory {}",
                directory.display()
            );
        }
    } else {
        std::fs::create_dir_all(directory)
            .map_err(|e| datafusion_common::DataFusionError::Execution(format!("{}: {e}", directory.display())))?;
    }
    let schema = input.schema();
    let key_index = schema.index_of(&table.key)?;
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&table.key, key_index));
    let repartitioned = Arc::new(RepartitionExec::try_new(
        input,
        Partitioning::Hash(vec![Arc::clone(&key)], table.partitions),
    )?);
    let ordering = LexOrdering::new([PhysicalSortExpr::new(key, key_order())])
        .ok_or_else(|| datafusion_common::DataFusionError::Internal("empty ordering".into()))?;
    let sorted: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(ordering, repartitioned).with_preserve_partitioning(true));
    if sorted.output_partitioning().partition_count() != table.partitions {
        return exec_err!("nutmeg: checkpoint lost its partition count during planning");
    }
    let writers = (0..table.partitions).map(|bucket| {
        let sorted = Arc::clone(&sorted);
        let context = Arc::clone(&context);
        let path = directory.join(format!("part-{bucket}.parquet"));
        let schema = Arc::clone(&schema);
        async move {
            let mut stream = sorted.execute(bucket, context)?;
            let file = File::create(&path)
                .map_err(|e| datafusion_common::DataFusionError::Execution(format!("{}: {e}", path.display())))?;
            let mut writer = ArrowWriter::try_new(file, schema, None)
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
            let mut rows = 0usize;
            while let Some(batch) = stream.try_next().await? {
                rows += batch.num_rows();
                writer
                    .write(&batch)
                    .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
            }
            writer
                .close()
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
            let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            Ok::<(usize, u64), datafusion_common::DataFusionError>((rows, bytes))
        }
    });
    let written = futures::future::try_join_all(writers).await?;
    let rows: usize = written.iter().map(|w| w.0).sum();
    let bytes: u64 = written.iter().map(|w| w.1).sum();
    Ok(arrow::array::RecordBatch::try_new(
        Arc::clone(&table.schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec![directory.to_string_lossy().into_owned()])),
            Arc::new(arrow::array::StringArray::from(vec![table.key.clone()])),
            Arc::new(arrow::array::Int64Array::from(vec![table.partitions as i64])),
            Arc::new(arrow::array::Int64Array::from(vec![rows as i64])),
            Arc::new(arrow::array::Int64Array::from(vec![bytes as i64])),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::{ExecutionPlanProperties, collect, displayable};
    use datafusion::prelude::{SessionConfig, SessionContext, col};
    use datafusion_common::hash_utils::create_hashes;
    use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
    use parquet::arrow::ArrowWriter;
    use std::collections::HashMap;

    fn scratch(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "nutmeg-checkpoint-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Write `(key, value)` rows bucketed by DataFusion's own hash of `key`
    /// into `n` files named the way Sail names them, sorted by key within
    /// each file.
    fn write_checkpoint(dir: &Path, keys: &[i64], values: &[f64], n: usize, value_name: &str) {
        let key_array: ArrayRef = Arc::new(Int64Array::from(keys.to_vec()));
        let mut hashes = vec![0u64; keys.len()];
        create_hashes(&[Arc::clone(&key_array)], REPARTITION_RANDOM_STATE.random_state(), &mut hashes).unwrap();
        let mut buckets: Vec<Vec<(i64, f64)>> = vec![Vec::new(); n];
        for (i, hash) in hashes.iter().enumerate() {
            buckets[(*hash % n as u64) as usize].push((keys[i], values[i]));
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(value_name, DataType::Float64, false),
        ]));
        for (i, mut rows) in buckets.into_iter().enumerate() {
            rows.sort_by_key(|(k, _)| *k);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
                    Arc::new(Float64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())),
                ],
            )
            .unwrap();
            let file = File::create(dir.join(format!("W8Cfa8C1ASY0kyfm_{i}.zst.parquet"))).unwrap();
            let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
    }


    #[tokio::test]
    async fn written_checkpoint_is_bucketed_by_the_engine_hash_sorted_and_joinable() {
        let n = 4;
        let dir = scratch("write");
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(n));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Float64, false),
        ]));
        let ids: Vec<i64> = (0..2000).map(|i| (i * 7919) % 613).collect();
        let vals: Vec<f64> = ids.iter().map(|k| *k as f64).collect();
        let batches: Vec<Vec<RecordBatch>> = (0..3)
            .map(|p| {
                let slice: Vec<usize> = (0..ids.len()).filter(|i| i % 3 == p).collect();
                vec![RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from(slice.iter().map(|i| ids[*i]).collect::<Vec<_>>())),
                        Arc::new(Float64Array::from(slice.iter().map(|i| vals[*i]).collect::<Vec<_>>())),
                    ],
                )
                .unwrap()]
            })
            .collect();
        let input = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(&batches, Arc::clone(&schema), None).unwrap();
        let left = dir.join("left");
        let receipt = ctx
            .read_table(Arc::new(CheckpointWriteTable::new(input.clone(), left.to_str().unwrap(), "id", n).unwrap()))
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows = receipt[0].column_by_name("rows").unwrap().as_any().downcast_ref::<Int64Array>().unwrap().value(0);
        assert_eq!(rows, 2000);
        // Every bucket is what DataFusion's own hash says it is, and sorted.
        let key_array: ArrayRef = Arc::new(Int64Array::from(ids.clone()));
        let mut hashes = vec![0u64; ids.len()];
        create_hashes(&[key_array], REPARTITION_RANDOM_STATE.random_state(), &mut hashes).unwrap();
        for bucket in 0..n {
            let expected: std::collections::BTreeMap<i64, usize> = ids
                .iter()
                .zip(&hashes)
                .filter(|(_, h)| (**h % n as u64) as usize == bucket)
                .fold(std::collections::BTreeMap::new(), |mut m, (k, _)| {
                    *m.entry(*k).or_default() += 1;
                    m
                });
            let file = File::open(left.join(format!("part-{bucket}.parquet"))).unwrap();
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
            let mut seen: Vec<i64> = Vec::new();
            for batch in reader {
                let batch = batch.unwrap();
                seen.extend(batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values());
            }
            assert!(seen.windows(2).all(|w| w[0] <= w[1]), "bucket {bucket} not sorted");
            let mut observed: std::collections::BTreeMap<i64, usize> = std::collections::BTreeMap::new();
            for k in seen {
                *observed.entry(k).or_default() += 1;
            }
            assert_eq!(observed, expected, "bucket {bucket} differs from the engine hash");
        }
        // A second checkpoint of the same keys co-partitions with the first.
        let right = dir.join("right");
        ctx.read_table(Arc::new(CheckpointWriteTable::new(input, right.to_str().unwrap(), "id", n).unwrap()))
            .unwrap()
            .collect()
            .await
            .unwrap();
        let l = ctx.read_table(Arc::new(CheckpointedTable::open(left.to_str().unwrap(), "id", n).unwrap())).unwrap();
        let r = ctx
            .read_table(Arc::new(CheckpointedTable::open(right.to_str().unwrap(), "id", n).unwrap()))
            .unwrap()
            .select(vec![col("id").alias("rid")])
            .unwrap();
        let joined = l.join(r, datafusion::common::JoinType::Inner, &["id"], &["rid"], None).unwrap();
        let plan = joined.clone().create_physical_plan().await.unwrap();
        let text = displayable(plan.as_ref()).indent(true).to_string();
        assert!(!text.contains("RepartitionExec"), "{text}");
        let joined_rows: usize = joined.collect().await.unwrap().iter().map(|b| b.num_rows()).sum();
        let mut per_key: HashMap<i64, usize> = HashMap::new();
        for k in &ids {
            *per_key.entry(*k).or_default() += 1;
        }
        assert_eq!(joined_rows, per_key.values().map(|c| c * c).sum::<usize>());
        // A non-empty directory is refused rather than overwritten.
        let again = ctx
            .read_table(Arc::new(CheckpointWriteTable::new(
                datafusion::datasource::memory::MemorySourceConfig::try_new_exec(&batches, Arc::clone(&schema), None).unwrap(),
                left.to_str().unwrap(),
                "id",
                n,
            ).unwrap()))
            .unwrap()
            .collect()
            .await;
        assert!(again.unwrap_err().to_string().contains("non-empty"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn bucket_index_reads_sail_and_graphframes_names() {
        assert_eq!(bucket_index("W8Cfa8C1ASY0kyfm_3.zst.parquet"), Some(3));
        assert_eq!(bucket_index("part-12.parquet"), Some(12));
        assert_eq!(bucket_index("part_0.parquet"), Some(0));
        assert_eq!(bucket_index("state.parquet"), None);
        assert_eq!(bucket_index("7.parquet"), None);
        assert_eq!(bucket_index("run3.parquet"), None);
    }

    #[tokio::test]
    async fn co_partitioned_join_has_no_repartition_and_no_sort_and_is_correct() {
        let n = 4;
        let dir = scratch("join");
        let (left, right) = (dir.join("left"), dir.join("right"));
        std::fs::create_dir_all(&left).unwrap();
        std::fs::create_dir_all(&right).unwrap();
        let keys: Vec<i64> = (0..1000).map(|i| (i * 7919) % 613).collect();
        let a: Vec<f64> = keys.iter().map(|k| *k as f64 * 0.5).collect();
        let b: Vec<f64> = keys.iter().map(|k| *k as f64 + 1.0).collect();
        write_checkpoint(&left, &keys, &a, n, "a");
        write_checkpoint(&right, &keys, &b, n, "b");

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(n));
        let l = ctx
            .read_table(Arc::new(
                CheckpointedTable::open(&format!("file://{}", left.display()), "id", n).unwrap(),
            ))
            .unwrap();
        let r = ctx
            .read_table(Arc::new(CheckpointedTable::open(right.to_str().unwrap(), "id", n).unwrap()))
            .unwrap()
            .select(vec![col("id").alias("rid"), col("b")])
            .unwrap();
        let joined = l.join(r, datafusion::common::JoinType::Inner, &["id"], &["rid"], None).unwrap();
        let plan = joined.clone().create_physical_plan().await.unwrap();
        let text = displayable(plan.as_ref()).indent(true).to_string();
        assert!(!text.contains("RepartitionExec"), "{text}");
        assert!(!text.contains("SortExec"), "{text}");
        assert!(text.contains("HashJoinExec") || text.contains("SortMergeJoin"), "{text}");
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let mut observed: Vec<(i64, f64, f64)> = Vec::new();
        for batch in &batches {
            let id = batch.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
            let av = batch.column_by_name("a").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
            let bv = batch.column_by_name("b").unwrap().as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..batch.num_rows() {
                observed.push((id.value(i), av.value(i), bv.value(i)));
            }
        }
        observed.sort_by(|x, y| x.partial_cmp(y).unwrap());
        // Reference: every left row pairs with every right row of the same key.
        let mut per_key: HashMap<i64, (Vec<f64>, Vec<f64>)> = HashMap::new();
        for (i, k) in keys.iter().enumerate() {
            let e = per_key.entry(*k).or_default();
            e.0.push(a[i]);
            e.1.push(b[i]);
        }
        let mut expected: Vec<(i64, f64, f64)> = Vec::new();
        for (k, (av, bv)) in per_key {
            for x in &av {
                for y in &bv {
                    expected.push((k, *x, *y));
                }
            }
        }
        expected.sort_by(|x, y| x.partial_cmp(y).unwrap());
        assert_eq!(observed.len(), expected.len());
        assert_eq!(observed, expected);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn group_by_on_the_key_needs_no_repartition() {
        let n = 3;
        let dir = scratch("agg");
        let keys: Vec<i64> = (0..500).map(|i| i % 97).collect();
        let v: Vec<f64> = keys.iter().map(|k| *k as f64).collect();
        write_checkpoint(&dir, &keys, &v, n, "v");
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(n));
        let t = ctx
            .read_table(Arc::new(CheckpointedTable::open(dir.to_str().unwrap(), "id", n).unwrap()))
            .unwrap();
        let agg = t
            .aggregate(vec![col("id")], vec![datafusion::functions_aggregate::sum::sum(col("v"))])
            .unwrap();
        let plan = agg.create_physical_plan().await.unwrap();
        let text = displayable(plan.as_ref()).indent(true).to_string();
        assert!(!text.contains("RepartitionExec"), "{text}");
        assert_eq!(plan.output_partitioning().partition_count(), n);
        let rows: usize = collect(plan, ctx.task_ctx()).await.unwrap().iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 97);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn refuses_missing_bucket_wrong_key_and_relative_path() {
        let n = 3;
        let dir = scratch("refuse");
        write_checkpoint(&dir, &[1, 2, 3], &[1.0, 2.0, 3.0], n, "v");
        assert!(CheckpointedTable::open(dir.to_str().unwrap(), "id", n).is_ok());
        assert!(CheckpointedTable::open(dir.to_str().unwrap(), "nope", n).unwrap_err().to_string().contains("not a column"));
        assert!(CheckpointedTable::open(dir.to_str().unwrap(), "id", n + 1).unwrap_err().to_string().contains("missing"));
        assert!(CheckpointedTable::open(dir.to_str().unwrap(), "id", n - 1).unwrap_err().to_string().contains("beyond"));
        assert!(CheckpointedTable::open("relative/dir", "id", n).unwrap_err().to_string().contains("absolute"));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
