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
use std::sync::Arc;

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
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{Result, plan_err};
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
        let ordering = LexOrdering::new([PhysicalSortExpr::new(
            Arc::clone(&key),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )])
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
