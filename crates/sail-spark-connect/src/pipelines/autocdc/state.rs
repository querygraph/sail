//! What a flow's stateful query keeps between batches, as Spark keeps it in
//! its state store: for a watermarked aggregate, the rows of windows still
//! open and the watermark, and for a deduplication, the keys already seen.
//! A snapshot is named by the progress it follows, so a replayed batch reads
//! the snapshot it started from and rewrites the one it ends at, and a reset
//! starts in a fresh checkpoint directory with none.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;

use crate::error::{SparkError, SparkResult};

const WATERMARK_KEY: &str = "autocdc.watermark";

/// Where a batch's state is read and written: the directory of the flow's
/// checkpoint, and the digests of the progress before and after the batch.
#[derive(Debug, Clone)]
pub(in crate::pipelines) struct StateKeys {
    pub dir: PathBuf,
    pub start: String,
    pub end: String,
}

/// One operator's state: rows, and for a watermarked aggregate the
/// watermark in microseconds.
pub(super) struct Snapshot {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub watermark: i64,
}

impl Snapshot {
    pub fn empty(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: vec![],
            watermark: 0,
        }
    }

    /// The rows as a plan scanned under `alias`.
    pub fn plan(&self, alias: &str) -> SparkResult<LogicalPlan> {
        let table = MemTable::try_new(self.schema.clone(), vec![self.batches.clone()])?;
        Ok(LogicalPlanBuilder::scan(alias, provider_as_source(Arc::new(table)), None)?.build()?)
    }
}

impl StateKeys {
    fn path(&self, operator: &str, digest: &str) -> PathBuf {
        self.dir.join(operator).join(format!("{digest}.arrow"))
    }

    /// The operator's state when the batch started. `None` when nothing was
    /// written for that progress.
    pub fn load(&self, operator: &str) -> SparkResult<Option<Snapshot>> {
        read_arrow(&self.path(operator, &self.start))
    }

    /// Write the operator's state after the batch, then remove snapshots that
    /// neither this batch nor its replay can read.
    pub fn store(&self, operator: &str, snapshot: &Snapshot) -> SparkResult<()> {
        let path = self.path(operator, &self.end);
        write_arrow(&path, snapshot)?;
        self.prune(
            operator,
            &[
                format!("{}.arrow", self.start),
                format!("{}.arrow", self.end),
            ],
        )
    }

    /// The state of an operator whose state only grows, when the batch
    /// started: every segment its manifest lists. `None` when no manifest
    /// was written for that progress.
    pub fn load_appended(&self, operator: &str) -> SparkResult<Option<Snapshot>> {
        let Some(segments) = self.manifest(operator, &self.start)? else {
            return Ok(None);
        };
        let mut snapshot: Option<Snapshot> = None;
        for segment in segments {
            let path = self.dir.join(operator).join(&segment);
            let loaded = read_arrow(&path)?.ok_or_else(|| {
                SparkError::internal(format!(
                    "AutoCDC state segment {} is missing",
                    path.display()
                ))
            })?;
            match &mut snapshot {
                Some(snapshot) => {
                    snapshot.batches.extend(loaded.batches);
                    snapshot.watermark = snapshot.watermark.max(loaded.watermark);
                }
                None => snapshot = Some(loaded),
            }
        }
        Ok(snapshot)
    }

    /// Add the batch's rows to an operator whose state only grows. Only the
    /// new rows are written, as one segment, with a manifest that lists every
    /// segment so far, so a batch's cost does not grow with the state.
    pub fn append(&self, operator: &str, rows: &Snapshot) -> SparkResult<()> {
        let mut segments = self.manifest(operator, &self.start)?.unwrap_or_default();
        let segment = format!("{}.segment", self.end);
        write_arrow(&self.dir.join(operator).join(&segment), rows)?;
        segments.retain(|existing| existing != &segment);
        segments.push(segment);
        let manifest = self
            .dir
            .join(operator)
            .join(format!("{}.manifest", self.end));
        let bytes = serde_json::to_vec(&segments)
            .map_err(|e| SparkError::internal(format!("encode AutoCDC state manifest: {e}")))?;
        write_atomically(&manifest, &bytes)?;
        let mut keep = vec![
            format!("{}.manifest", self.start),
            format!("{}.manifest", self.end),
        ];
        keep.extend(segments);
        self.prune(operator, &keep)
    }

    /// Replace an operator's appended state with `rows`, as one segment,
    /// when eviction has shrunk it. The start digest's segments stay for a
    /// replay.
    pub fn rewrite(&self, operator: &str, rows: &Snapshot) -> SparkResult<()> {
        let before = self.manifest(operator, &self.start)?.unwrap_or_default();
        let segment = format!("{}.segment", self.end);
        write_arrow(&self.dir.join(operator).join(&segment), rows)?;
        let manifest = self
            .dir
            .join(operator)
            .join(format!("{}.manifest", self.end));
        let bytes = serde_json::to_vec(&vec![segment.clone()])
            .map_err(|e| SparkError::internal(format!("encode AutoCDC state manifest: {e}")))?;
        write_atomically(&manifest, &bytes)?;
        let mut keep = vec![
            format!("{}.manifest", self.start),
            format!("{}.manifest", self.end),
            segment,
        ];
        keep.extend(before);
        self.prune(operator, &keep)
    }

    fn manifest(&self, operator: &str, digest: &str) -> SparkResult<Option<Vec<String>>> {
        let path = self.dir.join(operator).join(format!("{digest}.manifest"));
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(|e| {
                SparkError::internal(format!(
                    "read AutoCDC state manifest {}: {e}",
                    path.display()
                ))
            })?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(SparkError::internal(format!(
                "read AutoCDC state manifest {}: {error}",
                path.display()
            ))),
        }
    }

    /// Remove the operator's files other than `keep`.
    fn prune(&self, operator: &str, keep: &[String]) -> SparkResult<()> {
        let parent = self.dir.join(operator);
        for entry in std::fs::read_dir(&parent).map_err(|e| io_error(&parent, e))? {
            let entry = entry.map_err(|e| io_error(&parent, e))?;
            let name = entry.file_name().to_string_lossy().to_string();
            if !keep.contains(&name) {
                std::fs::remove_file(entry.path()).map_err(|e| io_error(&entry.path(), e))?;
            }
        }
        Ok(())
    }
}

fn io_error(path: &Path, error: std::io::Error) -> SparkError {
    SparkError::internal(format!("AutoCDC state {}: {error}", path.display()))
}

/// Write to a sibling file and rename, so a reader sees all of it or none.
fn write_atomically(path: &Path, bytes: &[u8]) -> SparkResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| SparkError::internal("state path has no parent"))?;
    std::fs::create_dir_all(parent).map_err(|e| io_error(parent, e))?;
    let staged = path.with_extension("next");
    std::fs::write(&staged, bytes).map_err(|e| io_error(&staged, e))?;
    std::fs::rename(&staged, path).map_err(|e| io_error(path, e))
}

fn write_arrow(path: &Path, snapshot: &Snapshot) -> SparkResult<()> {
    let schema = Arc::new(Schema::new_with_metadata(
        snapshot.schema.fields().clone(),
        HashMap::from([(WATERMARK_KEY.to_string(), snapshot.watermark.to_string())]),
    ));
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut bytes, &schema)?;
        for batch in &snapshot.batches {
            writer.write(&RecordBatch::try_new(
                schema.clone(),
                batch.columns().to_vec(),
            )?)?;
        }
        writer.finish()?;
    }
    write_atomically(path, &bytes)
}

/// Run a plan to its rows, with columns renamed to `prefix` and their
/// position, so rows from any plan make a table with distinct names. Each
/// column keeps the plan's nullability.
pub(super) async fn collect(
    ctx: &SessionContext,
    plan: LogicalPlan,
    prefix: &str,
) -> SparkResult<(SchemaRef, Vec<RecordBatch>)> {
    let physical = ctx.state().create_physical_plan(&plan).await?;
    let service = ctx.extension::<JobService>()?;
    let stream = service.runner().execute(ctx, physical.clone()).await?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    let schema = Arc::new(Schema::new(
        physical
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Field::new(
                    format!("{prefix}{index}"),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect::<Vec<_>>(),
    ));
    let batches = batches
        .into_iter()
        .map(|batch| {
            Ok(RecordBatch::try_new(
                schema.clone(),
                batch.columns().to_vec(),
            )?)
        })
        .collect::<SparkResult<Vec<_>>>()?;
    Ok((schema, batches))
}

fn read_arrow(path: &Path) -> SparkResult<Option<Snapshot>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(path, error)),
    };
    let reader = FileReader::try_new(Cursor::new(bytes), None)?;
    let stored = reader.schema();
    let watermark = stored
        .metadata()
        .get(WATERMARK_KEY)
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let schema = Arc::new(Schema::new(stored.fields().clone()));
    let batches = reader
        .map(|batch| {
            Ok(RecordBatch::try_new(
                schema.clone(),
                batch?.columns().to_vec(),
            )?)
        })
        .collect::<SparkResult<Vec<_>>>()?;
    Ok(Some(Snapshot {
        schema,
        batches,
        watermark,
    }))
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::DataType;

    use super::*;

    fn rows(values: &[i64]) -> Snapshot {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Int64, true)]));
        let column = Arc::new(Int64Array::from(values.to_vec()));
        Snapshot {
            batches: vec![RecordBatch::try_new(schema.clone(), vec![column]).unwrap()],
            schema,
            watermark: 0,
        }
    }

    fn values(snapshot: &Snapshot) -> Vec<i64> {
        snapshot
            .batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn keys(dir: &Path, start: &str, end: &str) -> StateKeys {
        StateKeys {
            dir: dir.to_path_buf(),
            start: start.into(),
            end: end.into(),
        }
    }

    #[test]
    fn appended_state_writes_only_each_batch_and_replays_exactly() {
        let dir = tempfile::tempdir().unwrap();
        let first = keys(dir.path(), "p0", "p1");
        assert!(first.load_appended("op").unwrap().is_none());
        first.append("op", &rows(&[1, 2])).unwrap();
        let second = keys(dir.path(), "p1", "p2");
        assert_eq!(
            values(&second.load_appended("op").unwrap().unwrap()),
            [1, 2]
        );
        second.append("op", &rows(&[3])).unwrap();
        // A replay of the second batch rewrites only its own segment.
        second.append("op", &rows(&[3])).unwrap();
        let third = keys(dir.path(), "p2", "p3");
        assert_eq!(
            values(&third.load_appended("op").unwrap().unwrap()),
            [1, 2, 3]
        );
        third.append("op", &rows(&[4])).unwrap();
        let mut files: Vec<String> = std::fs::read_dir(dir.path().join("op"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        files.sort();
        // Each segment is written once and kept while a manifest lists it.
        assert_eq!(
            files,
            [
                "p1.segment",
                "p2.manifest",
                "p2.segment",
                "p3.manifest",
                "p3.segment"
            ]
        );
    }
}
