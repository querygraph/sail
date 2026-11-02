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
        let path = self.path(operator, &self.start);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(SparkError::internal(format!(
                    "read AutoCDC state {}: {error}",
                    path.display()
                )));
            }
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

    /// Write the operator's state after the batch, then remove snapshots that
    /// neither this batch nor its replay can read.
    pub fn store(&self, operator: &str, snapshot: &Snapshot) -> SparkResult<()> {
        let path = self.path(operator, &self.end);
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
        let parent = path
            .parent()
            .ok_or_else(|| SparkError::internal("state path has no parent"))?;
        let io = |path: &Path, error: std::io::Error| {
            SparkError::internal(format!("AutoCDC state {}: {error}", path.display()))
        };
        std::fs::create_dir_all(parent).map_err(|e| io(parent, e))?;
        let staged = path.with_extension("next");
        std::fs::write(&staged, &bytes).map_err(|e| io(&staged, e))?;
        std::fs::rename(&staged, &path).map_err(|e| io(&path, e))?;
        let keep = [
            format!("{}.arrow", self.start),
            format!("{}.arrow", self.end),
        ];
        for entry in std::fs::read_dir(parent).map_err(|e| io(parent, e))? {
            let entry = entry.map_err(|e| io(parent, e))?;
            let name = entry.file_name().to_string_lossy().to_string();
            if !keep.contains(&name) {
                std::fs::remove_file(entry.path()).map_err(|e| io(&entry.path(), e))?;
            }
        }
        Ok(())
    }
}

/// Run a plan to its rows, with columns renamed to `prefix` and their
/// position, so rows from any plan make a table with distinct names.
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
                Field::new(format!("{prefix}{index}"), field.data_type().clone(), true)
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
