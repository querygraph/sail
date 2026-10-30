use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::apply_expression_roots;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_common::{Result, exec_err, internal_err, not_impl_err};
use datafusion_datasource::TableSchema;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use object_store::ObjectStore;

use super::OrcJavaTimeZoneConfig;
use super::requested_schema::OrcRequestedSchemaAdapter;
use crate::options::r#gen::OrcReadOptions;

#[path = "opener.rs"]
mod opener;

use opener::OrcOpener;

/// An ordinary file source with captured ORC semantics and a local runtime pool.
///
/// `table_schema` contains the complete requested file schema, even when a later
/// projection reads only some columns. The reader timezone is captured by the
/// driver; workers must reconstruct this source with that descriptor and their
/// own memory pool. The session timezone only labels logical timestamp output.
/// File-field nullability follows ORC read semantics; a listing entry point must
/// publish the same normalized schema before building its logical table plan.
/// Ordinary partition columns are supported. Reader-generated virtual columns
/// are not implemented and are rejected before constructing the projection.
#[derive(Debug, Clone)]
pub struct OrcSource {
    table_schema: TableSchema,
    options: OrcReadOptions,
    reader_timezone: OrcJavaTimeZoneConfig,
    session_timezone: String,
    read_case_sensitive: bool,
    pool: Arc<dyn MemoryPool>,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projection: SplitProjection,
}

impl OrcSource {
    pub fn try_new(
        table_schema: impl Into<TableSchema>,
        options: OrcReadOptions,
        reader_timezone: OrcJavaTimeZoneConfig,
        session_timezone: String,
        read_case_sensitive: bool,
        pool: Arc<dyn MemoryPool>,
    ) -> Result<Self> {
        // A deserialized worker descriptor must match the compiled profile
        // before any file is opened. Never recapture the worker's local zone.
        reader_timezone.native_options()?;
        let table_schema = table_schema.into();
        if !table_schema.virtual_columns().is_empty() {
            // ProjectionOpener treats all non-file fields as partition values.
            // A virtual field would otherwise index beyond partition_values.
            return not_impl_err!("ORC virtual columns are not supported");
        }
        let table_schema = TableSchema::builder(OrcRequestedSchemaAdapter::nullable_schema(
            table_schema.file_schema(),
        ))
        .with_table_partition_cols(table_schema.table_partition_cols().clone())
        .build();
        Ok(Self {
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            options,
            reader_timezone,
            session_timezone,
            read_case_sensitive,
            pool,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn options(&self) -> &OrcReadOptions {
        &self.options
    }

    pub fn reader_timezone(&self) -> &OrcJavaTimeZoneConfig {
        &self.reader_timezone
    }

    pub fn session_timezone(&self) -> &str {
        &self.session_timezone
    }

    pub fn read_case_sensitive(&self) -> bool {
        self.read_case_sensitive
    }
}

impl From<OrcSource> for Arc<dyn FileSource> {
    fn from(source: OrcSource) -> Self {
        Arc::new(source)
    }
}

impl FileSource for OrcSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let Some(batch_size) = self.batch_size else {
            return internal_err!("ORC batch size must be set before creating the file opener");
        };
        if batch_size == 0 {
            return exec_err!("ORC batch size must be greater than zero");
        }
        if base_config.file_compression_type.is_compressed() {
            return exec_err!("ORC does not support outer file compression");
        }
        let opener = Arc::new(OrcOpener::new(
            Arc::new(self.clone()),
            object_store,
            batch_size,
        ));
        ProjectionOpener::try_new(
            self.projection.clone(),
            opener,
            self.table_schema.file_schema(),
        )
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut source = self.clone();
        source.batch_size = Some(batch_size);
        Arc::new(source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let projection = self.projection.source.try_merge(projection)?;
        source.projection = SplitProjection::new(self.table_schema.file_schema(), &projection);
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.projection.source.iter(), f)
    }

    fn file_type(&self) -> &str {
        "orc"
    }

    fn supports_repartitioning(&self) -> bool {
        // Explicit ranges use stripe-start inclusion. Automatic splitting is
        // kept separate until optimizer/worker range reconstruction is qualified.
        false
    }
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
