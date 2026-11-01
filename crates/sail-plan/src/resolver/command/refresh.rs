use std::sync::Arc;

use datafusion_common::DFSchema;
use datafusion_expr::{EmptyRelation, LogicalPlan};
use sail_catalog::manager::CatalogManager;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Forget what is known about files, so the next read lists them again.
    /// Another session's write is not otherwise visible through these caches.
    /// The caches do not key by table, so this drops all their entries.
    pub(super) async fn resolve_command_refresh(
        &self,
        table: Option<spec::ObjectName>,
    ) -> PlanResult<LogicalPlan> {
        if let Some(table) = table {
            let name: Vec<String> = table.into();
            self.ctx
                .extension::<CatalogManager>()?
                .get_table_or_view(&name)
                .await?;
        }
        let runtime = self.ctx.runtime_env();
        if let Some(cache) = runtime.cache_manager.get_list_files_cache() {
            cache.clear();
        }
        if let Some(cache) = runtime.cache_manager.get_file_statistic_cache() {
            cache.clear();
        }
        Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        }))
    }
}
