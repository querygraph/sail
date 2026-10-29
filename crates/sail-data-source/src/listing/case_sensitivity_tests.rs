#![expect(clippy::unwrap_used)]

use std::sync::Mutex;

use bytes::Bytes;
use datafusion::datasource::physical_plan::JsonSource;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner};
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;

use super::*;
use crate::listing::planner::ListingPhysicalPlanner;

#[derive(Debug, Default)]
struct ScanObservations(Mutex<Vec<bool>>);

#[derive(Debug, Default)]
struct CaseFormatFactory;

#[derive(Debug)]
struct CaseReadFormat(Arc<ScanObservations>);

impl FormatFactory for CaseFormatFactory {
    type Read = CaseReadFormat;
    type Write = CaseReadFormat;

    fn name() -> &'static str {
        "case_policy_test"
    }

    fn read(ctx: &dyn Session, _options: Vec<OptionLayer>) -> Result<Self::Read> {
        Ok(CaseReadFormat(
            ctx.config().get_extension::<ScanObservations>().unwrap(),
        ))
    }

    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write> {
        Self::read(ctx, options)
    }
}

#[async_trait]
impl ReadFormat for CaseReadFormat {
    async fn infer_compression(
        &self,
        _ctx: &dyn Session,
        _files: &[ListingFileSample<'_>],
    ) -> Result<CompressionTypeVariant> {
        Ok(CompressionTypeVariant::UNCOMPRESSED)
    }

    async fn infer_schema(
        &self,
        _ctx: &dyn Session,
        _files: &[ListingFileSample<'_>],
        _compression: CompressionTypeVariant,
    ) -> Result<SchemaRef> {
        Ok(Arc::new(Schema::new(vec![Field::new(
            "Value",
            DataType::Int64,
            true,
        )])))
    }

    async fn scan(&self, _ctx: &dyn Session, input: ListingScanInput) -> Result<FileScanConfig> {
        self.0.0.lock().unwrap().push(input.read_case_sensitive);
        Ok(FileScanConfigBuilder::new(
            input.object_store_url,
            Arc::new(JsonSource::new(input.schema)),
        )
        .with_file_groups(input.file_groups)
        .with_constraints(input.constraints)
        .with_statistics(input.statistics)
        .with_projection_indices(input.projection)?
        .with_limit(input.limit)
        .with_output_ordering(input.output_ordering)
        .with_preserve_order(input.preserve_order)
        .with_output_partitioning(input.output_partitioning)
        .build())
    }
}

#[async_trait]
impl WriteFormat for CaseReadFormat {
    async fn sink(
        &self,
        _ctx: &dyn Session,
        _input: ListingSinkInput,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan_err!("test format does not support writing")
    }
}

#[tokio::test]
async fn source_read_case_policy_reaches_the_format_scan() -> Result<()> {
    let observations = Arc::new(ScanObservations::default());
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_extension(observations.clone())
            .with_collect_statistics(false)
            .with_target_partitions(1),
    );
    let store = Arc::new(InMemory::new());
    store
        .put(
            &Path::from("data/part.json"),
            Bytes::from_static(b"{\"Value\":1}\n").into(),
        )
        .await?;
    context.register_object_store(&Url::parse("memory://case-policy").unwrap(), store);
    let state = context.state();
    let planner = DefaultPhysicalPlanner::with_extension_planners(vec![]);

    for read_case_sensitive in [false, true] {
        let source = ListingDataSource::<CaseFormatFactory>::default()
            .create_source(
                &state,
                SourceInfo {
                    paths: vec!["memory://case-policy/data/".to_string()],
                    lakehouse_table: None,
                    schema: Some(Schema::new(vec![Field::new(
                        "value",
                        DataType::Int64,
                        true,
                    )])),
                    constraints: datafusion_common::Constraints::default(),
                    partition_by: vec![],
                    bucket_by: None,
                    sort_order: vec![],
                    options: vec![],
                    read_case_sensitive,
                },
            )
            .await?;
        // The existing up-front reconciliation remains unchanged; the captured
        // policy must also reach the format when it later opens individual files.
        let expected_name = if read_case_sensitive {
            "value"
        } else {
            "Value"
        };
        assert_eq!(source.schema().field(0).name(), expected_name);
        let plan = LogicalPlanBuilder::scan("case_policy", source, None)?.build()?;
        let LogicalPlan::TableScan(scan) = plan else {
            return plan_err!("expected a listing table scan");
        };
        let physical = ListingPhysicalPlanner
            .plan_table_scan(&planner, &scan, &state, &PhysicalPlanningContext::default())
            .await?
            .unwrap();
        assert_eq!(physical.schema().field(0).name(), expected_name);
    }
    assert_eq!(observations.0.lock().unwrap().as_slice(), &[false, true]);
    Ok(())
}
