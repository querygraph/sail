use datafusion_expr::TableSource;
use sail_catalog::provider::{CreateTableMode, CreateTableOptions};
use sail_common_datafusion::datasource::{DataSource, SinkInfo, SourceInfo, find_path_in_options};

use super::*;

struct CheckingWriter {
    owner: Option<Arc<TaskResourceScope>>,
    writes: Arc<AtomicUsize>,
    schemas: Arc<AtomicUsize>,
}
impl CheckingWriter {
    async fn metadata(&self, session: &dyn Session) -> Result<()> {
        let store = session
            .runtime_env()
            .object_store(ObjectStoreUrl::parse("memory://resolver")?)?;
        if let Some(expected) = &self.owner {
            let owner = session.extension::<TaskResourceScope>()?;
            assert!(Arc::ptr_eq(&owner, expected));
            assert!(Arc::ptr_eq(
                &session.task_ctx().extension::<TaskResourceScope>()?,
                expected
            ));
            assert_eq!(session.config().target_partitions(), 7);
            let value =
                owner
                    .run(move |_| async move {
                        Ok(store.get(&Path::from("input")).await?.bytes().await?)
                    })
                    .await?;
            assert_eq!(value.as_ref(), b"admitted");
        } else {
            assert!(session.extension::<TaskResourceScope>().is_err());
            assert_eq!(session.config().target_partitions(), 9);
            assert_eq!(
                store
                    .get(&Path::from("input"))
                    .await?
                    .bytes()
                    .await?
                    .as_ref(),
                b"parent"
            );
        }
        Ok(())
    }
}
#[tonic::async_trait]
impl DataSource for CheckingWriter {
    fn name(&self) -> &str {
        "writer_probe"
    }
    async fn create_source(&self, _: &dyn Session, _: SourceInfo) -> Result<Arc<dyn TableSource>> {
        datafusion_common::internal_err!("schema inference must not construct a read source")
    }
    async fn infer_schema(&self, session: &dyn Session, info: SourceInfo) -> Result<SchemaRef> {
        assert_eq!(info.paths, ["output"]);
        self.metadata(session).await?;
        self.schemas.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ])))
    }
    async fn create_writer(&self, session: &dyn Session, info: SinkInfo) -> Result<LogicalPlan> {
        assert_eq!(
            find_path_in_options(&info.options).as_deref(),
            Some("output")
        );
        self.metadata(session).await?;
        self.writes.fetch_add(1, Ordering::SeqCst);
        // Planning returns a plan only; no target publication happens here.
        Ok(info.input)
    }
}
async fn writer_case(scoped: bool, empty_catalog_schema: bool) -> TestResult {
    let fixture = Fixture::new().await?;
    let writes = Arc::new(AtomicUsize::new(0));
    let schemas = Arc::new(AtomicUsize::new(0));
    fixture
        .ctx
        .extension::<DataSourceRegistry>()?
        .register_data_source(Arc::new(CheckingWriter {
            owner: scoped.then(|| fixture.resources.clone()),
            writes: writes.clone(),
            schemas: schemas.clone(),
        }))?;
    let save_type = if empty_catalog_schema {
        fixture
            .ctx
            .extension::<CatalogManager>()?
            .create_table(
                &["target"],
                CreateTableOptions {
                    columns: vec![],
                    comment: None,
                    constraints: vec![],
                    location: Some("output".into()),
                    format: "writer_probe".into(),
                    partition_by: vec![],
                    sort_by: vec![],
                    bucket_by: None,
                    mode: CreateTableMode::Create,
                    properties: vec![],
                    is_external: true,
                    is_write_precondition: false,
                },
            )
            .await?;
        spec::SaveType::Table {
            table: spec::ObjectName::bare("target"),
            save_method: spec::TableSaveMethod::InsertInto,
        }
    } else {
        spec::SaveType::Path("output".into())
    };
    let resolver = if scoped {
        fixture.resolver()
    } else {
        PlanResolver::new(&fixture.ctx, Arc::new(PlanConfig::default()))
            .with_relation_scope(&fixture.inputs)
    };
    let resolved = resolver
        .resolve_named_plan(spec::Plan::Command(spec::CommandPlan::new(
            spec::CommandNode::Write(spec::Write {
                input: Box::new(input()),
                source: Some("writer_probe".into()),
                save_type,
                mode: Some(spec::SaveMode::Append),
                sort_columns: vec![],
                partitioning_columns: vec![],
                clustering_columns: vec![],
                bucket_by: None,
                options: vec![],
            }),
        )))
        .await?;
    assert!(matches!(resolved.plan, LogicalPlan::Extension(_)));
    assert_eq!(resolved.plan.schema().fields().len(), 2);
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    assert_eq!(
        schemas.load(Ordering::SeqCst),
        usize::from(empty_catalog_schema)
    );
    assert_eq!(
        fixture.calls.load(Ordering::SeqCst),
        0,
        "writer factory runs before physical planning"
    );
    assert_eq!(
        fixture.resources.drain().await?.registered,
        if scoped {
            1 + usize::from(empty_catalog_schema)
        } else {
            0
        }
    );
    assert!(fixture.ctx.extension::<TaskResourceScope>().is_err());
    assert_eq!(fixture.ctx.state().config().target_partitions(), 9);
    if empty_catalog_schema {
        let status = fixture
            .ctx
            .extension::<CatalogManager>()?
            .get_table(&["target"])
            .await?;
        let sail_common_datafusion::catalog::TableKind::Table { columns, .. } = status.kind else {
            return Err("target changed kind".into());
        };
        assert!(
            columns.is_empty(),
            "resolution must not rewrite catalog metadata"
        );
    }
    Ok(())
}
#[tokio::test]
async fn writer_factory_uses_selected_session_before_execution() -> TestResult {
    writer_case(true, false).await
}
#[tokio::test]
async fn writer_factory_without_view_preserves_authority() -> TestResult {
    writer_case(false, false).await
}
#[tokio::test]
async fn empty_catalog_schema_and_writer_share_selected_session() -> TestResult {
    writer_case(true, true).await
}
