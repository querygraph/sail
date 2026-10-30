use std::sync::Mutex;

use datafusion_expr::TableSource;
use sail_catalog::provider::{CreateTableMode, CreateTableOptions, CreateViewOptions};
use sail_common_datafusion::datasource::{DataSource, SinkInfo, SourceInfo};

use super::*;

struct CaseSource {
    seen: Arc<Mutex<Vec<bool>>>,
    partitions: usize,
}
#[tonic::async_trait]
impl DataSource for CaseSource {
    fn name(&self) -> &str {
        "case_probe"
    }
    async fn create_source(
        &self,
        session: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        assert_eq!(session.config().target_partitions(), self.partitions);
        self.seen
            .lock()
            .map_err(|e| datafusion_common::DataFusionError::Internal(e.to_string()))?
            .push(info.read_case_sensitive);
        Ok(provider_as_source(Arc::new(MemTable::try_new(
            Arc::new(Schema::new(vec![Field::new("foo", DataType::Int64, false)])),
            vec![vec![]],
        )?)))
    }
    async fn create_writer(&self, _: &dyn Session, _: SinkInfo) -> Result<LogicalPlan> {
        datafusion_common::internal_err!("read-only case probe")
    }
}
fn query(route: usize) -> spec::QueryPlan {
    let read_type = if route == 0 {
        spec::ReadType::DataSource(Box::new(spec::ReadDataSource {
            format: Some("case_probe".into()),
            schema: None,
            options: vec![],
            paths: vec!["input".into()],
            predicates: vec![],
        }))
    } else {
        spec::ReadType::NamedTable(Box::new(spec::ReadNamedTable {
            name: if route == 1 {
                vec!["case_probe", "input"].into()
            } else {
                spec::ObjectName::bare(if route == 2 { "stored" } else { "v" })
            },
            temporal: None,
            sample: None,
            options: vec![],
        }))
    };
    spec::QueryPlan::new(spec::QueryNode::Project {
        input: Some(Box::new(spec::QueryPlan::new(spec::QueryNode::Read {
            read_type,
            is_streaming: false,
        }))),
        expressions: vec![spec::Expr::UnresolvedAttribute {
            name: spec::ObjectName::bare("Foo"),
            plan_id: None,
            is_metadata_column: false,
        }],
    })
}
async fn case(scoped: bool) -> TestResult {
    let fixture = Fixture::new().await?;
    let seen = Arc::new(Mutex::new(vec![]));
    fixture
        .ctx
        .extension::<DataSourceRegistry>()?
        .register_data_source(Arc::new(CaseSource {
            seen: seen.clone(),
            partitions: if scoped { 7 } else { 9 },
        }))?;
    fixture
        .ctx
        .extension::<CatalogManager>()?
        .create_table(
            &["stored"],
            CreateTableOptions {
                columns: vec![],
                comment: None,
                constraints: vec![],
                location: Some("input".into()),
                format: "case_probe".into(),
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
    fixture
        .ctx
        .extension::<CatalogManager>()?
        .create_view(
            &["v"],
            CreateViewOptions {
                definition: "SELECT Foo FROM case_probe.input".into(),
                columns: vec![],
                if_not_exists: false,
                replace: false,
                comment: None,
                properties: vec![
                    ("view.catalogAndNamespace.numParts".into(), "2".into()),
                    ("view.catalogAndNamespace.part.0".into(), "catalog".into()),
                    ("view.catalogAndNamespace.part.1".into(), "db".into()),
                ],
            },
        )
        .await?;
    for semantic in [false, true] {
        for override_read in [false, true] {
            for route in 0..4 {
                seen.lock().map_err(|e| e.to_string())?.clear();
                let mut resolver = PlanResolver::new(
                    &fixture.ctx,
                    Arc::new(PlanConfig {
                        case_sensitive: semantic,
                        ..Default::default()
                    }),
                );
                if scoped {
                    resolver = resolver.with_scoped_query_session(&fixture.session);
                }
                if override_read {
                    // Last setter wins, while the semantic flag stays independent.
                    resolver = resolver
                        .with_source_read_case_sensitive(semantic)
                        .with_source_read_case_sensitive(!semantic);
                }
                let outcome = resolver
                    .resolve_named_plan(spec::Plan::Query(query(route)))
                    .await
                    .map(|_| ());
                assert_eq!(
                    outcome.is_ok(),
                    !semantic,
                    "semantic={semantic}, override={override_read}, route={route}: {outcome:?}"
                );
                assert_eq!(
                    *seen.lock().map_err(|e| e.to_string())?,
                    [if override_read { !semantic } else { semantic }]
                );
            }
        }
    }
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.resources.drain().await?.registered, 0);
    Ok(())
}
#[tokio::test]
async fn read_case_override_preserves_legacy_session_and_semantic_resolution() -> TestResult {
    case(false).await
}
#[tokio::test]
async fn read_case_override_preserves_scoped_session_including_view_children() -> TestResult {
    case(true).await
}
