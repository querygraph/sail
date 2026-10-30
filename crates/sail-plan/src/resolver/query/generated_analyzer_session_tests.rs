use sail_catalog::provider::{CreateTableColumnOptions, CreateTableMode, CreateTableOptions};

use super::*;

#[derive(Debug)]
struct GeneratedAnalyzer {
    expected: usize,
    calls: Arc<AtomicUsize>,
}
impl datafusion::optimizer::AnalyzerRule for GeneratedAnalyzer {
    fn name(&self) -> &str {
        "generated_column_native_analyzer_probe"
    }
    fn analyze(
        &self,
        plan: LogicalPlan,
        config: &datafusion_common::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
        assert_eq!(config.execution.target_partitions, self.expected);
        assert!(matches!(plan, LogicalPlan::Projection(_)));
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(plan)
    }
}
async fn generated(scoped: bool) -> TestResult {
    let analyzed = Arc::new(AtomicUsize::new(0));
    let fixture = Fixture::with_analyzer(Some(Arc::new(GeneratedAnalyzer {
        expected: if scoped { 7 } else { 9 },
        calls: analyzed.clone(),
    })))
    .await?;
    let manager = fixture.ctx.extension::<CatalogManager>()?;
    manager
        .create_table(
            &["target"],
            CreateTableOptions {
                columns: vec![
                    CreateTableColumnOptions {
                        name: "a".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        comment: None,
                        default: None,
                        generated_always_as: None,
                        identity: None,
                    },
                    CreateTableColumnOptions {
                        name: "g".into(),
                        data_type: DataType::Int64,
                        nullable: true,
                        comment: None,
                        default: None,
                        generated_always_as: Some("a".into()),
                        identity: None,
                    },
                ],
                comment: None,
                constraints: vec![],
                location: None,
                format: "delta".into(),
                partition_by: vec![],
                sort_by: vec![],
                bucket_by: None,
                mode: CreateTableMode::Create,
                properties: vec![],
                is_external: false,
                is_write_precondition: false,
            },
        )
        .await?;
    let resolver = fixture.resolver();
    let resolver = if scoped {
        resolver.with_scoped_query_session(&fixture.session)
    } else {
        resolver
    };
    let statement = sail_sql_analyzer::parser::parse_one_statement(
        "ALTER TABLE target REPLACE COLUMNS (a BIGINT, g BIGINT)",
    )?;
    let plan = sail_sql_analyzer::statement::from_ast_statement(statement)?;
    let _resolved = resolver.resolve_named_plan(plan).await?;
    assert_eq!(analyzed.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.calls.load(Ordering::SeqCst),
        0,
        "no physical planning"
    );
    assert_eq!(
        fixture.resources.drain().await?.registered,
        0,
        "no external metadata needed"
    );
    assert_eq!(fixture.ctx.state().config().target_partitions(), 9);
    assert!(fixture.ctx.extension::<TaskResourceScope>().is_err());
    Ok(())
}
#[tokio::test]
async fn generated_column_validation_uses_typed_captured_analyzer() -> TestResult {
    generated(true).await
}
#[tokio::test]
async fn generated_column_validation_keeps_generic_session_native_authority() -> TestResult {
    generated(false).await
}
