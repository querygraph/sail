use datafusion_expr::TableSource;
use sail_common_datafusion::datasource::{DataSource, SinkInfo, SourceInfo};
use sail_common_datafusion::lakeprocedure::{
    LakeProcedure, LakeProcedureAccess, LakeProcedureCall, LakeProcedureDataType,
    LakeProcedureParameter, LakeProcedurePlan, LakeProcedurePlanningTarget, LakeProcedureProvider,
    LakeProcedureResolution, LakeProcedureTarget,
};
use sail_common_datafusion::lakesource::{LakeSource, LakeSourceMetadata};

use super::lake_metadata::CheckingLake;
use super::*;

struct ProcedureSource {
    metadata: CheckingLake,
    predicate: bool,
    planned: Arc<AtomicUsize>,
}
impl ProcedureSource {
    fn descriptor(&self) -> LakeProcedure {
        LakeProcedure {
            name: "probe".into(),
            parameters: if self.predicate {
                vec![
                    LakeProcedureParameter::required("table", LakeProcedureDataType::Utf8),
                    LakeProcedureParameter::required("where", LakeProcedureDataType::SqlPredicate),
                ]
            } else {
                vec![]
            },
            output: vec![],
            access: LakeProcedureAccess::MetadataRead,
            target: if self.predicate {
                LakeProcedureTarget::TableOrPath {
                    parameter: "table".into(),
                }
            } else {
                LakeProcedureTarget::Catalog
            },
        }
    }
}
#[tonic::async_trait]
impl DataSource for ProcedureSource {
    fn name(&self) -> &str {
        "delta"
    }
    fn as_lake_source(self: Arc<Self>) -> Option<Arc<dyn LakeSource>> {
        Some(self)
    }
    async fn create_source(&self, _: &dyn Session, _: SourceInfo) -> Result<Arc<dyn TableSource>> {
        datafusion_common::internal_err!("procedure probe must not create a source")
    }
    async fn create_writer(&self, _: &dyn Session, _: SinkInfo) -> Result<LogicalPlan> {
        datafusion_common::internal_err!("procedure probe must not create a writer")
    }
}
#[tonic::async_trait]
impl LakeSource for ProcedureSource {
    fn procedure_provider(self: Arc<Self>) -> Option<Arc<dyn LakeProcedureProvider>> {
        Some(self)
    }
    async fn infer_metadata(
        &self,
        session: &dyn Session,
        info: SourceInfo,
    ) -> Result<LakeSourceMetadata> {
        self.metadata.infer_metadata(session, info).await
    }
}
#[tonic::async_trait]
impl LakeProcedureProvider for ProcedureSource {
    fn resolve_procedure(&self, namespace: &[String], name: &str) -> LakeProcedureResolution {
        if namespace == ["system"] && name == "probe" {
            self.get_procedure(name)
        } else {
            LakeProcedureResolution::Unrecognized
        }
    }
    fn get_procedure(&self, name: &str) -> LakeProcedureResolution {
        if name == "probe" {
            LakeProcedureResolution::Supported(self.descriptor())
        } else {
            LakeProcedureResolution::Unrecognized
        }
    }
    async fn plan_procedure(
        &self,
        session: &dyn Session,
        target: LakeProcedurePlanningTarget,
        call: &LakeProcedureCall,
    ) -> Result<LakeProcedurePlan> {
        assert_eq!(call.invocation.procedure, self.descriptor());
        match target {
            LakeProcedurePlanningTarget::Catalog { catalog } => {
                assert!(!self.predicate);
                assert_eq!(catalog, "catalog");
            }
            LakeProcedurePlanningTarget::Table { info, predicates } => {
                assert!(self.predicate);
                assert_eq!(info.paths, ["/input"]);
                assert_eq!(predicates.len(), 1);
                assert_eq!(predicates[0].name, "where");
            }
        }
        self.planned.fetch_add(1, Ordering::SeqCst);
        self.metadata.metadata(session).await?;
        datafusion_common::plan_err!("procedure probe stops before implementation planning")
    }
}
fn text(value: &str) -> spec::Expr {
    spec::Expr::Literal(spec::Literal::Utf8 {
        value: Some(value.into()),
    })
}
async fn procedure(scoped: bool, predicate: bool) -> TestResult {
    let fixture = Fixture::new().await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let planned = Arc::new(AtomicUsize::new(0));
    fixture
        .ctx
        .extension::<DataSourceRegistry>()?
        .register_data_source(Arc::new(ProcedureSource {
            metadata: CheckingLake::new(scoped.then(|| fixture.resources.clone()), calls.clone()),
            predicate,
            planned: planned.clone(),
        }))?;
    let command = spec::CommandNode::CallProcedure {
        procedure: vec!["system", "probe"].into(),
        arguments: if predicate {
            vec![text("delta.`/input`"), text("true")]
        } else {
            vec![]
        },
        named_arguments: vec![],
    };
    let resolver = if scoped {
        fixture.resolver()
    } else {
        PlanResolver::new(&fixture.ctx, Arc::new(PlanConfig::default()))
    };
    let result = resolver
        .resolve_command_plan(
            spec::CommandPlan::new(command),
            &mut PlanResolverState::new(),
        )
        .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => return Err("procedure probe must stop resolution".into()),
    };
    assert!(
        error
            .to_string()
            .contains("procedure probe stops before implementation planning"),
        "{error}"
    );
    assert_eq!(planned.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1 + usize::from(predicate));
    assert_eq!(
        fixture.resources.drain().await?.registered,
        if scoped {
            1 + usize::from(predicate)
        } else {
            0
        }
    );
    assert_eq!(
        fixture.calls.load(Ordering::SeqCst),
        0,
        "no physical planning"
    );
    assert!(fixture.ctx.extension::<TaskResourceScope>().is_err());
    Ok(())
}
#[tokio::test]
async fn catalog_procedure_uses_selected_session() -> TestResult {
    procedure(true, false).await
}
#[tokio::test]
async fn catalog_procedure_keeps_legacy_authority() -> TestResult {
    procedure(false, false).await
}
#[tokio::test]
async fn predicate_procedure_uses_selected_session() -> TestResult {
    procedure(true, true).await
}
#[tokio::test]
async fn predicate_procedure_keeps_legacy_authority() -> TestResult {
    procedure(false, true).await
}

#[derive(Debug)]
struct CapturedPredicateAnalyzer(Arc<AtomicUsize>);
impl datafusion::optimizer::AnalyzerRule for CapturedPredicateAnalyzer {
    fn name(&self) -> &str {
        "captured_procedure_predicate_probe"
    }
    fn analyze(
        &self,
        plan: LogicalPlan,
        config: &datafusion_common::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
        assert_eq!(config.execution.target_partitions, 7);
        assert!(matches!(plan, LogicalPlan::Filter(_)));
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(plan)
    }
}
#[tokio::test]
async fn predicate_procedure_uses_typed_captured_native_analyzer() -> TestResult {
    let analyzed = Arc::new(AtomicUsize::new(0));
    let fixture =
        Fixture::with_analyzer(Some(Arc::new(CapturedPredicateAnalyzer(analyzed.clone())))).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let planned = Arc::new(AtomicUsize::new(0));
    fixture
        .ctx
        .extension::<DataSourceRegistry>()?
        .register_data_source(Arc::new(ProcedureSource {
            metadata: CheckingLake::new(Some(fixture.resources.clone()), calls.clone()),
            predicate: true,
            planned: planned.clone(),
        }))?;
    let result = fixture
        .resolver()
        .with_scoped_query_session(&fixture.session)
        .resolve_command_plan(
            spec::CommandPlan::new(spec::CommandNode::CallProcedure {
                procedure: vec!["system", "probe"].into(),
                arguments: vec![text("delta.`/input`"), text("true")],
                named_arguments: vec![],
            }),
            &mut PlanResolverState::new(),
        )
        .await;
    let error = result.err().ok_or("procedure probe did not stop")?;
    assert!(
        error
            .to_string()
            .contains("procedure probe stops before implementation planning"),
        "{error}"
    );
    assert_eq!(analyzed.load(Ordering::SeqCst), 1);
    assert_eq!(planned.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.resources.drain().await?.registered, 2);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.ctx.state().config().target_partitions(), 9);
    Ok(())
}
