use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::common::DataFusionError;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{LogicalPlanBuilder, lit};
use datafusion::optimizer::{AnalyzerRule, OptimizerConfig, OptimizerRule};
use datafusion::prelude::SessionContext;
use tokio::runtime::Handle;

use super::*;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

#[derive(Debug)]
struct AnalyzeFailure(u32);
impl std::fmt::Display for AnalyzeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "analyzer failure {}", self.0)
    }
}
impl std::error::Error for AnalyzeFailure {}

#[derive(Debug)]
struct AnalyzerProbe {
    calls: Arc<AtomicUsize>,
    failure: bool,
}
impl AnalyzerRule for AnalyzerProbe {
    fn name(&self) -> &str {
        "captured_analyzer_probe"
    }
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        assert_eq!(config.execution.target_partitions, 7);
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.failure {
            Err(DataFusionError::External(Box::new(AnalyzeFailure(47))))
        } else {
            Ok(plan)
        }
    }
}
#[derive(Debug)]
struct OptimizerBarrier(Arc<AtomicUsize>);
impl OptimizerRule for OptimizerBarrier {
    fn name(&self) -> &str {
        "optimizer_must_not_run_during_analysis"
    }
    fn rewrite(&self, _: LogicalPlan, _: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        datafusion_common::plan_err!("analyzer-only helper ran optimization")
    }
}
async fn analyzer_case(failure: bool) -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let optimized = Arc::new(AtomicUsize::new(0));
    let context = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(7))
            .with_analyzer_rules(vec![Arc::new(AnalyzerProbe {
                calls: calls.clone(),
                failure,
            })])
            .with_optimizer_rules(vec![Arc::new(OptimizerBarrier(optimized.clone()))])
            .build(),
    );
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    let snapshot = ScopedSession::new(context.state(), resources.clone());
    context
        .state_ref()
        .write()
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = 9;
    let plan = LogicalPlanBuilder::empty(false)
        .filter(lit(true))?
        .build()?;
    let result = snapshot.analyze(plan.clone());
    if failure {
        let error = result.err().ok_or("analyzer failure was lost")?;
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        let mut found = false;
        while let Some(current) = cause {
            if let Some(failure) = current.downcast_ref::<AnalyzeFailure>() {
                assert_eq!(failure.0, 47);
                found = true;
                break;
            }
            cause = current.source();
        }
        assert!(found, "typed analyzer cause must remain reachable: {error}");
    } else {
        assert_eq!(
            result?, plan,
            "analysis must retain the Filter, not optimize it away"
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(optimized.load(Ordering::SeqCst), 0);
    assert_eq!(context.state().config().target_partitions(), 9);
    assert_eq!(resources.drain().await?.registered, 0);
    Ok(())
}
#[tokio::test]
async fn native_analysis_uses_captured_config_without_optimization() -> TestResult {
    analyzer_case(false).await
}
#[tokio::test]
async fn native_analysis_preserves_typed_failure_without_optimization() -> TestResult {
    analyzer_case(true).await
}
