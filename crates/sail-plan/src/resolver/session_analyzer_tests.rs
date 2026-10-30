use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::common::DataFusionError;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::{AnalyzerRule, OptimizerConfig, OptimizerRule};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, lit};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::task_resources::TaskResourceScope;
use tokio::runtime::Handle;

use super::*;
use crate::config::PlanConfig;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

#[derive(Debug)]
struct AnalyzerFailure(u32);
impl std::fmt::Display for AnalyzerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "native analyzer failure {}", self.0)
    }
}
impl std::error::Error for AnalyzerFailure {}

#[derive(Debug)]
struct Probe {
    expected: usize,
    calls: Arc<AtomicUsize>,
    failure: bool,
}
impl AnalyzerRule for Probe {
    fn name(&self) -> &str {
        "resolver_native_analyzer_probe"
    }
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        assert_eq!(config.execution.target_partitions, self.expected);
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.failure {
            Err(DataFusionError::External(Box::new(AnalyzerFailure(47))))
        } else {
            Ok(plan)
        }
    }
}
#[derive(Debug)]
struct OptimizerBarrier(Arc<AtomicUsize>);
impl OptimizerRule for OptimizerBarrier {
    fn name(&self) -> &str {
        "native_analysis_must_not_optimize"
    }
    fn rewrite(&self, _: LogicalPlan, _: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        datafusion_common::plan_err!("native analyzer ran optimization")
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Legacy,
    Generic,
    Scoped,
    GenericThenScoped,
    ScopedThenGeneric,
}
async fn check(mode: Mode, failure: bool) -> TestResult {
    let scoped = matches!(mode, Mode::Scoped | Mode::GenericThenScoped);
    let calls = Arc::new(AtomicUsize::new(0));
    let optimized = Arc::new(AtomicUsize::new(0));
    // A distinct post-capture authority rule catches both config and rule-set
    // fallback; updating only config would leave the same analyzer Arc in both.
    let captured_calls = Arc::new(AtomicUsize::new(0));
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(7))
            .with_analyzer_rules(vec![Arc::new(Probe {
                expected: 7,
                calls: captured_calls.clone(),
                failure,
            })])
            .with_optimizer_rules(vec![Arc::new(OptimizerBarrier(optimized.clone()))])
            .build(),
    );
    let resources = Arc::new(TaskResourceScope::new(Handle::current()));
    let snapshot = ScopedSession::new(ctx.state(), resources.clone());
    // Replace authority with a state built from itself, preserving its current
    // registries. This fixture tests query routing, not session command mutation.
    let state = SessionStateBuilder::new_from_existing(ctx.state())
        .with_config(SessionConfig::new().with_target_partitions(9))
        .with_analyzer_rules(vec![Arc::new(Probe {
            expected: 9,
            calls: calls.clone(),
            failure,
        })])
        .build();
    *ctx.state_ref().write() = state;
    let resolver = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()));
    let resolver = match mode {
        Mode::Legacy => resolver,
        Mode::Generic => resolver.with_query_session(&snapshot),
        Mode::Scoped => resolver.with_scoped_query_session(&snapshot),
        Mode::GenericThenScoped => resolver
            .with_query_session(&snapshot)
            .with_scoped_query_session(&snapshot),
        Mode::ScopedThenGeneric => resolver
            .with_scoped_query_session(&snapshot)
            .with_query_session(&snapshot),
    };
    let input = LogicalPlanBuilder::empty(false)
        .filter(lit(true))?
        .build()?;
    let result = resolver.analyze_native(input.clone());
    if failure {
        let error = result
            .err()
            .ok_or("typed native analyzer failure disappeared")?;
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
        let mut found = false;
        while let Some(current) = cause {
            if let Some(failure) = current.downcast_ref::<AnalyzerFailure>() {
                assert_eq!(failure.0, 47);
                found = true;
                break;
            }
            cause = current.source();
        }
        assert!(found, "typed native analyzer cause lost: {error}");
    } else {
        assert_eq!(result?, input, "analysis must retain the constant Filter");
    }
    assert_eq!(captured_calls.load(Ordering::SeqCst), usize::from(scoped));
    assert_eq!(calls.load(Ordering::SeqCst), usize::from(!scoped));
    assert_eq!(optimized.load(Ordering::SeqCst), 0);
    assert_eq!(ctx.state().config().target_partitions(), 9);
    assert!(ctx.extension::<TaskResourceScope>().is_err());
    assert_eq!(resources.drain().await?.registered, 0);
    Ok(())
}
#[tokio::test]
async fn typed_native_analyzer_is_captured_and_optimizer_free() -> TestResult {
    check(Mode::Scoped, false).await
}
#[tokio::test]
async fn typed_native_analyzer_retains_typed_failure() -> TestResult {
    check(Mode::Scoped, true).await
}
#[tokio::test]
async fn generic_session_and_absent_session_keep_authoritative_native_analysis() -> TestResult {
    check(Mode::Generic, false).await?;
    check(Mode::Legacy, false).await
}
#[tokio::test]
async fn last_session_setter_selects_native_analysis_mode() -> TestResult {
    check(Mode::ScopedThenGeneric, false).await?;
    check(Mode::GenericThenScoped, false).await
}
