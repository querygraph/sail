//! Tests for bounded logical-plan admission.

use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::common::{Result as DFResult, ScalarValue, TableReference};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::function::{
    AccumulatorArgs, PartitionEvaluatorArgs, WindowUDFFieldArgs,
};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, Expr, JoinType, LogicalPlanBuilder,
    PartitionEvaluator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    WindowUDF, WindowUDFImpl, col, exists, lit, scalar_subquery,
};

use super::super::intent::BatchIntent;
use super::super::source::SourceDescriptor;
use super::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn name(parts: &[&str]) -> Name {
    parts.iter().map(|part| part.to_string()).collect()
}

fn table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]))
}

fn provider() -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(MemTable::try_new(table_schema(), vec![vec![]])?))
}

fn scan_with(parts: &[&str], provider: Arc<dyn TableProvider>) -> DFResult<LogicalPlanBuilder> {
    let reference = TableReference::full(parts[0], parts[1], parts[2]);
    LogicalPlanBuilder::scan(reference, provider_as_source(provider), None)
}

/// The bindings of the tests: `clicks` and `views` are bound sources
/// with one range provider each, `dim` a static binding, `events` a
/// materialised local output; `scan` uses the bound providers.
struct Fixture {
    sources: Vec<SourceBinding>,
    statics: Vec<StaticBinding>,
    locals: Vec<LocalBinding>,
    reads: ReadSet,
}

impl Fixture {
    fn new() -> DFResult<Self> {
        let source = |parts: &[&str]| -> DFResult<SourceBinding> {
            Ok(SourceBinding {
                name: name(parts),
                schema: table_schema(),
                descriptor: SourceDescriptor {
                    kind: "test".into(),
                    descriptor: serde_json::json!({}),
                    partitions: vec![],
                },
                intent: BatchIntent { ranges: vec![] },
                scans: vec![provider()?],
            })
        };
        Ok(Self {
            sources: vec![
                source(&["c", "d", "clicks"])?,
                source(&["c", "d", "views"])?,
            ],
            statics: vec![StaticBinding {
                name: name(&["c", "d", "dim"]),
                identity: "version:7".into(),
                provider: provider()?,
            }],
            locals: vec![LocalBinding {
                name: name(&["c", "d", "events"]),
                identity: "run:1".into(),
                provider: provider()?,
            }],
            reads: ReadSet {
                streaming: BTreeSet::from([
                    name(&["c", "d", "clicks"]),
                    name(&["c", "d", "views"]),
                ]),
                local: BTreeSet::from([name(&["c", "d", "events"])]),
                statics: BTreeSet::from([name(&["c", "d", "dim"])]),
            },
        })
    }

    fn admit(&self, plan: &LogicalPlan) -> AdmissionEvidence {
        admit(
            plan,
            &self.reads,
            &Bindings {
                sources: &self.sources,
                statics: &self.statics,
                locals: &self.locals,
                empty_scans: &[],
            },
        )
    }

    /// A scan over the provider bound to `parts`, or over a fresh,
    /// unbound provider for any other name.
    fn scan(&self, parts: &[&str]) -> DFResult<LogicalPlanBuilder> {
        let target = name(parts);
        let bound = self
            .sources
            .iter()
            .find(|b| b.name == target)
            .and_then(|b| b.scans.first().cloned())
            .or_else(|| {
                self.statics
                    .iter()
                    .find(|b| b.name == target)
                    .map(|b| b.provider.clone())
            })
            .or_else(|| {
                self.locals
                    .iter()
                    .find(|b| b.name == target)
                    .map(|b| b.provider.clone())
            });
        match bound {
            Some(provider) => scan_with(parts, provider),
            None => scan_with(parts, provider()?),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Volatile {
    signature: Signature,
}

impl ScalarUDFImpl for Volatile {
    fn name(&self) -> &str {
        "volatile_test"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(
            datafusion::common::ScalarValue::Int64(Some(4)),
        ))
    }
}

fn volatile() -> Expr {
    ScalarUDF::from(Volatile {
        signature: Signature::nullary(Volatility::Volatile),
    })
    .call(vec![])
}

/// A `Stable` nullary function with the signature class of Sail's
/// `TimestampNow` (`current_timestamp`/`now`), which this crate cannot
/// name directly.
#[derive(Debug, PartialEq, Eq, Hash)]
struct StableClock {
    signature: Signature,
}

impl ScalarUDFImpl for StableClock {
    fn name(&self) -> &str {
        "stable_clock_test"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(
            datafusion::common::ScalarValue::Int64(Some(1_700_000_000)),
        ))
    }
}

fn stable_clock() -> Expr {
    ScalarUDF::from(StableClock {
        signature: Signature::nullary(Volatility::Stable),
    })
    .call(vec![])
}

/// A `Stable` aggregate with a trivial accumulator. The Aggregate and
/// Window plan nodes are rejected before their expressions are audited,
/// so the aggregate and window arms of the signature walk are reached
/// through a hand-built projection carrying the expression.
#[derive(Debug, PartialEq, Eq, Hash)]
struct StableAggregate {
    signature: Signature,
}

impl AggregateUDFImpl for StableAggregate {
    fn name(&self) -> &str {
        "stable_aggregate_test"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }
    fn accumulator(&self, _: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(Counter(0)))
    }
}

#[derive(Debug)]
struct Counter(i64);

impl Accumulator for Counter {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let rows = values.first().map_or(0, |array| array.len());
        self.0 += i64::try_from(rows).unwrap_or(i64::MAX);
        Ok(())
    }
    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.0)))
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.0))])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.update_batch(states)
    }
}

fn stable_aggregate() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::from(StableAggregate {
        signature: Signature::exact(vec![DataType::Int64], Volatility::Stable),
    }))
}

/// A `Stable` window function whose evaluator is never run.
#[derive(Debug, PartialEq, Eq, Hash)]
struct StableWindow {
    signature: Signature,
}

impl WindowUDFImpl for StableWindow {
    fn name(&self) -> &str {
        "stable_window_test"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn partition_evaluator(
        &self,
        _: PartitionEvaluatorArgs,
    ) -> DFResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(NeverEvaluated))
    }
    fn field(&self, args: WindowUDFFieldArgs) -> DFResult<FieldRef> {
        Ok(Arc::new(Field::new(args.name(), DataType::Int64, true)))
    }
}

#[derive(Debug)]
struct NeverEvaluated;

impl PartitionEvaluator for NeverEvaluated {}

fn stable_window() -> Arc<WindowUDF> {
    Arc::new(WindowUDF::from(StableWindow {
        signature: Signature::exact(vec![DataType::Int64], Volatility::Stable),
    }))
}

/// Shared body of the aggregate/window cases: the projection is admitted,
/// not replayable, and the evidence names the function as stable.
fn assert_projection_not_replayable(expr: Expr, name: &str) -> TestResult {
    let f = Fixture::new()?;
    let plan = f
        .scan(&["c", "d", "clicks"])?
        .project(vec![expr.alias("x")])?
        .build()?;
    let evidence = f.admit(&plan);
    assert!(evidence.admitted, "{evidence:?}");
    assert!(!evidence.replayable, "{evidence:?}");
    assert!(
        evidence
            .operators
            .iter()
            .any(|op| op.reason.contains(&format!("`{name}` is stable"))),
        "{evidence:?}"
    );
    Ok(())
}

/// Shared body of the stable-clock cases: the function must be declared
/// `Stable` (not merely assumed so), the plan is admitted, and it is not
/// replayable in content, with the evidence naming the function.
fn assert_stable_clock_not_replayable(clock: Expr) -> TestResult {
    let f = Fixture::new()?;
    let Expr::ScalarFunction(function) = &clock else {
        return Err("clock fixture must remain a scalar function".into());
    };
    assert_eq!(function.func.signature().volatility, Volatility::Stable);
    let name = function.func.name().to_string();
    let plan = f
        .scan(&["c", "d", "clicks"])?
        .project(vec![col("id"), clock.alias("clock")])?
        .build()?;
    let evidence = f.admit(&plan);
    assert!(evidence.admitted, "{evidence:?}");
    assert!(
        !evidence.replayable,
        "stable clock can change on replay: {evidence:?}"
    );
    assert!(
        evidence
            .operators
            .iter()
            .any(|op| op.reason.contains(&format!("`{name}` is stable"))),
        "{evidence:?}"
    );
    Ok(())
}

fn using(builder: LogicalPlanBuilder, right: LogicalPlan, kind: JoinType) -> DFResult<LogicalPlan> {
    builder
        .join(right, kind, (vec!["id"], vec!["id"]), None)?
        .build()
}

#[test]
fn a_filtered_projection_over_one_bound_range_is_admitted_and_replayable() -> TestResult {
    let f = Fixture::new()?;
    let plan = f
        .scan(&["c", "d", "clicks"])?
        .filter(col("v").gt(lit(1)))?
        .project(vec![col("id")])?
        .build()?;
    let evidence = f.admit(&plan);
    assert!(evidence.admitted, "{evidence:?}");
    assert!(evidence.replayable);
    assert!(evidence.provenance_verified);
    assert_eq!(
        evidence.sources_read,
        BTreeSet::from([name(&["c", "d", "clicks"])])
    );
    assert!(evidence.unbound_scans.is_empty());
    assert!(
        evidence
            .operators
            .iter()
            .all(|op| op.decision == Decision::Admit)
    );
    Ok(())
}

#[test]
fn a_join_needs_exactly_one_range_bearing_side() -> TestResult {
    let f = Fixture::new()?;
    let stream_static = using(
        f.scan(&["c", "d", "clicks"])?,
        f.scan(&["c", "d", "dim"])?.build()?,
        JoinType::Inner,
    )?;
    let evidence = f.admit(&stream_static);
    assert!(evidence.admitted, "{evidence:?}");
    assert_eq!(
        evidence.static_read,
        BTreeSet::from([name(&["c", "d", "dim"])])
    );

    let stream_stream = using(
        f.scan(&["c", "d", "clicks"])?,
        f.scan(&["c", "d", "views"])?.build()?,
        JoinType::Inner,
    )?;
    let evidence = f.admit(&stream_stream);
    assert!(!evidence.admitted);
    assert!(evidence.operators.iter().any(|op| op.operator == "Join"
        && op.decision == Decision::Reject
        && op.reason.contains("cross-batch")));

    // Through an alias and a projection, nested under another join with
    // a materialised local side: still two range-bearing sides.
    let nested = f
        .scan(&["c", "d", "clicks"])?
        .alias("a")?
        .project(vec![col("a.id")])?
        .join(
            f.scan(&["c", "d", "views"])?.alias("b")?.build()?,
            JoinType::Inner,
            (vec!["a.id"], vec!["b.id"]),
            None,
        )?
        .join(
            f.scan(&["c", "d", "events"])?.build()?,
            JoinType::Inner,
            (vec!["a.id"], vec!["id"]),
            None,
        )?
        .build()?;
    assert!(!f.admit(&nested).admitted);
    Ok(())
}

#[test]
fn join_kinds_follow_sparks_stream_static_orientation() -> TestResult {
    let f = Fixture::new()?;
    let cases: Vec<(JoinType, bool, bool)> = vec![
        // kind, admitted with the range side on the left, on the right
        (JoinType::Inner, true, true),
        (JoinType::Left, true, false),
        (JoinType::LeftSemi, true, false),
        (JoinType::LeftAnti, true, false),
        (JoinType::Right, false, true),
        (JoinType::Full, false, false),
        (JoinType::RightSemi, false, false),
        (JoinType::RightAnti, false, false),
        (JoinType::LeftMark, false, false),
        (JoinType::RightMark, false, false),
    ];
    for (kind, left_ok, right_ok) in cases {
        let stream_left = using(
            f.scan(&["c", "d", "clicks"])?,
            f.scan(&["c", "d", "dim"])?.build()?,
            kind,
        )?;
        let left = f.admit(&stream_left);
        assert_eq!(left.admitted, left_ok, "{kind:?} range left: {left:?}");
        let stream_right = using(
            f.scan(&["c", "d", "dim"])?,
            f.scan(&["c", "d", "clicks"])?.build()?,
            kind,
        )?;
        let right = f.admit(&stream_right);
        assert_eq!(right.admitted, right_ok, "{kind:?} range right: {right:?}");
    }
    // Static with static: every kind is admitted.
    let static_static = using(
        f.scan(&["c", "d", "dim"])?,
        f.scan(&["c", "d", "events"])?.build()?,
        JoinType::Full,
    )?;
    assert!(f.admit(&static_static).admitted);
    Ok(())
}

#[test]
fn stateful_operators_and_unbound_scans_are_rejected_with_evidence() -> TestResult {
    let f = Fixture::new()?;
    let aggregate = f
        .scan(&["c", "d", "clicks"])?
        .aggregate(vec![col("id")], Vec::<Expr>::new())?
        .build()?;
    let evidence = f.admit(&aggregate);
    assert!(!evidence.admitted);
    assert!(
        evidence
            .operators
            .iter()
            .any(|op| op.operator == "Aggregate" && op.decision == Decision::Reject)
    );
    let limited = f.scan(&["c", "d", "clicks"])?.limit(0, Some(5))?.build()?;
    assert!(!f.admit(&limited).admitted);
    let sorted = f
        .scan(&["c", "d", "clicks"])?
        .sort(vec![col("id").sort(true, true)])?
        .build()?;
    assert!(!f.admit(&sorted).admitted);
    let distinct = f.scan(&["c", "d", "clicks"])?.distinct()?.build()?;
    assert!(!f.admit(&distinct).admitted);

    let unbound = f.scan(&["c", "d", "elsewhere"])?.build()?;
    let evidence = f.admit(&unbound);
    assert!(!evidence.admitted);
    assert!(!evidence.provenance_verified);
    assert_eq!(evidence.unbound_scans, vec!["c.d.elsewhere".to_string()]);
    Ok(())
}

#[test]
fn a_scan_is_bound_by_provider_identity_not_by_name() -> TestResult {
    let f = Fixture::new()?;
    // Same name as a bound source, a different provider.
    let impostor = scan_with(&["c", "d", "clicks"], provider()?)?.build()?;
    let evidence = f.admit(&impostor);
    assert!(!evidence.admitted, "{evidence:?}");
    assert!(!evidence.provenance_verified);
    assert_eq!(evidence.unbound_scans, vec!["c.d.clicks".to_string()]);
    assert!(evidence.sources_read.is_empty());
    // Same name as the materialised local output, a substituted provider.
    let substituted = scan_with(&["c", "d", "events"], provider()?)?.build()?;
    let evidence = f.admit(&substituted);
    assert!(!evidence.admitted, "{evidence:?}");
    assert!(evidence.static_read.is_empty());
    // Same name as the static binding, a different provider.
    let other_dim = scan_with(&["c", "d", "dim"], provider()?)?.build()?;
    assert!(!f.admit(&other_dim).admitted);
    // The analysis placeholder cannot stand in for the rows of a local
    // output, while the materialised provider can.
    let placeholder = LogicalPlanBuilder::empty(false).build()?;
    let evidence = f.admit(&placeholder);
    assert!(!evidence.admitted, "{evidence:?}");
    assert!(
        evidence
            .operators
            .iter()
            .any(|op| op.operator == "EmptyRelation" && op.decision == Decision::Reject)
    );
    assert!(f.admit(&f.scan(&["c", "d", "events"])?.build()?).admitted);
    Ok(())
}

#[test]
fn stable_now_is_not_content_replayable() -> TestResult {
    assert_stable_clock_not_replayable(datafusion::functions::datetime::expr_fn::now())
}

#[test]
fn stable_current_date_is_not_content_replayable() -> TestResult {
    assert_stable_clock_not_replayable(datafusion::functions::datetime::expr_fn::current_date())
}

#[test]
fn a_stable_nullary_function_is_not_content_replayable() -> TestResult {
    assert_stable_clock_not_replayable(stable_clock())
}

#[test]
fn a_stable_function_inside_a_filter_is_not_content_replayable() -> TestResult {
    // Stability is audited on every operator's expressions, not only on
    // projections.
    let f = Fixture::new()?;
    let plan = f
        .scan(&["c", "d", "clicks"])?
        .filter(col("id").lt(stable_clock()))?
        .build()?;
    let evidence = f.admit(&plan);
    assert!(evidence.admitted, "{evidence:?}");
    assert!(!evidence.replayable, "{evidence:?}");
    Ok(())
}

#[test]
fn immutable_projection_remains_content_replayable() -> TestResult {
    let f = Fixture::new()?;
    let plan = f
        .scan(&["c", "d", "clicks"])?
        .project(vec![(col("id") + lit(1_i64)).alias("next")])?
        .build()?;
    let evidence = f.admit(&plan);
    assert!(evidence.admitted, "{evidence:?}");
    assert!(evidence.replayable, "{evidence:?}");
    Ok(())
}

#[test]
fn immutable_builtin_functions_remain_content_replayable() -> TestResult {
    // A real Immutable DataFusion function is the control for the
    // signature check: nothing about being a function call is penalised.
    let f = Fixture::new()?;
    let plan = f
        .scan(&["c", "d", "clicks"])?
        .project(vec![
            datafusion::functions::math::expr_fn::abs(col("v")).alias("a"),
        ])?
        .build()?;
    let evidence = f.admit(&plan);
    assert!(evidence.admitted, "{evidence:?}");
    assert!(evidence.replayable, "{evidence:?}");
    Ok(())
}

#[test]
fn a_stable_aggregate_signature_is_not_content_replayable() -> TestResult {
    assert_projection_not_replayable(
        stable_aggregate().call(vec![col("v")]),
        "stable_aggregate_test",
    )
}

#[test]
fn a_stable_aggregate_used_as_a_window_is_not_content_replayable() -> TestResult {
    let window = WindowFunction::new(
        WindowFunctionDefinition::AggregateUDF(stable_aggregate()),
        vec![col("v")],
    );
    assert_projection_not_replayable(
        Expr::WindowFunction(Box::new(window)),
        "stable_aggregate_test",
    )
}

#[test]
fn a_stable_window_function_is_not_content_replayable() -> TestResult {
    let window = WindowFunction::new(
        WindowFunctionDefinition::WindowUDF(stable_window()),
        vec![col("v")],
    );
    assert_projection_not_replayable(Expr::WindowFunction(Box::new(window)), "stable_window_test")
}

#[test]
fn an_immutable_aggregate_signature_keeps_content_replayable() -> TestResult {
    // Control: a real Immutable aggregate (`count`) in the same position.
    let f = Fixture::new()?;
    let count = datafusion::functions_aggregate::expr_fn::count(col("v"));
    let Expr::AggregateFunction(function) = &count else {
        return Err("count fixture must remain an aggregate function".into());
    };
    assert_eq!(function.func.signature().volatility, Volatility::Immutable);
    let plan = f
        .scan(&["c", "d", "clicks"])?
        .project(vec![count.alias("n")])?
        .build()?;
    let evidence = f.admit(&plan);
    assert!(evidence.admitted, "{evidence:?}");
    assert!(evidence.replayable, "{evidence:?}");
    Ok(())
}

#[test]
fn a_volatile_expression_is_admitted_but_not_replayable() -> TestResult {
    let f = Fixture::new()?;
    let plan = f
        .scan(&["c", "d", "clicks"])?
        .project(vec![col("id"), volatile().alias("r")])?
        .build()?;
    let evidence = f.admit(&plan);
    assert!(evidence.admitted, "{evidence:?}");
    assert!(!evidence.replayable);
    assert!(
        evidence
            .operators
            .iter()
            .any(|op| op.reason.contains("volatile"))
    );
    // Volatility inside literal rows is audited too.
    let values = LogicalPlanBuilder::values(vec![vec![volatile()]])?.build()?;
    assert!(!f.admit(&values).replayable);
    Ok(())
}

#[test]
fn subqueries_count_one_side_each() -> TestResult {
    let f = Fixture::new()?;
    // Union of ranges is row-local.
    let union = f
        .scan(&["c", "d", "clicks"])?
        .union(f.scan(&["c", "d", "views"])?.build()?)?
        .build()?;
    let evidence = f.admit(&union);
    assert!(evidence.admitted, "{evidence:?}");
    assert_eq!(evidence.sources_read.len(), 2);
    // EXISTS over a range-bearing subquery under a range-bearing input.
    let two = f
        .scan(&["c", "d", "clicks"])?
        .filter(exists(Arc::new(f.scan(&["c", "d", "views"])?.build()?)))?
        .build()?;
    assert!(!f.admit(&two).admitted);
    // EXISTS over a static subquery under a range-bearing input.
    let one = f
        .scan(&["c", "d", "clicks"])?
        .filter(exists(Arc::new(f.scan(&["c", "d", "dim"])?.build()?)))?
        .build()?;
    assert!(f.admit(&one).admitted);
    // Two range-bearing scalar subqueries under a static input, one of
    // them nested inside a function: two sides, rejected.
    let plan = f
        .scan(&["c", "d", "dim"])?
        .project(vec![
            scalar_subquery(Arc::new(f.scan(&["c", "d", "clicks"])?.build()?)).alias("a"),
            // Nested inside an arithmetic expression and a cast.
            datafusion::logical_expr::cast(
                scalar_subquery(Arc::new(f.scan(&["c", "d", "views"])?.build()?)) + lit(0_i64),
                DataType::Int64,
            )
            .alias("b"),
        ])?
        .build()?;
    let evidence = f.admit(&plan);
    assert!(!evidence.admitted, "{evidence:?}");
    assert!(
        evidence
            .operators
            .iter()
            .any(|op| op.operator == "Projection"
                && op.decision == Decision::Reject
                && op.reason.contains("cross-batch"))
    );
    // One range-bearing scalar subquery under a static input is admitted.
    let single = f
        .scan(&["c", "d", "dim"])?
        .project(vec![
            scalar_subquery(Arc::new(f.scan(&["c", "d", "clicks"])?.build()?)).alias("a"),
        ])?
        .build()?;
    assert!(f.admit(&single).admitted);
    // A range-bearing subquery in a join condition is rejected.
    let in_condition = f
        .scan(&["c", "d", "dim"])?
        .join_on(
            f.scan(&["c", "d", "events"])?.build()?,
            JoinType::Inner,
            vec![exists(Arc::new(f.scan(&["c", "d", "clicks"])?.build()?))],
        )?
        .build()?;
    assert!(!f.admit(&in_condition).admitted);
    // Literal rows holding a range-bearing subquery are range-bearing.
    let values = LogicalPlanBuilder::values(vec![vec![scalar_subquery(Arc::new(
        f.scan(&["c", "d", "clicks"])?.build()?,
    ))]])?
    .build()?;
    let evidence = f.admit(&values);
    assert!(evidence.admitted, "{evidence:?}");
    assert!(evidence.sources_read.contains(&name(&["c", "d", "clicks"])));
    Ok(())
}

#[test]
fn evidence_round_trips_through_serde() -> TestResult {
    let f = Fixture::new()?;
    let plan = f.scan(&["c", "d", "clicks"])?.build()?;
    let evidence = f.admit(&plan);
    let json = serde_json::to_string(&evidence)?;
    let decoded: AdmissionEvidence = serde_json::from_str(&json)?;
    assert_eq!(decoded, evidence);
    Ok(())
}
