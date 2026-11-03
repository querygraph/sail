//! The watermark model shared by the append path and AutoCDC: what a
//! watermarked aggregate needs from its query, found once, and the event-time
//! arithmetic that follows Spark 4.2.1. Each run keeps its own refusals as
//! checks after this discovery, and the late-row rule lives here once.

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{DataType, Int64Type, TimeUnit};
use datafusion::common::Column;
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, cast, col};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;

use super::analysis::GraphScope;
use super::append::{qualified, source_name};
use super::graph::Name;
use crate::error::{SparkError, SparkResult};

/// What a watermarked aggregate needs from its query.
pub(super) struct Windowed {
    pub source: Name,
    pub event_time: String,
    pub delay_micros: i64,
    pub window_micros: i64,
    /// The slide of a sliding window, `None` for a tumbling one.
    pub slide_micros: Option<i64>,
    /// Whether the window starts at an offset from the epoch.
    pub offset: bool,
    /// Every `withWatermark` under the aggregate, the first of them the one
    /// described above.
    pub watermarks: Vec<Watermark>,
}

/// One `withWatermark` under a watermarked aggregate.
#[derive(Debug, Clone)]
pub(super) struct Watermark {
    pub event_time: String,
    pub delay_micros: i64,
    /// What the watermark applies to.
    pub input: spec::QueryPlan,
    /// Whether that is a streaming read itself.
    pub direct: bool,
}

/// The watermarks under `plan`, through row-wise transformations and unions.
pub(super) fn find_watermarks(
    plan: &spec::QueryPlan,
    found: &mut Vec<Watermark>,
) -> SparkResult<()> {
    match &plan.node {
        spec::QueryNode::WithWatermark(watermark) => {
            let delay_micros = duration_micros(&watermark.delay_threshold).ok_or_else(|| {
                SparkError::unsupported(format!(
                    "the watermark delay '{}'",
                    watermark.delay_threshold
                ))
            })?;
            found.push(Watermark {
                event_time: watermark.event_time.clone(),
                delay_micros,
                input: watermark.input.as_ref().clone(),
                direct: matches!(
                    &watermark.input.node,
                    spec::QueryNode::Read {
                        is_streaming: true,
                        ..
                    }
                ),
            });
            Ok(())
        }
        spec::QueryNode::Project {
            input: Some(input), ..
        }
        | spec::QueryNode::Filter { input, .. }
        | spec::QueryNode::TableAlias { input, .. }
        | spec::QueryNode::WithColumns { input, .. }
        | spec::QueryNode::WithColumnsRenamed { input, .. }
        | spec::QueryNode::Drop { input, .. } => find_watermarks(input, found),
        spec::QueryNode::SetOperation(operation)
            if operation.set_op_type == spec::SetOpType::Union =>
        {
            find_watermarks(&operation.left, found)?;
            find_watermarks(&operation.right, found)
        }
        spec::QueryNode::Join(join) => {
            find_watermarks(&join.left, found)?;
            find_watermarks(&join.right, found)
        }
        spec::QueryNode::Deduplicate(deduplicate) => find_watermarks(&deduplicate.input, found),
        _ => Ok(()),
    }
}

/// The one streaming read under a chain of row-wise transformations.
fn streaming_read(scope: &GraphScope<'_>, plan: &spec::QueryPlan) -> SparkResult<Option<Name>> {
    match &plan.node {
        spec::QueryNode::Read {
            read_type: spec::ReadType::NamedTable(read),
            is_streaming: true,
        } => {
            let name: Vec<String> = read.name.clone().into();
            Ok(Some(qualified(scope, &name)?))
        }
        spec::QueryNode::Read {
            read_type: spec::ReadType::DataSource(source),
            is_streaming: true,
        } => Ok(Some(source_name(source))),
        spec::QueryNode::Project {
            input: Some(input), ..
        }
        | spec::QueryNode::Filter { input, .. }
        | spec::QueryNode::TableAlias { input, .. }
        | spec::QueryNode::WithColumns { input, .. }
        | spec::QueryNode::WithColumnsRenamed { input, .. }
        | spec::QueryNode::Drop { input, .. } => streaming_read(scope, input),
        _ => Ok(None),
    }
}

fn duration_micros(text: &str) -> Option<i64> {
    let text = text.trim();
    let text = text
        .strip_prefix("INTERVAL ")
        .or_else(|| text.strip_prefix("interval "))
        .unwrap_or(text)
        .trim()
        .trim_matches('\'');
    let (value, unit) = text.split_once(char::is_whitespace)?;
    let value: i64 = value.parse().ok()?;
    let scale = match unit.trim().to_ascii_lowercase().trim_end_matches('s') {
        "millisecond" => 1_000,
        "second" => 1_000_000,
        "minute" => 60_000_000,
        "hour" => 3_600_000_000,
        "day" => 86_400_000_000,
        _ => return None,
    };
    value.checked_mul(scale)
}

fn string_literal(expression: &spec::Expr) -> Option<&str> {
    match expression {
        spec::Expr::Literal(spec::Literal::Utf8 { value: Some(value) })
        | spec::Expr::Literal(spec::Literal::LargeUtf8 { value: Some(value) })
        | spec::Expr::Literal(spec::Literal::Utf8View { value: Some(value) }) => Some(value),
        _ => None,
    }
}

/// The `window(eventTime, '5 seconds')` duration among the grouping keys,
/// and the slide of `window(eventTime, '10 seconds', '5 seconds')`.
fn event_window(
    grouping: &[spec::Expr],
    event_time: &str,
) -> SparkResult<(i64, Option<i64>, bool)> {
    let duration = |expr: &spec::Expr| {
        string_literal(expr)
            .and_then(duration_micros)
            .filter(|micros| *micros > 0)
            .ok_or_else(|| SparkError::unsupported("this streaming window duration"))
    };
    for expression in grouping {
        let spec::Expr::UnresolvedFunction(function) = expression else {
            continue;
        };
        let name: Vec<String> = function.function_name.clone().into();
        if !name
            .last()
            .is_some_and(|x| x.eq_ignore_ascii_case("window"))
        {
            continue;
        }
        let (name, size, slide, start) = match function.arguments.as_slice() {
            [spec::Expr::UnresolvedAttribute { name, .. }, size] => (name, size, None, None),
            [spec::Expr::UnresolvedAttribute { name, .. }, size, slide] => {
                (name, size, Some(slide), None)
            }
            [
                spec::Expr::UnresolvedAttribute { name, .. },
                size,
                slide,
                start,
            ] => (name, size, Some(slide), Some(start)),
            _ => return Err(SparkError::unsupported("this streaming window")),
        };
        let column: Vec<String> = name.clone().into();
        if !column
            .last()
            .is_some_and(|x| x.eq_ignore_ascii_case(event_time))
        {
            return Err(SparkError::unsupported(
                "a streaming window over a column other than the watermark",
            ));
        }
        let size = duration(size)?;
        let slide = slide
            .map(duration)
            .transpose()?
            .filter(|slide| *slide != size);
        if slide.is_some_and(|slide| slide > size) {
            return Err(SparkError::unsupported(
                "a streaming window that slides past its length",
            ));
        }
        let offset = match start.map(string_literal) {
            None => false,
            Some(Some(start)) => {
                duration_micros(start)
                    .ok_or_else(|| SparkError::unsupported("this streaming window start time"))?
                    != 0
            }
            Some(None) => {
                return Err(SparkError::unsupported("this streaming window start time"));
            }
        };
        return Ok((size, slide, offset));
    }
    Err(SparkError::unsupported(
        "a watermarked aggregate without a tumbling window",
    ))
}

/// Find the watermarked aggregate of a query, through the nodes around it.
pub(super) fn windowed_aggregate(
    scope: &GraphScope<'_>,
    query: &spec::QueryPlan,
) -> SparkResult<Option<Windowed>> {
    let aggregate = match &query.node {
        spec::QueryNode::Aggregate(aggregate) => aggregate,
        spec::QueryNode::Project {
            input: Some(input), ..
        }
        | spec::QueryNode::Filter { input, .. }
        | spec::QueryNode::Sort { input, .. } => return windowed_aggregate(scope, input),
        _ => return Ok(None),
    };
    let mut watermarks = vec![];
    find_watermarks(&aggregate.input, &mut watermarks)?;
    let Some(first) = watermarks.first() else {
        return Ok(None);
    };
    let source = match streaming_read(scope, &first.input)? {
        Some(source) => source,
        None if watermarks.len() > 1 || !first.direct => vec![],
        None => {
            return Err(SparkError::unsupported(
                "a watermark on this streaming read",
            ));
        }
    };
    let (window_micros, slide_micros, offset) =
        event_window(&aggregate.grouping, &first.event_time)?;
    Ok(Some(Windowed {
        source,
        event_time: first.event_time.clone(),
        delay_micros: first.delay_micros,
        window_micros,
        slide_micros,
        offset,
        watermarks,
    }))
}

/// The event time as whole microseconds, for arithmetic.
pub(super) fn event_micros(plan: &LogicalPlan, column: &str) -> SparkResult<Expr> {
    let (_, field) = plan
        .schema()
        .qualified_field_with_unqualified_name(column)
        .map_err(|e| SparkError::invalid(e.to_string()))?;
    let DataType::Timestamp(unit, timezone) = field.data_type() else {
        return Err(SparkError::invalid(format!(
            "the watermark column {column} is not a timestamp"
        )));
    };
    let raw = col(Column::from_name(column));
    let micros = DataType::Timestamp(TimeUnit::Microsecond, timezone.clone());
    let _ = unit;
    Ok(cast(cast(raw, micros), DataType::Int64))
}

pub(super) async fn max_event(
    ctx: &SessionContext,
    plan: &LogicalPlan,
    column: &str,
) -> SparkResult<Option<i64>> {
    let micros = event_micros(plan, column)?;
    let plan = LogicalPlanBuilder::from(plan.clone())
        .aggregate(Vec::<Expr>::new(), vec![max(micros).alias("m")])?
        .build()?;
    let physical = ctx.state().create_physical_plan(&plan).await?;
    let service = ctx.extension::<JobService>()?;
    let mut stream = service.runner().execute(ctx, physical).await?;
    let mut value = None;
    while let Some(batch) = stream.try_next().await? {
        let column = batch.column(0).as_primitive::<Int64Type>();
        if !column.is_empty() && !column.is_null(0) {
            value = Some(column.value(0));
        }
    }
    Ok(value)
}

/// A `dropDuplicatesWithinWatermark` in a query, with the watermark beneath
/// it. Spark keeps each key until the watermark passes the first row's event
/// time plus the delay, and drops duplicates of a key it still keeps.
#[derive(Debug, Clone)]
pub(super) struct WithinWatermark {
    pub keys: Vec<String>,
    pub watermark: Watermark,
}

/// The deduplication within a watermark of `query`, through the nodes
/// around it. `None` when the query has none.
pub(super) fn within_watermark(query: &spec::QueryPlan) -> SparkResult<Option<WithinWatermark>> {
    let next = match &query.node {
        spec::QueryNode::Deduplicate(deduplicate) if deduplicate.within_watermark => {
            let mut watermarks = vec![];
            find_watermarks(&deduplicate.input, &mut watermarks)?;
            let [watermark] = watermarks.as_slice() else {
                return Err(SparkError::unsupported(
                    "dropDuplicatesWithinWatermark over other than one watermark",
                ));
            };
            if deduplicate.all_columns_as_keys {
                return Err(SparkError::unsupported(
                    "dropDuplicatesWithinWatermark over every column",
                ));
            }
            return Ok(Some(WithinWatermark {
                keys: deduplicate
                    .column_names
                    .iter()
                    .map(|name| name.clone().into())
                    .collect(),
                watermark: watermark.clone(),
            }));
        }
        spec::QueryNode::Deduplicate(deduplicate) => &deduplicate.input,
        spec::QueryNode::Project {
            input: Some(input), ..
        }
        | spec::QueryNode::Filter { input, .. }
        | spec::QueryNode::Sort { input, .. }
        | spec::QueryNode::TableAlias { input, .. }
        | spec::QueryNode::WithColumns { input, .. }
        | spec::QueryNode::WithColumnsRenamed { input, .. }
        | spec::QueryNode::Drop { input, .. } => input,
        _ => return Ok(None),
    };
    within_watermark(next)
}
