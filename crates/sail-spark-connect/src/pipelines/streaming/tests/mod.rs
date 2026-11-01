//! Component tests for the bounded source contract over the deterministic
//! source. Nothing here is wired into a flow; each test states what the
//! source reproduces.

use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::prelude::SessionContext;

use super::intent::{BatchIntent, PlanError, Progress, RunFrontier, next_intent};
use super::source::{
    BoundedSource, Cursor, Incarnation, PartitionId, Range, ScanError, SourceError,
};
use super::test_source::DeterministicSource;
use crate::error::{SparkError, SparkResult};

mod ranges;

/// The refusal a source call returned, if that is how it failed.
fn refused<T>(result: Result<T, SourceError>) -> Option<ScanError> {
    match result {
        Err(SourceError::Refused(error)) => Some(error),
        _ => None,
    }
}

/// The source refusal planning failed on, if that is how it failed.
fn refused_plan<T>(result: Result<T, PlanError>) -> Option<ScanError> {
    match result {
        Err(PlanError::Scan(SourceError::Refused(error))) => Some(error),
        _ => None,
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Utf8, true),
    ]))
}

fn row(id: i64, value: &str) -> Vec<ScalarValue> {
    vec![
        ScalarValue::Int64(Some(id)),
        ScalarValue::Utf8(Some(value.into())),
    ]
}

fn source() -> DeterministicSource {
    DeterministicSource::new("events", schema())
}

fn partition(id: &str) -> PartitionId {
    PartitionId(id.into())
}

/// The `id` values a scan of `range` produces, in scan order.
async fn ids(source: &dyn BoundedSource, range: &Range) -> SparkResult<Vec<i64>> {
    let ctx = SessionContext::new();
    let provider = source.scan(range)?;
    let plan = LogicalPlanBuilder::scan("range", provider_as_source(provider), None)?.build()?;
    let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
    Ok(batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int64Type>()
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect())
}

/// Plans and advances every intent of one run without scanning.
fn drain_sync(
    source: &dyn BoundedSource,
    frontier: &RunFrontier,
    mut progress: Progress,
    max_rows: usize,
) -> SparkResult<(Vec<BatchIntent>, (), Progress)> {
    let mut intents = Vec::new();
    while let Some(intent) = next_intent(source, frontier, &progress, max_rows)? {
        progress = progress.advance(&intent);
        intents.push(intent);
    }
    Ok((intents, (), progress))
}

/// Runs every intent of one run to completion, returning the intents and the
/// ids each range produced, in order.
async fn drain(
    source: &dyn BoundedSource,
    frontier: &RunFrontier,
    mut progress: Progress,
    max_rows: usize,
) -> SparkResult<(Vec<BatchIntent>, Vec<Vec<i64>>, Progress)> {
    let mut intents = Vec::new();
    let mut outputs = Vec::new();
    while let Some(intent) = next_intent(source, frontier, &progress, max_rows)? {
        for range in &intent.ranges {
            outputs.push(ids(source, range).await?);
        }
        progress = progress.advance(&intent);
        intents.push(intent);
    }
    Ok((intents, outputs, progress))
}
