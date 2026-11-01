use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use iggy::prelude::IggyMessage;
use sail_common_datafusion::streaming::bounded::{Incarnation, ScanError};

use super::*;
use crate::broker::{Page, PartitionState};
use crate::config::PayloadFormat;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// A partition holding messages at `offsets`, unless `script` says what the
/// next polls answer.
#[derive(Debug, Default)]
struct Fake {
    incarnation: u64,
    offsets: Vec<u64>,
    /// Overrides `partitions` once set: the partition as it is now.
    state: Option<PartitionState>,
    script: Mutex<VecDeque<(u64, Vec<u64>)>>,
    polls: Mutex<Vec<(u64, u32)>>,
    payload: Option<&'static str>,
}

impl Fake {
    fn holding(offsets: impl IntoIterator<Item = u64>) -> Self {
        Self {
            incarnation: 7,
            offsets: offsets.into_iter().collect(),
            ..Self::default()
        }
    }

    fn answering(self, pages: Vec<(u64, Vec<u64>)>) -> Self {
        *self.script.lock().unwrap_or_else(|e| e.into_inner()) = pages.into();
        self
    }

    fn message(&self, offset: u64) -> Result<IggyMessage, SourceError> {
        let fallback = format!(r#"{{"id":{offset},"name":"n{offset}"}}"#);
        let payload = self.payload.map_or(fallback, str::to_string);
        let mut message = IggyMessage::builder()
            .payload(Bytes::from(payload))
            .build()
            .map_err(|e| SourceError::Internal { cause: Box::new(e) })?;
        message.header.offset = offset;
        Ok(message)
    }
}

#[async_trait]
impl Broker for Fake {
    async fn partitions(&self) -> Result<Vec<PartitionState>, SourceError> {
        Ok(vec![self.state.unwrap_or(PartitionState {
            id: 0,
            incarnation: self.incarnation,
            current_offset: self.offsets.last().copied().unwrap_or(0),
            messages_count: self.offsets.len() as u64,
        })])
    }

    async fn poll(&self, partition: u32, offset: u64, count: u32) -> Result<Page, SourceError> {
        if partition != 0 {
            return Err(ScanError::UnknownPartition {
                partition: partition_id(partition),
            }
            .into());
        }
        self.polls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((offset, count));
        let scripted = self
            .script
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front();
        let (incarnation, offsets) = scripted.unwrap_or_else(|| {
            let offsets = self
                .offsets
                .iter()
                .copied()
                .filter(|x| *x >= offset)
                .take(count as usize)
                .collect();
            (self.incarnation, offsets)
        });
        Ok(Page {
            incarnation,
            messages: offsets
                .into_iter()
                .map(|offset| self.message(offset))
                .collect::<Result<_, _>>()?,
        })
    }
}

/// A broker that never answers, as the SDK behaves while it reconnects.
#[derive(Debug)]
struct Silent;

#[async_trait]
impl Broker for Silent {
    async fn partitions(&self) -> Result<Vec<PartitionState>, SourceError> {
        std::future::pending().await
    }

    async fn poll(&self, _: u32, _: u64, _: u32) -> Result<Page, SourceError> {
        std::future::pending().await
    }
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn config() -> IggyBoundedConfig {
    IggyBoundedConfig::new("iggy://u:p@host:1", "s", "t", PayloadFormat::Json)
}

fn source(broker: Arc<dyn Broker>) -> DataFusionResult<IggyBoundedSource> {
    IggyBoundedSource::with_broker(config(), schema(), broker)
}

fn p0() -> PartitionId {
    partition_id(0)
}

fn range(start: u64, end: u64) -> Range {
    Range {
        partition: p0(),
        incarnation: incarnation(7),
        start: cursor(start),
        end: cursor(end),
    }
}

fn refusal<T>(result: Result<T, SourceError>) -> Option<ScanError> {
    match result {
        Err(SourceError::Refused(error)) => Some(error),
        _ => None,
    }
}

fn state(current_offset: u64, messages_count: u64) -> PartitionState {
    PartitionState {
        id: 0,
        incarnation: 7,
        current_offset,
        messages_count,
    }
}

#[test]
fn the_frontier_is_one_past_the_last_message_and_an_empty_partition_is_zero() {
    assert_eq!(state(0, 0).next_offset(), 0);
    assert_eq!(state(0, 1).next_offset(), 1);
    assert_eq!(state(4, 5).next_offset(), 5);
    // Retention took the messages, the offsets stay taken.
    assert_eq!(state(4, 0).next_offset(), 5);
    assert_eq!(state(u64::MAX, 1).next_offset(), u64::MAX);
}

#[test]
fn only_minted_cursors_and_partitions_are_understood() {
    assert_eq!(offset_of(&p0(), &cursor(12)).ok(), Some(12));
    for foreign in [
        "12",
        "offset:",
        "offset:-1",
        "offset:012",
        "offset:+1",
        "pos:1",
    ] {
        let foreign = Cursor(foreign.to_string());
        assert_eq!(
            offset_of(&p0(), &foreign).err(),
            Some(ScanError::ForeignCursor {
                partition: p0(),
                cursor: foreign,
            })
        );
    }
    assert_eq!(partition_number(&PartitionId("3".into())).ok(), Some(3));
    for foreign in ["p0", "03", "", "-1"] {
        let partition = PartitionId(foreign.to_string());
        assert_eq!(
            partition_number(&partition).err(),
            Some(ScanError::UnknownPartition { partition })
        );
    }
}

#[test]
fn describe_names_the_partitions_and_their_incarnations() -> TestResult {
    let source = source(Arc::new(Fake::holding(0..3)))?;
    let descriptor = source.describe()?;
    assert_eq!(descriptor.kind, "iggy");
    assert_eq!(
        descriptor.partitions,
        vec![Partition {
            id: p0(),
            incarnation: Incarnation("incarnation:7".into()),
        }]
    );
    assert!(!descriptor.descriptor.to_string().contains('@'));
    Ok(())
}

#[test]
fn earliest_is_the_first_retained_offset_and_the_frontier_when_nothing_is() -> TestResult {
    let retained = source(Arc::new(Fake::holding(9_000..9_005)))?;
    assert_eq!(retained.earliest(&p0())?, cursor(9_000));
    assert_eq!(retained.frontier(&p0())?, cursor(9_005));
    let empty = source(Arc::new(Fake::holding([])))?;
    assert_eq!(empty.earliest(&p0())?, cursor(0));
    assert_eq!(empty.frontier(&p0())?, cursor(0));
    let expired = Fake {
        state: Some(state(41, 0)),
        ..Fake::holding([])
    };
    assert_eq!(source(Arc::new(expired))?.earliest(&p0())?, cursor(42));
    Ok(())
}

#[test]
fn plan_end_stops_at_the_limit_or_the_frontier_and_never_goes_back() -> TestResult {
    let source = source(Arc::new(Fake::holding([])))?;
    let plan =
        |start, frontier, max| source.plan_end(&p0(), &cursor(start), &cursor(frontier), max);
    assert_eq!(plan(10, 100, 5)?, cursor(15));
    assert_eq!(plan(10, 12, 5)?, cursor(12));
    assert_eq!(plan(10, 10, 5)?, cursor(10));
    assert_eq!(plan(10, 3, 5)?, cursor(10));
    assert_eq!(plan(u64::MAX - 1, u64::MAX, usize::MAX)?, cursor(u64::MAX));
    Ok(())
}

async fn rows(source: &IggyBoundedSource, range: &Range) -> Result<Vec<i64>, BoxError> {
    let state = SessionStateBuilder::new().build();
    let plan = source.scan(range)?.scan(&state, None, &[], None).await?;
    let mut ids = Vec::new();
    for batch in collect(plan, state.task_ctx()).await? {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("id is not Int64")?;
        ids.extend(column.iter().flatten());
    }
    Ok(ids)
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::test(flavor = "multi_thread")]
async fn a_range_comes_back_exactly_across_pages() -> TestResult {
    let broker = Arc::new(Fake::holding(0..100));
    let mut config = config();
    config.page_size = 4;
    let source = IggyBoundedSource::with_broker(config, schema(), broker.clone())?;
    assert_eq!(
        rows(&source, &range(10, 21)).await?,
        (10..21).collect::<Vec<_>>()
    );
    assert_eq!(
        *broker.polls.lock().unwrap_or_else(|e| e.into_inner()),
        vec![(10, 4), (14, 4), (18, 3)]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reply_past_the_end_of_the_range_is_cut() -> TestResult {
    let broker = Fake::holding(0..10).answering(vec![(7, vec![2, 3, 4, 5, 6])]);
    let source = source(Arc::new(broker))?;
    assert_eq!(rows(&source, &range(2, 4)).await?, vec![2, 3]);
    Ok(())
}

#[test]
fn an_empty_range_asks_the_broker_nothing() -> TestResult {
    let broker = Arc::new(Fake::holding(0..10));
    source(broker.clone())?.scan(&range(4, 4))?;
    assert!(
        broker
            .polls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    );
    Ok(())
}

#[test]
fn a_poll_that_lands_past_the_start_is_a_retention_gap() -> TestResult {
    // What the broker does: no error, the first retained message instead.
    let source = source(Arc::new(Fake::holding(9_000..9_010)))?;
    assert_eq!(
        refusal(source.scan(&range(100, 200))),
        Some(ScanError::RetentionGap {
            partition: p0(),
            expected: cursor(100),
            first_retained: cursor(9_000),
        })
    );
    Ok(())
}

#[test]
fn nothing_retained_from_the_start_is_a_retention_gap() -> TestResult {
    let broker = Fake {
        state: Some(state(41, 0)),
        ..Fake::holding([])
    };
    assert_eq!(
        refusal(source(Arc::new(broker))?.scan(&range(10, 20))),
        Some(ScanError::RetentionGap {
            partition: p0(),
            expected: cursor(10),
            first_retained: cursor(42),
        })
    );
    Ok(())
}

#[test]
fn a_hole_inside_a_range_is_not_contiguous() -> TestResult {
    let inside_a_page = Fake::holding(0..10).answering(vec![(7, vec![2, 3, 5, 6])]);
    assert_eq!(
        refusal(source(Arc::new(inside_a_page))?.scan(&range(2, 7))),
        Some(ScanError::NonContiguous {
            partition: p0(),
            expected: cursor(4),
            actual: cursor(5),
        })
    );
    let between_pages = Fake::holding(0..10).answering(vec![(7, vec![2, 3]), (7, vec![6])]);
    assert_eq!(
        refusal(source(Arc::new(between_pages))?.scan(&range(2, 7))),
        Some(ScanError::NonContiguous {
            partition: p0(),
            expected: cursor(4),
            actual: cursor(6),
        })
    );
    Ok(())
}

#[test]
fn a_range_cut_short_is_not_contiguous() -> TestResult {
    let broker = Fake {
        state: Some(state(3, 2)),
        ..Fake::holding(0..10).answering(vec![(7, vec![2, 3]), (7, vec![])])
    };
    assert_eq!(
        refusal(source(Arc::new(broker))?.scan(&range(2, 7))),
        Some(ScanError::NonContiguous {
            partition: p0(),
            expected: cursor(4),
            actual: cursor(4),
        })
    );
    Ok(())
}

#[test]
fn a_message_behind_the_position_is_not_contiguous_even_at_the_start() -> TestResult {
    let broker = Fake::holding(0..10).answering(vec![(7, vec![1, 2])]);
    assert_eq!(
        refusal(source(Arc::new(broker))?.scan(&range(2, 4))),
        Some(ScanError::NonContiguous {
            partition: p0(),
            expected: cursor(2),
            actual: cursor(1),
        })
    );
    Ok(())
}

#[test]
fn a_recreated_partition_is_refused_before_its_offsets_are_believed() -> TestResult {
    // Offsets restart, so the rows look like exactly the right range.
    let broker = Fake::holding(0..10).answering(vec![(8, vec![2, 3])]);
    assert_eq!(
        refusal(source(Arc::new(broker))?.scan(&range(2, 4))),
        Some(ScanError::IncarnationMismatch {
            partition: p0(),
            expected: incarnation(7),
            actual: incarnation(8),
        })
    );
    Ok(())
}

#[test]
fn an_incarnation_change_between_pages_loses_the_first_page_too() -> TestResult {
    let broker = Fake::holding(0..10).answering(vec![(7, vec![2, 3]), (8, vec![4])]);
    assert_eq!(
        refusal(source(Arc::new(broker))?.scan(&range(2, 5))),
        Some(ScanError::IncarnationMismatch {
            partition: p0(),
            expected: incarnation(7),
            actual: incarnation(8),
        })
    );
    Ok(())
}

#[test]
fn a_partition_recreated_empty_is_a_mismatch_and_not_a_gap() -> TestResult {
    let broker = Fake {
        state: Some(PartitionState {
            incarnation: 8,
            ..state(0, 0)
        }),
        ..Fake::holding([])
    };
    assert_eq!(
        refusal(source(Arc::new(broker))?.scan(&range(2, 4))),
        Some(ScanError::IncarnationMismatch {
            partition: p0(),
            expected: incarnation(7),
            actual: incarnation(8),
        })
    );
    Ok(())
}

#[test]
fn inverted_and_foreign_ranges_are_refused_without_a_poll() -> TestResult {
    let broker = Arc::new(Fake::holding(0..10));
    let source = source(broker.clone())?;
    assert_eq!(
        refusal(source.scan(&range(4, 2))),
        Some(ScanError::ForeignCursor {
            partition: p0(),
            cursor: cursor(2),
        })
    );
    let mut foreign = range(0, 2);
    foreign.start = Cursor("pos:0".into());
    assert!(matches!(
        refusal(source.scan(&foreign)),
        Some(ScanError::ForeignCursor { .. })
    ));
    assert!(
        broker
            .polls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    );
    Ok(())
}

#[test]
fn one_undecodable_message_fails_the_range() -> TestResult {
    for payload in [
        r#"{"id":"#,
        "[1]",
        r#"{"id":"text"}"#,
        r#"{"name":"no id"}"#,
    ] {
        let broker = Fake {
            payload: Some(payload),
            ..Fake::holding(0..3)
        };
        let result = source(Arc::new(broker))?.scan(&range(0, 3));
        assert!(
            matches!(result, Err(SourceError::Corrupt { .. })),
            "{payload}"
        );
    }
    Ok(())
}

#[test]
fn a_type_no_message_could_fill_is_refused_when_the_source_is_built() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "tags",
        DataType::new_list(DataType::Utf8, true),
        true,
    )]));
    let result = IggyBoundedSource::with_broker(config(), schema, Arc::new(Fake::holding([])));
    let error = result.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(
        error.contains("unsupported Iggy JSON field type for tags"),
        "{error}"
    );
}

#[test]
fn raw_and_envelope_schemas_are_fixed() -> TestResult {
    let mut raw = config();
    raw.format = PayloadFormat::Raw;
    assert!(IggyBoundedSource::with_broker(raw.clone(), schema(), Arc::new(Silent)).is_err());
    IggyBoundedSource::with_broker(raw, Arc::new(crate::raw_schema()), Arc::new(Silent))?;
    let mut envelope = config();
    envelope.format = PayloadFormat::Envelope;
    let fixed = Arc::new(crate::envelope_schema());
    assert!(
        IggyBoundedSource::with_broker(envelope.clone(), fixed.clone(), Arc::new(Silent)).is_err(),
        "an envelope needs a source id"
    );
    envelope.source_id = Some("origin".into());
    IggyBoundedSource::with_broker(envelope, fixed, Arc::new(Silent))?;
    Ok(())
}

#[test]
fn raw_rows_carry_the_broker_offset() -> TestResult {
    let mut raw = config();
    raw.format = PayloadFormat::Raw;
    let schema = Arc::new(crate::raw_schema());
    let source =
        IggyBoundedSource::with_broker(raw, schema.clone(), Arc::new(Fake::holding(0..4)))?;
    let id = 0;
    let messages = [Fake::holding([]).message(3)?];
    let batch = decode_messages(&source.config, id, &schema, &messages)?;
    let offsets = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("offset is not Int64")?;
    assert_eq!(offsets.value(0), 3);
    Ok(())
}

#[test]
fn json_fills_nullable_columns_with_null_and_reads_timestamps() -> TestResult {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]));
    let broker = Fake {
        payload: Some(r#"{"id":1,"at":"2026-01-01T00:00:00.000001Z"}"#),
        ..Fake::holding([])
    };
    let batch = decode_messages(&config(), 0, &schema, &[broker.message(0)?])?;
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("name is not Utf8")?;
    assert!(names.is_null(0));
    let times = batch
        .column(2)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or("at is not a timestamp")?;
    assert_eq!(times.value(0), 1_767_225_600_000_001);
    Ok(())
}

fn assert_gives_up_promptly(source: &IggyBoundedSource) {
    let started = Instant::now();
    let result = source.describe();
    assert!(matches!(result, Err(SourceError::Unavailable { .. })));
    let error = source.earliest(&p0()).err().map(|e| e.retryable());
    assert_eq!(error, Some(true));
    assert!(started.elapsed() < Duration::from_secs(5));
}

fn silent() -> DataFusionResult<IggyBoundedSource> {
    let mut config = config();
    config.timeout = Duration::from_millis(50);
    IggyBoundedSource::with_broker(config, schema(), Arc::new(Silent))
}

#[test]
fn a_silent_broker_is_unavailable_within_the_deadline_outside_a_runtime() -> TestResult {
    assert_gives_up_promptly(&silent()?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_silent_broker_is_unavailable_within_the_deadline_on_a_multi_thread_runtime() -> TestResult
{
    assert_gives_up_promptly(&silent()?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_silent_broker_is_unavailable_within_the_deadline_on_a_current_thread_runtime()
-> TestResult {
    assert_gives_up_promptly(&silent()?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_source_can_be_dropped_inside_async_code() -> TestResult {
    let source = source(Arc::new(Fake::holding(0..3)))?;
    source.frontier(&p0())?;
    drop(source);
    Ok(())
}
