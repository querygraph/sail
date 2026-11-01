//! Reading one exact range. A range either comes back whole, in order, under
//! the incarnation it was captured with, or the read fails and returns
//! nothing.

use iggy::prelude::IggyMessage;
use sail_common_datafusion::streaming::bounded::{
    Cursor, Incarnation, PartitionId, Range, ScanError, SourceError,
};

use crate::broker::{Broker, PartitionState};

const CURSOR_PREFIX: &str = "offset:";
const INCARNATION_PREFIX: &str = "incarnation:";
const MAX_COUNT: u64 = u32::MAX as u64;

pub(crate) fn cursor(offset: u64) -> Cursor {
    Cursor(format!("{CURSOR_PREFIX}{offset}"))
}

pub(crate) fn incarnation(value: u64) -> Incarnation {
    Incarnation(format!("{INCARNATION_PREFIX}{value}"))
}

pub(crate) fn partition_id(id: u32) -> PartitionId {
    PartitionId(id.to_string())
}

/// The numeric id behind a [`PartitionId`] this adapter minted.
pub(crate) fn partition_number(partition: &PartitionId) -> Result<u32, ScanError> {
    partition
        .0
        .parse::<u32>()
        .ok()
        .filter(|id| id.to_string() == partition.0)
        .ok_or_else(|| ScanError::UnknownPartition {
            partition: partition.clone(),
        })
}

/// The offset behind a cursor this adapter minted, and only those: `offset:01`
/// names the same number, but no call here produced it.
pub(crate) fn offset_of(partition: &PartitionId, value: &Cursor) -> Result<u64, ScanError> {
    value
        .0
        .strip_prefix(CURSOR_PREFIX)
        .and_then(|offset| offset.parse::<u64>().ok())
        .filter(|offset| cursor(*offset) == *value)
        .ok_or_else(|| ScanError::ForeignCursor {
            partition: partition.clone(),
            cursor: value.clone(),
        })
}

pub(crate) async fn partition_state(
    broker: &dyn Broker,
    partition: &PartitionId,
) -> Result<PartitionState, SourceError> {
    let id = partition_number(partition)?;
    broker
        .partitions()
        .await?
        .into_iter()
        .find(|state| state.id == id)
        .ok_or_else(|| {
            ScanError::UnknownPartition {
                partition: partition.clone(),
            }
            .into()
        })
}

/// The first readable offset: polling from zero lands on it, since the
/// broker skips over what retention removed. An empty reply means there is
/// nothing retained, so a first run starts at the frontier.
pub(crate) async fn earliest(
    broker: &dyn Broker,
    partition: &PartitionId,
) -> Result<u64, SourceError> {
    let id = partition_number(partition)?;
    let page = broker.poll(id, 0, 1).await?;
    match page.messages.first() {
        Some(message) => Ok(message.header.offset),
        None => Ok(partition_state(broker, partition).await?.next_offset()),
    }
}

/// An empty reply inside a range: the partition was recreated empty, or
/// nothing from here on is retained. The reply's context is not used for
/// this, only the broker's own record of the partition.
async fn empty_page(
    broker: &dyn Broker,
    range: &Range,
    next: u64,
    start: u64,
) -> Result<SourceError, SourceError> {
    let state = partition_state(broker, &range.partition).await?;
    let actual = incarnation(state.incarnation);
    if actual != range.incarnation {
        return Ok(mismatch(range, actual).into());
    }
    let retained = cursor(state.next_offset());
    Ok(if next == start {
        ScanError::RetentionGap {
            partition: range.partition.clone(),
            expected: range.start.clone(),
            first_retained: retained,
        }
    } else {
        ScanError::NonContiguous {
            partition: range.partition.clone(),
            expected: cursor(next),
            actual: retained,
        }
    }
    .into())
}

fn mismatch(range: &Range, actual: Incarnation) -> ScanError {
    ScanError::IncarnationMismatch {
        partition: range.partition.clone(),
        expected: range.incarnation.clone(),
        actual,
    }
}

/// Every message of `[start, end)`, exactly once, in offset order.
pub(crate) async fn read_range(
    broker: &dyn Broker,
    range: &Range,
    page_size: u32,
) -> Result<Vec<IggyMessage>, SourceError> {
    let id = partition_number(&range.partition)?;
    let start = offset_of(&range.partition, &range.start)?;
    let end = offset_of(&range.partition, &range.end)?;
    if end < start {
        return Err(ScanError::ForeignCursor {
            partition: range.partition.clone(),
            cursor: range.end.clone(),
        }
        .into());
    }
    let page_size = u64::from(page_size.max(1));
    let mut messages = Vec::new();
    let mut next = start;
    while next < end {
        let count = u32::try_from((end - next).min(page_size).min(MAX_COUNT)).unwrap_or(u32::MAX);
        let page = broker.poll(id, next, count).await?;
        if page.messages.is_empty() {
            return Err(empty_page(broker, range, next, start).await?);
        }
        let actual = incarnation(page.incarnation);
        if actual != range.incarnation {
            return Err(mismatch(range, actual).into());
        }
        for message in page.messages {
            // A reply running past the range is cut at its end.
            if next == end {
                break;
            }
            let offset = message.header.offset;
            if offset != next {
                return Err(if next == start && offset > start {
                    ScanError::RetentionGap {
                        partition: range.partition.clone(),
                        expected: range.start.clone(),
                        first_retained: cursor(offset),
                    }
                } else {
                    ScanError::NonContiguous {
                        partition: range.partition.clone(),
                        expected: cursor(next),
                        actual: cursor(offset),
                    }
                }
                .into());
            }
            messages.push(message);
            next = next.saturating_add(1);
        }
    }
    Ok(messages)
}
