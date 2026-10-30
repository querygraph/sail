//! The run record's two streaming entries: the frontier captured once per
//! run, and the per-batch intent of explicit ranges. Both are serialisable
//! so a replay re-plans exactly the recorded ranges and nothing else; output
//! row counts never enter progress.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::source::{BoundedSource, Cursor, Incarnation, PartitionId, Range, ScanError};

/// The per-partition end captured at the start of a run, with the
/// incarnation it was captured under.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFrontier {
    pub partitions: BTreeMap<PartitionId, (Incarnation, Cursor)>,
}

impl RunFrontier {
    /// `frontier(partitions)` for every partition the source describes now.
    pub fn capture(source: &dyn BoundedSource) -> Result<Self, ScanError> {
        let mut partitions = BTreeMap::new();
        for partition in source.describe().partitions {
            let end = source.frontier(&partition.id)?;
            partitions.insert(partition.id, (partition.incarnation, end));
        }
        Ok(Self { partitions })
    }
}

/// Where each partition's next range starts, bound to the incarnation the
/// position was reached under: a position means nothing under another
/// incarnation. A partition absent here is one the run has not seen: its
/// first range starts at the source's earliest position at planning time.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    pub positions: BTreeMap<PartitionId, (Incarnation, Cursor)>,
}

impl Progress {
    /// Every range of a completed intent advances its partition to the
    /// range's end under the range's incarnation.
    pub fn advance(mut self, intent: &BatchIntent) -> Self {
        for range in &intent.ranges {
            self.positions.insert(
                range.partition.clone(),
                (range.incarnation.clone(), range.end.clone()),
            );
        }
        self
    }
}

/// Why an intent could not be planned. Both fail closed: neither is "caught up".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanError {
    Scan(ScanError),
    /// A limit of zero rows per partition can never advance.
    ZeroLimit,
    /// The adapter returned a range end equal to its start although the
    /// partition has not reached the captured frontier.
    Stalled {
        partition: PartitionId,
        start: Cursor,
        frontier: Cursor,
    },
    /// Progress was reached under another incarnation of the partition.
    IncarnationChanged {
        partition: PartitionId,
        progressed: Incarnation,
        captured: Incarnation,
    },
}

impl From<ScanError> for PlanError {
    fn from(error: ScanError) -> Self {
        Self::Scan(error)
    }
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scan(error) => write!(f, "{error}"),
            Self::ZeroLimit => f.write_str("max rows per partition must be at least one"),
            Self::Stalled {
                partition,
                start,
                frontier,
            } => write!(
                f,
                "planning stalled on partition {partition}: no range from {} towards {}",
                start.0, frontier.0
            ),
            Self::IncarnationChanged {
                partition,
                progressed,
                captured,
            } => write!(
                f,
                "progress on partition {partition} was reached under incarnation {} but the run captured {}",
                progressed.0, captured.0
            ),
        }
    }
}

impl From<PlanError> for crate::error::SparkError {
    fn from(error: PlanError) -> Self {
        crate::error::SparkError::invalid(format!("streaming intent: {error}"))
    }
}

/// The ranges one batch covers, decided before the batch runs and replayed
/// unchanged after a failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchIntent {
    pub ranges: Vec<Range>,
}

/// Plans the next intent against the captured frontier: one range per
/// partition that has not reached its captured end, each bounded by the
/// adapter's `plan_end`. `None` once every partition is caught up. Generic
/// code decides only "caught up" by cursor equality under the same
/// incarnation; the adapter decides how far a range reaches. A progress
/// entry from another incarnation, a zero limit and a non-advancing
/// `plan_end` are errors, never "caught up".
pub fn next_intent(
    source: &dyn BoundedSource,
    frontier: &RunFrontier,
    progress: &Progress,
    max_rows_per_partition: usize,
) -> Result<Option<BatchIntent>, PlanError> {
    if max_rows_per_partition == 0 {
        return Err(PlanError::ZeroLimit);
    }
    let mut ranges = Vec::new();
    for (partition, (incarnation, end)) in &frontier.partitions {
        let start = match progress.positions.get(partition) {
            Some((progressed, start)) => {
                if progressed != incarnation {
                    return Err(PlanError::IncarnationChanged {
                        partition: partition.clone(),
                        progressed: progressed.clone(),
                        captured: incarnation.clone(),
                    });
                }
                start.clone()
            }
            None => source.earliest(partition)?,
        };
        if &start == end {
            continue;
        }
        let range_end = source.plan_end(partition, &start, end, max_rows_per_partition)?;
        if range_end == start {
            return Err(PlanError::Stalled {
                partition: partition.clone(),
                start,
                frontier: end.clone(),
            });
        }
        ranges.push(Range {
            partition: partition.clone(),
            incarnation: incarnation.clone(),
            start,
            end: range_end,
        });
    }
    Ok((!ranges.is_empty()).then_some(BatchIntent { ranges }))
}
