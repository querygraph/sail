use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::datasource::{MemTable, TableProvider};
use sail_common_datafusion::streaming::bounded::{
    BoundedSource, Cursor, Incarnation, Partition, PartitionId, Range, ScanError, SourceDescriptor,
    SourceError,
};

use super::{BoundedOffset, MAX_RANGE_ROWS, plan_ranges};
use crate::pipelines::streaming::intent::PlanError;

#[derive(Debug)]
struct Fake {
    partitions: Vec<(&'static str, &'static str, u64, u64)>,
    origin: bool,
    /// Cannot plan past any offset, although its frontier is ahead.
    stall: bool,
}

impl Fake {
    fn position(&self, partition: &PartitionId) -> (u64, u64) {
        self.partitions
            .iter()
            .find(|x| x.0 == partition.0)
            .map_or((0, 0), |x| (x.2, x.3))
    }
}

fn cursor(position: u64) -> Cursor {
    Cursor(position.to_string())
}

impl BoundedSource for Fake {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }

    fn describe(&self) -> Result<SourceDescriptor, SourceError> {
        Ok(SourceDescriptor {
            kind: "fake".into(),
            descriptor: serde_json::Value::Null,
            partitions: self
                .partitions
                .iter()
                .map(|x| Partition {
                    id: PartitionId(x.0.into()),
                    incarnation: Incarnation(x.1.into()),
                })
                .collect(),
        })
    }

    fn earliest(&self, partition: &PartitionId) -> Result<Cursor, SourceError> {
        Ok(cursor(self.position(partition).0))
    }

    fn retains_origin(&self, _: &PartitionId) -> Result<bool, SourceError> {
        Ok(self.origin)
    }

    fn frontier(&self, partition: &PartitionId) -> Result<Cursor, SourceError> {
        Ok(cursor(self.position(partition).1))
    }

    fn plan_end(
        &self,
        _: &PartitionId,
        start: &Cursor,
        frontier: &Cursor,
        max_rows: usize,
    ) -> Result<Cursor, SourceError> {
        if self.stall {
            return Ok(start.clone());
        }
        let (start, frontier): (u64, u64) = (
            start.0.parse().unwrap_or_default(),
            frontier.0.parse().unwrap_or_default(),
        );
        Ok(cursor(frontier.min(start + max_rows as u64)))
    }

    fn scan(&self, _: &Range) -> Result<Arc<dyn TableProvider>, SourceError> {
        MemTable::try_new(self.schema(), vec![vec![]])
            .map(|table| Arc::new(table) as Arc<dyn TableProvider>)
            .map_err(|e| SourceError::Internal { cause: Box::new(e) })
    }
}

fn source(partitions: Vec<(&'static str, &'static str, u64, u64)>) -> Fake {
    Fake {
        partitions,
        origin: true,
        stall: false,
    }
}

fn offset(incarnation: &str, position: u64) -> BoundedOffset {
    BoundedOffset {
        incarnation: Incarnation(incarnation.into()),
        cursor: cursor(position),
    }
}

type Offsets = BTreeMap<String, BoundedOffset>;

#[test]
fn a_new_partition_starts_at_its_earliest_position() -> Result<(), PlanError> {
    let fake = source(vec![("p0", "a", 5, 9)]);
    let (ranges, more) = plan_ranges(&fake, &Offsets::new(), &mut BTreeMap::new(), false)?;
    assert!(!more);
    assert_eq!(ranges.len(), 1);
    assert_eq!((&ranges[0].start, &ranges[0].end), (&cursor(5), &cursor(9)));
    Ok(())
}

#[test]
fn a_caught_up_partition_has_no_range() -> Result<(), PlanError> {
    let fake = source(vec![("p0", "a", 0, 9)]);
    let offsets = Offsets::from([("p0".to_string(), offset("a", 9))]);
    let (ranges, more) = plan_ranges(&fake, &offsets, &mut BTreeMap::new(), false)?;
    assert!(ranges.is_empty() && !more);
    Ok(())
}

#[test]
fn a_long_partition_is_read_in_capped_batches_up_to_one_frontier() -> Result<(), PlanError> {
    let length = MAX_RANGE_ROWS as u64 + 10;
    let fake = source(vec![("p0", "a", 0, length)]);
    let mut frontiers = BTreeMap::new();
    let (ranges, more) = plan_ranges(&fake, &Offsets::new(), &mut frontiers, false)?;
    assert!(more);
    assert_eq!(ranges[0].end, cursor(MAX_RANGE_ROWS as u64));

    // Rows that arrive during the run are left for the next run.
    let grown = source(vec![("p0", "a", 0, length + 1000)]);
    let offsets = Offsets::from([("p0".to_string(), offset("a", MAX_RANGE_ROWS as u64))]);
    let (ranges, more) = plan_ranges(&grown, &offsets, &mut frontiers, false)?;
    assert!(!more);
    assert_eq!(ranges[0].end, cursor(length));
    Ok(())
}

#[test]
fn a_recreated_partition_is_refused() {
    let fake = source(vec![("p0", "b", 0, 9)]);
    let offsets = Offsets::from([("p0".to_string(), offset("a", 3))]);
    let result = plan_ranges(&fake, &offsets, &mut BTreeMap::new(), false);
    assert!(matches!(
        result,
        Err(PlanError::Scan(SourceError::Refused(
            ScanError::IncarnationMismatch { .. }
        )))
    ));
}

#[test]
fn a_source_that_cannot_advance_is_an_error_not_a_caught_up_partition() {
    let stuck = Fake {
        partitions: vec![("p0", "a", 0, 9)],
        origin: true,
        stall: true,
    };
    let offsets = Offsets::from([("p0".to_string(), offset("a", 3))]);
    let result = plan_ranges(&stuck, &offsets, &mut BTreeMap::new(), false);
    assert!(matches!(result, Err(PlanError::Stalled { .. })));
}

#[test]
fn a_partition_that_is_gone_is_refused() {
    let fake = source(vec![("p1", "a", 0, 9)]);
    let offsets = Offsets::from([("p0".to_string(), offset("a", 3))]);
    let result = plan_ranges(&fake, &offsets, &mut BTreeMap::new(), false);
    assert!(matches!(
        result,
        Err(PlanError::Scan(SourceError::Refused(
            ScanError::UnknownPartition { .. }
        )))
    ));
}

#[test]
fn a_full_refresh_needs_the_start_of_the_partition() -> Result<(), PlanError> {
    let lost = Fake {
        partitions: vec![("p0", "a", 5, 9)],
        origin: false,
        stall: false,
    };
    let result = plan_ranges(&lost, &Offsets::new(), &mut BTreeMap::new(), true);
    assert!(matches!(
        result,
        Err(PlanError::Scan(SourceError::Refused(
            ScanError::RetentionGap { .. }
        )))
    ));
    // An ordinary first run starts from what the source holds.
    let (ranges, _) = plan_ranges(&lost, &Offsets::new(), &mut BTreeMap::new(), false)?;
    assert_eq!(ranges[0].start, cursor(5));
    Ok(())
}
