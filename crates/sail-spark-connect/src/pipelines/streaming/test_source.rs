//! An in-process bounded source whose every behaviour is chosen by the test:
//! partitions with immutable incarnations, rows at consecutive positions,
//! retention that can expire a prefix, appends after a frontier was
//! captured, and partition recreation under a new incarnation. Cursors are
//! minted as `pos:<n>` tokens that only this adapter parses.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::datasource::{MemTable, TableProvider};

use super::source::{
    BoundedSource, Cursor, Incarnation, Partition, PartitionId, Range, ScanError, SourceDescriptor,
    SourceError,
};

#[derive(Debug, Clone)]
struct PartitionState {
    incarnation: u64,
    /// Position of the first retained row.
    earliest: u64,
    /// Rows at positions `earliest .. earliest + rows.len()`.
    rows: Vec<Vec<ScalarValue>>,
}

#[derive(Debug)]
pub struct DeterministicSource {
    name: String,
    schema: SchemaRef,
    partitions: Mutex<BTreeMap<PartitionId, PartitionState>>,
}

impl DeterministicSource {
    pub fn new(name: &str, schema: SchemaRef) -> Self {
        Self {
            name: name.into(),
            schema,
            partitions: Mutex::new(BTreeMap::new()),
        }
    }

    fn with_partitions<T>(
        &self,
        f: impl FnOnce(&mut BTreeMap<PartitionId, PartitionState>) -> T,
    ) -> T {
        let mut guard = match self.partitions.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        f(&mut guard)
    }

    /// Creates an empty partition under incarnation 1, or recreates an
    /// existing one empty under the next incarnation.
    pub fn create_partition(&self, id: &str) -> Incarnation {
        self.with_partitions(|partitions| {
            let id = PartitionId(id.into());
            let incarnation = partitions.get(&id).map_or(1, |state| state.incarnation + 1);
            partitions.insert(
                id,
                PartitionState {
                    incarnation,
                    earliest: 0,
                    rows: Vec::new(),
                },
            );
            Incarnation(format!("inc:{incarnation}"))
        })
    }

    /// Appends rows at the next positions of an existing partition.
    pub fn append(&self, id: &str, rows: Vec<Vec<ScalarValue>>) {
        self.with_partitions(|partitions| {
            if let Some(state) = partitions.get_mut(&PartitionId(id.into())) {
                state.rows.extend(rows);
            }
        });
    }

    /// Drops every row before `position`, as retention would.
    pub fn expire_before(&self, id: &str, position: u64) {
        self.with_partitions(|partitions| {
            if let Some(state) = partitions.get_mut(&PartitionId(id.into()))
                && position > state.earliest
            {
                let drop = usize::try_from(position - state.earliest).unwrap_or(usize::MAX);
                let drop = drop.min(state.rows.len());
                state.rows.drain(..drop);
                state.earliest += u64::try_from(drop).unwrap_or(0);
            }
        });
    }

    pub fn remove_partition(&self, id: &str) {
        self.with_partitions(|partitions| {
            partitions.remove(&PartitionId(id.into()));
        });
    }

    fn cursor(position: u64) -> Cursor {
        Cursor(format!("pos:{position}"))
    }

    fn position(partition: &PartitionId, cursor: &Cursor) -> Result<u64, ScanError> {
        cursor
            .0
            .strip_prefix("pos:")
            .and_then(|digits| digits.parse().ok())
            .ok_or_else(|| ScanError::ForeignCursor {
                partition: partition.clone(),
                cursor: cursor.clone(),
            })
    }

    fn state(&self, partition: &PartitionId) -> Result<PartitionState, ScanError> {
        self.with_partitions(|partitions| partitions.get(partition).cloned())
            .ok_or_else(|| ScanError::UnknownPartition {
                partition: partition.clone(),
            })
    }

    fn end_of(state: &PartitionState) -> u64 {
        state.earliest + u64::try_from(state.rows.len()).unwrap_or(0)
    }
}

impl BoundedSource for DeterministicSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn describe(&self) -> Result<SourceDescriptor, SourceError> {
        let partitions = self.with_partitions(|partitions| {
            partitions
                .iter()
                .map(|(id, state)| Partition {
                    id: id.clone(),
                    incarnation: Incarnation(format!("inc:{}", state.incarnation)),
                })
                .collect()
        });
        Ok(SourceDescriptor {
            kind: "deterministic".into(),
            descriptor: serde_json::json!({ "name": self.name }),
            partitions,
        })
    }

    fn earliest(&self, partition: &PartitionId) -> Result<Cursor, SourceError> {
        Ok(Self::cursor(self.state(partition)?.earliest))
    }

    fn frontier(&self, partition: &PartitionId) -> Result<Cursor, SourceError> {
        Ok(Self::cursor(Self::end_of(&self.state(partition)?)))
    }

    fn plan_end(
        &self,
        partition: &PartitionId,
        start: &Cursor,
        frontier: &Cursor,
        max_rows: usize,
    ) -> Result<Cursor, SourceError> {
        let start = Self::position(partition, start)?;
        let frontier = Self::position(partition, frontier)?;
        let step = u64::try_from(max_rows).unwrap_or(u64::MAX);
        Ok(Self::cursor(
            start.saturating_add(step).min(frontier).max(start),
        ))
    }

    fn scan(&self, range: &Range) -> Result<Arc<dyn TableProvider>, SourceError> {
        let state = self.state(&range.partition)?;
        let actual = Incarnation(format!("inc:{}", state.incarnation));
        if actual != range.incarnation {
            return Err(ScanError::IncarnationMismatch {
                partition: range.partition.clone(),
                expected: range.incarnation.clone(),
                actual,
            }
            .into());
        }
        let start = Self::position(&range.partition, &range.start)?;
        let end = Self::position(&range.partition, &range.end)?;
        if start < state.earliest {
            return Err(ScanError::RetentionGap {
                partition: range.partition.clone(),
                expected: range.start.clone(),
                first_retained: Self::cursor(state.earliest),
            }
            .into());
        }
        let available = Self::end_of(&state);
        if end > available {
            // The range reaches past what the partition holds: the page is
            // not contiguous up to `end`.
            return Err(ScanError::NonContiguous {
                partition: range.partition.clone(),
                expected: range.end.clone(),
                actual: Self::cursor(available),
            }
            .into());
        }
        let first = usize::try_from(start - state.earliest).unwrap_or(usize::MAX);
        let last = usize::try_from(end - state.earliest).unwrap_or(usize::MAX);
        let rows = &state.rows[first.min(state.rows.len())..last.min(state.rows.len())];
        let columns: Result<Vec<ArrayRef>, _> = (0..self.schema.fields().len())
            .map(|column| {
                let values = rows.iter().map(|row| row[column].clone());
                ScalarValue::iter_to_array(values).or_else(|_| {
                    ScalarValue::try_from(self.schema.field(column).data_type())
                        .and_then(|null| null.to_array_of_size(0))
                })
            })
            .collect();
        let columns = columns.map_err(|_| ScanError::ForeignCursor {
            partition: range.partition.clone(),
            cursor: range.start.clone(),
        })?;
        let batch = RecordBatch::try_new(self.schema.clone(), columns).map_err(|_| {
            ScanError::ForeignCursor {
                partition: range.partition.clone(),
                cursor: range.start.clone(),
            }
        })?;
        let table = MemTable::try_new(self.schema.clone(), vec![vec![batch]]).map_err(|_| {
            ScanError::UnknownPartition {
                partition: range.partition.clone(),
            }
        })?;
        Ok(Arc::new(table))
    }
}
