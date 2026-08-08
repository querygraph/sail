// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Apply Iceberg REST `TableUpdate`s to a [`TableMetadata`].
//!
//! A catalog receiving an `updateTable` (commit) request needs to evolve the
//! current table metadata by the requested updates to produce the new metadata
//! document it then persists. This is engine-owned table-format work, so it
//! lives in Sail rather than being hand-rolled in the catalog.
//!
//! The metadata-only and removal updates are implemented fully. Updates that
//! require deeper engine machinery (partition-spec binding, snapshot
//! sequencing, snapshot-ref state) return `NotImplemented` rather than
//! silently producing incorrect metadata; those belong in a complete
//! `TableMetadataBuilder` port.

use datafusion_common::{not_impl_err, DataFusionError, Result};

use crate::spec::catalog::TableUpdate;
use crate::spec::metadata::table_metadata::{SnapshotLog, TableMetadata};

/// Apply a sequence of `TableUpdate`s to `metadata` in order. On any applied
/// change, `last_updated_ms` is advanced to `now_ms`.
pub fn apply_table_updates(
    metadata: &mut TableMetadata,
    updates: &[TableUpdate],
    now_ms: i64,
) -> Result<()> {
    for update in updates {
        apply_one(metadata, update, now_ms)?;
    }
    if !updates.is_empty() {
        metadata.last_updated_ms = now_ms;
    }
    Ok(())
}

fn apply_one(metadata: &mut TableMetadata, update: &TableUpdate, now_ms: i64) -> Result<()> {
    match update {
        TableUpdate::SetProperties { updates } => {
            for (k, v) in updates {
                metadata.properties.insert(k.clone(), v.clone());
            }
        }
        TableUpdate::RemoveProperties { removals } => {
            for k in removals {
                metadata.properties.remove(k);
            }
        }
        TableUpdate::SetLocation { location } => {
            metadata.location = location.clone();
        }
        TableUpdate::AssignUuid { uuid } => {
            metadata.table_uuid = Some(*uuid);
        }
        TableUpdate::UpgradeFormatVersion { format_version } => {
            metadata.format_version = *format_version;
        }
        TableUpdate::AddSchema { schema } => {
            let schema_id = schema.schema_id();
            metadata.last_column_id = metadata.last_column_id.max(schema.highest_field_id());
            if let Some(existing) = metadata
                .schemas
                .iter_mut()
                .find(|s| s.schema_id() == schema_id)
            {
                *existing = (**schema).clone();
            } else {
                metadata.schemas.push((**schema).clone());
            }
        }
        TableUpdate::SetCurrentSchema { schema_id } => {
            let resolved = if *schema_id == -1 {
                metadata
                    .schemas
                    .last()
                    .map(|s| s.schema_id())
                    .ok_or_else(|| invalid("set-current-schema -1 with no schemas added"))?
            } else {
                *schema_id
            };
            if !metadata.schemas.iter().any(|s| s.schema_id() == resolved) {
                return Err(invalid(format!("unknown schema-id {resolved}")));
            }
            metadata.current_schema_id = resolved;
        }
        TableUpdate::SetDefaultSpec { spec_id } => {
            let resolved = if *spec_id == -1 {
                metadata
                    .partition_specs
                    .last()
                    .map(|s| s.spec_id())
                    .ok_or_else(|| invalid("set-default-spec -1 with no specs added"))?
            } else {
                *spec_id
            };
            metadata.default_spec_id = resolved;
        }
        TableUpdate::AddSortOrder { sort_order } => {
            let order_id = sort_order.order_id;
            if let Some(existing) = metadata
                .sort_orders
                .iter_mut()
                .find(|o| o.order_id == order_id)
            {
                *existing = sort_order.clone();
            } else {
                metadata.sort_orders.push(sort_order.clone());
            }
        }
        TableUpdate::SetDefaultSortOrder { sort_order_id } => {
            let resolved = if *sort_order_id == -1 {
                metadata
                    .sort_orders
                    .last()
                    .map(|o| o.order_id)
                    .ok_or_else(|| invalid("set-default-sort-order -1 with no sort orders added"))?
            } else {
                *sort_order_id
            };
            metadata.default_sort_order_id = Some(resolved as i32);
        }
        TableUpdate::RemoveSnapshots { snapshot_ids } => {
            metadata
                .snapshots
                .retain(|s| !snapshot_ids.contains(&s.snapshot_id()));
            if let Some(current) = metadata.current_snapshot_id {
                if snapshot_ids.contains(&current) {
                    metadata.current_snapshot_id = None;
                }
            }
        }
        TableUpdate::RemoveSnapshotRef { ref_name } => {
            metadata.refs.remove(ref_name);
            if ref_name == "main" {
                metadata.current_snapshot_id = None;
            }
        }
        TableUpdate::RemovePartitionSpecs { spec_ids } => {
            metadata
                .partition_specs
                .retain(|s| !spec_ids.contains(&s.spec_id()));
        }
        TableUpdate::RemoveSchemas { schema_ids } => {
            metadata
                .schemas
                .retain(|s| !schema_ids.contains(&s.schema_id()));
        }
        TableUpdate::SetStatistics { statistics } => {
            let snapshot_id = statistics.snapshot_id;
            metadata.statistics.retain(|s| s.snapshot_id != snapshot_id);
            metadata.statistics.push(statistics.clone());
        }
        TableUpdate::RemoveStatistics { snapshot_id } => {
            metadata
                .statistics
                .retain(|s| s.snapshot_id != *snapshot_id);
        }
        TableUpdate::AddSnapshot { snapshot } => {
            // Append the snapshot to the table's snapshot set. Per the Iceberg
            // spec, `add-snapshot` only adds the snapshot and advances
            // `last-sequence-number` (and the v3 row-id counter); the current ref
            // and snapshot log are moved by a following `set-snapshot-ref`. A
            // stock append (pyiceberg/Spark) sends both in one commit.
            let seq = snapshot.sequence_number();
            if seq > metadata.last_sequence_number {
                metadata.last_sequence_number = seq;
            }
            if let Some(added_rows) = snapshot.added_rows {
                metadata.advance_next_row_id(added_rows);
            }
            metadata.snapshots.push(snapshot.clone());
        }
        TableUpdate::SetSnapshotRef {
            ref_name,
            reference,
        } => {
            // Set or move a branch/tag ref. The referenced snapshot must already
            // be present (added earlier in this commit, or pre-existing). Moving
            // `main` advances the table's current snapshot and records a
            // snapshot-log entry.
            if !metadata
                .snapshots
                .iter()
                .any(|s| s.snapshot_id() == reference.snapshot_id)
            {
                return Err(invalid(format!(
                    "set-snapshot-ref '{ref_name}' references unknown snapshot-id {}",
                    reference.snapshot_id
                )));
            }
            metadata.refs.insert(ref_name.clone(), reference.clone());
            if ref_name == "main" {
                metadata.current_snapshot_id = Some(reference.snapshot_id);
                metadata.snapshot_log.push(SnapshotLog {
                    timestamp_ms: now_ms,
                    snapshot_id: reference.snapshot_id,
                });
            }
        }
        // `add-spec` still requires partition-spec binding machinery — defer to a
        // complete builder rather than produce incorrect metadata.
        other => {
            return not_impl_err!(
                "TableUpdate not yet supported by apply_table_updates: {}",
                update_kind(other)
            );
        }
    }
    Ok(())
}

fn update_kind(update: &TableUpdate) -> &'static str {
    match update {
        TableUpdate::AddSpec { .. } => "add-spec",
        _ => "unsupported",
    }
}

fn invalid(msg: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(msg.into())
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::spec::snapshots::snapshot::{
        Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
    };

    /// A minimal, snapshot-free format-version-2 table metadata.
    fn empty_v2_metadata() -> TableMetadata {
        let json = br#"{
            "format-version": 2,
            "table-uuid": "00000000-0000-0000-0000-000000000001",
            "location": "s3://warehouse/t",
            "last-sequence-number": 0,
            "last-updated-ms": 0,
            "last-column-id": 1,
            "current-schema-id": 0,
            "schemas": [
                {"type": "struct", "schema-id": 0,
                 "fields": [{"id": 1, "name": "x", "required": false, "type": "long"}]}
            ],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {},
            "current-snapshot-id": null,
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "refs": {}
        }"#;
        TableMetadata::from_json(json).expect("valid v2 metadata")
    }

    fn snapshot(id: i64, seq: i64) -> Snapshot {
        Snapshot {
            snapshot_id: id,
            parent_snapshot_id: None,
            sequence_number: seq,
            timestamp_ms: 1_000,
            manifest_list: format!("s3://warehouse/t/metadata/snap-{id}.avro"),
            manifests: None,
            summary: Summary::new(Operation::Append),
            schema_id: None,
            first_row_id: None,
            added_rows: Some(10),
            key_id: None,
        }
    }

    fn main_branch(snapshot_id: i64) -> SnapshotReference {
        SnapshotReference {
            snapshot_id,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        }
    }

    #[test]
    fn append_adds_snapshot_and_moves_main() {
        let mut m = empty_v2_metadata();
        let updates = vec![
            TableUpdate::AddSnapshot {
                snapshot: snapshot(100, 1),
            },
            TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                reference: main_branch(100),
            },
        ];
        apply_table_updates(&mut m, &updates, 5_000).expect("append applies");

        assert_eq!(m.snapshots.len(), 1);
        assert_eq!(m.snapshots[0].snapshot_id, 100);
        assert_eq!(m.last_sequence_number, 1);
        assert_eq!(m.current_snapshot_id, Some(100));
        assert_eq!(m.refs.get("main").map(|r| r.snapshot_id), Some(100));
        assert_eq!(m.snapshot_log.len(), 1);
        assert_eq!(m.snapshot_log[0].snapshot_id, 100);
        assert_eq!(m.snapshot_log[0].timestamp_ms, 5_000);
        assert_eq!(m.last_updated_ms, 5_000);
    }

    #[test]
    fn add_snapshot_alone_does_not_move_current_or_log() {
        let mut m = empty_v2_metadata();
        apply_table_updates(
            &mut m,
            &[TableUpdate::AddSnapshot {
                snapshot: snapshot(7, 3),
            }],
            5_000,
        )
        .expect("add-snapshot applies");

        assert_eq!(m.snapshots.len(), 1);
        assert_eq!(m.last_sequence_number, 3);
        // Per the spec, add-snapshot alone does not move the current ref or log.
        assert_eq!(m.current_snapshot_id, None);
        assert!(m.snapshot_log.is_empty());
    }

    #[test]
    fn set_snapshot_ref_to_unknown_snapshot_errors() {
        let mut m = empty_v2_metadata();
        let err = apply_table_updates(
            &mut m,
            &[TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                reference: main_branch(999),
            }],
            5_000,
        )
        .expect_err("ref to unknown snapshot is rejected");
        assert!(err.to_string().contains("unknown snapshot-id 999"), "{err}");
    }

    #[test]
    fn second_append_keeps_both_snapshots_and_advances() {
        let mut m = empty_v2_metadata();
        apply_table_updates(
            &mut m,
            &[
                TableUpdate::AddSnapshot {
                    snapshot: snapshot(1, 1),
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: "main".to_string(),
                    reference: main_branch(1),
                },
            ],
            1_000,
        )
        .expect("first append");
        apply_table_updates(
            &mut m,
            &[
                TableUpdate::AddSnapshot {
                    snapshot: snapshot(2, 2),
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: "main".to_string(),
                    reference: main_branch(2),
                },
            ],
            2_000,
        )
        .expect("second append");

        assert_eq!(m.snapshots.len(), 2);
        assert_eq!(m.current_snapshot_id, Some(2));
        assert_eq!(m.last_sequence_number, 2);
        assert_eq!(m.snapshot_log.len(), 2);
    }
}
