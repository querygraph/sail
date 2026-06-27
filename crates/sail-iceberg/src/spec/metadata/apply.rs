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
