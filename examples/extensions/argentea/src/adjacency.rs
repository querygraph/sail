//! Immutable admitted adjacency shared by Argentea's reference and residual PR.
use crate::{Operation, Resources, Result, reserve_vec, source_index::SourceIndex};
use grust_procedures::MemoryReservation;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
static NEXT_ADJACENCY_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub(crate) struct Adjacency {
    pub(crate) identity: u64,
    pub(crate) vertices: Vec<i64>,
    pub(crate) offsets: Vec<usize>,
    pub(crate) targets: Vec<i64>,
    _admission: MemoryReservation,
}
impl Adjacency {
    pub(crate) fn build(
        operation: &Operation,
        partition: usize,
        vertices: &[i64],
        edges: &[(i64, i64)],
        resources: &Resources,
    ) -> Result<Arc<Self>> {
        operation.validate()?;
        if partition >= operation.partitions || vertices.len() as u64 > operation.vertices {
            return Err("invalid graph partition dimensions".into());
        }
        let n = vertices.len();
        let bytes = n
            .checked_mul(64 - size_of::<usize>())
            .and_then(|v| edges.len().checked_mul(16).and_then(|e| v.checked_add(e)))
            .and_then(|v| v.checked_add(4096 + size_of::<usize>()))
            .ok_or("partition admission overflow")?;
        let admission = resources
            .execution
            .reserve(bytes)
            .map_err(|e| e.to_string())?;
        let mut ids = reserve_vec(n)?;
        ids.extend_from_slice(vertices);
        ids.sort_unstable();
        let mut meter = resources.execution.work_meter();
        for (i, id) in ids.iter().enumerate() {
            meter.charge(1).map_err(|e| e.to_string())?;
            if operation.owner(*id) != partition || (i > 0 && ids[i - 1] == *id) {
                return Err("vertices are duplicated or belong to another partition".into());
            }
        }
        let sources = SourceIndex::new(&ids, operation.partitions);
        let mut offsets = reserve_vec(n + 1)?;
        offsets.resize(n + 1, 0usize);
        for &(source, _) in edges {
            meter.charge(1).map_err(|e| e.to_string())?;
            let local = sources.lookup(source).ok_or("source is not owned")?;
            offsets[local + 1] += 1;
        }
        for i in 0..n {
            meter.charge(1).map_err(|e| e.to_string())?;
            offsets[i + 1] += offsets[i];
        }
        // Reuse starts as fill cursors; restore them from the resulting ends.
        let mut targets = reserve_vec(edges.len())?;
        targets.resize(edges.len(), 0i64);
        for &(source, target) in edges {
            meter.charge(1).map_err(|e| e.to_string())?;
            let local = sources.lookup(source).ok_or("source is not owned")?;
            targets[offsets[local]] = target;
            offsets[local] += 1;
        }
        offsets.copy_within(..n, 1);
        offsets[0] = 0;
        // Retain conservative metadata and all CSR bytes, release build scratch.
        admission
            .shrink(n * 8 + (n + 1) * size_of::<usize>() + edges.len() * 8 + 4096)
            .map_err(|e| e.to_string())?;
        Ok(Arc::new(Self {
            identity: NEXT_ADJACENCY_ID
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .map_err(|_| "adjacency identity overflow")?,
            vertices: ids,
            offsets,
            targets,
            _admission: admission,
        }))
    }
}
