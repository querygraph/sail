//! Source-owned weighted CSR. Target existence is a distributed topology check.
use crate::{Operation, Resources, Result, reserve_vec, source_index::SourceIndex};
use grust_procedures::MemoryReservation;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
static NEXT_WEIGHTED_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct WeightedAdjacency {
    identity: u64,
    vertices: Vec<i64>,
    offsets: Vec<usize>,
    arcs: Vec<(i64, f64)>,
    _admission: MemoryReservation,
    _lease: Arc<sail_native_resource_ffi::MemoryLease>,
}
impl WeightedAdjacency {
    pub fn build(
        operation: &Operation,
        partition: usize,
        vertices: &[i64],
        edges: &[(i64, i64, f64)],
        resources: &Resources,
    ) -> Result<Arc<Self>> {
        operation.validate()?;
        if partition >= operation.partitions || vertices.len() as u64 > operation.vertices {
            return Err("invalid weighted partition dimensions".into());
        }
        let n = vertices.len();
        let bytes = n
            .checked_mul(64 - size_of::<usize>())
            .and_then(|x| edges.len().checked_mul(24).and_then(|e| x.checked_add(e)))
            .and_then(|x| x.checked_add(4096 + size_of::<usize>()))
            .ok_or("weighted adjacency admission overflow")?;
        // Prepay sorting/build scratch as well as final storage before allocating.
        let admission = resources
            .execution
            .reserve(bytes)
            .map_err(|e| e.to_string())?;
        resources
            .execution
            .charge_work(
                n.checked_mul(n.max(1).ilog2() as usize + 1)
                    .ok_or("weighted sorting work overflow")?,
            )
            .map_err(|e| e.to_string())?;
        let mut ids = reserve_vec(n)?;
        ids.extend_from_slice(vertices);
        ids.sort_unstable();
        let mut meter = resources.execution.work_meter();
        for (i, &id) in ids.iter().enumerate() {
            meter.charge(1).map_err(|e| e.to_string())?;
            if operation.owner(id) != partition || i > 0 && ids[i - 1] == id {
                return Err("weighted vertices duplicated or owned by another partition".into());
            }
        }
        let sources = SourceIndex::new(&ids, operation.partitions);
        let mut offsets = reserve_vec(n + 1)?;
        offsets.resize(n + 1, 0usize);
        for &(source, _, weight) in edges {
            meter
                .charge(sources.lookup_work())
                .map_err(|e| e.to_string())?;
            if !weight.is_finite() || weight < 0.0 {
                return Err("SSSP weight must be finite and nonnegative".into());
            }
            let local = sources
                .lookup(source)
                .ok_or("weighted source is not owned")?;
            offsets[local + 1] = offsets[local + 1]
                .checked_add(1)
                .ok_or("weighted degree overflow")?;
        }
        for i in 0..n {
            meter.charge(1).map_err(|e| e.to_string())?;
            offsets[i + 1] = offsets[i + 1]
                .checked_add(offsets[i])
                .ok_or("weighted arc count overflow")?;
        }
        // Reuse starts as fill cursors; restore them from the resulting ends.
        let mut arcs = reserve_vec(edges.len())?;
        arcs.resize(edges.len(), (0, 0.0));
        for &(source, target, weight) in edges {
            meter
                .charge(sources.lookup_work())
                .map_err(|e| e.to_string())?;
            let local = sources
                .lookup(source)
                .ok_or("weighted source is not owned")?;
            arcs[offsets[local]] = (target, if weight == 0.0 { 0.0 } else { weight });
            offsets[local] += 1;
        }
        meter.finish();
        resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        offsets.copy_within(..n, 1);
        offsets[0] = 0;
        let retained = std::mem::size_of_val(ids.as_slice())
            + std::mem::size_of_val(offsets.as_slice())
            + std::mem::size_of_val(arcs.as_slice())
            + 4096;
        admission.shrink(retained).map_err(|e| e.to_string())?;
        Ok(Arc::new(Self {
            identity: NEXT_WEIGHTED_ID
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| x.checked_add(1))
                .map_err(|_| "weighted adjacency identity overflow")?,
            vertices: ids,
            offsets,
            arcs,
            _admission: admission,
            _lease: resources.lease.clone(),
        }))
    }
    pub fn identity(&self) -> u64 {
        self.identity
    }
    pub fn vertices(&self) -> &[i64] {
        &self.vertices
    }
    pub fn arc_count(&self) -> usize {
        self.arcs.len()
    }
    pub(super) fn outgoing_at(&self, index: usize) -> &[(i64, f64)] {
        &self.arcs[self.offsets[index]..self.offsets[index + 1]]
    }
    pub fn outgoing(&self, vertex: i64) -> Result<&[(i64, f64)]> {
        let index = self
            .vertices
            .binary_search(&vertex)
            .map_err(|_| "unknown local weighted vertex")?;
        Ok(&self.arcs[self.offsets[index]..self.offsets[index + 1]])
    }
}
