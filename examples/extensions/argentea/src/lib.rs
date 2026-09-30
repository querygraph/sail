//! Extension-owned primitives for Argentea's first distributed PageRank spike.
//!
//! This crate does not schedule work or provide a transport. Sail integration
//! must establish placement, input completeness, operation cleanup and a real
//! host memory lease before these primitives can execute remotely.
mod adjacency;
#[cfg(test)]
mod adjacency_tests;
mod source_index;
mod sssp;
pub use sssp::*;
mod wcc;
pub use wcc::{
    WccAlgorithm, WccCapFailure, WccCompletion, WccCompletionValues, WccConvergence,
    WccEmissionCursor, WccMessage, WccMessageValues, WccMode, WccOptions, WccOrigin, WccPartition,
    WccPayload, WccRow, WccRowCursor, WccStatistics, WccStatisticsValues, WccWork, wcc_head,
};
mod bfs;
pub use bfs::{
    BfsAlgorithm, BfsCapFailure, BfsCompletion, BfsCompletionValues, BfsConvergence,
    BfsEmissionCursor, BfsMessage, BfsMessageValues, BfsMode, BfsOptions, BfsOrigin, BfsPartition,
    BfsPayload, BfsRow, BfsRowCursor, BfsStatistics, BfsStatisticsValues, BfsWork,
};
mod delta;
mod pagerank;
pub use delta::{
    Convergence, DeltaCapFailure, DeltaCompletion, DeltaContribution, DeltaEmissionCursor,
    DeltaMode, DeltaOptions, DeltaPartition, DeltaRankCursor, DeltaStatistics,
    DeltaStatisticsValues,
};

use grust_procedures::ExecutionContext;
pub use pagerank::{
    Contribution, Emission, EmissionCursor, PageRankPartition, RankCursor, RoundResult,
};
use sail_native_resource_ffi::MemoryLease;
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, String>;

/// Host-scoped execution identity, never a driver pointer or graph handle.
/// The host must check package identity before constructing this descriptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Operation {
    pub package: String,
    pub session: String,
    pub operation: String,
    pub snapshot: String,
    pub generation: u64,
    pub partitions: usize,
    pub vertices: u64,
}

impl Operation {
    pub fn validate(&self) -> Result<()> {
        if [
            &self.package,
            &self.session,
            &self.operation,
            &self.snapshot,
        ]
        .iter()
        .any(|value| value.is_empty() || value.len() > 256)
            || self.generation == 0
            || self.partitions == 0
            || self.partitions > 65_536
            || self.vertices == 0
        {
            return Err("invalid operation identity or graph dimensions".into());
        }
        Ok(())
    }

    pub fn owner(&self, vertex: i64) -> usize {
        vertex.rem_euclid(self.partitions as i64) as usize
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Round {
    pub operation: Operation,
    pub number: u64,
}

/// Uses Banda's resource accounting and the existing Sail lease ABI.
/// One domain should be shared by all Argentea partitions in a worker operation.
#[derive(Clone, Debug)]
pub struct Resources {
    pub execution: ExecutionContext,
    pub lease: Arc<MemoryLease>,
}

impl Resources {
    pub fn new(execution: ExecutionContext, lease: MemoryLease) -> Result<Self> {
        if execution.limits().memory_bytes as u64 > lease.bytes() {
            return Err("native limit exceeds the prepaid Sail lease".into());
        }
        Ok(Self {
            execution,
            lease: Arc::new(lease),
        })
    }
}

fn reserve_vec<T>(length: usize) -> Result<Vec<T>> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(length)
        .map_err(|e| e.to_string())?;
    Ok(result)
}
