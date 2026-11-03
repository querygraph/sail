//! Ordinary Arrow table scans over a pinned graph revision. No CSR is built.
use super::*;

pub(crate) struct HostOwner(pub Arc<dyn std::any::Any + Send + Sync>);

impl std::fmt::Debug for HostOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Read the field: its only operational purpose is retaining the lease.
        f.debug_tuple("HostOwner")
            .field(&Arc::strong_count(&self.0))
            .finish()
    }
}

/// A session graph revision. Rows and projection caches stay shared with all
/// other readers of this revision, even after the name is overwritten/dropped.
#[derive(Clone)]
pub struct GraphSnapshot {
    name: String,
    revision: u64,
    entry: Arc<RwLock<Entry>>,
    _owner: Option<Arc<HostOwner>>,
}

impl std::fmt::Debug for GraphSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphSnapshot")
            .field("name", &self.name)
            .field("revision", &self.revision)
            .finish_non_exhaustive()
    }
}

impl GraphSnapshot {
    pub(crate) fn new(
        name: String,
        entry: Arc<RwLock<Entry>>,
        owner: Option<Arc<HostOwner>>,
    ) -> Result<Self> {
        let revision = entry.read().map_err(|_| poisoned())?.revision;
        Ok(Self {
            name,
            revision,
            entry,
            _owner: owner,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Current diagnostics for this pinned revision, including its shared cache.
    pub fn info(&self) -> Result<GraphInfo> {
        let e = self.entry.read().map_err(|_| poisoned())?;
        Ok(GraphInfo {
            name: self.name.clone(),
            revision: self.revision,
            staged_nodes: e.nodes.iter().map(RecordBatch::num_rows).sum(),
            staged_edges: e.edges.iter().map(RecordBatch::num_rows).sum(),
            staged_bytes: e.node_bytes.bytes() + e.edge_bytes.bytes(),
            projections: e.projections.len(),
        })
    }

    /// Scan staged normalized node columns, including isolated nodes and properties.
    /// Unsorted (`AsStaged`) batches must have a common schema to be scanned.
    pub fn nodes(&self) -> Result<Arc<dyn TableProvider>> {
        self.table(Part::Nodes)
    }

    /// Scan staged normalized edge columns. Parallel edges are preserved.
    /// Unsorted (`AsStaged`) batches must have a common schema to be scanned.
    pub fn edges(&self) -> Result<Arc<dyn TableProvider>> {
        self.table(Part::Edges)
    }

    fn table(&self, part: Part) -> Result<Arc<dyn TableProvider>> {
        let owner = Arc::new(self.clone());
        let entry = self.entry.read().map_err(|_| poisoned())?;
        let (batches, declared) = match part {
            Part::Nodes => (&entry.nodes, &entry.node_schema),
            Part::Edges => (&entry.edges, &entry.edge_schema),
        };
        let schema = declared.clone().unwrap_or_else(|| match part {
            Part::Nodes => node_schema(),
            Part::Edges => Arc::new(Schema::new(vec![
                Field::new("source", DataType::Utf8, true),
                Field::new("target", DataType::Utf8, true),
                Field::new("label", DataType::Utf8, true),
                Field::new("edge_id", DataType::Utf8, true),
            ])),
        });
        let batches = batches
            .iter()
            .cloned()
            .map(|batch| retain_owner(batch, owner.clone()))
            .collect::<Result<Vec<_>>>()?;
        // One partition preserves the driver-only extension contract. Sail may
        // redistribute its output for normal relational joins/aggregations.
        Ok(Arc::new(MemTable::try_new(schema, vec![batches])?))
    }
}

/// Tie admission to the buffers themselves, not only to a plan or stream.
/// Arrow's C data release callbacks preserve these owners through native FFI.
pub(crate) fn retain_owner<T: Send + Sync + 'static>(
    batch: RecordBatch,
    owner: Arc<T>,
) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|array| grust_arrow::retain_array_owner(array, owner.clone()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

#[cfg(test)]
mod tests;
