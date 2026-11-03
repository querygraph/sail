//! Session ownership for the Sail extension proof of concept.
use super::*;

/// A separately admitted graph store. Dropping its last owner releases staged rows.
#[derive(Clone)]
pub struct SessionRegistry {
    pub(super) store: Arc<Store>,
    pub(super) reads: Arc<std::sync::Mutex<ReadLog>>,
}

impl std::fmt::Debug for SessionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionRegistry").finish_non_exhaustive()
    }
}

impl SessionRegistry {
    pub fn new(memory_bytes: usize) -> Self {
        Self {
            store: Arc::new(Store::new(memory_bytes)),
            reads: Default::default(),
        }
    }

    /// Admit the session through a host lease. Snapshots, detached producers and
    /// exported Arrow buffers keep this owner alive independently of the binding.
    pub fn new_with_owner(
        memory_bytes: usize,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        let mut store = Store::new(memory_bytes);
        store.owner = Some(Arc::new(graph_tables::HostOwner(owner)));
        Self {
            store: Arc::new(store),
            reads: Default::default(),
        }
    }

    /// Pin a committed revision without building any topology projection.
    pub fn snapshot(&self, graph: &str) -> Result<GraphSnapshot> {
        GraphSnapshot::new(
            graph.to_owned(),
            self.store.existing(graph)?,
            self.store.owner.clone(),
        )
    }

    pub fn nodes(&self, graph: &str) -> Result<Arc<dyn TableProvider>> {
        self.snapshot(graph)?.nodes()
    }

    pub fn edges(&self, graph: &str) -> Result<Arc<dyn TableProvider>> {
        self.snapshot(graph)?.edges()
    }

    /// Prepare both parts privately; only `finish` makes them visible.
    pub fn replacing<'a>(
        &'a self,
        name: &str,
        nodes: &ColumnMapping,
        edges: &ColumnMapping,
        order: StageOrder,
    ) -> GraphStaging<'a> {
        GraphStaging {
            registry: self,
            name: name.to_owned(),
            nodes: self.store.staging(name, Part::Nodes, nodes, true, order),
            edges: self.store.staging(name, Part::Edges, edges, true, order),
        }
    }

    pub fn drop_graph(&self, name: &str) -> Result<bool> {
        self.store.as_ref().drop(name)
    }

    pub fn list(&self) -> Result<Vec<GraphInfo>> {
        self.store.list()
    }

    pub fn memory(&self) -> Result<MemoryInfo> {
        self.store.memory()
    }

    /// Keep the host admission lease with an auxiliary exported batch too.
    pub fn retain_output_owner(&self, batch: RecordBatch) -> Result<RecordBatch> {
        match &self.store.owner {
            Some(owner) => graph_tables::retain_owner(batch, owner.clone()),
            None => Ok(batch),
        }
    }

    pub fn reads(&self) -> Result<Vec<ReadInfo>> {
        let log = self.reads.lock().map_err(|_| poisoned())?;
        let running = log
            .running
            .values()
            .map(|(info, query)| with_usage(info.clone(), query));
        Ok(log.ended.iter().cloned().chain(running).collect())
    }

    /// Pin the graph's current rows/revision before returning a lazy read.
    /// The snapshot shares admitted buffers and the session memory pool; later
    /// overwrite/drop cannot change the graph this provider reads.
    pub fn algorithm(
        &self,
        algorithm: &str,
        graph: &str,
        options: &serde_json::Map<String, serde_json::Value>,
        names: ColumnNames,
    ) -> Result<AlgorithmTable> {
        let entry = self.store.existing(graph)?;
        // Session writes replace the map entry atomically; they never modify a
        // published entry's rows. Share that revision and its synchronized CSR
        // cache across providers, while overwrite/drop only changes the map.
        let store = Store {
            graphs: RwLock::new(HashMap::from([(graph.to_owned(), entry)])),
            pool: self.store.pool.clone(),
            owner: self.store.owner.clone(),
        };
        let mut table = AlgorithmTable::build(algorithm, graph.to_owned(), options, names, false)?;
        table.session = Some(Self {
            store: Arc::new(store),
            reads: self.reads.clone(),
        });
        Ok(table)
    }
}

/// A two-input overwrite transaction, admitted under one session budget.
pub struct GraphStaging<'a> {
    registry: &'a SessionRegistry,
    name: String,
    nodes: Staging<'a>,
    edges: Staging<'a>,
}

impl GraphStaging<'_> {
    pub fn push_nodes(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() > 0 {
            let (id, _) = pick(
                batch,
                &self.nodes.mapping.id,
                &["node_id", "id"],
                "node id",
                true,
            )?
            .expect("required");
            if id.null_count() != 0 {
                return plan_err!("nutmeg: node id cannot be null");
            }
        }
        self.nodes.push(batch)
    }

    pub fn push_edges(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() > 0 {
            for (mapping, candidates, name) in [
                (
                    &self.edges.mapping.source,
                    &["source", "src_id", "src"][..],
                    "source",
                ),
                (
                    &self.edges.mapping.target,
                    &["target", "dst_id", "dst"][..],
                    "target",
                ),
            ] {
                let (column, _) = pick(batch, mapping, candidates, name, true)?.expect("required");
                if column.null_count() != 0 {
                    return plan_err!("nutmeg: edge {name} cannot be null");
                }
            }
        }
        self.edges.push(batch)
    }

    /// Linearize one graph revision only after both parts are complete.
    pub fn finish(self) -> Result<GraphInfo> {
        let entry = RwLock::new(Entry::default());
        self.nodes.swap_into(&entry)?;
        self.edges.swap_into(&entry)?;
        let mut prepared = entry.into_inner().map_err(|_| poisoned())?;
        let mut graphs = self.registry.store.graphs.write().map_err(|_| poisoned())?;
        let previous = graphs
            .get(&self.name)
            .map(|e| e.read().map(|e| e.revision))
            .transpose()
            .map_err(|_| poisoned())?
            .unwrap_or(0);
        prepared.revision = previous
            .checked_add(1)
            .ok_or_else(|| err("graph revision overflow"))?;
        let info = GraphInfo {
            name: self.name.clone(),
            staged_nodes: prepared.nodes.iter().map(RecordBatch::num_rows).sum(),
            staged_edges: prepared.edges.iter().map(RecordBatch::num_rows).sum(),
            staged_bytes: prepared.node_bytes.bytes() + prepared.edge_bytes.bytes(),
            revision: prepared.revision,
            projections: 0,
        };
        graphs.insert(self.name, Arc::new(RwLock::new(prepared)));
        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edges(source: &[&str], target: &[&str]) -> RecordBatch {
        RecordBatch::try_from_iter([
            (
                "source",
                Arc::new(StringArray::from(source.to_vec())) as ArrayRef,
            ),
            (
                "target",
                Arc::new(StringArray::from(target.to_vec())) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn stage(registry: &SessionRegistry, edges: RecordBatch) {
        let mapping = ColumnMapping::default();
        let mut tx = registry.replacing("g", &mapping, &mapping, StageOrder::Canonical);
        tx.push_edges(&edges).unwrap();
        tx.finish().unwrap();
    }

    #[test]
    fn admission_refusal_preserves_both_parts_and_revision() {
        let registry = SessionRegistry::new(1 << 20);
        stage(&registry, edges(&["a"], &["b"]));
        let before = registry.list().unwrap();
        let mapping = ColumnMapping::default();
        let mut tx = registry.replacing("g", &mapping, &mapping, StageOrder::Canonical);
        let huge = "x".repeat(2 << 20);
        let nodes = RecordBatch::try_from_iter([(
            "node_id",
            Arc::new(StringArray::from(vec![huge])) as ArrayRef,
        )])
        .unwrap();
        assert!(
            tx.push_nodes(&nodes)
                .unwrap_err()
                .to_string()
                .contains("memory budget")
        );
        drop(tx);
        let after = registry.list().unwrap();
        assert_eq!(after[0].staged_nodes, before[0].staged_nodes);
        assert_eq!(after[0].staged_edges, before[0].staged_edges);
        assert_eq!(after[0].revision, before[0].revision);
        assert_eq!(after[0].staged_bytes, before[0].staged_bytes);
    }

    #[test]
    fn isolated_sessions_atomic_refusal_and_pinned_reads() {
        prepare_output_schemas().unwrap();
        let a = SessionRegistry::new(16 << 20);
        let b = SessionRegistry::new(16 << 20);
        stage(&a, edges(&["a"], &["b"]));
        stage(&b, edges(&["x", "y"], &["y", "z"]));
        assert_eq!(a.list().unwrap()[0].staged_edges, 1);
        assert_eq!(b.list().unwrap()[0].staged_edges, 2);
        let old = a
            .algorithm("degree", "g", &Default::default(), ColumnNames::Grust)
            .unwrap();
        stage(&a, edges(&["a", "b"], &["b", "c"]));
        assert_eq!(
            old.batches()
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        assert_eq!(
            a.algorithm("degree", "g", &Default::default(), ColumnNames::Grust)
                .unwrap()
                .batches()
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            3
        );
        let mapping = ColumnMapping::default();
        let mut bad = a.replacing("g", &mapping, &mapping, StageOrder::Canonical);
        bad.push_nodes(
            &RecordBatch::try_from_iter([(
                "node_id",
                Arc::new(StringArray::from(vec!["bad"])) as ArrayRef,
            )])
            .unwrap(),
        )
        .unwrap();
        assert!(
            bad.push_edges(
                &RecordBatch::try_from_iter([(
                    "wrong",
                    Arc::new(StringArray::from(vec!["bad"])) as ArrayRef
                )])
                .unwrap()
            )
            .is_err()
        );
        drop(bad);
        assert_eq!(a.list().unwrap()[0].staged_edges, 2);
        assert_eq!(a.list().unwrap()[0].revision, 2);
        a.drop_graph("g").unwrap();
        assert_eq!(b.list().unwrap()[0].staged_edges, 2);
        assert_eq!(a.reads().unwrap().len(), 2);
        assert!(b.reads().unwrap().is_empty());
    }
}
