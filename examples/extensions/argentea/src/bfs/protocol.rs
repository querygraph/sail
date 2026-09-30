//! Ordered typed streams; candidate state publishes only after both EOFs.
use super::*;
#[derive(Debug)]
struct EdgeBuffer {
    values: Vec<(i64, i64)>,
    admission: MemoryReservation,
}
impl EdgeBuffer {
    fn new(resources: &Resources) -> Result<Self> {
        Ok(Self {
            values: Vec::new(),
            admission: resources
                .execution
                .reserve(128)
                .map_err(|e| e.to_string())?,
        })
    }
    fn push(&mut self, edge: (i64, i64), resources: &Resources) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = self
                .values
                .capacity()
                .checked_mul(2)
                .and_then(|x| x.checked_add(64))
                .ok_or("BFS incoming capacity overflow")?;
            let bytes = capacity
                .checked_mul(16)
                .and_then(|x| x.checked_add(128))
                .ok_or("BFS incoming admission overflow")?;
            // Admit a complete replacement while the old buffer is still live.
            let admission = resources
                .execution
                .reserve(bytes)
                .map_err(|e| e.to_string())?;
            let mut replacement = reserve_vec(capacity)?;
            replacement.extend_from_slice(&self.values);
            self.values = replacement;
            self.admission = admission;
        }
        self.values.push(edge);
        Ok(())
    }
}
#[derive(Debug)]
pub(super) struct Inbox {
    pub mode: BfsMode,
    expected: Vec<BfsStatisticsValues>,
    sequences: Vec<u64>,
    finished: Vec<bool>,
    last_membership: Vec<Option<i64>>,
    parents: Vec<Option<i64>>,
    membership: Vec<bool>,
    edges: Option<EdgeBuffer>,
    pub emitted: Arc<OnceLock<BfsWork>>,
    received: u64,
    _admission: MemoryReservation,
}
impl Inbox {
    pub fn new(
        mode: BfsMode,
        reports: &[Option<BfsStatisticsValues>],
        n: usize,
        ghosts: usize,
        resources: &Resources,
    ) -> Result<Self> {
        let p = reports.len();
        let bytes = n
            .checked_mul(16)
            .and_then(|x| x.checked_add(ghosts))
            .and_then(|x| x.checked_add(p * (size_of::<BfsStatisticsValues>() + 32) + 256))
            .ok_or("BFS inbox admission overflow")?;
        let admission = resources
            .execution
            .reserve(bytes)
            .map_err(|e| e.to_string())?;
        let mut expected = reserve_vec(p)?;
        for r in reports {
            expected.push(r.ok_or("incomplete BFS statistics")?);
        }
        Ok(Self {
            mode,
            expected,
            sequences: filled(p, 0)?,
            finished: filled(p, false)?,
            last_membership: filled(p, None)?,
            parents: filled(n, None)?,
            membership: filled(ghosts, false)?,
            edges: None,
            emitted: Arc::new(OnceLock::new()),
            received: 0,
            _admission: admission,
        })
    }
}
impl BfsPartition {
    pub fn receive(&mut self, message: &BfsMessage) -> Result<()> {
        self.receive_values(&message.phase, message.values())
    }
    pub fn receive_values(&mut self, phase: &Round, message: BfsMessageValues) -> Result<()> {
        self.check_phase(phase)?;
        let result = self.receive_inner(&message);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }
    fn receive_inner(&mut self, m: &BfsMessageValues) -> Result<()> {
        let State::Receiving(inbox) = &mut self.state else {
            return Err("BFS is not receiving updates".into());
        };
        let p = m.producer;
        if m.recipient != self.partition
            || p >= self.operation.partitions
            || self.origins[p] != Some(m.origin)
            || inbox.mode != m.mode
            || inbox.finished[p]
            || inbox.sequences[p] != m.sequence
        {
            return Err("foreign, replayed, misrouted or late BFS update".into());
        }
        self.resources
            .execution
            .charge_work(1 + self.adjacency.vertices.len().max(1).ilog2() as usize)
            .map_err(|e| e.to_string())?;
        match m.payload {
            BfsPayload::Topology { source, target } => {
                if inbox.mode != BfsMode::Topology || self.operation.owner(source) != p {
                    return Err("invalid BFS topology source or mode".into());
                }
                self.adjacency
                    .vertices
                    .binary_search(&target)
                    .map_err(|_| "unknown BFS topology destination")?;
                if self.options.algorithm == BfsAlgorithm::DirectionOptimizing {
                    if inbox.edges.is_none() {
                        inbox.edges = Some(EdgeBuffer::new(&self.resources)?);
                    }
                    inbox
                        .edges
                        .as_mut()
                        .unwrap()
                        .push((target, source), &self.resources)?;
                }
            }
            BfsPayload::Candidate {
                target,
                parent,
                distance,
            } => {
                if !matches!(inbox.mode, BfsMode::Reference | BfsMode::Push)
                    || self.operation.owner(parent) != p
                    || distance != self.levels + 1
                {
                    return Err("invalid BFS candidate mode, parent or distance".into());
                }
                let i = self
                    .adjacency
                    .vertices
                    .binary_search(&target)
                    .map_err(|_| "unknown BFS candidate destination")?;
                if self.values.depths[i].is_none() {
                    inbox.parents[i] = Some(inbox.parents[i].map_or(parent, |old| old.min(parent)));
                }
            }
            BfsPayload::Membership { vertex } => {
                if inbox.mode != BfsMode::Pull
                    || self.operation.owner(vertex) != p
                    || inbox.last_membership[p].is_some_and(|old| old >= vertex)
                    || m.sequence >= inbox.expected[p].frontier
                {
                    return Err("invalid, duplicated or unordered BFS membership".into());
                }
                let incoming = self
                    .incoming
                    .as_ref()
                    .ok_or("BFS pull missing incoming adjacency")?;
                self.resources
                    .execution
                    .charge_work(incoming.ghosts.len().max(1).ilog2() as usize)
                    .map_err(|e| e.to_string())?;
                if let Ok(i) = incoming.ghosts.binary_search(&vertex) {
                    inbox.membership[i] = true;
                }
                inbox.last_membership[p] = Some(vertex);
            }
        }
        inbox.sequences[p] = m
            .sequence
            .checked_add(1)
            .ok_or("BFS receive sequence overflow")?;
        inbox.received = inbox
            .received
            .checked_add(1)
            .ok_or("BFS received count overflow")?;
        Ok(())
    }
    pub fn finish_producer(&mut self, c: &BfsCompletion) -> Result<()> {
        if c.sequences.len() != self.operation.partitions {
            return self.fail("invalid BFS completion partition count");
        }
        let Some(total_messages) = c.sequences.iter().try_fold(0u64, |a, b| a.checked_add(*b))
        else {
            return self.fail("BFS completion total overflow");
        };
        self.finish_producer_values(
            &c.phase,
            BfsCompletionValues {
                origin: c.origin,
                producer: c.producer,
                mode: c.mode,
                sequence: c.sequences[self.partition],
                total_messages,
            },
        )
    }
    /// Per-recipient sequence and one checked total: no P-length vector on wire.
    pub fn finish_producer_values(&mut self, phase: &Round, c: BfsCompletionValues) -> Result<()> {
        self.check_phase(phase)?;
        let State::Receiving(inbox) = &mut self.state else {
            return self.fail("BFS is not receiving updates");
        };
        let p = c.producer;
        if p >= self.operation.partitions
            || self.origins[p] != Some(c.origin)
            || c.mode != inbox.mode
            || inbox.finished[p]
            || c.sequence != inbox.sequences[p]
            || c.sequence > c.total_messages
        {
            return self.fail("incomplete, foreign or repeated BFS completion");
        }
        if (inbox.mode == BfsMode::Pull
            && (c.sequence != inbox.expected[p].frontier
                || inbox.expected[p]
                    .frontier
                    .checked_mul(self.operation.partitions as u64)
                    != Some(c.total_messages)))
            || (inbox.mode == BfsMode::Done && (c.sequence != 0 || c.total_messages != 0))
            || (inbox.mode == BfsMode::Topology && c.total_messages != inbox.expected[p].arcs)
            // Reference and push each emit once per frontier arc, including
            // parallel arcs and candidates whose destination is already reached.
            || (matches!(inbox.mode, BfsMode::Reference | BfsMode::Push)
                && c.total_messages != inbox.expected[p].frontier_edges)
        {
            return self.fail("BFS completion count does not match producer statistics");
        }
        inbox.finished[p] = true;
        Ok(())
    }
    /// Only call after input EOF: markers alone cannot rule out later rows.
    pub fn finish(&mut self, phase: &Round) -> Result<()> {
        self.check_phase(phase)?;
        let State::Receiving(inbox) = std::mem::replace(&mut self.state, State::Failed) else {
            return Err("BFS is not receiving updates".into());
        };
        let mut inbox = *inbox;
        if !inbox.finished.iter().all(|x| *x) {
            return Err("incomplete BFS update barrier".into());
        }
        let mut work = *inbox
            .emitted
            .get()
            .ok_or("BFS local emission not complete")?;
        work.received_messages = inbox.received;
        let mut incoming = self.incoming.clone();
        let mut candidate = None;
        let mut levels = self.levels;
        if inbox.mode == BfsMode::Topology {
            if self.options.algorithm == BfsAlgorithm::DirectionOptimizing {
                incoming = Some(self.build_incoming(inbox.edges.take())?);
            }
        } else if inbox.mode != BfsMode::Done {
            let mut meter = self.resources.execution.work_meter();
            if inbox.mode == BfsMode::Pull {
                let graph = self
                    .incoming
                    .as_ref()
                    .ok_or("BFS pull missing incoming adjacency")?;
                for i in 0..self.adjacency.vertices.len() {
                    meter.charge(1).map_err(|e| e.to_string())?;
                    work.examined_vertices += 1;
                    if self.values.depths[i].is_some() {
                        continue;
                    }
                    for &parent in &graph.adjacency.targets
                        [graph.adjacency.offsets[i]..graph.adjacency.offsets[i + 1]]
                    {
                        meter
                            .charge(1 + graph.ghosts.len().max(1).ilog2() as usize)
                            .map_err(|e| e.to_string())?;
                        work.examined_edges += 1;
                        let ghost = graph
                            .ghosts
                            .binary_search(&parent)
                            .map_err(|_| "BFS incoming ghost missing")?;
                        if inbox.membership[ghost] {
                            // Incoming IDs are sorted: first match is the minimum
                            // numeric parent, not arrival order or string order.
                            inbox.parents[i] = Some(parent);
                            break;
                        }
                    }
                }
            }
            let mut next = self.values.copy(&self.resources)?;
            for (i, parent) in inbox.parents.iter().enumerate() {
                meter.charge(1).map_err(|e| e.to_string())?;
                if let Some(parent) = parent.filter(|_| self.values.depths[i].is_none()) {
                    next.depths[i] = Some(self.levels + 1);
                    next.parents[i] = Some(parent);
                    next.frontier.push(i);
                    next.reached += 1;
                    next.remaining = next
                        .remaining
                        .checked_sub(
                            (self.adjacency.offsets[i + 1] - self.adjacency.offsets[i]) as u64,
                        )
                        .ok_or("BFS remaining edge underflow")?;
                }
            }
            levels = levels.checked_add(1).ok_or("BFS level overflow")?;
            candidate = Some(Arc::new(next));
        }
        // Allocate the next barrier before publishing any graph or labels.
        let next = StatisticsInbox::new(self.operation.partitions, &self.resources)?;
        let next_phase = self.next_phase.checked_add(1).ok_or("BFS phase overflow")?;
        if let Some(values) = candidate {
            self.values = values;
        }
        self.incoming = incoming;
        self.levels = levels;
        self.next_phase = next_phase;
        self.completed = inbox.mode;
        self.work = work;
        self.state = State::Statistics(next);
        Ok(())
    }
    fn build_incoming(&self, edges: Option<EdgeBuffer>) -> Result<Arc<Incoming>> {
        let mut edges = match edges {
            Some(edges) => edges,
            None => EdgeBuffer::new(&self.resources)?,
        };
        let count = edges.values.len();
        self.resources
            .execution
            .charge_work(
                count
                    .checked_mul(count.max(1).ilog2() as usize + 1)
                    .ok_or("BFS incoming sort work overflow")?,
            )
            .map_err(|e| e.to_string())?;
        edges.values.sort_unstable();
        let adjacency = Adjacency::build(
            &self.operation,
            self.partition,
            &self.adjacency.vertices,
            &edges.values,
            &self.resources,
        )?;
        let admission = self
            .resources
            .execution
            .reserve(
                count
                    .checked_mul(8)
                    .and_then(|n| n.checked_add(128))
                    .ok_or("BFS ghost admission overflow")?,
            )
            .map_err(|e| e.to_string())?;
        let mut ghosts = reserve_vec(count)?;
        ghosts.extend(adjacency.targets.iter().copied());
        ghosts.sort_unstable();
        ghosts.dedup();
        Ok(Arc::new(Incoming {
            adjacency,
            ghosts,
            _admission: admission,
        }))
    }
}
