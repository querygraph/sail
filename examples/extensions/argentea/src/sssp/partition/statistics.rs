//! Complete global active-bucket and termination decision; no float sums.
use super::*;
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SsspStatisticsValues {
    pub options: SsspOptions,
    pub origin: SsspOrigin,
    pub completed: SsspMode,
    pub rounds: u64,
    pub vertices: u64,
    pub arcs: u64,
    pub source_count: u64,
    pub reached: u64,
    pub reachable_edges: u64,
    pub active: u64,
    pub bucket: Option<f64>,
    pub bucket_edges: u64,
}
#[derive(Clone, Debug)]
pub struct SsspStatistics {
    pub phase: Arc<Round>,
    pub producer: usize,
    pub values: SsspStatisticsValues,
    _ownership: Arc<Ownership>,
}
#[derive(Debug)]
pub(super) struct StatisticsInbox {
    pub slots: Vec<Option<SsspStatisticsValues>>,
    closed: bool,
    _admission: MemoryReservation,
}
impl StatisticsInbox {
    pub fn new(p: usize, r: &Resources) -> Result<Self> {
        let admission = r
            .execution
            .reserve(p * size_of::<Option<SsspStatisticsValues>>() + 128)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            slots: filled(p, None)?,
            closed: false,
            _admission: admission,
        })
    }
}
#[derive(Default)]
pub(super) struct Totals {
    pub vertices: u64,
    pub sources: u64,
    pub reached: u64,
    pub active: u64,
    pub bucket: Option<f64>,
}
impl SsspPartition {
    fn local_statistics(&self) -> Result<SsspStatisticsValues> {
        let mut bucket = None;
        let mut bucket_edges = 0;
        if self.options.algorithm == SsspAlgorithm::DeltaStar {
            for &i in &self.values.active {
                self.resources
                    .execution
                    .charge_work(1)
                    .map_err(|e| e.to_string())?;
                let b = self.values.labels[i]
                    .ok_or("active SSSP vertex lacks a label")?
                    .bucket(self.options.delta)?;
                let degree = self.adjacency.outgoing_at(i).len() as u64;
                if bucket.is_none_or(|old| b < old) {
                    bucket = Some(b);
                    bucket_edges = degree;
                } else if bucket == Some(b) {
                    bucket_edges = add(bucket_edges, degree)?;
                }
            }
        }
        Ok(SsspStatisticsValues {
            options: self.options,
            origin: self.origin,
            completed: self.completed,
            rounds: self.rounds,
            vertices: self.adjacency.vertices().len() as u64,
            arcs: self.adjacency.arc_count() as u64,
            // Source membership is immutable and checked against local storage
            // before the first own report pins this shape. Later barriers,
            // including Done relays, need not search the graph again.
            source_count: self.shapes[self.partition].map_or_else(
                || {
                    u64::from(
                        self.adjacency
                            .vertices()
                            .binary_search(&self.options.source)
                            .is_ok(),
                    )
                },
                |(_, _, sources)| sources,
            ),
            reached: self.values.reached,
            reachable_edges: self.values.reachable_edges,
            active: self.values.active.len() as u64,
            bucket,
            bucket_edges,
        })
    }
    pub fn statistics(&self) -> Result<SsspStatistics> {
        if !matches!(self.state, State::Statistics(_)) {
            return Err("SSSP is not collecting statistics".into());
        }
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        Ok(SsspStatistics {
            phase: Arc::new(Round {
                operation: self.operation.clone(),
                number: self.next_phase,
            }),
            producer: self.partition,
            values: self.local_statistics()?,
            _ownership: Ownership::new(&self.resources, 2048)?,
        })
    }
    pub fn receive_statistics(&mut self, r: &SsspStatistics) -> Result<()> {
        self.receive_statistics_values(&r.phase, r.producer, r.values)
    }
    pub fn receive_statistics_values(
        &mut self,
        phase: &Round,
        producer: usize,
        r: SsspStatisticsValues,
    ) -> Result<()> {
        self.check_phase(phase)?;
        if producer >= self.operation.partitions
            || r.options != self.options
            || r.completed != self.completed
            || r.rounds != self.rounds
            || r.origin.adjacency_id == 0
            || (r.vertices == 0 && r.arcs != 0)
            || (r.reached == 0 && r.reachable_edges != 0)
            || r.vertices > self.operation.vertices
            || r.reached > r.vertices
            || r.active > r.reached
            || r.source_count > 1
            || r.source_count > r.reached
            || r.reachable_edges > r.arcs
            || r.bucket_edges > r.reachable_edges
            || r.bucket
                .is_some_and(|b| !b.is_finite() || b < 0.0 || b.fract() != 0.0)
            || (self.options.algorithm == SsspAlgorithm::DeltaStar
                && (r.active > 0) != r.bucket.is_some())
            || (self.options.algorithm == SsspAlgorithm::Reference
                && (r.bucket.is_some() || r.bucket_edges != 0))
            || (r.active == 0 && r.bucket_edges != 0)
        {
            return self.fail("invalid SSSP statistics");
        }
        if self.origins[producer].is_some_and(|v| v != r.origin) {
            return self.fail("SSSP producer origin changed");
        }
        let shape = (r.vertices, r.arcs, r.source_count);
        if self.shapes[producer].is_some_and(|v| v != shape) {
            return self.fail("SSSP partition shape changed");
        }
        if producer == self.partition && r != self.local_statistics()? {
            return self.fail("SSSP own report differs from local state");
        }
        let State::Statistics(inbox) = &mut self.state else {
            return self.fail("SSSP not collecting statistics");
        };
        if inbox.closed || inbox.slots[producer].is_some() {
            return self.fail("replayed or late SSSP statistics");
        }
        self.origins[producer] = Some(r.origin);
        self.shapes[producer] = Some(shape);
        inbox.slots[producer] = Some(r);
        Ok(())
    }
    pub fn finish_statistics(&mut self, phase: &Round) -> Result<()> {
        self.check_phase(phase)?;
        let State::Statistics(inbox) = &mut self.state else {
            return self.fail("SSSP not collecting statistics");
        };
        if inbox.closed || inbox.slots.iter().any(Option::is_none) {
            return self.fail("incomplete or repeated SSSP statistics EOF");
        }
        inbox.closed = true;
        if let Err(e) = self.global_statistics() {
            return self.fail(e);
        }
        Ok(())
    }
    pub(super) fn global_statistics(&self) -> Result<Totals> {
        let State::Statistics(inbox) = &self.state else {
            return Err("SSSP not collecting statistics".into());
        };
        if !inbox.closed {
            return Err("SSSP statistics input has not reached EOF".into());
        }
        let mut totals = Totals::default();
        for r in &inbox.slots {
            self.resources
                .execution
                .charge_work(1)
                .map_err(|e| e.to_string())?;
            let r = r.ok_or("missing SSSP statistics producer")?;
            totals.vertices = add(totals.vertices, r.vertices)?;
            totals.sources = add(totals.sources, r.source_count)?;
            totals.reached = add(totals.reached, r.reached)?;
            totals.active = add(totals.active, r.active)?;
            if let Some(b) = r.bucket {
                totals.bucket = Some(totals.bucket.map_or(b, |old| old.min(b)));
            }
        }
        if totals.vertices != self.operation.vertices || totals.sources != 1 {
            return Err("SSSP requires complete vertices and exactly one source".into());
        }
        Ok(totals)
    }
}
