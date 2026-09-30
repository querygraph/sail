//! Candidate publication occurs only after complete producer markers and EOF.
use super::*;
#[derive(Debug)]
pub(super) struct Inbox {
    mode: SsspMode,
    bucket: Option<f64>,
    expected: Vec<SsspStatisticsValues>,
    sequences: Vec<u64>,
    finished: Vec<bool>,
    candidates: Vec<Option<SsspLabel>>,
    pub emitted: Arc<OnceLock<SsspWork>>,
    received: u64,
    _admission: MemoryReservation,
}
impl Inbox {
    pub fn new(
        mode: SsspMode,
        bucket: Option<f64>,
        reports: &[Option<SsspStatisticsValues>],
        n: usize,
        r: &Resources,
    ) -> Result<Self> {
        // A converged phase accepts completions only. Keep producer validation,
        // but do not allocate a vertex-sized candidate array for an empty relay.
        let candidates = if mode == SsspMode::Done { 0 } else { n };
        let bytes = candidates
            .checked_mul(size_of::<Option<SsspLabel>>())
            .and_then(|x| {
                x.checked_add(reports.len() * (size_of::<SsspStatisticsValues>() + 16) + 512)
            })
            .ok_or("SSSP inbox admission overflow")?;
        let admission = r.execution.reserve(bytes).map_err(|e| e.to_string())?;
        let mut expected = reserve_vec(reports.len())?;
        for r in reports {
            expected.push(r.ok_or("incomplete SSSP reports")?);
        }
        Ok(Self {
            mode,
            bucket,
            expected,
            sequences: filled(reports.len(), 0)?,
            finished: filled(reports.len(), false)?,
            candidates: filled(candidates, None)?,
            emitted: Arc::new(OnceLock::new()),
            received: 0,
            _admission: admission,
        })
    }
}
impl SsspPartition {
    pub fn receive(&mut self, m: &SsspMessage) -> Result<()> {
        self.receive_values(&m.phase, m.values)
    }
    pub fn receive_values(&mut self, phase: &Round, m: SsspMessageValues) -> Result<()> {
        self.check_phase(phase)?;
        let result = self.receive_inner(m);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }
    fn receive_inner(&mut self, m: SsspMessageValues) -> Result<()> {
        let State::Receiving(inbox) = &mut self.state else {
            return Err("SSSP not receiving messages".into());
        };
        let p = m.producer;
        if p >= self.operation.partitions
            || m.recipient != self.partition
            || self.origins[p] != Some(m.origin)
            || m.mode != inbox.mode
            || m.bucket != inbox.bucket
            || inbox.finished[p]
            || inbox.sequences[p] != m.sequence
        {
            return Err("foreign, replayed, misrouted or late SSSP message".into());
        }
        self.resources
            .execution
            .charge_work(1 + self.adjacency.vertices().len().max(1).ilog2() as usize)
            .map_err(|e| e.to_string())?;
        match m.payload {
            SsspPayload::Topology { source, target } => {
                if inbox.mode != SsspMode::Topology || self.operation.owner(source) != p {
                    return Err("invalid SSSP topology source or mode".into());
                }
                self.adjacency
                    .vertices()
                    .binary_search(&target)
                    .map_err(|_| "unknown SSSP destination")?;
            }
            SsspPayload::Candidate { target, label } => {
                if !matches!(inbox.mode, SsspMode::Reference | SsspMode::DeltaStar)
                    || label.hops() == 0
                    || self.operation.owner(label.parent()) != p
                {
                    return Err("invalid SSSP candidate source or mode".into());
                }
                let i = self
                    .adjacency
                    .vertices()
                    .binary_search(&target)
                    .map_err(|_| "unknown SSSP destination")?;
                if inbox.candidates[i].is_none_or(|old| label.precedes(old)) {
                    inbox.candidates[i] = Some(label);
                }
            }
        }
        inbox.sequences[p] = add(m.sequence, 1)?;
        inbox.received = add(inbox.received, 1)?;
        Ok(())
    }
    pub fn finish_producer(&mut self, c: &SsspCompletion) -> Result<()> {
        if c.sequences.len() != self.operation.partitions {
            return self.fail("invalid SSSP completion width");
        }
        let total = match c.sequences.iter().try_fold(0u64, |a, &b| add(a, b)) {
            Ok(v) => v,
            Err(e) => return self.fail(e),
        };
        self.finish_producer_values(
            &c.phase,
            SsspCompletionValues {
                origin: c.origin,
                producer: c.producer,
                mode: c.mode,
                bucket: c.bucket,
                sequence: c.sequences[self.partition],
                total_messages: total,
            },
        )
    }
    pub fn finish_producer_values(&mut self, phase: &Round, c: SsspCompletionValues) -> Result<()> {
        self.check_phase(phase)?;
        let result = self.finish_producer_inner(c);
        if result.is_err() {
            self.state = State::Failed;
        }
        result
    }
    fn finish_producer_inner(&mut self, c: SsspCompletionValues) -> Result<()> {
        let State::Receiving(inbox) = &mut self.state else {
            return Err("SSSP not receiving completions".into());
        };
        let p = c.producer;
        if p >= self.operation.partitions
            || self.origins[p] != Some(c.origin)
            || c.mode != inbox.mode
            || c.bucket != inbox.bucket
            || inbox.finished[p]
            || c.sequence != inbox.sequences[p]
            || c.sequence > c.total_messages
        {
            return Err("incomplete, foreign or replayed SSSP completion".into());
        }
        let s = inbox.expected[p];
        let expected = match inbox.mode {
            SsspMode::Topology => s.arcs,
            SsspMode::Reference => s.reachable_edges,
            SsspMode::DeltaStar => {
                if s.bucket == inbox.bucket {
                    s.bucket_edges
                } else {
                    0
                }
            }
            SsspMode::Done => 0,
        };
        if expected != c.total_messages {
            return Err("SSSP completion differs from producer statistics".into());
        }
        inbox.finished[p] = true;
        Ok(())
    }
    /// Call after EOF, not merely after the last completion marker.
    pub fn finish(&mut self, phase: &Round) -> Result<()> {
        self.check_phase(phase)?;
        let State::Receiving(inbox) = std::mem::replace(&mut self.state, State::Failed) else {
            return Err("SSSP not receiving updates".into());
        };
        if !inbox.finished.iter().all(|x| *x) {
            return Err("incomplete SSSP producer barrier".into());
        }
        let mut work = *inbox
            .emitted
            .get()
            .ok_or("local SSSP emission not complete")?;
        work.received_messages = inbox.received;
        if inbox.mode == SsspMode::Done {
            // All producer markers and local emission EOF were checked above.
            // Preserve the immutable labels and round count, admitting the next
            // barrier before publishing any phase change, just as active rounds do.
            let state = State::Statistics(StatisticsInbox::new(
                self.operation.partitions,
                &self.resources,
            )?);
            let next_phase = add(self.next_phase, 1)?;
            self.resources
                .execution
                .checkpoint()
                .map_err(|e| e.to_string())?;
            self.completed = SsspMode::Done;
            self.work = work;
            self.next_phase = next_phase;
            self.state = state;
            return Ok(());
        }
        // Charge replacement labels and a pending mask before copying/mutation.
        let n = self.values.labels.len();
        let pending_admission = self
            .resources
            .execution
            .reserve(n + 128)
            .map_err(|e| e.to_string())?;
        let mut pending = filled(n, false)?;
        let mut next = self.values.copy(&self.resources)?;
        for &i in &self.values.active {
            self.resources
                .execution
                .charge_work(1)
                .map_err(|e| e.to_string())?;
            pending[i] = match inbox.mode {
                SsspMode::Topology => true,
                SsspMode::DeltaStar => {
                    self.values.labels[i]
                        .ok_or("active label missing")?
                        .bucket(self.options.delta)?
                        != inbox.bucket.ok_or("missing selected bucket")?
                }
                _ => false,
            };
        }
        for (i, candidate) in inbox.candidates.iter().enumerate() {
            self.resources
                .execution
                .charge_work(1)
                .map_err(|e| e.to_string())?;
            if let Some(candidate) = candidate
                && next.labels[i].is_none_or(|old| candidate.precedes(old))
            {
                if next.labels[i].is_none() {
                    next.reached = add(next.reached, 1)?;
                    next.reachable_edges = add(
                        next.reachable_edges,
                        self.adjacency.outgoing_at(i).len() as u64,
                    )?;
                    pending[i] = true;
                } else if candidate.changes_outgoing(next.labels[i].unwrap()) {
                    pending[i] = true;
                }
                next.labels[i] = Some(*candidate);
            }
            if pending[i] {
                next.active.push(i);
            }
        }
        drop(pending);
        drop(pending_admission);
        let rounds = if matches!(inbox.mode, SsspMode::Reference | SsspMode::DeltaStar) {
            add(self.rounds, 1)?
        } else {
            self.rounds
        };
        let state = State::Statistics(StatisticsInbox::new(
            self.operation.partitions,
            &self.resources,
        )?);
        let next_phase = add(self.next_phase, 1)?;
        self.resources
            .execution
            .checkpoint()
            .map_err(|e| e.to_string())?;
        self.values = Arc::new(next);
        self.rounds = rounds;
        self.completed = inbox.mode;
        self.work = work;
        self.next_phase = next_phase;
        self.state = state;
        Ok(())
    }
}
