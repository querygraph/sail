//! Bounded streaming input for declarative pipelines, following the design
//! review's repeatable-range contract (SDP-FABLE-DESIGN-REVIEW §5.2): a source
//! adapter describes partitions with immutable incarnations and opaque
//! cursors, the frontier is captured once per run and read back from the run
//! record afterwards, each batch is an intent of explicit `[start, end)`
//! ranges that the adapter planned, and a scan yields exactly that range or
//! fails closed. Nothing here runs a flow, writes a sink, persists a record
//! or talks to a broker; the in-process [`test_source::DeterministicSource`]
//! exists so Spark assertions can run without one.
#![cfg_attr(
    not(test),
    expect(dead_code, reason = "wired by the streaming admission unit")
)]

pub(super) mod admission;
pub(super) mod binding;
pub(super) mod intent;
pub(super) mod lower;
pub(super) mod source;
pub(super) mod test_source;

#[cfg(test)]
mod tests;
