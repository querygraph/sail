//! The pure control model of `CDC-CONTROL-STORE-DESIGN.md`: identities and
//! fence tokens, the durable records, the guarded transitions with their
//! linearization points, canonical request digests and typed outcomes.
//!
//! [`transitions`] are pure and decide. [`control`] holds the operations, one
//! transaction each over the [`store`] seam, so every backend shares one code
//! path. [`memory`] is an in-process store for model tests that is explicitly
//! not an authority; `turso_store` is the authority adapter behind the
//! `turso` feature.

pub mod control;
pub mod digest;
pub mod identity;
pub mod memory;
pub mod records;
pub mod store;
pub mod transitions;
#[cfg(feature = "turso")]
pub mod turso_store;

#[cfg(test)]
mod tests;
