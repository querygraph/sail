//! The pure control model of `CDC-CONTROL-STORE-DESIGN.md`: identities and
//! fence tokens, the durable records, the guarded transitions with their
//! linearization points, canonical request digests and typed outcomes.
//!
//! Nothing here performs I/O. A store adapter owns one database transaction
//! per operation, loads the guard row, applies a transition from
//! [`transitions`] and persists the resulting records atomically; the
//! transition decides, the adapter only serialises. [`memory`] is an
//! in-process store for model tests that is explicitly not an authority.

pub mod digest;
pub mod identity;
pub mod memory;
pub mod records;
pub mod transitions;

#[cfg(test)]
mod tests;
