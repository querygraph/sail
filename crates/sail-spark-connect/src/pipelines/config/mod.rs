//! Flow configuration phases for declarative pipelines.
//!
//! `merge` is pure std-only string-map arithmetic. `trace` discovers ordered
//! graph inputs; `resolve` invokes the native planner and performs the
//! success-only merge and conditional second call. Provider ownership and
//! final command execution remain the callers' responsibility.

pub mod merge;
pub(super) mod resolve;
mod trace;

#[cfg(test)]
mod runtime_tests;
