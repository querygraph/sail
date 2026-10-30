//! Pure flow-configuration arithmetic for Spark 4.2.1 Declarative Pipelines
//! parity: the first-pass private configuration a flow is analyzed under, and
//! the post-success merge of a flow's declared configuration with the accepted
//! configurations of the graph views it read.
//!
//! Everything here is std-only and operates on raw SQL-conf strings. Binding
//! order, name display, table exclusion, resolver wiring and the conversion of
//! a conflict into a Spark error belong to the callers.

pub mod merge;
