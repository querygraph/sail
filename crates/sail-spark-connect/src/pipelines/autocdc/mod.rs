//! Spark 4.2.1 Auto CDC support for declarative pipelines.
//!
//! The modules here port `sql/pipelines/.../autocdc` and the AutoCDC parts of
//! `graph/FlowExecution.scala` and `graph/DatasetManager.scala` at Spark pin
//! `617618396`. Nothing in this module executes a flow: the auxiliary state
//! table is created by the SCD1 write path, which is a later unit, and the
//! only effect wired today is the unconditional drop of that table on a full
//! refresh (`DatasetManager.scala:306–318`), issued from
//! [`super::materialize`].

pub(super) mod auxiliary;

/// Common reserved-name prefix shared by AutoCDC internal columns and
/// internal tables (`AutoCdcReservedNames.prefix`).
pub(super) const RESERVED_PREFIX: &str = "__spark_autocdc_";
