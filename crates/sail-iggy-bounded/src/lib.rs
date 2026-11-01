//! Apache Iggy as a [`BoundedSource`](sail_common_datafusion::streaming::bounded::BoundedSource):
//! one stream and topic read as exact offset ranges.
//!
//! The rules come from the behaviour recorded on the pinned broker: a poll
//! past retained data silently resumes at the first retained offset, and a
//! recreated partition reuses its id under a new incarnation with offsets
//! restarting at zero. Neither is an error at the broker, so every read here
//! checks the offsets and the incarnation it got back.
//!
//! Reads never acknowledge. Automatic commits stay off and no consumer
//! position is stored, so a failed read can be issued again unchanged, and a
//! read leaves nothing that would pin the broker's retention.

mod broker;
mod config;
mod data_source;
mod decode;
mod envelope;
mod range;
mod source;

pub use config::{IggyBoundedConfig, PayloadFormat};
pub use data_source::{FORMAT, IggyDataSource, declared_schema};
pub use decode::raw_schema;
pub use envelope::envelope_schema;
pub use source::{IggyBoundedSource, KIND};
