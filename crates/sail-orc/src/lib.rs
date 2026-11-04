//! ORC read and write for Sail, as an optional crate.
//!
//! The host enables it with the `orc` cargo feature (off by default); without
//! it, a request for the `orc` format is refused by name. The format's options
//! are generated from `data/options/*.yaml` like the built-in formats', and
//! the option and error vocabularies are the shared ones of `sail-data-source`.

pub mod error {
    pub use sail_data_source::error::*;
}

pub mod options {
    pub use sail_data_source::options::{BuildPartialOptions, PartialOptions, ResolveOptions};

    /// Test helper: one option layer from literal pairs (the same shape the
    /// shared data-source tests use).
    #[cfg(test)]
    pub(crate) fn option_list(
        items: &[(&str, &str)],
    ) -> sail_common_datafusion::datasource::OptionLayer {
        sail_common_datafusion::datasource::OptionLayer::OptionList {
            items: items
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        }
    }

    pub mod parsers {
        pub use sail_data_source::options::parsers::*;
    }

    pub mod r#gen {
        include!(concat!(env!("OUT_DIR"), "/options/orc.rs"));
        include!(concat!(env!("OUT_DIR"), "/options/orc_write.rs"));
        include!(concat!(env!("OUT_DIR"), "/options/orc_listing.rs"));
    }
}

mod factory;
mod listing_options;
mod metadata;
mod read;
mod reader;
mod requested_schema;
mod resource_limits;
mod schema;
mod sink;
mod source;
mod timezone;
mod write_options;
mod write_schema;

pub use factory::{OrcDataSource, OrcFormatFactory, OrcWriteFormat};
pub use listing_options::{OrcListingSessionDefaults, OrcReadSessionModes};
pub use metadata::OrcSchemaBridge;
pub use read::OrcReadFormat;
pub use reader::OrcObjectReader;
pub use requested_schema::OrcRequestedSchemaAdapter;
pub use resource_limits::OrcReadResourceLimits;
pub use schema::infer_schema as infer_orc_schema;
pub use sink::{OrcSink, OrcWriterOptions};
pub use source::OrcSource;
pub use timezone::OrcJavaTimeZoneConfig;
pub use write_options::{OrcWriteCompression, OrcWriteConfig, OrcWriteSessionDefaults};
pub use write_schema::OrcWriteSchema;

#[cfg(test)]
mod fixture_tests;

#[cfg(test)]
mod timestamp_fixture_tests;
