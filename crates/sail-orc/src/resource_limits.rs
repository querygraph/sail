use datafusion::catalog::Session;
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool};
use orc_rust::ArrowReaderBuilder;
use orc_rust::reader::metadata::MetadataReadOptions;
use sail_common_datafusion::datasource::OptionLayer;

use crate::error::DataSourceResult;
use crate::options::r#gen::{OrcReadOptions, OrcReadPartialOptions};
use crate::options::{BuildPartialOptions, PartialOptions, ResolveOptions};

impl ResolveOptions for OrcReadOptions {
    fn resolve(_ctx: &dyn Session, options: Vec<OptionLayer>) -> DataSourceResult<Self> {
        let mut partial = OrcReadPartialOptions::initialize();
        for layer in options {
            partial.merge(layer.build_partial_options()?);
        }
        partial.finalize()
    }
}

/// Effective metadata limits bound to a reader's local runtime pool.
///
/// Transport the configured [`OrcReadOptions`] fields and derive these limits
/// again on each worker. These limits do not reserve memory or bound total heap
/// use: encoded range admission belongs to [`super::OrcObjectReader`], while
/// native protobuf objects, codec scratch and decoded arrays have other lifetimes.
#[derive(Debug, Clone, Copy)]
pub struct OrcReadResourceLimits {
    metadata: MetadataReadOptions,
    max_decoded_stripe_footer_bytes: usize,
}

impl OrcReadOptions {
    /// Derive byte ceilings from the configured values and the local pool's
    /// total finite limit. Current reservations and partition counts do not
    /// change this policy; the pool separately admits live encoded buffers.
    pub fn resource_limits(&self, pool: &dyn MemoryPool) -> OrcReadResourceLimits {
        let addressable = isize::MAX as usize;
        let byte_cap = match pool.memory_limit() {
            MemoryLimit::Finite(limit) => limit.min(addressable),
            MemoryLimit::Unknown | MemoryLimit::Infinite => addressable,
        };
        OrcReadResourceLimits {
            metadata: MetadataReadOptions {
                max_encoded_metadata_bytes: Some(self.max_encoded_metadata_bytes.min(byte_cap)),
                max_decoded_metadata_bytes: Some(self.max_decoded_metadata_bytes.min(byte_cap)),
                max_schema_depth: Some(self.max_schema_depth),
            },
            max_decoded_stripe_footer_bytes: self.max_decoded_stripe_footer_bytes.min(byte_cap),
        }
    }
}

impl OrcReadResourceLimits {
    /// File-tail limits to apply before converting or projecting its type tree.
    /// An explicit zero remains `Some(0)`, rather than the native unrestricted default.
    pub fn metadata_options(&self) -> MetadataReadOptions {
        self.metadata
    }

    pub fn max_decoded_stripe_footer_bytes(&self) -> usize {
        self.max_decoded_stripe_footer_bytes
    }

    /// Bound each started stripe footer without replacing the builder's schema,
    /// projection, timestamp policy or other execution controls.
    pub fn configure_reader<R>(&self, builder: ArrowReaderBuilder<R>) -> ArrowReaderBuilder<R> {
        builder.with_max_decoded_stripe_footer_bytes(self.max_decoded_stripe_footer_bytes)
    }
}

#[cfg(test)]
#[path = "options_tests.rs"]
mod tests;
