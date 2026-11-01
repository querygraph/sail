//! The multipart ownership adapter lives below the data sources that use it.

pub use sail_common_datafusion::owned_multipart::*;

#[cfg(test)]
#[path = "owned_multipart_tests.rs"]
mod tests;
