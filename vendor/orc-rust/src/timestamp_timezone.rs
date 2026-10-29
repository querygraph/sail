// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Opt-in writer-to-reader timestamp conversion using caller-supplied timezone rules.

use std::{fmt::Debug, sync::Arc};

use crate::{
    error::{OutOfSpecSnafu, Result},
    schema::DataType,
};

/// An immutable timezone used to interpret ordinary ORC `TIMESTAMP` values.
///
/// Implementations must return consistent results for the lifetime of the reader.
/// The caller owns timezone database selection, versioning, and name semantics.
/// These rules are independent of the Arrow timestamp's display timezone.
pub trait TimestampTimeZone: Debug + Send + Sync {
    /// Unix seconds at local midnight on 2015-01-01, the ORC timestamp epoch.
    ///
    /// Compute this with the same rules as [`Self::offset_millis`]. Supplying the
    /// epoch explicitly avoids imposing a local-time gap/fold policy on callers.
    fn orc_epoch_seconds(&self) -> i64;

    /// Offset east of UTC, in milliseconds, at a UTC epoch millisecond.
    ///
    /// Return an error when the requested instant is unsupported; do not substitute
    /// a different timezone or narrow the timestamp to a smaller calendar range.
    fn offset_millis(&self, utc_millis: i64) -> Result<i32>;
}

/// Resolves nonempty, unmodified writer timezone names from ORC stripe metadata.
pub trait TimestampTimeZoneResolver: Debug + Send + Sync {
    /// Return immutable rules for this name, or an explicit error.
    ///
    /// Alias, custom-offset, and unknown-name policies belong to the resolver.
    /// No resolver is called for absent or empty names: those retain the reader's
    /// current writer timezone, initially the configured reader timezone.
    fn resolve(&self, name: &str) -> Result<Arc<dyn TimestampTimeZone>>;
}

/// Optional timezone conversion, independent of fractional precision handling.
///
/// Ordinary timestamps use the Java ORC two-offset conversion, preserving the
/// original instant in same-zone DST folds. `TIMESTAMP_INSTANT` is unaffected.
/// Inferred ordinary timestamp fields are labeled UTC; an explicitly supplied
/// Arrow timestamp timezone changes only the output label.
/// Only Arrow `Timestamp` outputs are supported in this mode; the default
/// reader's `Decimal128` timestamp override is not converted.
///
/// No timezone database is bundled or consulted by this configuration. Exact
/// compatibility with another reader requires supplying that reader's rules.
/// Offset queries and intermediate candidates must fit signed 64-bit epoch
/// milliseconds; final results must fit the requested Arrow unit. This covers
/// the entire microsecond input range without imposing a calendar-year limit.
#[derive(Debug, Clone)]
pub struct TimestampTimeZoneOptions {
    reader: Arc<dyn TimestampTimeZone>,
    resolver: Arc<dyn TimestampTimeZoneResolver>,
}

impl TimestampTimeZoneOptions {
    /// Configure immutable reader rules and a resolver for stripe writer names.
    pub fn new(
        reader: Arc<dyn TimestampTimeZone>,
        resolver: Arc<dyn TimestampTimeZoneResolver>,
    ) -> Self {
        Self { reader, resolver }
    }
}

/// State follows stripes actually started by one reader, including range starts.
#[derive(Debug)]
pub(crate) struct TimestampTimeZoneState {
    options: TimestampTimeZoneOptions,
    writer: Arc<dyn TimestampTimeZone>,
}

impl TimestampTimeZoneState {
    pub(crate) fn new(options: TimestampTimeZoneOptions) -> Self {
        Self {
            writer: options.reader.clone(),
            options,
        }
    }

    pub(crate) fn start_stripe(
        &mut self,
        name: Option<&str>,
    ) -> Result<TimestampTimeZoneConversion> {
        if let Some(name) = name.filter(|name| !name.is_empty()) {
            self.writer = self.options.resolver.resolve(name)?;
        }
        Ok(TimestampTimeZoneConversion {
            writer: self.writer.clone(),
            reader: self.options.reader.clone(),
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TimestampTimeZoneConversion {
    writer: Arc<dyn TimestampTimeZone>,
    reader: Arc<dyn TimestampTimeZone>,
}

impl TimestampTimeZoneConversion {
    pub(crate) fn orc_epoch_seconds(&self) -> i64 {
        self.writer.orc_epoch_seconds()
    }

    pub(crate) fn convert(&self, nanos: i128) -> Result<i128> {
        if Arc::ptr_eq(&self.writer, &self.reader) {
            return Ok(nanos);
        }
        let millis = i64::try_from(nanos.div_euclid(1_000_000))
            .map_err(|_| invalid("timestamp exceeds timezone millisecond range"))?;
        let writer_offset = i128::from(self.writer.offset_millis(millis)?);
        let reader_offset = i128::from(self.reader.offset_millis(millis)?);
        let candidate = i64::try_from(i128::from(millis) + writer_offset - reader_offset)
            .map_err(|_| invalid("timezone candidate exceeds millisecond range"))?;
        let adjustment = writer_offset - i128::from(self.reader.offset_millis(candidate)?);
        nanos
            .checked_add(adjustment * 1_000_000)
            .ok_or_else(|| invalid("timezone adjustment exceeds timestamp range"))
    }
}

pub(crate) fn contains_timestamp(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamp { .. } => true,
        DataType::Struct { children, .. } => children
            .iter()
            .any(|child| contains_timestamp(child.data_type())),
        DataType::List { child, .. } => contains_timestamp(child),
        DataType::Map { key, value, .. } => contains_timestamp(key) || contains_timestamp(value),
        DataType::Union { variants, .. } => variants.iter().any(contains_timestamp),
        _ => false,
    }
}

fn invalid(message: &str) -> crate::error::OrcError {
    OutOfSpecSnafu {
        msg: message.to_owned(),
    }
    .build()
}

#[cfg(test)]
#[path = "timestamp_timezone_tests.rs"]
mod tests;
