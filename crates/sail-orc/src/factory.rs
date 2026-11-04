use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::sink::DataSinkExec;
use sail_common::error::CommonResult;
use sail_common::utils::datetime::get_system_timezone;
use sail_common_datafusion::datasource::OptionLayer;
use sail_common_datafusion::utils::datetime::arrow_timezone_label;
use sail_data_source::listing::source::{
    FormatFactory, ListingDataSource, ListingSinkInput, WriteFormat,
};

use super::{OrcJavaTimeZoneConfig, OrcReadFormat, OrcSink, OrcWriteConfig, OrcWriterOptions};
use crate::options::ResolveOptions;
use crate::options::r#gen::{OrcListingReadOptions, OrcReadOptions};

pub type OrcDataSource = ListingDataSource<OrcFormatFactory>;

#[derive(Debug, Default)]
pub struct OrcFormatFactory;

impl FormatFactory for OrcFormatFactory {
    type Read = OrcReadFormat;
    type Write = OrcWriteFormat;

    fn name() -> &'static str {
        "orc"
    }

    fn read(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Read> {
        read_with_timezone(ctx, options, get_system_timezone)
    }

    fn write(ctx: &dyn Session, options: Vec<OptionLayer>) -> Result<Self::Write> {
        Ok(OrcWriteFormat {
            config: OrcWriteConfig::resolve(ctx, options)?,
        })
    }
}

fn read_with_timezone(
    ctx: &dyn Session,
    options: Vec<OptionLayer>,
    timezone: impl FnOnce() -> CommonResult<String>,
) -> Result<OrcReadFormat> {
    let resource_options = OrcReadOptions::resolve(ctx, options.clone())?;
    let listing_options = OrcListingReadOptions::resolve(ctx, options)?;
    // This is Sail's host IANA-zone capture policy, not a JVM-default lookup.
    // Neither later session display changes nor workers recapture this value.
    let reader_timezone = OrcJavaTimeZoneConfig::for_reader_timezone(
        timezone().map_err(|error| DataFusionError::External(Box::new(error)))?,
    )?;
    let session_timezone = arrow_timezone_label(
        ctx.config_options()
            .execution
            .time_zone
            .as_deref()
            .unwrap_or("UTC"),
    )
    .into_owned();
    Ok(OrcReadFormat {
        options: resource_options,
        listing_options,
        reader_timezone,
        session_timezone,
    })
}

#[derive(Debug)]
pub struct OrcWriteFormat {
    config: OrcWriteConfig,
}

#[async_trait]
impl WriteFormat for OrcWriteFormat {
    async fn sink(
        &self,
        _ctx: &dyn Session,
        mut input: ListingSinkInput,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        input.sink.file_extension = "orc".to_string();
        let sink = OrcSink::try_new(
            input.sink,
            OrcWriterOptions {
                compression: self.config.compression,
            },
        )?;
        Ok(Arc::new(DataSinkExec::new(
            input.input,
            Arc::new(sink),
            input.sort_order,
        )))
    }
}

#[cfg(test)]
#[path = "factory_tests.rs"]
mod tests;
