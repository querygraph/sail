use std::sync::Arc;

use datafusion::common::Result;
use sail_common_datafusion::datasource::DataSourceRegistry;
use sail_data_source::formats::arrow::ArrowDataSource;
use sail_data_source::formats::avro::AvroDataSource;
use sail_data_source::formats::binary::BinaryDataSource;
use sail_data_source::formats::console::ConsoleDataSource;
use sail_data_source::formats::csv::CsvDataSource;
use sail_data_source::formats::json::JsonDataSource;
use sail_data_source::formats::noop::NoopDataSource;
use sail_data_source::formats::parquet::ParquetDataSource;
use sail_data_source::formats::python::{PythonDataSourceAdapter, discover_data_sources};
use sail_data_source::formats::rate::RateDataSource;
use sail_data_source::formats::socket::SocketDataSource;
use sail_data_source::formats::text::TextDataSource;
use sail_delta_lake::DeltaLakeSource;
use sail_iceberg::IcebergLakeSource;
#[cfg(feature = "iggy")]
use sail_iggy_bounded::IggyDataSource;
#[cfg(feature = "orc")]
use sail_orc::OrcDataSource;

pub(crate) fn create_data_source_registry() -> Result<Arc<DataSourceRegistry>> {
    let registry = Arc::new(DataSourceRegistry::new());
    register_builtin_data_sources(&registry)?;
    register_external_data_sources(&registry)?;
    Ok(registry)
}

fn register_builtin_data_sources(registry: &DataSourceRegistry) -> Result<()> {
    registry.register_data_source(Arc::new(ArrowDataSource::default()))?;
    registry.register_data_source(Arc::new(AvroDataSource::default()))?;
    registry.register_data_source(Arc::new(BinaryDataSource::default()))?;
    registry.register_data_source(Arc::new(CsvDataSource::default()))?;
    registry.register_data_source(Arc::new(JsonDataSource::default()))?;
    #[cfg(feature = "orc")]
    registry.register_data_source(Arc::new(OrcDataSource::default()))?;
    #[cfg(not(feature = "orc"))]
    registry.register_data_source(Arc::new(orc_off::OrcUnavailable))?;
    registry.register_data_source(Arc::new(ParquetDataSource::default()))?;
    registry.register_data_source(Arc::new(TextDataSource::default()))?;
    registry.register_data_source(Arc::new(SocketDataSource))?;
    registry.register_data_source(Arc::new(RateDataSource))?;
    registry.register_data_source(Arc::new(ConsoleDataSource))?;
    registry.register_data_source(Arc::new(NoopDataSource))?;
    registry.register_data_source(Arc::new(DeltaLakeSource))?;
    registry.register_data_source(Arc::new(IcebergLakeSource))?;
    #[cfg(feature = "iggy")]
    registry.register_data_source(Arc::new(IggyDataSource))?;
    Ok(())
}

fn register_external_data_sources(registry: &DataSourceRegistry) -> Result<()> {
    // Register Python data sources
    {
        discover_data_sources()?;
        PythonDataSourceAdapter::register_all(registry)?;
    }

    Ok(())
}

/// Without the `orc` feature the format is still a known name, so a request
/// for it is refused with the reason rather than as an unknown provider.
#[cfg(not(feature = "orc"))]
mod orc_off {
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::catalog::Session;
    use datafusion::common::{Result, plan_err};
    use datafusion::logical_expr::{LogicalPlan, TableSource};
    use sail_common_datafusion::datasource::{DataSource, SinkInfo, SourceInfo};

    pub(super) const ORC_FEATURE_OFF: &str =
        "the orc format needs Sail built with the `orc` feature; this build has it off";

    pub(super) struct OrcUnavailable;

    #[async_trait]
    impl DataSource for OrcUnavailable {
        fn name(&self) -> &str {
            "orc"
        }

        async fn create_source(
            &self,
            _ctx: &dyn Session,
            _info: SourceInfo,
        ) -> Result<Arc<dyn TableSource>> {
            plan_err!("{ORC_FEATURE_OFF}")
        }

        async fn create_writer(&self, _ctx: &dyn Session, _info: SinkInfo) -> Result<LogicalPlan> {
            plan_err!("{ORC_FEATURE_OFF}")
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::prelude::SessionContext;
    use sail_common_datafusion::datasource::{OptionLayer, SinkInfo, SinkMode};

    use super::*;

    #[test]
    fn separates_data_sources_from_lake_sources() -> Result<()> {
        let registry = DataSourceRegistry::new();
        register_builtin_data_sources(&registry)?;

        let names = if cfg!(feature = "orc") {
            vec!["orc", "parquet", "rate", "console"]
        } else {
            vec!["parquet", "rate", "console"]
        };
        for name in names {
            assert!(registry.get_data_source(name).is_ok());
            assert!(registry.get_lake_source(name).is_err());
        }
        for name in ["delta", "iceberg"] {
            assert!(registry.get_data_source(name).is_ok());
            assert!(registry.get_lake_source(name).is_ok());
        }
        Ok(())
    }

    #[cfg(feature = "orc")]
    #[tokio::test]
    async fn registered_orc_source_plans_writing() -> Result<()> {
        let registry = DataSourceRegistry::new();
        register_builtin_data_sources(&registry)?;
        let directory = tempfile::tempdir()?;
        let output = directory.path().join("output");
        let context = SessionContext::new();
        registry
            .get_data_source("orc")?
            .create_writer(
                &context.state(),
                SinkInfo {
                    input: LogicalPlanBuilder::empty(true).build()?,
                    mode: SinkMode::Append,
                    partition_by: vec![],
                    bucket_by: None,
                    sort_order: vec![],
                    options: vec![OptionLayer::OptionList {
                        items: vec![("path".to_string(), output.to_string_lossy().into_owned())],
                    }],
                    lakehouse_table: None,
                },
            )
            .await?;
        assert!(!output.exists());
        Ok(())
    }

    #[cfg(not(feature = "orc"))]
    #[tokio::test]
    async fn orc_is_refused_by_its_reason_without_the_feature() -> Result<()> {
        let registry = DataSourceRegistry::new();
        register_builtin_data_sources(&registry)?;
        let context = SessionContext::new();
        let Err(error) = registry
            .get_data_source("orc")?
            .create_writer(
                &context.state(),
                SinkInfo {
                    input: LogicalPlanBuilder::empty(true).build()?,
                    mode: SinkMode::Append,
                    partition_by: vec![],
                    bucket_by: None,
                    sort_order: vec![],
                    lakehouse_table: None,
                    options: vec![OptionLayer::OptionList { items: vec![] }],
                },
            )
            .await
        else {
            return datafusion::common::internal_err!("a default build must refuse the orc format");
        };
        assert!(
            error.to_string().contains(super::orc_off::ORC_FEATURE_OFF),
            "{error}"
        );
        Ok(())
    }
}
