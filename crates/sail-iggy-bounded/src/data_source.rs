//! The `iggy` format as the resolver sees it: options and a schema. A topic
//! is unbounded, so the format gives planning a schema and nothing more; the
//! one reader of its rows is a durable pipeline flow, which reads exact
//! ranges through [`IggyBoundedSource`](crate::IggyBoundedSource).

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{Result, not_impl_err, plan_err};
use datafusion::datasource::{TableProvider, TableType, provider_as_source};
use datafusion::logical_expr::{Expr, LogicalPlan, TableSource};
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{DataSource, OptionLayer, SinkInfo, SourceInfo};

use crate::config::{IggyBoundedConfig, PayloadFormat};
use crate::decode::{raw_schema, validate_schema};
use crate::envelope::envelope_schema;

pub const FORMAT: &str = "iggy";

/// The schema a read has: the declared one, which JSON needs, or the
/// format's own. The schema is validated against the format either way.
pub fn declared_schema(config: &IggyBoundedConfig, declared: Option<Schema>) -> Result<SchemaRef> {
    let declared = declared.filter(|schema| !schema.fields.is_empty());
    let schema = match (config.format, declared) {
        (_, Some(schema)) => schema,
        (PayloadFormat::Raw, None) => raw_schema(),
        (PayloadFormat::Envelope, None) => envelope_schema(),
        (PayloadFormat::Json, None) => {
            return plan_err!("Iggy JSON reads require an explicit schema");
        }
    };
    validate_schema(config, &schema)?;
    Ok(Arc::new(schema))
}

#[derive(Debug)]
pub struct IggyDataSource;

#[async_trait]
impl DataSource for IggyDataSource {
    fn name(&self) -> &str {
        FORMAT
    }

    async fn create_source(
        &self,
        _ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableSource>> {
        if !info.paths.is_empty() {
            return plan_err!("the Iggy format reads a topic, not paths");
        }
        let mut options = Vec::new();
        for layer in info.options {
            match layer {
                OptionLayer::OptionList { items } | OptionLayer::TablePropertyList { items } => {
                    options.extend(items);
                }
                _ => return plan_err!("the Iggy format supports options only"),
            }
        }
        let config = IggyBoundedConfig::from_options(options)?;
        let schema = declared_schema(&config, info.schema)?;
        Ok(provider_as_source(Arc::new(PipelineOnly { schema })))
    }

    async fn create_writer(&self, _ctx: &dyn Session, _info: SinkInfo) -> Result<LogicalPlan> {
        not_impl_err!("writing to the Iggy format")
    }
}

/// A schema for planning. Anything that gets as far as scanning it is
/// outside the pipeline path, and is told so instead of reading no rows.
#[derive(Debug)]
struct PipelineOnly {
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for PipelineOnly {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan_err!(
            "the Iggy format is read by pipeline Auto CDC flows; a topic has no end for a batch or ordinary streaming read"
        )
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::Constraints;
    use datafusion::execution::SessionStateBuilder;

    use super::*;

    fn info(options: &[(&str, &str)], schema: Option<Schema>) -> SourceInfo {
        SourceInfo {
            paths: vec![],
            lakehouse_table: None,
            schema,
            constraints: Constraints::default(),
            partition_by: vec![],
            bucket_by: None,
            sort_order: vec![],
            options: vec![OptionLayer::OptionList {
                items: options
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
            }],
            read_case_sensitive: false,
        }
    }

    const REQUIRED: [(&str, &str); 3] = [("url", "iggy://h:1"), ("stream", "s"), ("topic", "t")];

    fn declared() -> Schema {
        Schema::new(vec![Field::new("id", DataType::Int64, false)])
    }

    #[tokio::test]
    async fn planning_gets_the_schema_and_scanning_is_refused() -> Result<()> {
        let state = SessionStateBuilder::new().build();
        let info = info(&REQUIRED, Some(declared()));
        let source = IggyDataSource.create_source(&state, info).await?;
        assert_eq!(source.schema().fields(), declared().fields());
        let provider = datafusion::datasource::source_as_provider(&source)?;
        let error = provider
            .scan(&state, None, &[], None)
            .await
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(error.contains("read by pipeline Auto CDC flows"), "{error}");
        Ok(())
    }

    #[tokio::test]
    async fn json_needs_a_schema_and_the_other_formats_bring_theirs() -> Result<()> {
        let state = SessionStateBuilder::new().build();
        let error = IggyDataSource
            .create_source(&state, info(&REQUIRED, None))
            .await
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(error.contains("require an explicit schema"), "{error}");
        let mut raw = REQUIRED.to_vec();
        raw.push(("payload_format", "raw"));
        let source = IggyDataSource
            .create_source(&state, info(&raw, None))
            .await?;
        assert_eq!(source.schema().fields(), raw_schema().fields());
        Ok(())
    }
}
