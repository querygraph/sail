use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use sail_catalog::command::CatalogCommand;
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{CreateViewColumnOptions, CreateViewOptions};
use sail_common::spec;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::session::job::JobService;
use sail_execution::job_runner::LocalJobRunner;

use super::sql;
use crate::session::SparkSession;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

pub(super) fn context() -> Result<SessionContext, Box<dyn std::error::Error>> {
    let context = sql::tests::resolution_context()?;
    let state = SessionStateBuilder::new_from_existing(context.state())
        .with_query_planner(sail_session::planner::new_query_planner())
        .build();
    *context.state_ref().write() = state;
    context
        .state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(JobService::new(Box::new(
            LocalJobRunner::default(),
        ))));
    Ok(context)
}
#[tokio::test]
async fn prepared_catalog_view_keeps_metadata_and_executes_only_when_consumed() -> TestResult {
    let ctx = context()?;
    let manager = ctx.extension::<CatalogManager>()?;
    let options = CreateViewOptions {
        definition: "SELECT 1 AS id /* original */".into(),
        columns: vec![CreateViewColumnOptions {
            name: "id".into(),
            data_type: DataType::Int32,
            nullable: false,
            comment: Some("column comment".into()),
        }],
        comment: Some("view comment".into()),
        if_not_exists: false,
        replace: true,
        properties: vec![
            ("user".into(), "keep".into()),
            ("view.catalogAndNamespace.numParts".into(), "2".into()),
            (
                "view.catalogAndNamespace.part.0".into(),
                "test_catalog".into(),
            ),
            ("view.catalogAndNamespace.part.1".into(), "test_db".into()),
        ],
    };
    let (plan, info) = sail_plan::prepare_catalog_command(
        &ctx,
        CatalogCommand::CreateView {
            view: vec!["target".into()],
            options: options.clone(),
        },
    )
    .await?;
    assert_eq!(info.len(), 3);
    assert!(manager.get_table_or_view(&["target"]).await.is_err());
    let service = ctx.extension::<JobService>()?;
    let output = service.runner().execute(&ctx, plan).await?;
    let batches = output.try_collect::<Vec<_>>().await?;
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let status = manager.get_table_or_view(&["target"]).await?;
    let TableKind::View {
        definition,
        columns,
        comment,
        properties,
    } = status.kind
    else {
        return Err("catalog did not publish a view".into());
    };
    assert_eq!(definition, options.definition);
    assert_eq!(comment, options.comment);
    assert_eq!(properties, options.properties);
    assert_eq!(columns[0].name, "id");
    assert_eq!(columns[0].data_type, DataType::Int32);
    assert!(!columns[0].nullable);
    assert_eq!(columns[0].comment.as_deref(), Some("column comment"));
    service.runner().stop().await?;
    Ok(())
}
#[tokio::test]
async fn prepared_catalog_command_matches_resolved_command_results_and_diagnostics() -> TestResult {
    let ctx = context()?;
    let (prepared, prepared_info) =
        sail_plan::prepare_catalog_command(&ctx, CatalogCommand::CurrentDatabase).await?;
    let (native, native_info) = sail_plan::resolve_and_execute_plan(
        &ctx,
        ctx.extension::<SparkSession>()?.plan_config()?,
        spec::Plan::Command(spec::CommandPlan::new(spec::CommandNode::CurrentDatabase)),
    )
    .await?;
    let service = ctx.extension::<JobService>()?;
    let prepared = service
        .runner()
        .execute(&ctx, prepared)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let native = service
        .runner()
        .execute(&ctx, native)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(prepared, native);
    assert_eq!(prepared_info.len(), native_info.len());
    assert_eq!(
        prepared_info
            .iter()
            .map(|item| item.plan.as_str())
            .collect::<Vec<_>>(),
        native_info
            .iter()
            .map(|item| item.plan.as_str())
            .collect::<Vec<_>>()
    );
    service.runner().stop().await?;
    Ok(())
}
