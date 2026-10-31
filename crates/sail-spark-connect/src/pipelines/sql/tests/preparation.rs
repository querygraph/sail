use std::sync::atomic::{AtomicBool, Ordering};

use sail_sql_analyzer::pipeline::PipelineStatement;
use sail_sql_analyzer::source::SqlSource;
use tokio::sync::Barrier;
use tokio::task::JoinHandle;

use super::super::{
    PreparationFailure, PreparedSql, PreparedStatement, register, resolve_statement,
};
use super::*;
use crate::error::StructuredSparkError;

fn command(sql: &str) -> pc::DefineSqlGraphElements {
    pc::DefineSqlGraphElements {
        sql_text: Some(sql.into()),
        sql_file_path: Some("preparation.sql".into()),
        ..Default::default()
    }
}

fn paused_registration(
    spark: Arc<SparkSession>,
    id: String,
) -> (JoinHandle<SparkResult<()>>, Arc<Barrier>, Arc<Barrier>) {
    let entered = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let entered_task = entered.clone();
    let resume_task = resume.clone();
    let task = tokio::spawn(async move {
        register(&spark, &id, async {
            entered_task.wait().await;
            resume_task.wait().await;
            prepare(
                command("CREATE MATERIALIZED VIEW prepared AS SELECT 1"),
                |_| async { Err(SparkError::internal("unexpected expression in static SQL")) },
            )
            .await
        })
        .await
    });
    (task, entered, resume)
}

async fn wait_until_preparing(
    task: &mut JoinHandle<SparkResult<()>>,
    entered: &Barrier,
) -> SparkResult<()> {
    tokio::select! {
        _ = entered.wait() => Ok(()),
        result = task => {
            result??;
            Err(SparkError::internal("registration completed before preparation barrier"))
        }
    }
}

#[tokio::test]
async fn preparation_releases_graph_lock_and_retains_concurrent_outputs()
-> Result<(), Box<dyn std::error::Error>> {
    let context = resolution_context()?;
    let spark = context.extension::<SparkSession>()?;
    let id = spark.add_pipeline(graph())?;
    let (mut task, entered, resume) = paused_registration(spark.clone(), id.clone());
    wait_until_preparing(&mut task, &entered).await?;
    {
        let graph = spark.pipeline(&id)?;
        graph
            .try_lock()
            .map_err(|error| SparkError::internal(error.to_string()))?
            .define_output(pc::DefineOutput {
                output_name: Some("concurrent".into()),
                output_type: Some(OutputType::MaterializedView as i32),
                ..Default::default()
            })?;
    }
    resume.wait().await;
    task.await??;
    let graph = spark.pipeline(&id)?;
    let graph = graph
        .lock()
        .map_err(|error| SparkError::internal(error.to_string()))?;
    assert_eq!(
        graph
            .definition
            .outputs
            .iter()
            .map(|output| output.name.clone())
            .collect::<Vec<_>>(),
        vec![name("concurrent"), name("prepared")]
    );
    Ok(())
}

#[tokio::test]
async fn graph_dropped_during_preparation_is_not_committed()
-> Result<(), Box<dyn std::error::Error>> {
    let context = resolution_context()?;
    let spark = context.extension::<SparkSession>()?;
    let id = spark.add_pipeline(graph())?;
    let (mut task, entered, resume) = paused_registration(spark.clone(), id.clone());
    wait_until_preparing(&mut task, &entered).await?;
    spark.drop_pipeline(&id)?;
    resume.wait().await;
    let error = task
        .await?
        .err()
        .ok_or_else(|| SparkError::internal("expected graph-not-found"))?;
    assert!(
        error.to_string().contains("DATAFLOW_GRAPH_NOT_FOUND"),
        "{error}"
    );
    assert!(spark.pipeline(&id).is_err());
    Ok(())
}

#[tokio::test]
async fn run_started_during_preparation_rejects_commit() -> Result<(), Box<dyn std::error::Error>> {
    let context = resolution_context()?;
    let spark = context.extension::<SparkSession>()?;
    let mut initial = graph();
    register_sql(
        &mut initial,
        "CREATE MATERIALIZED VIEW initial AS SELECT 1",
        "initial.sql",
    )
    .await?;
    let id = spark.add_pipeline(initial)?;
    let (mut task, entered, resume) = paused_registration(spark.clone(), id.clone());
    wait_until_preparing(&mut task, &entered).await?;
    let (_, active) = spark
        .pipeline(&id)?
        .lock()
        .map_err(|error| SparkError::internal(error.to_string()))?
        .start(&id)?;
    resume.wait().await;
    let error = task
        .await?
        .err()
        .ok_or_else(|| SparkError::internal("expected active-run rejection"))?;
    assert!(error.to_string().contains("already active"), "{error}");
    assert_eq!(
        spark
            .pipeline(&id)?
            .lock()
            .map_err(|error| SparkError::internal(error.to_string()))?
            .definition
            .outputs
            .len(),
        1
    );
    drop(active);
    Ok(())
}

#[tokio::test]
async fn missing_graph_is_checked_before_polling_preparation() -> SparkResult<()> {
    let context = resolution_context().map_err(|error| SparkError::internal(error.to_string()))?;
    let spark = context.extension::<SparkSession>()?;
    let polled = AtomicBool::new(false);
    let result = register(&spark, "missing", async {
        polled.store(true, Ordering::SeqCst);
        prepare(command("SELECT 1"), |_| async { Ok("unused".into()) }).await
    })
    .await;
    assert!(result.is_err());
    assert!(!polled.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn earlier_registration_error_wins_and_discards_the_entire_draft() -> SparkResult<()> {
    let mut graph = graph();
    let prepared = prepare(
        command(
            "SET conf.test = changed; USE other_db; \
         CREATE MATERIALIZED VIEW transient AS SELECT 1; \
         CREATE GLOBAL TEMPORARY VIEW rejected AS SELECT 1; SELECT 2",
        ),
        |_| async { Ok("unused".into()) },
    )
    .await?;
    let error = prepared
        .commit(&mut graph)
        .err()
        .ok_or_else(|| SparkError::internal("expected registration failure"))?;
    assert!(
        error
            .to_string()
            .contains("SQL statement in pipeline definitions"),
        "{error}"
    );
    assert!(
        !error.to_string().contains("Unsupported query statement"),
        "{error}"
    );
    assert!(graph.definition.outputs.is_empty());
    assert!(graph.definition.flows.is_empty());
    assert_eq!(graph.definition.catalog, "test_catalog");
    assert_eq!(graph.definition.database, "test_db");
    assert!(graph.definition.sql_conf.is_empty());
    Ok(())
}

#[tokio::test]
async fn typed_catalog_preparation_uses_shared_resolution_and_preserves_error_kinds()
-> Result<(), Box<dyn std::error::Error>> {
    let context = resolution_context()?;
    let spark = context.extension::<SparkSession>()?;
    let mut config = (*spark.plan_config()?).clone();
    config.ansi_mode = true;
    let resolver = &PlanResolver::new(&context, Arc::new(config));
    let mut resolve = |expression| async move {
        resolver
            .resolve_catalog_name(expression)
            .await
            .map_err(SparkError::from)
    };
    let statement = |sql| -> SparkResult<PipelineStatement> {
        Ok(PipelineStatement::Command(
            spec::CommandNode::SetCurrentCatalogExpression {
                catalog: sail_sql_analyzer::expression::from_ast_expression(
                    sail_sql_analyzer::parser::parse_expression(sql)?,
                )?,
            },
        ))
    };
    let result = resolve_statement(statement("CONCAT('test_', 'catalog')")?, &mut resolve).await;
    assert!(
        matches!(result, Ok(PipelineStatement::Command(spec::CommandNode::SetCurrentCatalog { catalog }))
        if catalog.as_ref() == "test_catalog")
    );
    let result = resolve_statement(statement("CAST(NULL AS STRING)")?, &mut resolve).await;
    assert!(matches!(result, Err(PreparationFailure::Analysis(message))
        if message.starts_with("Failed to resolve catalog expression: ") && message.contains("NOT_A_CONSTANT_STRING.NULL")));
    for sql in [
        "IDENTIFIER(missing)",
        "IDENTIFIER(concat(current_catalog(), missing))",
        "IDENTIFIER(typeof(missing))",
    ] {
        let result = resolve_statement(statement(sql)?, &mut resolve).await;
        assert!(
            matches!(result, Err(PreparationFailure::Analysis(message))
            if message.starts_with("Failed to resolve catalog expression: ")
                && message.contains("UNRESOLVED_COLUMN.WITHOUT_SUGGESTION")),
            "{sql}"
        );
    }
    let result = resolve_statement(
        statement("CAST(CAST('bad' AS INT) AS STRING)")?,
        &mut resolve,
    )
    .await;
    assert!(matches!(result, Err(PreparationFailure::Evaluation(_))));
    let text = "SET CATALOG 'placeholder'";
    let source = SqlSource::new(text)?;
    let (_, span) = source
        .parse_statements_with_spans()?
        .pop()
        .ok_or_else(|| SparkError::internal("expected statement span"))?;
    let prepared = PreparedSql {
        text: text.into(),
        file: Some("preparation.sql".into()),
        statements: vec![PreparedStatement { span, result }],
    };
    let error = prepared
        .commit(&mut graph())
        .err()
        .ok_or_else(|| SparkError::internal("expected evaluation failure"))?;
    assert!(matches!(error, SparkError::DataFusionError(_)), "{error}");
    assert!(
        !error
            .to_string()
            .contains("PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR")
    );
    Ok(())
}

#[tokio::test]
async fn structured_catalog_errors_preserve_registration_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    for (class, condition, analysis) in [
        (
            "org.apache.spark.sql.AnalysisException",
            "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
            true,
        ),
        (
            "org.apache.spark.SparkArithmeticException",
            "DIVIDE_BY_ZERO",
            false,
        ),
    ] {
        let prepared = prepare(command("SET CATALOG 'placeholder'"), |_| {
            std::future::ready(Err(SparkError::Structured(Box::new(
                StructuredSparkError {
                    class: class.into(),
                    condition: condition.into(),
                    sql_state: "test".into(),
                    message: "original error".into(),
                    parameters: HashMap::new(),
                },
            ))))
        })
        .await?;
        let mut graph = graph();
        let error = prepared
            .commit(&mut graph)
            .err()
            .ok_or_else(|| SparkError::internal("catalog failure was accepted"))?;
        let SparkError::Structured(error) = error else {
            return Err(SparkError::internal("structured error was lost").into());
        };
        if analysis {
            assert_eq!(
                error.condition,
                "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR"
            );
            assert!(
                error
                    .message
                    .contains("Failed to resolve catalog expression: ")
            );
            assert!(error.message.contains(condition));
        } else {
            assert_eq!(error.class, class);
            assert_eq!(error.condition, condition);
            assert_eq!(error.message, "original error");
        }
        assert!(graph.definition.outputs.is_empty());
        assert!(graph.definition.flows.is_empty());
    }
    Ok(())
}
