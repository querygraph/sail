use super::*;

// MaterializeTablesSuite:42–123, including its mixed nullability and three
// successive column-comment states. Sail exposes the reserved table comment
// as TableKind::Table.comment rather than Spark's PROP_COMMENT map entry.
#[tokio::test]
async fn basic_metadata_refresh_preserves_exact_schema_and_updates_then_removes_comments()
-> TestResult {
    let ctx = context()?;
    for comments in [
        Some(("comment1", "comment2")),
        Some(("comment3", "comment4")),
        None,
    ] {
        let (first, second) = comments
            .map(|(first, second)| {
                (
                    format!(" COMMENT '{first}'"),
                    format!(" COMMENT '{second}'"),
                )
            })
            .unwrap_or_default();
        let graph = graph(&format!(
            "CREATE MATERIALIZED VIEW a (x INT NOT NULL{first}, x2 INT{second}) \
             COMMENT 'p-comment' AS SELECT 1 AS x, 1 AS x2 UNION ALL SELECT 2 AS x, 3 AS x2"
        ))
        .await?;
        assert_eq!(graph.outputs.len(), 1);
        assert_eq!(graph.outputs[0].kind, OutputType::MaterializedView);
        materialize(&ctx, &graph).await?;
        let status = ctx
            .extension::<CatalogManager>()?
            .get_table_or_view(&["a"])
            .await?;
        let TableKind::Table {
            columns, comment, ..
        } = status.kind
        else {
            return Err("expected a materialized catalog table".into());
        };
        let expected = [
            ("x", false, comments.map(|(first, _)| first)),
            ("x2", true, comments.map(|(_, second)| second)),
        ];
        assert_eq!(columns.len(), expected.len());
        for (column, (name, nullable, comment)) in columns.iter().zip(expected) {
            assert_eq!(column.name, name);
            assert_eq!(column.data_type, DataType::Int32);
            assert_eq!(column.nullable, nullable);
            assert_eq!(column.comment.as_deref(), comment);
            assert_eq!(column.default, None);
            assert_eq!(column.generated_always_as, None);
            assert_eq!(column.identity, None);
        }
        assert_eq!(comment.as_deref(), Some("p-comment"));
        assert_eq!(row_count(&ctx, "a").await?, 0);
    }
    finish(&ctx).await?;
    Ok(())
}

// MaterializeTablesSuite:236–262: use the SQL DDL path for the existing INT
// target, then materialize a Table with a separately inferred batch-flow schema.
#[tokio::test]
async fn inferred_streaming_metadata_matches_sql_created_existing_int_schema() -> TestResult {
    let ctx = context()?;
    let plan = sail_sql_analyzer::statement::from_ast_statement(
        sail_sql_analyzer::parser::parse_one_statement("CREATE TABLE test_db.t2 (x INT)")?,
    )?;
    let spec::Plan::Command(command) = plan else {
        return Err("expected CREATE TABLE command".into());
    };
    execution::run_command(
        &ctx,
        ctx.extension::<SparkSession>()?.plan_config()?,
        command.node,
        None,
    )
    .await?;
    for refresh in [false, true] {
        if refresh {
            let graph = graph("CREATE STREAMING TABLE t2 AS SELECT 1 AS x UNION ALL SELECT 2 AS x UNION ALL SELECT 3 AS x").await?;
            assert_eq!(graph.outputs[0].kind, OutputType::Table);
            materialize(&ctx, &graph).await?;
        }
        let columns = catalog_columns(&ctx, "t2").await?;
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "x");
        assert_eq!(columns[0].data_type, DataType::Int32);
        assert!(columns[0].nullable);
        assert_eq!(columns[0].comment, None);
        assert_eq!(columns[0].default, None);
        assert_eq!(columns[0].generated_always_as, None);
        assert_eq!(columns[0].identity, None);
        assert_eq!(row_count(&ctx, "t2").await?, 0);
    }
    finish(&ctx).await?;
    Ok(())
}
