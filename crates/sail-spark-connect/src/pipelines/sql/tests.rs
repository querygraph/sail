use std::collections::HashMap;

use super::{PipelineGraph, register};
use crate::error::{SparkError, SparkResult};
use crate::spark::connect::{OutputType, pipeline_command as pc};

fn graph() -> PipelineGraph {
    PipelineGraph::new("test_catalog".into(), "test_db".into(), HashMap::new())
}

fn register_sql(graph: &mut PipelineGraph, sql: &str, file: &str) -> SparkResult<()> {
    register(
        graph,
        pc::DefineSqlGraphElements {
            sql_text: Some(sql.into()),
            sql_file_path: Some(file.into()),
            ..Default::default()
        },
    )
}

fn name(table: &str) -> Vec<String> {
    vec!["test_catalog".into(), "test_db".into(), table.into()]
}

#[test]
fn identity_partition_columns_keep_declared_order() -> SparkResult<()> {
    let mut graph = graph();
    register_sql(
        &mut graph,
        "CREATE MATERIALIZED VIEW a PARTITIONED BY (id1, id2) \
         AS SELECT id AS id1, id AS id2 FROM range(1, 2)",
        "partition.sql",
    )?;
    let [output] = graph.definition.outputs.as_slice() else {
        return Err(SparkError::internal("expected one partitioned output"));
    };
    assert_eq!(output.name, name("a"));
    assert_eq!(output.kind, OutputType::MaterializedView);
    let Some(pc::define_output::Details::TableDetails(details)) = &output.definition.details else {
        return Err(SparkError::internal("expected table details"));
    };
    assert_eq!(details.partition_cols, vec!["id1", "id2"]);
    Ok(())
}

#[test]
fn streaming_table_properties_are_registered_without_execution() -> SparkResult<()> {
    let mut graph = graph();
    // The original Spark fixture resolves registration without validating streamingness.
    register_sql(
        &mut graph,
        "CREATE STREAMING TABLE st TBLPROPERTIES ('prop1'='foo', 'prop2'='bar') AS SELECT 1",
        "properties.sql",
    )?;
    let [output] = graph.definition.outputs.as_slice() else {
        return Err(SparkError::internal("expected one streaming output"));
    };
    assert_eq!(output.name, name("st"));
    assert_eq!(output.kind, OutputType::Table);
    let Some(pc::define_output::Details::TableDetails(details)) = &output.definition.details else {
        return Err(SparkError::internal("expected table details"));
    };
    assert_eq!(
        details.table_properties,
        HashMap::from([
            ("prop1".into(), "foo".into()),
            ("prop2".into(), "bar".into()),
        ])
    );
    Ok(())
}

#[test]
fn sql_configuration_is_captured_per_flow_and_reset_per_file() -> SparkResult<()> {
    let mut graph = graph();
    register_sql(
        &mut graph,
        "CREATE MATERIALIZED VIEW a AS SELECT id FROM range(1, 2); \
         SET conf.test = a; \
         CREATE VIEW b AS SELECT id FROM range(1, 2); \
         SET conf.test = b; \
         SET conf.test2 = c; \
         CREATE STREAMING TABLE c AS SELECT id FROM range(1, 2);",
        "configuration.sql",
    )?;
    let configurations: Vec<_> = graph
        .definition
        .flows
        .iter()
        .map(|flow| (flow.name.clone(), flow.sql_conf.clone()))
        .collect();
    assert_eq!(
        configurations,
        vec![
            (name("a"), HashMap::new()),
            (name("b"), HashMap::from([("conf.test".into(), "a".into())])),
            (
                name("c"),
                HashMap::from([
                    ("conf.test".into(), "b".into()),
                    ("conf.test2".into(), "c".into()),
                ])
            ),
        ]
    );
    assert!(graph.definition.sql_conf.is_empty());
    register_sql(
        &mut graph,
        "CREATE MATERIALIZED VIEW d AS SELECT 1 AS value",
        "next.sql",
    )?;
    let Some(flow) = graph.definition.flows.last() else {
        return Err(SparkError::internal("expected next-file flow"));
    };
    assert_eq!(flow.name, name("d"));
    assert!(flow.sql_conf.is_empty());
    Ok(())
}
