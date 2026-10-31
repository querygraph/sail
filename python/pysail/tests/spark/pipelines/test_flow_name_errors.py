"""Named-flow registration keeps Spark's error kind and API-specific spelling."""

from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException, UnsupportedOperationException
from pyspark.sql.connect import proto as pb


def _define(pipeline_spark, pipeline, api, name, target="st", operation=None):
    graph_id, registry, _ = pipeline
    if api == "sql":
        operation = operation or f"INSERT INTO {target} BY NAME SELECT 1"
        registry.register_sql(f"CREATE FLOW {name} AS {operation}", Path("flow-names.sql"))
    else:
        command = pb.Command()
        flow = command.pipeline_command.define_flow
        flow.dataflow_graph_id = graph_id
        flow.flow_name = name
        flow.target_dataset_name = target
        flow.relation_flow_details.relation.range.end = 1
        flow.relation_flow_details.relation.range.step = 1
        pipeline_spark.client.execute_command(command)


def _check_named(error, expected):
    assert error.getCondition() == "MULTIPART_FLOW_NAME_NOT_SUPPORTED"
    assert error.getSqlState() == "0A000"
    assert error.getMessageParameters() == {"flowName": expected}
    assert f"Flow with multipart name '{expected}' is not supported." in str(error)
    assert "flow-names.sql" not in str(error)


@pytest.mark.parametrize("api", ["sql", "protocol"])
@pytest.mark.parametrize(
    ("name", "unquoted"),
    [("Some_DB.F", "Some_DB.F"), ("Cat.Db.F", "Cat.Db.F"), ("`D.b`.`F``Q`", "D.b.F`Q")],
)
def test_named_flow_rejection_preserves_api_spelling(pipeline_spark, pipeline, api, name, unquoted):
    with pytest.raises(AnalysisException) as captured:
        _define(pipeline_spark, pipeline, api, name)
    _check_named(captured.value, unquoted if api == "sql" else name)


@pytest.mark.parametrize("api", ["sql", "protocol"])
@pytest.mark.parametrize(
    ("name", "target", "invalid"),
    [
        ("A.b.c.d", "st", "A.b.c.d"),
        ("A.b.c.d", "A.b.c.d", "A.b.c.d"),
        ("f", "A.b.c.d", "A.b.c.d"),
        ("A.b.c.d.e", "st", "A.b.c.d.e"),
    ],
)
def test_flow_identifier_arity_has_no_invented_error_fields(pipeline_spark, pipeline, api, name, target, invalid):
    with pytest.raises(UnsupportedOperationException) as captured:
        _define(pipeline_spark, pipeline, api, name, target)
    error = captured.value
    assert error.getCondition() is None
    assert error.getSqlState() is None
    assert error.getMessageParameters() is None
    assert f"4+ part table identifier {invalid} is not supported." in str(error)
    assert "[]" not in str(error)


@pytest.mark.parametrize("api", ["sql", "protocol"])
def test_quoted_dot_is_one_flow_name_component(pipeline_spark, pipeline, api):
    _define(pipeline_spark, pipeline, api, "`literal.dot`")


def test_protocol_implicit_flow_keeps_multipart_name_exception(pipeline_spark, pipeline):
    _define(pipeline_spark, pipeline, "protocol", "cat.db.f", "cat.db.f")


@pytest.mark.parametrize(
    "operation",
    [
        "INSERT OVERWRITE st BY NAME SELECT 1",
        "INSERT INTO st PARTITION (p) BY NAME SELECT 1",
        "INSERT INTO st (a) SELECT 1",
    ],
)
def test_multipart_name_error_precedes_insert_policy(pipeline_spark, pipeline, operation):
    with pytest.raises(AnalysisException) as captured:
        _define(pipeline_spark, pipeline, "sql", "Some_DB.F", operation=operation)
    _check_named(captured.value, "Some_DB.F")


def test_original_sql_multipart_flow_rejection(pipeline_spark, pipeline):
    # SqlPipelineSuite:580 uses both external-table fixtures from beforeEach.
    catalog = pipeline_spark.catalog.currentCatalog()
    database = pipeline_spark.catalog.currentDatabase()

    def qualified(name):
        return ".".join("`" + part.replace("`", "``") + "`" for part in [catalog, database, name])

    tables = [qualified("external_t1"), qualified("external_t2")]
    try:
        for table, rows in zip(tables, [3, 4]):
            pipeline_spark.sql(f"CREATE TABLE {table} AS SELECT * FROM RANGE({rows})").collect()
        _, registry, _ = pipeline
        with pytest.raises(AnalysisException) as captured:
            registry.register_sql(
                "\nCREATE STREAMING TABLE st;\n"
                "CREATE FLOW some_database.f AS INSERT INTO st BY NAME\n"
                f"SELECT * FROM STREAM {tables[0]};\n",
                Path("a.sql"),
            )
        _check_named(captured.value, "some_database.f")
    finally:
        for table in tables:
            pipeline_spark.sql(f"DROP TABLE IF EXISTS {table}").collect()
