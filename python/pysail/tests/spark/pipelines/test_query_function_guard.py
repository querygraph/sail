"""Spark 4.2.1 limits on eager work during pipeline definition (PythonPipelineSuite)."""

import pyspark.pipelines as dp
import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph

GUARD = "ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION"
BLOCKED_SQL = "UNSUPPORTED_PIPELINE_SPARK_SQL_COMMAND"

UNSUPPORTED_SQL = [
    ("SET CATALOG some_catalog", "SetCatalogCommand"),
    ("USE SCHEMA some_schema", "SetNamespaceCommand"),
    ("SET `test_conf` = `true`", "SetCommand"),
    ("CREATE TABLE some_table (id INT)", "CreateTable"),
    ("CREATE VIEW some_view AS SELECT * FROM some_table", "CreateView"),
    ("INSERT INTO some_table VALUES (1)", "InsertIntoStatement"),
    ("ALTER TABLE some_table RENAME TO some_new_table", "RenameTable"),
    ("CREATE NAMESPACE some_namespace", "CreateNamespace"),
    ("DROP VIEW some_view", "DropView"),
    ("CREATE MATERIALIZED VIEW some_view AS SELECT * FROM some_table", "CreateMaterializedViewAsSelect"),
    ("CREATE STREAMING TABLE some_table AS SELECT * FROM some_table", "CreateStreamingTableAsSelect"),
]
SUPPORTED_SQL = [
    "DESCRIBE TABLE {src}",
    "SHOW TABLES",
    "SHOW TBLPROPERTIES {src}",
    "SHOW NAMESPACES",
    "SHOW COLUMNS FROM {src}",
    "SHOW FUNCTIONS",
    "SHOW VIEWS",
    "SHOW CATALOGS",
    "SELECT * FROM RANGE(5)",
    "SELECT * FROM {src}",
]
EAGER_EXECUTION = ["df.collect()", "df.first()", "df.head(0)", "df.toPandas()"]
EAGER_ANALYSIS = ["df.schema", "df.isStreaming", "df.isLocal()"]


def build_graph(spark, body):
    graph = create_dataflow_graph(spark, None, None, None)
    registry = SparkConnectGraphElementRegistry(spark, graph)
    scope = {"spark": spark, "dp": dp}
    with (
        add_pipeline_analysis_context(spark=spark, dataflow_graph_id=graph, flow_name=None),
        graph_element_registration_context(registry),
    ):
        exec(body, scope)  # noqa: S102 - the port of Spark's buildGraph runs its snippet


def in_function(statement):
    return f"""
@dp.materialized_view()
def mv():
    df = spark.range(5)
    {statement}
    return df
"""


def at_definition(statement):
    return f"""
df = spark.range(5)
{statement}

@dp.materialized_view()
def mv():
    return spark.range(5)
"""


@pytest.fixture
def source(pipeline_spark):
    name = f"{pipeline_spark.catalog.currentCatalog()}.{pipeline_spark.catalog.currentDatabase()}.src"
    pipeline_spark.sql(f"CREATE TABLE {name} AS SELECT * FROM RANGE(5)")
    yield name
    pipeline_spark.sql(f"DROP TABLE {name}")


@pytest.mark.parametrize("wrap", [at_definition, in_function])
@pytest.mark.parametrize(("sql", "command"), UNSUPPORTED_SQL)
def test_unsupported_sql_is_blocked_with_or_without_a_flow(pipeline_spark, wrap, sql, command):
    with pytest.raises(AnalysisException) as error:
        build_graph(pipeline_spark, wrap(f'spark.sql("{sql}")'))
    assert error.value.getCondition() == BLOCKED_SQL
    assert error.value.getSqlState() == "0A000"
    assert error.value.getMessageParameters() == {"command": command}


@pytest.mark.parametrize("wrap", [at_definition, in_function])
@pytest.mark.parametrize("sql", SUPPORTED_SQL)
def test_supported_sql_is_allowed_with_or_without_a_flow(pipeline_spark, source, wrap, sql):
    build_graph(pipeline_spark, wrap(f'spark.sql("{sql.format(src=source)}")'))


def test_sql_inside_a_query_function_is_deferred_not_executed(pipeline_spark):
    # A failing query proves no analysis ran: the missing table goes unreported.
    build_graph(pipeline_spark, in_function('spark.sql("SELECT * FROM missing_table")'))


def test_sql_outside_a_query_function_stays_lazy_for_queries(pipeline_spark):
    build_graph(pipeline_spark, at_definition('spark.sql("SELECT 1")'))


def test_blocked_sql_has_no_effect(pipeline_spark):
    with pytest.raises(AnalysisException):
        build_graph(pipeline_spark, in_function('spark.sql("CREATE TABLE guarded_table (id INT)")'))
    assert not pipeline_spark.catalog.tableExists("guarded_table")


@pytest.mark.parametrize("statement", EAGER_EXECUTION + EAGER_ANALYSIS)
def test_eager_work_inside_a_query_function_is_blocked(pipeline_spark, statement):
    with pytest.raises(AnalysisException) as error:
        build_graph(pipeline_spark, in_function(statement))
    assert error.value.getCondition() == GUARD
    assert error.value.getSqlState() == "0A000"
    assert error.value.getMessageParameters() == {}
    assert (
        "Operations that trigger DataFrame analysis or execution are not allowed in pipeline "
        "query functions. Move code outside of the pipeline query function." in error.value.getMessage()
    )


@pytest.mark.parametrize("statement", EAGER_EXECUTION + EAGER_ANALYSIS)
def test_eager_work_outside_a_query_function_is_allowed(pipeline_spark, statement):
    build_graph(pipeline_spark, at_definition(statement))


def test_eager_write_inside_a_query_function_is_blocked(pipeline_spark):
    with pytest.raises(AnalysisException) as error:
        build_graph(pipeline_spark, in_function('df.write.format("noop").mode("overwrite").save()'))
    assert error.value.getCondition() == GUARD


def test_a_flow_name_is_what_marks_a_query_function(pipeline_spark):
    graph = create_dataflow_graph(pipeline_spark, None, None, None)
    df = pipeline_spark.range(1)
    with add_pipeline_analysis_context(spark=pipeline_spark, dataflow_graph_id=graph, flow_name=None):
        assert df.collect()
    with add_pipeline_analysis_context(spark=pipeline_spark, dataflow_graph_id=graph, flow_name="f"):
        with pytest.raises(AnalysisException) as error:
            df.collect()
    assert error.value.getCondition() == GUARD
    assert df.collect()
