"""Batch behaviors a pipeline depends on, each measured on Spark 4.2.1."""

from pathlib import Path

import pyspark.pipelines as dp
import pytest
from pyspark.errors import AnalysisException
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run

from pysail.testing.spark.utils.common import is_jvm_spark


class Suite:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.storage = storage

    def registry(self):
        graph = create_dataflow_graph(self.spark, self.spark.catalog.currentCatalog(), self.schema, None)
        return graph, SparkConnectGraphElementRegistry(self.spark, graph)

    def run(self, *, sql=None, python=None, dry=False):
        graph, registry = self.registry()
        if python:
            with (
                add_pipeline_analysis_context(spark=self.spark, dataflow_graph_id=graph, flow_name=None),
                graph_element_registration_context(registry),
            ):
                exec(python, {"spark": self.spark, "dp": dp})  # noqa: S102 the graph under test
        if sql:
            registry.register_sql(sql, Path("pipeline.sql"))
        messages = []
        for item in start_run(self.spark, graph, None, False, None, dry, self.storage):
            if "pipeline_event_result" in item:
                messages.append(item["pipeline_event_result"].event.message)
        return messages

    def attempt(self, **kwargs):
        with pytest.raises(AnalysisException) as error:
            self.run(**kwargs)
        return error.value

    def exists(self, table):
        return self.spark.catalog.tableExists(f"{self.schema}.{table}")


@pytest.fixture
def suite(spark, tmp_path, request):
    schema = "bp_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    spark.catalog.setCurrentDatabase(schema)
    yield Suite(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql("USE default")
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_a_write_statement_has_no_rows(suite):
    suite.spark.sql("CREATE TABLE t (id INT) USING parquet")
    assert suite.spark.sql("INSERT INTO t VALUES (1)").collect() == []
    assert suite.spark.sql("CREATE TABLE c USING parquet AS SELECT 1 AS id").collect() == []
    assert suite.spark.table("t").collect()[0].id == 1


def test_partition_columns_move_last_in_a_file_table(suite):
    suite.spark.sql("CREATE TABLE t (x BOOLEAN, y INT) USING parquet PARTITIONED BY (x)")
    assert suite.spark.table("t").columns == ["y", "x"]
    suite.spark.sql("INSERT INTO t VALUES (1, true)")
    assert [tuple(row) for row in suite.spark.table("t").collect()] == [(1, True)]


def test_partition_columns_move_last_in_a_selected_table(suite):
    suite.spark.sql("CREATE TABLE q USING parquet PARTITIONED BY (x) AS SELECT true AS x, 1 AS y")
    assert suite.spark.table("q").columns == ["y", "x"]


def test_refresh_takes_a_table_and_requires_it(suite):
    suite.spark.sql("CREATE TABLE t (id INT) USING parquet")
    suite.spark.sql("REFRESH TABLE t")
    suite.spark.catalog.refreshTable(f"{suite.schema}.t")
    with pytest.raises(AnalysisException):
        suite.spark.sql("REFRESH TABLE no_such_table")


def test_a_bad_partition_column_is_each_attempt_of_its_flow(suite):
    error = suite.attempt(
        python="""
@dp.materialized_view(partition_cols=["nope"])
def bad():
  return spark.range(5)

@dp.materialized_view
def good():
  return spark.range(2)

@dp.materialized_view
def down():
  return spark.read.table("bad")
"""
    )
    assert error.getCondition() == "COLUMN_NOT_DEFINED_IN_TABLE"
    assert error.getMessageParameters()["colName"] == "`nope`"
    assert [suite.exists(table) for table in ["bad", "good"]] == [True, True]
    assert suite.spark.table("good").count() == 2


BASE = "CREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(2); CREATE TEMPORARY VIEW v AS SELECT * FROM a;"


def test_repeated_columns_pass_a_dry_run(suite):
    suite.run(sql=BASE + "CREATE MATERIALIZED VIEW m AS SELECT * FROM a, v", dry=True)
    assert not suite.exists("m")


def test_repeated_columns_fail_the_flow_and_skip_its_readers(suite):
    error = suite.attempt(
        sql=BASE + "CREATE MATERIALIZED VIEW m AS SELECT * FROM a, v; CREATE MATERIALIZED VIEW r AS SELECT * FROM m"
    )
    assert error.getCondition() == "AMBIGUOUS_REFERENCE"
    assert suite.exists("m")


def test_a_bare_outer_is_an_alias_of_the_left_table(suite):
    rows = suite.spark.sql("SELECT OUTER.id FROM RANGE(1) OUTER JOIN RANGE(1) AS b ON true").collect()
    assert [row.id for row in rows] == [0]


def test_an_aliased_table_cannot_take_a_bare_outer(suite):
    with pytest.raises(Exception, match=r"(?i)syntax|OUTER"):
        suite.spark.sql("SELECT * FROM RANGE(1) a OUTER JOIN RANGE(1) b ON true").collect()


def test_values_widen_a_float_and_a_decimal_to_a_double(suite):
    frame = suite.spark.sql("SELECT * FROM VALUES (1, CAST('NaN' AS DOUBLE)), (2, 2.5) AS t(a, b) ORDER BY a")
    assert frame.schema["b"].dataType.simpleString() == "double"
    assert frame.collect()[1].b == 2.5  # noqa: PLR2004
    frame = suite.spark.sql("SELECT * FROM VALUES (1, 1.5F), (2, 2.5) AS t(a, b) ORDER BY a")
    assert frame.schema["b"].dataType.simpleString() == "double"


def test_values_with_arrays_that_differ_in_nullability(suite):
    rows = suite.spark.sql(
        "SELECT * FROM VALUES (1, array(CAST('NaN' AS FLOAT), 1.0F)), (2, array(1.0F, 2.0F)) AS t(id, c) ORDER BY id"
    ).collect()
    assert rows[1].c == [1.0, 2.0]


@pytest.mark.skipif(is_jvm_spark(), reason="needs a server with delta-spark on its classpath")
def test_a_delta_write_statement_as_select_has_no_rows(suite, tmp_path):
    location = (tmp_path / "ctas").as_uri()
    frame = suite.spark.sql(f"CREATE TABLE d USING delta LOCATION '{location}' AS SELECT 1 AS id")
    assert frame.collect() == []
    assert frame.schema.names == []
