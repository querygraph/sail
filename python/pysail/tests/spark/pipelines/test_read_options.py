"""ReadOptionsPropagationOnAnalysisSuite, through the public client.

The Scala suite reads an unused option out of the analyzed plan. A client has no plan, so these
read a file option with a visible effect: a filter that matches no file empties the read, and a
malformed streaming option fails it. Each case has a control that reads the same table without it.
"""

from pathlib import Path

import pyspark.pipelines as dp
import pytest

from pysail.testing.spark.utils.common import is_jvm_spark
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run

NO_FILE = "*.matches_no_file"


class Options:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.storage = storage

    def run(self, body, sql=None):
        graph = create_dataflow_graph(self.spark, self.spark.catalog.currentCatalog(), self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        with (
            add_pipeline_analysis_context(spark=self.spark, dataflow_graph_id=graph, flow_name=None),
            graph_element_registration_context(registry),
        ):
            exec(body, {"spark": self.spark, "dp": dp, "schema": self.schema})  # noqa: S102 the graph under test
        if sql:
            registry.register_sql(sql, Path("pipeline.sql"))
        list(start_run(self.spark, graph, None, False, None, False, self.storage))

    def external(self):
        self.spark.range(10).write.format("parquet").saveAsTable(f"{self.schema}.a")

    def count(self, table):
        return self.spark.table(f"{self.schema}.{table}").count()


@pytest.fixture
def options(spark, tmp_path, request):
    schema = "ro_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Options(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def internal_batch(read):
    return f"""
@dp.materialized_view
def a():
  return spark.createDataFrame([(1,), (2,)], ["id"])

@dp.materialized_view
def b():
  return {read}
"""


@pytest.mark.parametrize(
    ("read", "expected"),
    [
        ('spark.read.table("a")', 2),
        (f'spark.read.option("pathGlobFilter", "{NO_FILE}").table("a")', 0),
    ],
    ids=["control", "option"],
)
@pytest.mark.xfail(not is_jvm_spark(), strict=True, reason="A batch read of a graph dataset does not apply its read options")
def test_internal_batch_read_options_are_propagated(options, read, expected):
    options.run(internal_batch(read))
    assert options.count("b") == expected


def internal_stream(read):
    return f"""
@dp.table
def b():
  return spark.readStream.table(f"{{schema}}.a")

@dp.table
def c():
  return {read}
"""


@pytest.mark.parametrize(
    ("read", "expected"),
    [
        ('spark.readStream.table("b")', 10),
        (f'spark.readStream.option("pathGlobFilter", "{NO_FILE}").table("b")', 0),
    ],
    ids=["control", "option"],
)
def test_internal_stream_read_options_are_propagated(options, read, expected):
    options.external()
    options.run(internal_stream(read))
    assert options.count("c") == expected


def test_internal_stream_read_malformed_option_fails_the_flow(options):
    options.external()
    with pytest.raises(Exception, match="maxFilesPerTrigger"):
        options.run(internal_stream('spark.readStream.option("maxFilesPerTrigger", "abc").table("b")'))


@pytest.mark.parametrize(
    ("read", "expected"),
    [
        ('spark.read.table(f"{schema}.a")', 10),
        (f'spark.read.option("pathGlobFilter", "{NO_FILE}").table(f"{{schema}}.a")', 0),
    ],
    ids=["control", "option"],
)
def test_external_batch_read_options_are_propagated(options, read, expected):
    options.external()
    options.run(f"@dp.materialized_view\ndef b():\n  return {read}\n")
    assert options.count("b") == expected


@pytest.mark.parametrize(
    ("read", "expected"),
    [
        ('spark.readStream.table(f"{schema}.a")', 10),
        (f'spark.readStream.option("pathGlobFilter", "{NO_FILE}").table(f"{{schema}}.a")', 0),
    ],
    ids=["control", "option"],
)
def test_external_stream_read_options_are_propagated(options, read, expected):
    options.external()
    options.run(f"@dp.table\ndef b():\n  return {read}\n")
    assert options.count("b") == expected


def test_external_stream_read_malformed_option_fails_the_flow(options):
    options.external()
    read = 'spark.readStream.option("maxFilesPerTrigger", "abc").table(f"{schema}.a")'
    with pytest.raises(Exception, match="maxFilesPerTrigger"):
        options.run(f"@dp.table\ndef b():\n  return {read}\n")
