"""PythonPipelineSuite's graph tests, through the public client.

The Scala suite inspects the resolved graph directly. Here each graph is built
with the same Python and checked by what a client can see: a dry run accepts
or rejects it, and a real run produces its tables. Whether a flow is streaming
is covered by the read mode checks, and event locations are not ported.
"""

import pyspark.pipelines as dp
import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


class Suite:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.catalog = spark.catalog.currentCatalog()
        self.storage = storage

    def build(self, body):
        graph = create_dataflow_graph(self.spark, self.catalog, self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        scope = {"spark": self.spark, "dp": dp, "F": F, "catalog": self.catalog, "schema": self.schema}
        scope.update(StructType=StructType, StructField=StructField, LongType=LongType, StringType=StringType)
        with (
            add_pipeline_analysis_context(spark=self.spark, dataflow_graph_id=graph, flow_name=None),
            graph_element_registration_context(registry),
        ):
            exec(body, scope)  # noqa: S102 the graph definition under test
        return graph

    def dry(self, body):
        list(start_run(self.spark, self.build(body), None, False, None, True, self.storage))

    def run(self, body):
        list(start_run(self.spark, self.build(body), None, False, None, False, self.storage))

    def rows(self, table):
        return sorted(tuple(row) for row in self.spark.table(f"{self.schema}.{table}").collect())


@pytest.fixture
def suite(spark, tmp_path, request):
    schema = "py_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Suite(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


RATE = "spark.readStream.format('rate').load()"


def test_basic(suite):
    suite.dry(f"@dp.table\ndef table1():\n  return {RATE}\n")


def test_inverted_topological_order(suite):
    suite.run(
        """
@dp.table()
def b():
  return spark.readStream.table("a")

@dp.table()
def c():
  return spark.readStream.table("a")

@dp.materialized_view()
def d():
  return spark.read.table("a")

@dp.materialized_view()
def a():
  return spark.range(5)
"""
    )
    for table in "abcd":
        assert suite.rows(table) == [(i,) for i in range(5)]


def test_flows(suite):
    suite.dry(
        f"""
@dp.table()
def a():
  return {RATE}

@dp.append_flow(target = "a")
def supplement():
  return {RATE}
"""
    )


def test_external_sink_registers(suite):
    suite.build(
        f"""
dp.create_sink("myKafkaSink", format = "kafka", options = {{"kafka.bootstrap.servers": "host1:port1,host2:port2"}})

@dp.append_flow(target = "myKafkaSink")
def mySinkFlow():
  return {RATE}
"""
    )


INTERNAL = """
@dp.materialized_view
def src():
  return spark.range(5)

@dp.materialized_view
def a():
  return spark.read.table("src")

@dp.table
def b():
  return spark.readStream.table("src")

@dp.materialized_view
def c():
  return spark.sql("SELECT * FROM src")

@dp.table
def d():
  return spark.sql("SELECT * FROM STREAM src")
"""


def test_referencing_internal_datasets(suite):
    suite.run(INTERNAL)
    for table in ["a", "b", "c", "d"]:
        assert suite.rows(table) == [(i,) for i in range(5)]


def test_referencing_external_datasets(suite, spark):
    spark.sql(f"CREATE TABLE {suite.schema}.ext AS SELECT * FROM RANGE(5)")
    name = f"{suite.catalog}.{suite.schema}.ext"
    suite.run(
        f"""
@dp.materialized_view
def a():
  return spark.read.table("{name}")

@dp.materialized_view
def b():
  return spark.table("{name}")

@dp.table
def c():
  return spark.readStream.table("{name}")

@dp.materialized_view
def d():
  return spark.sql("SELECT * FROM {name}")

@dp.table
def e():
  return spark.sql("SELECT * FROM STREAM {name}")
"""
    )
    for table in "abcde":
        assert suite.rows(table) == [(i,) for i in range(5)]


def test_referencing_a_missing_dataset_fails(suite):
    body = """
@dp.table
def a():
  return spark.read.table("src")

@dp.table
def b():
  return spark.table("src")

@dp.table
def c():
  return spark.readStream.table("src")

@dp.materialized_view
def d():
  return spark.sql("SELECT * FROM src")

@dp.table
def e():
  return spark.sql("SELECT * FROM STREAM src")
"""
    with pytest.raises(AnalysisException) as error:
        suite.dry(body)
    assert "Failed to resolve flows in the pipeline" in str(error.value)


def test_reading_external_datasets_outside_a_query_function(suite, spark):
    spark.sql(f"CREATE TABLE {suite.schema}.ext AS SELECT * FROM RANGE(5)")
    name = f"{suite.catalog}.{suite.schema}.ext"
    suite.run(
        f"""
spark_sql_df = spark.sql("SELECT * FROM {name}")
read_table_df = spark.read.table("{name}")

@dp.materialized_view
def mv_from_spark_sql_df():
  return spark_sql_df

@dp.materialized_view
def mv_from_read_table_df():
  return read_table_df
"""
    )
    assert suite.rows("mv_from_spark_sql_df") == [(i,) for i in range(5)]
    assert suite.rows("mv_from_read_table_df") == [(i,) for i in range(5)]


def test_reading_internal_datasets_outside_a_query_function(suite):
    suite.run(
        """
@dp.materialized_view
def src():
  return spark.range(5)

read_table_df = spark.read.table("src")

@dp.materialized_view
def mv_from_read_table_df():
  return read_table_df
"""
    )
    assert suite.rows("mv_from_read_table_df") == [(i,) for i in range(5)]


@pytest.mark.parametrize("command", ['spark.sql("SELECT * FROM src")', 'spark.read.table("src").collect()'])
def test_eager_reads_of_internal_datasets_outside_a_query_function_fail(suite, command):
    body = f"""
@dp.materialized_view
def src():
  return spark.range(5)

spark_sql_df = {command}

@dp.materialized_view
def mv_from_spark_sql_df():
  return spark_sql_df
"""
    with pytest.raises(AnalysisException) as error:
        suite.build(body)
    assert "TABLE_OR_VIEW_NOT_FOUND" in str(error.value)
    assert "`src`" in str(error.value)


def test_two_datasets_with_one_name_fail(suite):
    with pytest.raises(AnalysisException) as error:
        suite.dry(
            """
@dp.materialized_view
def a():
  return spark.range(1)

@dp.materialized_view(name = "a")
def b():
  return spark.range(1)
"""
        )
    assert error.value.getCondition() == "PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT"


def test_datasets_with_qualified_names(suite, spark):
    other = f"{suite.schema}_a"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {other}")
    try:
        suite.run(
            f"""
@dp.materialized_view
def mv_1():
  return spark.range(5)

@dp.materialized_view(name = "{other}.mv_2")
def irrelevant_1():
  return spark.range(5)

@dp.materialized_view(name = "{suite.catalog}.{other}.mv_3")
def irrelevant_2():
  return spark.range(5)
"""
        )
        assert spark.table(f"{suite.schema}.mv_1").count() == 5
        assert spark.table(f"{other}.mv_2").count() == 5
        assert spark.table(f"{other}.mv_3").count() == 5
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {other} CASCADE")


def test_temporary_views(suite):
    suite.run(
        """
@dp.materialized_view
def mv_1():
  return spark.read.table("view_3")

@dp.temporary_view
def view_1():
  return spark.range(5)

@dp.temporary_view(name = "view_2")
def irrelevant_1():
  return spark.read.table("view_1")

@dp.temporary_view(name = "view_3")
def irrelevant_2():
  return spark.read.table("view_2")
"""
    )
    assert suite.rows("mv_1") == [(i,) for i in range(5)]


def test_a_flow_name_cannot_have_several_parts(suite):
    with pytest.raises(AnalysisException) as error:
        suite.build(
            f"""
@dp.table
def src():
  return spark.readStream.table("src0")

@dp.append_flow(name = "some_schema.some_flow", target = "src")
def some_flow():
  return {RATE}
"""
        )
    assert "MULTIPART_FLOW_NAME_NOT_SUPPORTED" in str(error.value)


@pytest.mark.parametrize("arguments", ['target = "{schema}.a"', 'target = "{schema}.a", name = "something"'])
def test_a_flow_can_name_a_multipart_target(suite, arguments):
    arguments = arguments.format(schema=suite.schema)
    suite.dry(
        f"""
@dp.table()
def a():
  return {RATE}

@dp.append_flow({arguments})
def supplement():
  return {RATE}
"""
    )


def test_groupby_and_rollup_with_internal_datasets(suite):
    suite.run(
        """
from pyspark.sql.functions import col, sum, count

@dp.materialized_view
def src():
  return spark.range(3)

@dp.materialized_view
def groupby_with_col_result():
  return spark.read.table("src").groupBy(col("id")).agg(sum("id").alias("sum_id"), count("*").alias("cnt"))

@dp.materialized_view
def groupby_with_str_result():
  return spark.read.table("src").groupBy("id").agg(sum("id").alias("sum_id"), count("*").alias("cnt"))

@dp.materialized_view
def rollup_with_col_result():
  return spark.read.table("src").rollup(col("id")).agg(sum("id").alias("sum_id"), count("*").alias("cnt"))

@dp.materialized_view
def rollup_with_str_result():
  return spark.read.table("src").rollup("id").agg(sum("id").alias("sum_id"), count("*").alias("cnt"))
"""
    )
    for table in ["groupby_with_col_result", "groupby_with_str_result"]:
        assert sorted(row[0] for row in suite.rows(table)) == [0, 1, 2]
    for table in ["rollup_with_col_result", "rollup_with_str_result"]:
        frame = suite.spark.table(f"{suite.schema}.{table}")
        assert frame.count() == 4
        total = frame.filter("id IS NULL").collect()[0]
        assert (total[1], total[2]) == (3, 3)


def test_partition_columns_on_a_view_and_a_table(suite):
    suite.run(
        """
from pyspark.sql.functions import col

@dp.materialized_view(partition_cols = ["id_mod"])
def mv():
  return spark.range(5).withColumn("id_mod", col("id") % 2)

@dp.table(partition_cols = ["id_mod"])
def st():
  return spark.readStream.table("mv")
"""
    )
    expected = [(i, i % 2) for i in range(5)]
    for table in ["mv", "st"]:
        assert sorted((row.id, int(row.id_mod)) for row in suite.spark.table(f"{suite.schema}.{table}").collect()) == expected
        columns = suite.spark.catalog.listColumns(f"{suite.schema}.{table}")
        assert [column.name for column in columns if column.isPartition] == ["id_mod"]


def test_an_empty_cluster_by_means_no_clustering(suite):
    suite.run(
        """
from pyspark.sql.functions import col

@dp.materialized_view(cluster_by = [])
def mv():
  return spark.range(5).withColumn("id_mod", col("id") % 2)

@dp.table(cluster_by = [])
def st():
  return spark.readStream.table("mv")
"""
    )
    assert suite.spark.table(f"{suite.schema}.st").count() == 5


@pytest.mark.parametrize(
    "body",
    [
        "spark.range(1)\n",
        "@dp.temporary_view\ndef view_1():\n  return spark.range(5)\n",
        '@dp.append_flow(target = "a")\ndef flow():\n  return spark.range(5)\n',
    ],
)
def test_an_empty_pipeline_fails(suite, body):
    with pytest.raises(AnalysisException) as error:
        suite.dry(body)
    assert error.value.getCondition() == "RUN_EMPTY_PIPELINE"


@pytest.mark.parametrize(
    "schema",
    [
        '"id LONG, name STRING"',
        'StructType([StructField("id", LongType(), True), StructField("name", StringType(), True)])',
    ],
)
def test_a_declared_schema(suite, schema):
    suite.run(
        f"""
@dp.materialized_view(schema={schema})
def declared():
  return spark.range(5).withColumn("name", F.lit("test"))
"""
    )
    assert suite.spark.table(f"{suite.schema}.declared").schema.simpleString() == "struct<id:bigint,name:string>"


@pytest.mark.parametrize(
    "schema",
    [
        '"id LONG, name STRING"',
        'StructType([StructField("id", LongType(), True), StructField("name", StringType(), True)])',
    ],
)
def test_a_declared_schema_must_match(suite, schema):
    body = f"""
@dp.materialized_view(schema={schema})
def table_with_wrong_schema():
  return spark.range(5).withColumn("wrong_column", F.lit("test"))
"""
    with pytest.raises(AnalysisException) as error:
        suite.run(body)
    assert "has a user-specified schema that is incompatible" in str(error.value)
    assert "table_with_wrong_schema" in str(error.value)


@pytest.mark.parametrize("sink_format", ["parquet", "json"])
def test_a_sink_writes_a_stream_to_its_path(suite, spark, tmp_path, sink_format):
    spark.sql(f"CREATE TABLE {suite.schema}.ext AS SELECT * FROM RANGE(5)")
    target = (tmp_path / "sink").as_posix()
    body = f"""
dp.create_sink("mySink", format = "{sink_format}", options = {{"path": "{target}"}})

@dp.append_flow(target = "mySink")
def mySinkFlow():
  return spark.readStream.table("{suite.catalog}.{suite.schema}.ext")
"""
    suite.run(body)
    read = spark.read.format(sink_format).load(target)
    assert sorted(row.id for row in read.collect()) == list(range(5))
    spark.sql(f"INSERT INTO {suite.schema}.ext VALUES (5)")
    suite.run(body)
    assert sorted(row.id for row in spark.read.format(sink_format).load(target).collect()) == list(range(6))


@pytest.mark.parametrize("root", ["relative/path", "./relative/path", "pipelines", "/tmp/test", "/absolute/path"])
def test_a_storage_root_needs_a_scheme_and_an_absolute_path(suite, spark, root):
    graph = suite.build("@dp.materialized_view\ndef a():\n  return spark.range(1)\n")
    with pytest.raises(Exception) as error:  # noqa: PT011 the condition is the assertion
        list(start_run(spark, graph, None, False, None, False, root))
    assert error.value.getCondition() == "PIPELINE_STORAGE_ROOT_INVALID"
    assert root in str(error.value)
