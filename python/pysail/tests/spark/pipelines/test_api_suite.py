"""APITest, the end to end suite behind the spark-pipelines CLI, through the public client.

Each case registers the same SQL and Python sources. The CLI's own spec file is not
involved, and selective refresh is covered by test_refresh_functional.py.
"""

from pathlib import Path

import pyspark.pipelines as dp
import pytest
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run


class Api:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.storage = storage

    def run(self, *sources, dry=False):
        graph = create_dataflow_graph(self.spark, self.spark.catalog.currentCatalog(), self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        for index, source in enumerate(sources):
            if source.lstrip().startswith(("from ", "@", "dp.")):
                scope = {"spark": self.spark, "dp": dp}
                with (
                    add_pipeline_analysis_context(spark=self.spark, dataflow_graph_id=graph, flow_name=None),
                    graph_element_registration_context(registry),
                ):
                    exec(source, scope)  # noqa: S102 the pipeline source under test
            else:
                registry.register_sql(source, Path(f"definition_{index}.sql"))
        list(start_run(self.spark, graph, None, False, None, dry, self.storage))

    def rows(self, table):
        return sorted(tuple(row) for row in self.spark.table(f"{self.schema}.{table}").collect())

    def sql(self, text):
        return self.spark.sql(text.format(schema=self.schema))


FIVE = [(i,) for i in range(5)]


@pytest.fixture
def api(spark, tmp_path, request):
    schema = "ap_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Api(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_sql_with_mv_st_and_flows(api):
    api.run(
        "CREATE STREAMING TABLE st; CREATE FLOW f AS INSERT INTO st BY NAME SELECT * FROM STREAM mv WHERE id > 2",
        "CREATE MATERIALIZED VIEW mv AS SELECT * FROM RANGE(5)",
    )
    assert api.rows("st") == [(3,), (4,)]


def test_sql_with_cte(api):
    api.run(
        """
        CREATE MATERIALIZED VIEW a AS SELECT 1;
        CREATE MATERIALIZED VIEW d AS
        WITH c AS (WITH b AS (SELECT * FROM a) SELECT * FROM b)
        SELECT * FROM c
        """
    )
    assert api.rows("d") == [(1,)]


def test_sql_with_subquery(api):
    api.run(
        """
        CREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(5);
        CREATE MATERIALIZED VIEW b AS SELECT * FROM RANGE(5) WHERE id = (SELECT max(id) FROM a)
        """
    )
    assert api.rows("b") == [(4,)]


def test_sql_with_join(api):
    api.run(
        """
        CREATE TEMPORARY VIEW a AS SELECT id FROM range(1, 3);
        CREATE TEMPORARY VIEW b AS SELECT id FROM range(1, 3);
        CREATE MATERIALIZED VIEW c AS SELECT a.id AS id1, b.id AS id2 FROM a JOIN b ON a.id = b.id
        """
    )
    assert api.rows("c") == [(1, 1), (2, 2)]


def test_sql_with_aggregation(api):
    api.run(
        """
        CREATE MATERIALIZED VIEW a AS SELECT id AS value, (id % 2) AS isOdd FROM range(1, 10);
        CREATE MATERIALIZED VIEW b AS SELECT isOdd, max(value) AS maximum FROM a GROUP BY isOdd LIMIT 2
        """
    )
    assert api.rows("b") == [(0, 8), (1, 9)]


def properties(api, table):
    rows = api.sql("SHOW TBLPROPERTIES {schema}." + table).collect()
    return {row[0]: row[1] for row in rows}


def test_sql_with_table_properties(api):
    api.run(
        """
        CREATE MATERIALIZED VIEW mv TBLPROPERTIES ('prop1' = 'foo1', 'prop2' = 'bar2') AS SELECT 1 AS x;
        CREATE STREAMING TABLE st TBLPROPERTIES ('prop3' = 'foo3', 'prop4' = 'bar4') AS SELECT * FROM STREAM(mv)
        """
    )
    assert {"prop1": "foo1", "prop2": "bar2"}.items() <= properties(api, "mv").items()
    assert {"prop3": "foo3", "prop4": "bar4"}.items() <= properties(api, "st").items()


def test_sql_with_schema(api):
    api.run(
        """
        CREATE MATERIALIZED VIEW a (id LONG COMMENT 'comment') AS SELECT * FROM RANGE(5);
        CREATE STREAMING TABLE b (id LONG COMMENT 'comment') AS SELECT * FROM STREAM a
        """
    )
    for table in "ab":
        columns = api.spark.catalog.listColumns(f"{api.schema}.{table}")
        assert [(column.name, column.description) for column in columns] == [("id", "comment")]


def test_python_and_sql_together(api):
    api.run(
        "CREATE STREAMING TABLE c; CREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(5)",
        """
@dp.append_flow(target = "c", name = "append_to_c")
def flow():
  return spark.readStream.table("b").filter("id >= 3")

@dp.materialized_view
def b():
  return spark.read.table("a")
""",
    )
    assert api.rows("b") == FIVE
    assert api.rows("c") == [(3,), (4,)]


def test_referencing_internal_datasets(api):
    api.run(
        """
@dp.materialized_view
def src():
  return spark.range(5)
""",
        """
@dp.materialized_view
def a():
  return spark.read.table("src")

@dp.table
def b():
  return spark.readStream.table("src")
""",
        "CREATE STREAMING TABLE c; CREATE FLOW f AS INSERT INTO c BY NAME SELECT * FROM STREAM b WHERE id > 2",
    )
    assert api.rows("a") == FIVE
    assert api.rows("b") == FIVE
    assert api.rows("c") == [(3,), (4,)]


def test_referencing_external_datasets(api):
    api.sql("CREATE TABLE {schema}.src AS SELECT * FROM RANGE(5)")
    api.run(
        f"""
@dp.materialized_view
def a():
  return spark.read.table("{api.schema}.src")

@dp.table
def b():
  return spark.readStream.table("{api.schema}.src")
""",
        "CREATE STREAMING TABLE c; CREATE FLOW f AS INSERT INTO c BY NAME SELECT * FROM STREAM b WHERE id > 2",
    )
    assert api.rows("a") == FIVE
    assert api.rows("b") == FIVE
    assert api.rows("c") == [(3,), (4,)]


def test_the_spark_placeholder_and_an_explicit_session(api):
    api.run(
        """
from pyspark.sql import SparkSession
spark = SparkSession.active()

@dp.materialized_view
def mv():
  return spark.range(5)
"""
    )
    assert api.rows("mv") == FIVE


def test_materialized_view_streaming_table_and_append_flow(api):
    api.run(
        """
dp.create_streaming_table(name = "a", schema = "id LONG", comment = "streaming table a")

@dp.append_flow(target = "a", name = "append_to_a")
def flow():
  return spark.readStream.table("src")
""",
        """
@dp.materialized_view(name = "src", comment = "source table", schema = "id LONG")
def irrelevant():
  return spark.range(5)
""",
    )
    assert api.rows("a") == FIVE


def test_a_temporary_view(api):
    api.run(
        """
@dp.temporary_view(name = "view_1", comment = "temporary view 1")
def irrelevant():
  return spark.range(5)

@dp.materialized_view(name = "mv_1")
def irrelevant_1():
  return spark.read.table("view_1")
"""
    )
    assert api.rows("mv_1") == FIVE


def test_cluster_columns(api):
    api.run(
        """
from pyspark.sql.functions import col

@dp.materialized_view(cluster_by = ["cluster_col1"])
def mv():
  df = spark.range(10)
  df = df.withColumn("cluster_col1", col("id") % 3)
  df = df.withColumn("cluster_col2", col("id") % 2)
  return df

@dp.table(cluster_by = ["cluster_col1"])
def st():
  return spark.readStream.table("mv")
"""
    )
    expected = [(i, i % 3, i % 2) for i in range(10)]
    for table in ["mv", "st"]:
        assert api.rows(table) == expected
        columns = api.spark.catalog.listColumns(f"{api.schema}.{table}")
        assert [column.name for column in columns if column.isCluster] == ["cluster_col1"]


def test_a_dry_run_creates_nothing(api):
    api.run(
        "CREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(5); CREATE MATERIALIZED VIEW b AS SELECT * FROM a WHERE id > 2",
        dry=True,
    )
    assert not api.spark.catalog.tableExists(f"{api.schema}.a")
    assert not api.spark.catalog.tableExists(f"{api.schema}.b")
