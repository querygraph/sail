"""MaterializeTablesSuite cases that pysail/tests/spark/pipelines/ does not cover yet.

The Scala suite calls `materializeGraph`, which creates tables and stops. A client
has only a real run, which also runs the flows, so a streaming table here is fed
by a stream: the Scala suite's batch queries into streaming tables never have to
pass validation. A table's columns are read with `catalog.listColumns`, and its
comment and partitioning through the catalog, in place of `TableCatalog.loadTable`.

`MemoryStream` is replaced by a parquet table read as a file stream. No case here
depends on what the stream delivers, only on the schema it declares, so the
substitution proves the same thing. The `rate` source is replaced too: it never
ends, and a client's run has to.

Already covered elsewhere, and so not ported:
- temporary views don't get materialized: test_batch_dependencies_and_view_isolation
- Invalid table properties: test_pipeline_table_properties
- Complete tables should not evolve schema (both): test_materialized_view_refresh_replaces_schema_in_place
- tables with arrays and maps, and nested: test_nested_table_schemas_after_two_materializations
"""

from pathlib import Path

import pyspark.pipelines as dp
import pytest

from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run

MERGE = "Failed to merge incompatible data types"
PARTITIONS = "CANNOT_UPDATE_PARTITION_COLUMNS"


def row_order(row):
    return tuple((value is None, 0 if value is None else value) for value in row)


class Suite:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.catalog = spark.catalog.currentCatalog()
        self.storage = storage

    def name(self, dataset):
        return f"{self.catalog}.{self.schema}.{dataset}"

    def build(self, sql=None, python=None):
        graph = create_dataflow_graph(self.spark, self.catalog, self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        if sql is not None:
            registry.register_sql(sql, Path("pipeline.sql"))
        if python is not None:
            with (
                add_pipeline_analysis_context(spark=self.spark, dataflow_graph_id=graph, flow_name=None),
                graph_element_registration_context(registry),
            ):
                exec(python, {"spark": self.spark, "dp": dp})  # noqa: S102 the graph definition under test
        return graph

    def run(self, sql=None, python=None, *, full=None, full_all=False, refresh=None):
        stream = start_run(self.spark, self.build(sql, python), full, full_all, refresh, False, self.storage)
        return [item["pipeline_event_result"].event.message for item in stream if "pipeline_event_result" in item]

    def fails(self, sql=None, python=None):
        with pytest.raises(Exception) as error:  # noqa: PT011 an unnamed Connect error carries the text
            self.run(sql, python)
        return str(error.value)

    def sql(self, statement):
        return self.spark.sql(statement.format(schema=self.schema))

    def source(self, ddl, values=None, table="src"):
        self.spark.sql(f"CREATE TABLE {self.schema}.{table}({ddl}) USING parquet")
        if values:
            self.spark.sql(f"INSERT INTO {self.schema}.{table} VALUES {values}")

    def columns(self, table):
        listed = self.spark.catalog.listColumns(f"{self.schema}.{table}")
        return [(column.name, column.dataType) for column in listed]

    def comments(self, table):
        listed = self.spark.catalog.listColumns(f"{self.schema}.{table}")
        return [column.description for column in listed]

    def nullable(self, table):
        listed = self.spark.catalog.listColumns(f"{self.schema}.{table}")
        return [column.nullable for column in listed]

    def partitions(self, table):
        listed = self.spark.catalog.listColumns(f"{self.schema}.{table}")
        return [column.name for column in listed if column.isPartition]

    def clusters(self, table):
        listed = self.spark.catalog.listColumns(f"{self.schema}.{table}")
        return [column.name for column in listed if column.isCluster]

    def exists(self, table):
        return self.spark.catalog.tableExists(self.name(table))

    def rows(self, table):
        return sorted((tuple(row) for row in self.spark.table(f"{self.schema}.{table}").collect()), key=row_order)

    def properties(self, table):
        return {row.key: row.value for row in self.spark.sql(f"SHOW TBLPROPERTIES {self.schema}.{table}").collect()}

    def table_comment(self, table):
        return self.spark.catalog.getTable(self.name(table)).description


@pytest.fixture
def suite(spark, tmp_path, request):
    schema = "mt_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Suite(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_basic(suite):
    for first, second in [("comment1", "comment2"), ("comment3", "comment4")]:
        suite.run(
            f"""
            CREATE MATERIALIZED VIEW a (x INT NOT NULL COMMENT '{first}', x2 INT COMMENT '{second}')
            COMMENT 'p-comment' AS SELECT * FROM VALUES (1, 1), (2, 3) AS t(x, x2)
            """
        )
        assert suite.columns("a") == [("x", "int"), ("x2", "int")]
        assert suite.nullable("a") == [False, True]
        assert suite.comments("a") == [first, second]
        assert suite.table_comment("a") == "p-comment"

    suite.run(
        """
        CREATE MATERIALIZED VIEW a (x INT NOT NULL, x2 INT)
        COMMENT 'p-comment' AS SELECT * FROM VALUES (1, 1), (2, 3) AS t(x, x2)
        """
    )
    assert suite.columns("a") == [("x", "int"), ("x2", "int")]
    assert suite.nullable("a") == [False, True]
    assert suite.comments("a") == [None, None]
    assert suite.table_comment("a") == "p-comment"


def test_multiple(suite):
    suite.run(
        """
        CREATE MATERIALIZED VIEW t1 AS SELECT * FROM VALUES (1), (2), (3) AS t(x);
        CREATE MATERIALIZED VIEW t2 AS SELECT * FROM VALUES ('a'), ('b') AS t(y)
        """
    )
    assert suite.columns("t1") == [("x", "int")]
    assert suite.columns("t2") == [("y", "string")]


def test_batch_flow_reading_from_a_streaming_table(suite):
    suite.source("value BIGINT, timestamp TIMESTAMP", "(1, TIMESTAMP '2020-01-01 00:00:00')")
    table = "CREATE STREAMING TABLE a AS SELECT * FROM STREAM(src);"

    # Defines a column called timestamp as a long.
    suite.run(f"{table} CREATE MATERIALIZED VIEW b AS SELECT value AS timestamp FROM a")
    assert suite.columns("b") == [("timestamp", "bigint")]

    # Defines a column called timestamp as a timestamp.
    suite.run(f"{table} CREATE MATERIALIZED VIEW b AS SELECT timestamp FROM a")
    assert suite.columns("b") == [("timestamp", "timestamp")]


def test_schema_matches_an_existing_table_schema(suite):
    suite.sql("CREATE TABLE {schema}.t2(x INT)")
    suite.source("x INT", "(1), (2), (3)")
    suite.run("CREATE STREAMING TABLE t2 AS SELECT * FROM STREAM(src)")
    assert suite.columns("t2") == [("x", "int")]
    assert suite.rows("t2") == [(1,), (2,), (3,)]


def test_invalid_schema_merge(suite):
    suite.source("value INT", "(1), (2)", table="ints")
    suite.source("value STRING", "('a'), ('b')", table="strings")
    suite.run("CREATE STREAMING TABLE b AS SELECT value AS x FROM STREAM(ints)")
    text = suite.fails("CREATE STREAMING TABLE b AS SELECT value AS x FROM STREAM(strings)")
    assert MERGE in text


def test_a_table_is_materialized_with_a_specified_schema_even_if_different_from_inferred(suite):
    suite.sql("CREATE TABLE {schema}.t4(x INT)")
    declared = "x INT COMMENT 'this is column x', z BIGINT COMMENT 'this is column z'"

    # The Scala query's SMALLINT stops a client in validation, which `materializeGraph` skips.
    text = suite.fails(f"CREATE MATERIALIZED VIEW t4 ({declared}) AS SELECT CAST(1 AS SMALLINT) AS x")
    assert "USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE" in text
    assert suite.columns("t4") == [("x", "int")]

    # With an INT, the table is materialized. Then the flow fails, since z has no input, which
    # is past where the Scala test stops; see test_missing_declared_columns_fail_after_table_materialization.
    suite.fails(f"CREATE MATERIALIZED VIEW t4 ({declared}) AS SELECT * FROM VALUES (1), (2) AS t(x)")
    assert suite.columns("t4") == [("x", "int"), ("z", "bigint")]
    assert suite.comments("t4") == ["this is column x", "this is column z"]


def test_a_specified_schema_incompatible_with_an_existing_table(suite):
    suite.sql("CREATE TABLE {schema}.t6(x BOOLEAN)")
    suite.source("x INT", "(1), (2)")
    text = suite.fails("CREATE STREAMING TABLE t6 (x INT) AS SELECT * FROM STREAM(src)")
    assert MERGE in text
    assert suite.columns("t6") == [("x", "boolean")]

    # Works for a complete table.
    suite.run("CREATE MATERIALIZED VIEW t6 (x INT) AS SELECT * FROM VALUES (1), (2) AS t(x)")
    assert suite.columns("t6") == [("x", "int")]


def test_partition_columns_with_a_user_schema(suite):
    suite.source("x1 INT, x2 INT", "(1, 1), (2, 3)")
    suite.run("CREATE STREAMING TABLE a (x1 INT, x2 INT) PARTITIONED BY (x2) AS SELECT * FROM STREAM(src)")
    assert suite.columns("a") == [("x1", "int"), ("x2", "int")]
    assert suite.partitions("a") == ["x2"]


def test_specifying_a_partition_column_with_an_existing_partitioned_table(suite):
    suite.sql("CREATE TABLE {schema}.t7(x BOOLEAN, y INT) PARTITIONED BY (x)")
    assert suite.partitions("t7") == ["x"]
    suite.source("x BOOLEAN, y INT", "(true, 1), (false, 3)")

    # The same partition column.
    suite.run("CREATE STREAMING TABLE t7 PARTITIONED BY (x) AS SELECT * FROM STREAM(src)")
    assert suite.columns("t7") == [("y", "int"), ("x", "boolean")]
    assert suite.partitions("t7") == ["x"]

    # No partition column.
    assert PARTITIONS in suite.fails("CREATE STREAMING TABLE t7 AS SELECT * FROM STREAM(src)")
    assert suite.columns("t7") == [("y", "int"), ("x", "boolean")]
    assert suite.partitions("t7") == ["x"]


def test_specifying_a_partition_column_different_from_an_existing_partitioned_table(suite):
    suite.sql("CREATE TABLE {schema}.t8(x BOOLEAN, y INT) PARTITIONED BY (x)")
    suite.source("x BOOLEAN, y INT", "(true, 1), (false, 3)")
    assert PARTITIONS in suite.fails("CREATE STREAMING TABLE t8 PARTITIONED BY (y) AS SELECT * FROM STREAM(src)")
    assert suite.partitions("t8") == ["x"]


def test_table_properties_are_set_when_a_table_gets_materialized(suite):
    suite.source("value INT", "(1)")
    suite.run(
        """
        CREATE STREAMING TABLE a TBLPROPERTIES ('pipelines.reset.allowed' = 'true', 'some.prop' = 'foo')
        AS SELECT * FROM STREAM(src);
        CREATE STREAMING TABLE b TBLPROPERTIES ('pipelines.reset.alloweD' = 'true', 'some.prop' = 'foo')
        AS SELECT * FROM STREAM(a)
        """
    )
    for table in ["a", "b"]:
        properties = suite.properties(table)
        assert properties["pipelines.reset.allowed"] == "true"
        assert properties["some.prop"] == "foo"


def test_materialization_succeeds_with_unknown_pipeline_properties_on_the_existing_table(suite):
    # The Scala test creates t9 and then materializes an unrelated table, so it never reaches t9.
    suite.sql("CREATE TABLE {schema}.t9(x INT) TBLPROPERTIES ('pipelines.someProperty' = 'foo')")
    suite.source("x INT", "(1)")
    suite.run("CREATE STREAMING TABLE t9 AS SELECT * FROM STREAM(src)")
    assert suite.rows("t9") == [(1,)]


@pytest.mark.parametrize("full_refresh", [False, True])
def test_streaming_tables_evolve_schema_only_without_a_full_refresh(suite, full_refresh):
    suite.source("value INT", "(1), (2), (3), (4)")
    suite.run("CREATE STREAMING TABLE b AS SELECT value AS x FROM STREAM(src)")
    assert suite.columns("b") == [("x", "int")]

    suite.run("CREATE STREAMING TABLE b AS SELECT value AS y FROM STREAM(src)", full_all=full_refresh)
    if full_refresh:
        assert suite.columns("b") == [("y", "int")]
    else:
        assert suite.columns("b") == [("x", "int"), ("y", "int")]


def test_materialize_only_selected_tables(suite):
    suite.run(
        """
        CREATE MATERIALIZED VIEW a AS SELECT * FROM VALUES (1, 2), (2, 3) AS t(x, y);
        CREATE MATERIALIZED VIEW b AS SELECT x FROM a;
        CREATE MATERIALIZED VIEW c AS SELECT y FROM a
        """,
        refresh=["a"],
        full=["c"],
    )
    assert suite.columns("a") == [("x", "int"), ("y", "int")]
    assert not suite.exists("b")
    assert suite.columns("c") == [("y", "int")]


CLUSTER_SOURCE = "x1 INT, x2 INT, x3 STRING", "(1, 1, 'x'), (2, 3, 'y')"
CLUSTER_COLUMNS = [("x1", "int"), ("x2", "int"), ("x3", "string")]
THREE_COLUMNS = "x INT, y BOOLEAN, z STRING", "(1, true, 'a'), (2, false, 'b')"


def clustered(name, arguments):
    # Python, since Spark's SQL registration refuses CLUSTER BY with an invalid partitioning transform.
    return f"""
@dp.table(name = "{name}"{arguments})
def query():
  return spark.readStream.table("src")
"""


def test_cluster_columns_with_a_user_schema(suite):
    suite.source(*CLUSTER_SOURCE)
    suite.run(python=clustered("a", ', schema = "x1 INT, x2 INT, x3 STRING", cluster_by = ["x1", "x3"]'))
    assert suite.columns("a") == CLUSTER_COLUMNS
    assert suite.clusters("a") == ["x1", "x3"]


def test_specifying_cluster_columns_with_an_existing_clustered_table(suite):
    suite.source(*THREE_COLUMNS)
    suite.run(python=clustered("t10", ', cluster_by = ["x", "z"]'))
    assert suite.clusters("t10") == ["x", "z"]

    # The same cluster columns.
    suite.run(python=clustered("t10", ', cluster_by = ["x", "z"]'))
    assert suite.clusters("t10") == ["x", "z"]

    # None, on a table that has them.
    assert PARTITIONS in suite.fails(python=clustered("t10", ""))


def test_specifying_cluster_columns_different_from_an_existing_clustered_table(suite):
    suite.source(*THREE_COLUMNS)
    suite.run(python=clustered("t11", ', cluster_by = ["x"]'))
    assert PARTITIONS in suite.fails(python=clustered("t11", ', cluster_by = ["y"]'))
    assert suite.clusters("t11") == ["x"]


def test_cluster_columns_only(suite):
    suite.source(*CLUSTER_SOURCE)
    suite.run(python=clustered("t12", ', schema = "x1 INT, x2 INT, x3 STRING", cluster_by = ["x1", "x3"]'))
    assert suite.columns("t12") == CLUSTER_COLUMNS
    assert suite.partitions("t12") == []
    assert suite.clusters("t12") == ["x1", "x3"]


def test_a_materialized_view_with_cluster_columns(suite):
    suite.run(
        python="""
@dp.materialized_view(cluster_by = ["x1", "x2"])
def mv1():
  return spark.sql("SELECT * FROM VALUES (1, 1, 'x'), (2, 3, 'y') AS t(x1, x2, x3)")
"""
    )
    assert suite.columns("mv1") == CLUSTER_COLUMNS
    assert suite.clusters("mv1") == ["x1", "x2"]


def test_partition_and_cluster_columns_together_fail(suite):
    suite.source(*CLUSTER_SOURCE)
    text = suite.fails(
        python="""
@dp.table(partition_cols = ["x2"], cluster_by = ["x1", "x3"])
def invalid_table():
  return spark.readStream.table("src")
"""
    )
    assert "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED" in text


def test_a_cluster_column_missing_from_the_table_schema_fails(suite):
    suite.source(*CLUSTER_SOURCE)
    text = suite.fails(python=clustered("invalid_cluster_table", ', cluster_by = ["nonexistent_column"]'))
    assert "nonexistent_column" in text
