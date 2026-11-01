"""SchemaInferenceUtilsSuite, as a client sees it: rerun a pipeline whose schema changed.

diffSchemas is a unit under DatasetManager, which applies its changes to the table. A materialized view
takes the new schema whole. A streaming table merges it into the existing one, keeping old columns and
comments and refusing a changed type, until a full refresh replaces it. Spark's mergeSchemas accepts no
widening: INT to BIGINT is refused too. Materialized view add, drop, retype and comment changes are in
test_schema.py, so only the rest of its matrix is here.
"""

from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run

MV = "MATERIALIZED VIEW"
ST = "STREAMING TABLE"


class Change:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.storage = storage
        spark.sql(f"CREATE TABLE {schema}.src (id BIGINT, v BIGINT) USING parquet")
        spark.sql(f"INSERT INTO {schema}.src VALUES (1, 10)")

    def run(self, kind, text, full_all=False):
        graph = create_dataflow_graph(self.spark, self.spark.catalog.currentCatalog(), self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        source = f"STREAM {self.schema}.src" if kind == ST else f"{self.schema}.src"
        registry.register_sql(f"CREATE {kind} t {text.format(src=source)}", Path("pipeline.sql"))
        list(start_run(self.spark, graph, None, full_all, None, False, self.storage))

    def rerun(self, kind, first, second, full_all=False):
        """A run, a new source row, and a run of the changed definition."""
        self.run(kind, first)
        self.spark.sql(f"INSERT INTO {self.schema}.src VALUES (2, 20)")
        self.run(kind, second, full_all)

    def columns(self):
        return [(f.name, f.dataType.simpleString()) for f in self.spark.table(f"{self.schema}.t").schema]

    def comments(self):
        return [f.metadata.get("comment") for f in self.spark.table(f"{self.schema}.t").schema]

    def rows(self):
        return sorted((tuple(row) for row in self.spark.table(f"{self.schema}.t").collect()), key=str)


@pytest.fixture
def change(spark, tmp_path, request):
    schema = "sc_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Change(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


AS_INT = "AS SELECT CAST(id AS INT) AS id FROM {src}"
AS_LONG = "AS SELECT id FROM {src}"
AS_DOUBLE = "AS SELECT CAST(id AS DOUBLE) AS id FROM {src}"
AS_DECIMAL = "AS SELECT CAST(id AS DECIMAL(10,2)) AS id FROM {src}"
AS_STRING = "AS SELECT CAST(id AS STRING) AS id FROM {src}"
WITH_V = "AS SELECT id, v FROM {src}"
OLD_COMMENT = "(id BIGINT COMMENT 'old') AS SELECT id FROM {src}"
NEW_COMMENT = "(id BIGINT COMMENT 'new') AS SELECT id FROM {src}"
COMPLEX_BEFORE = "AS SELECT CAST(id AS INT) AS id, v AS name, v AS old FROM {src}"
COMPLEX_AFTER = "(id BIGINT, name BIGINT COMMENT 'Full name', fresh BIGINT) AS SELECT id, v AS name, v AS fresh FROM {src}"

DECIMALS = [(Decimal("1.00"),), (Decimal("2.00"),)]


@pytest.mark.parametrize("full_all", [False, True], ids=["refresh", "full-refresh"])
@pytest.mark.parametrize(
    ("first", "second", "columns", "rows"),
    [
        (AS_INT, AS_LONG, [("id", "bigint")], [(1,), (2,)]),
        (AS_LONG, AS_INT, [("id", "int")], [(1,), (2,)]),
        (AS_DOUBLE, AS_DECIMAL, [("id", "decimal(10,2)")], DECIMALS),
    ],
    ids=["widened", "narrowed", "double-to-decimal"],
)
def test_a_materialized_view_takes_a_changed_type(change, first, second, columns, rows, full_all):
    change.rerun(MV, first, second, full_all)
    assert change.columns() == columns
    assert change.rows() == rows


@pytest.mark.parametrize("full_all", [False, True], ids=["refresh", "full-refresh"])
def test_a_materialized_view_takes_a_mixed_change(change, full_all):
    change.rerun(MV, COMPLEX_BEFORE, COMPLEX_AFTER, full_all)
    assert change.columns() == [("id", "bigint"), ("name", "bigint"), ("fresh", "bigint")]
    assert change.comments() == [None, "Full name", None]
    assert change.rows() == [(1, 10, 10), (2, 20, 20)]


def test_a_streaming_table_adds_a_column_for_new_rows(change):
    change.rerun(ST, AS_LONG, WITH_V)
    assert change.columns() == [("id", "bigint"), ("v", "bigint")]
    assert change.rows() == [(1, None), (2, 20)]


def test_a_streaming_table_keeps_a_dropped_column(change):
    change.rerun(ST, WITH_V, AS_LONG)
    assert change.columns() == [("id", "bigint"), ("v", "bigint")]
    assert change.rows() == [(1, 10), (2, None)]


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [(OLD_COMMENT, NEW_COMMENT, "old"), (AS_LONG, NEW_COMMENT, None)],
    ids=["changed", "added"],
)
def test_a_streaming_table_keeps_its_comment_until_a_full_refresh(change, first, second, expected):
    change.rerun(ST, first, second)
    assert change.comments() == [expected]
    assert change.rows() == [(1,), (2,)]
    change.run(ST, second, full_all=True)
    assert change.comments() == ["new"]


@pytest.mark.parametrize(
    ("first", "second", "left", "right", "before"),
    [
        (AS_INT, AS_LONG, "INT", "BIGINT", "int"),
        (AS_LONG, AS_INT, "BIGINT", "INT", "bigint"),
        (AS_DOUBLE, AS_DECIMAL, "DOUBLE", "DECIMAL(10,2)", "double"),
        (AS_LONG, AS_STRING, "BIGINT", "STRING", "bigint"),
    ],
    ids=["widened", "narrowed", "double-to-decimal", "retyped"],
)
def test_a_streaming_table_refuses_a_changed_type(change, first, second, left, right, before):
    with pytest.raises(Exception, match="CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE") as error:
        change.rerun(ST, first, second)
    assert f'"{left}" and "{right}"' in str(error.value)
    assert change.columns() == [("id", before)]
    assert len(change.rows()) == 1


def test_a_streaming_table_refuses_a_mixed_change_as_a_whole(change):
    with pytest.raises(Exception, match="CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE"):
        change.rerun(ST, COMPLEX_BEFORE, COMPLEX_AFTER)
    assert change.columns() == [("id", "int"), ("name", "bigint"), ("old", "bigint")]
    assert change.comments() == [None, None, None]


@pytest.mark.parametrize(
    ("first", "second", "columns", "rows"),
    [
        (AS_LONG, WITH_V, [("id", "bigint"), ("v", "bigint")], [(1, 10), (2, 20)]),
        (WITH_V, AS_LONG, [("id", "bigint")], [(1,), (2,)]),
        (AS_INT, AS_LONG, [("id", "bigint")], [(1,), (2,)]),
        (AS_LONG, AS_STRING, [("id", "string")], [("1",), ("2",)]),
        (AS_DOUBLE, AS_DECIMAL, [("id", "decimal(10,2)")], DECIMALS),
    ],
    ids=["added", "dropped", "widened", "retyped", "double-to-decimal"],
)
def test_a_full_refresh_replaces_a_streaming_table_schema(change, first, second, columns, rows):
    change.rerun(ST, first, second, full_all=True)
    assert change.columns() == columns
    assert change.rows() == rows


def test_a_full_refresh_applies_a_mixed_change(change):
    change.rerun(ST, COMPLEX_BEFORE, COMPLEX_AFTER, full_all=True)
    assert change.columns() == [("id", "bigint"), ("name", "bigint"), ("fresh", "bigint")]
    assert change.comments() == [None, "Full name", None]
    assert change.rows() == [(1, 10, 10), (2, 20, 20)]


NOT_NULL = "(id BIGINT NOT NULL) AS SELECT coalesce(id, 0) AS id FROM {src}"
NULLABLE = "(id BIGINT) AS SELECT coalesce(id, 0) AS id FROM {src}"


@pytest.mark.parametrize("kind", [MV, ST], ids=["mv", "st"])
@pytest.mark.parametrize(("first", "second"), [(NOT_NULL, NULLABLE), (NULLABLE, NOT_NULL)], ids=["relaxed", "tightened"])
def test_a_nullability_change_keeps_the_rows(change, kind, first, second):
    """A published column reads as nullable whichever way the declaration went."""
    change.rerun(kind, first, second)
    assert change.rows() == [(1,), (2,)]
    assert [field.nullable for field in change.spark.table(f"{change.schema}.t").schema] == [True]
