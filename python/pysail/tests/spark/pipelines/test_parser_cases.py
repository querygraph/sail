"""CreatePipelineDatasetAsSelectParserSuiteBase's remaining cases, through the public client.

The Scala suite inspects the parsed plan. Here the statement reaches the parser through a query or
through SQL registration, and what it parsed to is read from the rows and metadata of a real run.
"""

from pathlib import Path

import pytest

from pysail.testing.spark.utils.common import is_jvm_spark
from pyspark.errors.exceptions.connect import ParseException
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run


class Parser:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.storage = storage

    def registry(self):
        graph = create_dataflow_graph(self.spark, self.spark.catalog.currentCatalog(), self.schema, None)
        return graph, SparkConnectGraphElementRegistry(self.spark, graph)

    def run(self, sql):
        graph, registry = self.registry()
        registry.register_sql(sql, Path("pipeline.sql"))
        list(start_run(self.spark, graph, None, False, None, False, self.storage))

    def rows(self, table):
        return sorted(tuple(row) for row in self.spark.table(f"{self.schema}.{table}").collect())


@pytest.fixture
def parser(spark, tmp_path, request):
    schema = "pc_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Parser(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


KINDS = ["MATERIALIZED VIEW", "STREAMING TABLE"]


@pytest.mark.parametrize("api", ["direct", "registration"])
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.xfail(not is_jvm_spark(), strict=True, reason="Sail's grammar has no column constraints, so this is a syntax error")
def test_column_constraints_are_unsupported(parser, kind, api):
    sql = f"CREATE {kind} table1 (id INT CHECK(id > 0)) AS SELECT 1"
    with pytest.raises(ParseException) as error:
        if api == "direct":
            parser.spark.sql(sql).collect()
        else:
            parser.registry()[1].register_sql(sql, Path("parser-fixture.sql"))
    assert error.value.getCondition() == "_LEGACY_ERROR_TEMP_0035"
    assert error.value.getMessageParameters() == {
        "message": (
            "Pipeline datasets do not currently support column constraints. "
            "Please remove any CHECK, UNIQUE, PK, and FK constraints specified on the pipeline dataset."
        )
    }


def sources(parser):
    parser.spark.sql(f"CREATE TABLE {parser.schema}.input (a BIGINT) USING parquet")
    parser.spark.sql(f"INSERT INTO {parser.schema}.input VALUES (7)")
    parser.spark.sql(f"CREATE TABLE {parser.schema}.input2 (a BIGINT, b BIGINT, c BIGINT, d BIGINT) USING parquet")
    parser.spark.sql(f"INSERT INTO {parser.schema}.input2 VALUES (1, 2, 3, 4)")
    parser.spark.sql(f"CREATE TABLE {parser.schema}.orders (id BIGINT, amount BIGINT, customer_id BIGINT) USING parquet")
    parser.spark.sql(f"INSERT INTO {parser.schema}.orders VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)")
    parser.spark.sql(f"CREATE TABLE {parser.schema}.customers (id BIGINT, region STRING) USING parquet")
    parser.spark.sql(f"INSERT INTO {parser.schema}.customers VALUES (100, 'east'), (200, NULL)")


JOIN = """
SELECT o.id, o.amount, c.region
FROM {orders} o
JOIN (
  SELECT id, region
  FROM customers
  WHERE region IS NOT NULL
) AS c
ON o.customer_id = c.id
"""


@pytest.mark.parametrize(
    ("subquery", "expected"),
    [
        ("SELECT 1", [(1,)]),
        ("SELECT * FROM input", [(7,)]),
        ("SELECT a, b, c FROM input2", [(1, 2, 3)]),
        (JOIN.format(orders="orders"), [(1, 10, "east")]),
    ],
    ids=["literal", "star", "columns", "join"],
)
def test_materialized_view_subquery_is_parsed_into_the_plan(parser, subquery, expected):
    sources(parser)
    parser.run(f"\nCREATE MATERIALIZED VIEW table1 AS\n{subquery}")
    assert parser.rows("table1") == expected


@pytest.mark.parametrize(
    ("subquery", "expected"),
    [
        ("SELECT * FROM STREAM input", [(7,)]),
        ("SELECT a, b, c FROM STREAM input2", [(1, 2, 3)]),
        (JOIN.format(orders="STREAM orders"), [(1, 10, "east")]),
    ],
    ids=["star", "columns", "join"],
)
def test_streaming_table_subquery_is_parsed_into_the_plan(parser, subquery, expected):
    """A streaming table needs a stream to run, so these read one where the Scala text reads a table."""
    sources(parser)
    parser.run(f"\nCREATE STREAMING TABLE table1 AS\n{subquery}")
    assert parser.rows("table1") == expected


def collation(parser, table):
    rows = parser.spark.sql(f"DESCRIBE TABLE EXTENDED {parser.schema}.{table}").collect()
    return {row.col_name: row.data_type for row in rows}.get("Collation")


@pytest.mark.parametrize("kind", KINDS)
def test_collation_is_parsed_and_the_published_table_has_none(parser, kind):
    """DatasetManager creates every table with collation = None, so the clause parses and has no effect."""
    sources(parser)
    source = "STREAM strings" if kind == "STREAMING TABLE" else "strings"
    parser.spark.sql(f"CREATE TABLE {parser.schema}.strings (s STRING) USING parquet")
    parser.spark.sql(f"INSERT INTO {parser.schema}.strings VALUES ('a')")
    parser.run(f"CREATE {kind} table1 DEFAULT COLLATION UTF8_LCASE AS SELECT * FROM {source}")
    assert parser.rows("table1") == [("a",)]
    assert collation(parser, "table1") is None
    assert parser.spark.table(f"{parser.schema}.table1").dtypes == [("s", "string")]
