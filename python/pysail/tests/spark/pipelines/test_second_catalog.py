"""The SqlPipelineSuite cases that cross catalogs, against a server with a second catalog.

Spark's suite uses InMemoryTableCatalog for the other catalog, a Spark test class. On Sail the other catalog
is a second memory catalog, so these run only against a server configured with one. See
scripts/spark-pipelines-tests/run-second-catalog.sh.
"""

import os
from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException, SparkConnectGrpcException
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run

from pysail.testing.spark.utils.common import is_jvm_spark

OTHER_CATALOG = "sdp_other"

pytestmark = pytest.mark.skipif(
    is_jvm_spark() or OTHER_CATALOG not in os.environ.get("SAIL_CATALOG__LIST", ""),
    reason=f"needs a Sail server with a second memory catalog named {OTHER_CATALOG}",
)


class Space:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.catalog = spark.catalog.currentCatalog()
        self.schema = schema
        self.storage = storage
        self.database2 = f"{schema}_d2"
        self.other_database = f"{schema}_o"

    def run(self, *files, catalog=None, schema=None):
        graph = create_dataflow_graph(self.spark, catalog or self.catalog, schema or self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        for index, sql in enumerate(files):
            registry.register_sql(sql, Path(f"file{index}.sql"))
        return [
            item["pipeline_event_result"].event.message
            for item in start_run(self.spark, graph, None, False, None, False, self.storage)
            if "pipeline_event_result" in item
        ]

    def rows(self, name):
        return sorted(tuple(row) for row in self.spark.sql(f"SELECT * FROM {name}").collect())


@pytest.fixture
def space(spark, tmp_path, request):
    # Creating a graph moves the session to the graph default, on Spark too (PipelinesHandler), and this
    # session lasts for the module, so each test starts and ends in the pipeline catalog.
    home = spark.catalog.currentCatalog(), spark.catalog.currentDatabase()
    assert home[0] != OTHER_CATALOG
    schema = "sc_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:40]
    result = Space(spark, schema, (tmp_path / "storage").as_uri())
    names = [schema, result.database2, f"{OTHER_CATALOG}.{result.other_database}"]
    for name in names:
        spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")
        spark.sql(f"CREATE SCHEMA {name}")
    yield result
    spark.catalog.setCurrentCatalog(home[0])
    spark.catalog.setCurrentDatabase(home[1])
    for name in names:
        spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")


def test_use_database_and_set_catalog(space):
    """SqlPipelineSuite "Use database and set catalog works", its two files and five reads."""
    pipeline, schema, database2 = space.catalog, space.schema, space.database2
    space.run(
        f"""
-- Create table in default (pipeline) catalog and database
     CREATE MATERIALIZED VIEW mv AS SELECT * FROM RANGE(3);

-- Change database
USE DATABASE {database2};

-- Create mv2 under new database, implicitly
CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM RANGE(1, 5);

-- Unqualified names in subquery should implicitly use current database.
-- Fully qualified names in subquery should use specified database.
CREATE MATERIALIZED VIEW mv3 AS
WITH mv AS (SELECT * FROM {pipeline}.{schema}.mv)
SELECT mv2.id FROM
mv JOIN mv2 ON mv.id=mv2.id;

-- Change to database that lives in another catalog. Same behavior expected.
SET CATALOG {OTHER_CATALOG};
USE DATABASE {space.other_database};

-- Temporary views should always be created in the pipeline catalog and database.
CREATE TEMPORARY VIEW tv AS SELECT * FROM {pipeline}.{schema}.mv;

CREATE MATERIALIZED VIEW mv4 AS
WITH mv2 AS (SELECT * FROM {pipeline}.{database2}.mv2)
SELECT * FROM STREAM(mv2) WHERE mv2.id % 2 == 0;

-- Use namespace command should also work, setting both catalog and database.
USE NAMESPACE {pipeline}.{database2};
-- mv2 was originally created in this same namespace, so implicit qualification should work.
CREATE MATERIALIZED VIEW mv5 AS SELECT * FROM mv2;

-- Temp views should always resolve to pipeline catalog and database.
CREATE MATERIALIZED VIEW mv6 AS SELECT * FROM tv;
""",
        f"""
-- The previous file's current catalog/database should not impact other files.
CREATE MATERIALIZED VIEW mv6 AS SELECT * FROM {pipeline}.{database2}.mv5;
""",
    )
    assert space.rows(f"{pipeline}.{database2}.mv3") == [(1,), (2,)]
    assert space.rows(f"{OTHER_CATALOG}.{space.other_database}.mv4") == [(2,), (4,)]
    assert space.rows(f"{database2}.mv5") == [(1,), (2,), (3,), (4,)]
    assert space.rows(f"{database2}.mv6") == [(0,), (1,), (2,)]
    assert space.rows(f"{pipeline}.{schema}.mv6") == [(1,), (2,), (3,), (4,)]


@pytest.mark.parametrize(
    ("upstream", "downstream"),
    [
        ("upstream_mv", "downstream_mv"),
        ("upstream_mv2", "{database2}.downstream_mv2"),
        ("upstream_mv3", "{other}.downstream_mv3"),
        ("{database2}.upstream_mv4", "downstream_mv4"),
        ("{other}.upstream_mv5", "downstream_mv5"),
        ("{other}.upstream_mv6", "{database2}.downstream_mv6"),
    ],
)
def test_writing_and_reading_from_qualified_names(space, upstream, downstream):
    """SqlPipelineSuite "Writing/reading datasets from fully and partially qualified names works"."""
    names = {"database2": space.database2, "other": f"{OTHER_CATALOG}.{space.other_database}"}
    upstream = upstream.format(**names)
    downstream = downstream.format(**names)
    space.run(
        f"""
CREATE MATERIALIZED VIEW {upstream} (id BIGINT) AS SELECT * FROM RANGE(10);
CREATE MATERIALIZED VIEW {downstream} AS SELECT id FROM {upstream}
WHERE (id%2)=0;
"""
    )
    assert space.rows(downstream) == [(0,), (2,), (4,), (6,), (8,)]
    space.spark.sql(f"DROP TABLE {upstream}")
    space.spark.sql(f"DROP TABLE {downstream}")


def test_set_catalog_clears_the_database_of_the_file(space):
    """Spark's SetCatalogCommandHandler clears it too, so an unqualified name needs a USE after SET CATALOG."""
    with pytest.raises(AnalysisException, match="current pipeline database is unset"):
        space.run(f"SET CATALOG {OTHER_CATALOG}; CREATE MATERIALIZED VIEW mv AS SELECT 1 AS id;")


def test_an_output_in_a_missing_catalog_names_it(space):
    """Spark's handler resolves only the catalog expression, so the name is not checked at SET CATALOG."""
    with pytest.raises(SparkConnectGrpcException, match="sdp_missing"):
        space.run(f"SET CATALOG sdp_missing; CREATE MATERIALIZED VIEW {space.schema}.mv AS SELECT 1 AS id;")


def test_set_catalog_in_a_file_does_not_move_the_session(space):
    space.run(
        f"""
SET CATALOG {OTHER_CATALOG};
USE DATABASE {space.other_database};
CREATE MATERIALIZED VIEW a AS SELECT 7 AS id;
"""
    )
    # The graph default, where creating the graph put the session, and not where the file went.
    assert space.spark.catalog.currentCatalog() == space.catalog
    assert space.spark.catalog.currentDatabase() == space.schema
    assert space.rows(f"{OTHER_CATALOG}.{space.other_database}.a") == [(7,)]
    assert not space.spark.catalog.tableExists(f"{space.catalog}.{space.schema}.a")


def test_a_graph_whose_default_is_the_second_catalog(space):
    """Unqualified outputs follow the graph default, and a name can still reach the session's catalog."""
    space.spark.sql(f"CREATE TABLE {space.schema}.src (id BIGINT) USING parquet")
    space.spark.sql(f"INSERT INTO {space.schema}.src VALUES (1), (2)")
    space.run(
        f"""
CREATE MATERIALIZED VIEW copied AS SELECT * FROM {space.catalog}.{space.schema}.src;
CREATE MATERIALIZED VIEW doubled AS SELECT id * 2 AS id FROM copied;
""",
        catalog=OTHER_CATALOG,
        schema=space.other_database,
    )
    assert space.spark.catalog.currentCatalog() == OTHER_CATALOG
    assert space.rows(f"{OTHER_CATALOG}.{space.other_database}.doubled") == [(2,), (4,)]
    assert not space.spark.catalog.tableExists(f"{space.catalog}.{space.schema}.doubled")


def test_a_streaming_table_crosses_catalogs_both_ways(space):
    """Spark leaves streaming out, since InMemoryTableCatalog cannot store a stream. Sail's catalogs can."""
    other = f"{OTHER_CATALOG}.{space.other_database}"
    space.spark.sql(f"CREATE TABLE {space.schema}.src (id BIGINT) USING parquet")
    space.spark.sql(f"INSERT INTO {space.schema}.src VALUES (1), (2)")
    space.run(
        f"""
CREATE STREAMING TABLE {other}.st AS SELECT * FROM STREAM {space.schema}.src;
CREATE STREAMING TABLE back AS SELECT * FROM STREAM {other}.st;
"""
    )
    assert space.rows(f"{other}.st") == [(1,), (2,)]
    assert space.rows(f"{space.catalog}.{space.schema}.back") == [(1,), (2,)]
    assert not space.spark.catalog.tableExists(f"{space.catalog}.{space.schema}.st")


def test_a_flow_writes_into_the_other_catalog(space):
    other = f"{OTHER_CATALOG}.{space.other_database}"
    space.spark.sql(f"CREATE TABLE {space.schema}.src (id BIGINT) USING parquet")
    space.spark.sql(f"INSERT INTO {space.schema}.src VALUES (3)")
    space.run(
        f"""
CREATE STREAMING TABLE {other}.target;
CREATE FLOW f AS INSERT INTO {other}.target BY NAME
SELECT * FROM STREAM {space.schema}.src;
"""
    )
    assert space.rows(f"{other}.target") == [(3,)]
