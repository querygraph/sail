"""The SqlPipelineSuite cases the existing tests leave out, through the public client."""

from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run


class Sql:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.storage = storage
        self.catalog = spark.catalog.currentCatalog()

    def run(self, *files, dry=False):
        graph = create_dataflow_graph(self.spark, self.catalog, self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        for index, sql in enumerate(files):
            registry.register_sql(sql, Path(f"file{index}.sql"))
        stream = start_run(self.spark, graph, None, False, None, dry, self.storage)
        return [item["pipeline_event_result"].event.message for item in stream if "pipeline_event_result" in item]

    def rows(self, table):
        name = table if "." in table else f"{self.schema}.{table}"
        return sorted(tuple(row) for row in self.spark.table(name).collect())

    def full(self, name):
        return f"{self.catalog}.{self.schema}.{name}"


@pytest.fixture
def space(spark, tmp_path, request):
    schema = "sq_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Sql(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


SIMPLE = """
CREATE MATERIALIZED VIEW mv AS SELECT 1;
CREATE STREAMING TABLE st AS SELECT * FROM STREAM {schema}.t1;
CREATE VIEW v AS SELECT * FROM mv;
CREATE FLOW f AS INSERT INTO {target} BY NAME
SELECT * FROM STREAM {schema}.t2;
"""


def simple_sources(space):
    space.spark.sql(f"CREATE TABLE {space.schema}.t1 AS SELECT * FROM range(3)")
    space.spark.sql(f"CREATE TABLE {space.schema}.t2 AS SELECT * FROM range(3, 6)")


def test_simple_register_sql_dataset_validates(space):
    simple_sources(space)
    messages = space.run(SIMPLE.format(schema=space.schema, target="st"), dry=True)
    assert messages[-1].startswith("Run is COMPLETED"), messages


def test_simple_register_sql_dataset_runs_its_flows(space):
    """A real run names the flows, without v's, which is published and never queued.

    f gets its own target: Spark's two flows into one file sink race.
    """
    simple_sources(space)
    messages = space.run("CREATE STREAMING TABLE st2;" + SIMPLE.format(schema=space.schema, target="st2"))
    queued = {message for message in messages if message.endswith("is QUEUED.")}
    assert queued == {f"Flow {space.full(name)} is QUEUED." for name in ["mv", "st", "f"]}
    assert space.rows("mv") == [(1,)]
    assert space.rows("v") == [(1,)]
    assert space.rows("st") == [(i,) for i in range(3)]
    assert space.rows("st2") == [(i,) for i in range(3, 6)]


def test_use_namespace_and_use_database_scope_each_file(space, spark):
    """Use database and set catalog works. InMemoryTableCatalog is a Spark test class, so one catalog is used."""
    pipeline = space.schema
    other = f"{space.schema}_o"
    spark.sql(f"CREATE SCHEMA {other}")
    try:
        space.run(
            f"""
-- Create table in default (pipeline) catalog and database
     CREATE MATERIALIZED VIEW mv AS SELECT * FROM RANGE(3);

-- Change database
USE DATABASE {other};

-- Create mv2 under new database, implicitly
CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM RANGE(1, 5);

-- Unqualified names in subquery should implicitly use current database.
CREATE MATERIALIZED VIEW mv3 AS
WITH mv AS (SELECT * FROM {space.catalog}.{pipeline}.mv)
SELECT mv2.id FROM
mv JOIN mv2 ON mv.id=mv2.id;

-- Temporary views are always created in the pipeline catalog and database.
CREATE TEMPORARY VIEW tv AS SELECT * FROM {space.catalog}.{pipeline}.mv;

-- Use namespace command should also work, setting both catalog and database.
USE NAMESPACE {space.catalog}.{other};
CREATE MATERIALIZED VIEW mv5 AS SELECT * FROM mv2;

-- Temp views resolve to the pipeline namespace despite the active one.
CREATE MATERIALIZED VIEW mv6 AS SELECT * FROM tv;
""",
            f"""
-- The previous file's current database does not impact other files.
CREATE MATERIALIZED VIEW mv6 AS SELECT * FROM {space.catalog}.{other}.mv5;
""",
        )
        assert space.rows(f"{other}.mv3") == [(1,), (2,)]
        assert space.rows(f"{other}.mv5") == [(i,) for i in range(1, 5)]
        assert space.rows(f"{other}.mv6") == [(i,) for i in range(3)]
        assert space.rows(f"{pipeline}.mv6") == [(i,) for i in range(1, 5)]
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {other} CASCADE")


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("upstream_mv", "downstream_mv"),
        ("upstream_mv2", "{other}.downstream_mv2"),
        ("{other}.upstream_mv4", "downstream_mv4"),
    ],
    ids=["both-default", "downstream-qualified", "upstream-qualified"],
)
def test_writing_and_reading_from_qualified_names(space, spark, first, second):
    """The cases that need a second catalog are left out: InMemoryTableCatalog is a Spark test class."""
    other = f"{space.schema}_o"
    spark.sql(f"CREATE SCHEMA {other}")
    upstream = first.format(other=other)
    downstream = second.format(other=other)
    try:
        space.run(
            f"""
CREATE MATERIALIZED VIEW {upstream} (id BIGINT) AS SELECT * FROM RANGE(10);
CREATE MATERIALIZED VIEW {downstream} AS SELECT id FROM {upstream}
WHERE (id%2)=0;
"""
        )
        assert space.rows(downstream) == [(0,), (2,), (4,), (6,), (8,)]
        for name in [upstream, downstream]:
            spark.sql(f"DROP TABLE {name if '.' in name else space.schema + '.' + name}")
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {other} CASCADE")


@pytest.mark.parametrize("file_format", ["parquet", "orc", "json", "csv"])
def test_a_streaming_table_reads_a_table_loaded_from_files(space, spark, tmp_path, file_format):
    """The half test_file_sources.py leaves out: a table loaded from files, and a stream of that table."""
    directory = (tmp_path / "files").as_posix()
    spark.sql("SELECT * FROM RANGE(3)").write.format(file_format).mode("overwrite").save(directory)
    spark.sql(f"CREATE TABLE {space.schema}.t AS SELECT * FROM {file_format}.`{directory}`")
    space.run(
        f"""
CREATE MATERIALIZED VIEW a AS SELECT * FROM {file_format}.`{directory}`;
CREATE STREAMING TABLE b AS SELECT * FROM STREAM {space.schema}.t
"""
    )
    expected = [("0",), ("1",), ("2",)] if file_format == "csv" else [(0,), (1,), (2,)]
    assert space.rows("a") == expected
    assert space.rows("b") == expected


def test_a_read_of_a_missing_path_reports_it(space):
    messages = []
    graph = create_dataflow_graph(space.spark, space.catalog, space.schema, None)
    registry = SparkConnectGraphElementRegistry(space.spark, graph)
    registry.register_sql("CREATE MATERIALIZED VIEW a AS SELECT * FROM csv.`/non/existing/file`", Path("a.sql"))
    with pytest.raises(AnalysisException):
        for item in start_run(space.spark, graph, None, False, None, False, space.storage):
            if "pipeline_event_result" in item:
                messages.append(item["pipeline_event_result"].event.message)
    failures = [message for message in messages if "Failed to resolve flow" in message]
    assert any("Path does not exist: file:/non/existing/file" in message for message in failures), messages
