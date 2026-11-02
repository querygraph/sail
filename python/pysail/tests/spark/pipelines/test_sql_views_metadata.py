"""The rest of SqlPipelineSuite and ViewSuite, and SystemMetadataSuite, through the public client."""

from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run


class Space:
    def __init__(self, spark, schema, root):
        self.spark = spark
        self.schema = schema
        self.root = root
        self.catalog = spark.catalog.currentCatalog()

    def run(self, sql, *, full_all=False, dry=False):
        graph = create_dataflow_graph(self.spark, self.catalog, self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        registry.register_sql(sql, Path("pipeline.sql"))
        stream = start_run(self.spark, graph, None, full_all, None, dry, self.root.as_uri())
        return [item["pipeline_event_result"].event.message for item in stream if "pipeline_event_result" in item]

    def rows(self, table):
        return sorted(tuple(row) for row in self.spark.table(f"{self.schema}.{table}").collect())

    def full(self, name):
        return f"{self.catalog}.{self.schema}.{name}"

    def versions(self, *parts):
        return sorted(path.name for path in self.root.joinpath("_checkpoints", *parts).glob("*"))


@pytest.fixture
def space(spark, tmp_path, request):
    schema = "sv_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    spark.sql(f"CREATE TABLE {schema}.t1 AS SELECT * FROM range(3)")
    yield Space(spark, schema, tmp_path / "storage")
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_a_view_can_depend_on_a_streaming_table(space):
    space.run("CREATE STREAMING TABLE a AS SELECT * FROM STREAM(t1); CREATE MATERIALIZED VIEW b AS SELECT * FROM a")
    assert space.rows("b") == [(0,), (1,), (2,)]


def test_tables_in_other_schemas_can_be_read(space, spark):
    other = f"{space.schema}_o"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {other}")
    try:
        spark.sql(f"CREATE TABLE {other}.tbl AS SELECT * FROM range(1, 4)")
        space.run(
            f"CREATE MATERIALIZED VIEW a AS SELECT * FROM {other}.tbl;"
            f"CREATE STREAMING TABLE b AS SELECT * FROM STREAM({other}.tbl);"
            "CREATE TEMPORARY VIEW c AS SELECT * FROM a"
        )
        for table in "ab":
            assert space.rows(table) == [(1,), (2,), (3,)]
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {other} CASCADE")


def test_a_streaming_table_can_be_fed_only_by_flows(space):
    space.run("CREATE STREAMING TABLE st; CREATE FLOW f AS INSERT INTO st BY NAME SELECT * FROM STREAM t1")
    assert space.rows("st") == [(0,), (1,), (2,)]


def test_partition_columns_through_sql(space):
    space.run(
        "CREATE MATERIALIZED VIEW mv PARTITIONED BY (id_mod) AS SELECT id, id % 2 AS id_mod FROM range(5);"
        "CREATE STREAMING TABLE st PARTITIONED BY (id_mod) AS SELECT id, id_mod FROM STREAM(mv)"
    )
    for table in ["mv", "st"]:
        assert sorted((row.id, int(row.id_mod)) for row in space.spark.table(f"{space.schema}.{table}").collect()) == [
            (i, i % 2) for i in range(5)
        ]


def test_a_persisted_view_of_a_missing_dataset_fails(space):
    with pytest.raises(AnalysisException) as error:
        space.run("CREATE VIEW myview AS SELECT * FROM nonexistent_view")
    assert "myview" in str(error.value)


def test_a_persisted_view_cannot_read_a_stream(space):
    with pytest.raises(AnalysisException) as error:
        space.run(
            "CREATE STREAMING TABLE source AS SELECT * FROM STREAM(t1);"
            "CREATE VIEW v AS SELECT * FROM STREAM(source)"
        )
    assert error.value.getCondition() == "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_PERSISTED_VIEW"


def test_a_persisted_view_can_read_a_streaming_table(space):
    space.run("CREATE STREAMING TABLE myst AS SELECT * FROM STREAM(t1); CREATE VIEW v AS SELECT * FROM myst")
    assert space.rows("v") == [(0,), (1,), (2,)]


def test_a_failed_persisted_view_skips_its_reader_and_the_run_completes(space):
    events = space.run("CREATE VIEW pv2 AS SELECT * FROM pv1; CREATE VIEW pv1 AS SELECT 1 + 1")
    joined = "\n".join(events)
    assert f"Flow '{space.full('pv1')}' has FAILED." in joined
    assert "CREATE_PERMANENT_VIEW_WITHOUT_ALIAS" in joined
    assert f"Flow '{space.full('pv2')}' has been processed by a previous iteration" in joined
    assert events[-1] == "Run is COMPLETED."
    assert not space.spark.catalog.tableExists(f"{space.schema}.pv1")


@pytest.mark.parametrize(("full_refresh", "expected"), [(False, ["0"]), (True, ["0", "1"])])
def test_a_flow_checkpoint_version_follows_full_refresh(space, full_refresh, expected):
    graph = "CREATE STREAMING TABLE st AS SELECT * FROM STREAM(t1)"
    space.run(graph)
    parts = [space.catalog, space.schema, "st", "st"]
    assert space.versions(*parts) == ["0"]
    space.run(graph, full_all=full_refresh)
    assert space.versions(*parts) == expected


def test_a_flow_with_its_own_name_checkpoints_under_it(space):
    graph = "CREATE STREAMING TABLE st; CREATE FLOW f AS INSERT INTO st BY NAME SELECT * FROM STREAM t1"
    space.run(graph)
    assert space.versions(space.catalog, space.schema, "st", "f") == ["0"]


def test_tables_with_one_name_in_two_schemas_do_not_share_checkpoints(space, spark):
    other = f"{space.schema}_o"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {other}")
    try:
        space.run(
            f"CREATE STREAMING TABLE st AS SELECT * FROM STREAM(t1);"
            f"CREATE STREAMING TABLE {other}.st AS SELECT * FROM STREAM(t1)"
        )
        assert space.versions(space.catalog, space.schema, "st", "st") == ["0"]
        assert space.versions(space.catalog, other, "st", "st") == ["0"]
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {other} CASCADE")


def test_a_sink_flow_checkpoint_version_follows_full_refresh(space, tmp_path):
    from pyspark import pipelines as dp  # noqa: PLC0415
    from pyspark.pipelines.graph_element_registry import graph_element_registration_context  # noqa: PLC0415

    def run(full_all):
        graph = create_dataflow_graph(space.spark, space.catalog, space.schema, None)
        registry = SparkConnectGraphElementRegistry(space.spark, graph)
        with graph_element_registration_context(registry):
            dp.create_sink("sink", format="parquet", options={"path": (tmp_path / "out").as_posix()})

            @dp.append_flow(target="sink")
            def to_sink():
                return space.spark.readStream.table(f"{space.schema}.t1")

        list(start_run(space.spark, graph, None, full_all, None, False, space.root.as_uri()))

    run(False)
    assert space.versions("sink", "to_sink") == ["0"]
    run(True)
    assert space.versions("sink", "to_sink") == ["0", "1"]


def test_a_watermarked_window_is_released_by_a_later_event(space, spark):
    source = f"{space.schema}.events"
    spark.sql(f"CREATE TABLE {source} (id STRING, eventTime TIMESTAMP) USING parquet")
    for name, second in [("a", 1), ("b", 2), ("a", 3)]:
        spark.sql(f"INSERT INTO {source} VALUES ('{name}', timestamp_seconds({second}))")
    graph = f"""
CREATE STREAMING TABLE b AS
SELECT CAST(window.start AS LONG) AS wStart, CAST(window.end AS LONG) AS wEnd, id, count(*) AS cnt
FROM STREAM {source} WATERMARK eventTime DELAY OF INTERVAL 10 seconds
GROUP BY window(eventTime, '5 seconds'), id
"""
    space.run(graph)
    assert space.rows("b") == []
    spark.sql(f"INSERT INTO {source} VALUES ('a', timestamp_seconds(20))")
    space.run(graph)
    assert space.rows("b") == [(0, 5, "a", 2), (0, 5, "b", 1)]
    # A later run neither repeats a closed window nor loses an open one.
    spark.sql(f"INSERT INTO {source} VALUES ('a', timestamp_seconds(40))")
    space.run(graph)
    assert space.rows("b") == [(0, 5, "a", 2), (0, 5, "b", 1), (20, 25, "a", 1)]


# Spark refuses a persisted view whose output has an auto-generated column name
# (verifyAutoGeneratedAliasesNotExists), wherever in the query that column comes from.
ALIAS_SHAPES = {
    "expression under LIMIT": ("SELECT 1 + 1 FROM t1 LIMIT 1", False),
    "named under LIMIT": ("SELECT id FROM t1 LIMIT 1", True),
    "CTE with an unnamed column": ("WITH s AS (SELECT 1 + 1) SELECT * FROM s", False),
    "count(*)": ("SELECT count(*) FROM t1", False),
    "named aggregate": ("SELECT count(*) AS n FROM t1", True),
    "GROUP BY with count(*)": ("SELECT id, count(*) FROM t1 GROUP BY id", False),
    "max(id)": ("SELECT max(id) FROM t1", False),
    "UNION ALL of named columns": ("SELECT * FROM t1 UNION ALL SELECT * FROM t1", True),
    "UNION ALL of expressions": ("SELECT 1 + 1 FROM t1 UNION ALL SELECT 2 + 2 FROM t1", False),
    "DISTINCT expression": ("SELECT DISTINCT id + 1 FROM t1", False),
    "expression over a subquery": ("SELECT id + 1 FROM (SELECT id FROM t1) sub", False),
    "named over a subquery": ("SELECT id + 1 AS next FROM (SELECT id FROM t1) sub", True),
}


@pytest.mark.parametrize("shape", list(ALIAS_SHAPES))
def test_a_persisted_view_needs_named_columns_wherever_they_come_from(space, shape):
    query, accepted = ALIAS_SHAPES[shape]
    events = space.run(f"CREATE VIEW v AS {query}")
    joined = "\n".join(events)
    exists = space.spark.catalog.tableExists(f"{space.schema}.v")
    if accepted:
        assert exists, joined
        assert "CREATE_PERMANENT_VIEW_WITHOUT_ALIAS" not in joined
    else:
        assert "CREATE_PERMANENT_VIEW_WITHOUT_ALIAS" in joined, joined
        assert not exists
