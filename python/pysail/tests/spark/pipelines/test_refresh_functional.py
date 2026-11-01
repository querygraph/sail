"""PipelineRefreshFunctionalSuite, through the public client."""

from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import IllegalArgumentException
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run

PIPELINE = """
CREATE STREAMING TABLE a AS SELECT id FROM STREAM source_data;
CREATE STREAMING TABLE b AS SELECT id FROM STREAM source_data;
CREATE MATERIALIZED VIEW mv AS SELECT id FROM a
"""
OVERLAP = "Datasets specified for refresh and full refresh cannot overlap"


class Refresh:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.storage = storage

    def graph(self):
        graph = create_dataflow_graph(self.spark, self.spark.catalog.currentCatalog(), self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        registry.register_sql(PIPELINE, Path("pipeline.sql"))
        return graph

    def run(self, *, full=None, full_all=False, refresh=None):
        stream = start_run(self.spark, self.graph(), full, full_all, refresh, False, self.storage)
        return [
            item["pipeline_event_result"].event.message for item in stream if "pipeline_event_result" in item
        ]

    def populate_then_replace_source(self):
        self.run()
        for table in ["a", "b", "mv"]:
            assert self.ids(table) == [1]
        self.spark.sql(f"INSERT OVERWRITE TABLE {self.schema}.source_data SELECT * FROM VALUES (2), (3) AS t(id)")

    def ids(self, table):
        return sorted(row.id for row in self.spark.table(f"{self.schema}.{table}").collect())

    def name(self, table):
        return f"{self.spark.catalog.currentCatalog()}.{self.schema}.{table}"


@pytest.fixture
def refresh(spark, tmp_path, request):
    schema = "rf_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    spark.sql(f"CREATE TABLE {schema}.source_data AS SELECT * FROM RANGE(1, 2)")
    yield Refresh(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_selective_full_refresh(refresh):
    refresh.populate_then_replace_source()
    events = refresh.run(full=["a"])
    assert refresh.ids("a") == [2, 3]
    assert refresh.ids("b") == [1]
    assert refresh.ids("mv") == [1]
    joined = "\n".join(events)
    assert f"Flow '{refresh.name('b')}' is EXCLUDED." in joined
    assert f"Flow {refresh.name('a')} has COMPLETED." in joined
    assert f"Flow '{refresh.name('mv')}' is EXCLUDED." in joined
    assert "Run is COMPLETED" in joined


def test_selective_full_refresh_and_selective_refresh(refresh):
    refresh.populate_then_replace_source()
    refresh.run(full=["a", "mv"], refresh=["b"])
    assert refresh.ids("a") == [2, 3]
    assert refresh.ids("b") == [1, 2, 3]
    assert refresh.ids("mv") == [2, 3]


def test_refresh_by_default(refresh):
    refresh.populate_then_replace_source()
    refresh.run()
    assert refresh.ids("a") == [1, 2, 3]
    assert refresh.ids("b") == [1, 2, 3]
    assert refresh.ids("mv") == [1, 2, 3]


def test_full_refresh_all(refresh):
    refresh.populate_then_replace_source()
    refresh.run(full_all=True)
    for table in ["a", "b", "mv"]:
        assert refresh.ids(table) == [2, 3]


@pytest.mark.parametrize(
    ("options", "message"),
    [
        (
            {"full_all": True, "refresh": ["a"]},
            "Cannot specify a subset to refresh when full refresh all is set to true",
        ),
        (
            {"full_all": True, "full": ["a"]},
            "Cannot specify a subset to full refresh when full refresh all is set to true",
        ),
        ({"refresh": ["a"], "full": ["a"]}, OVERLAP),
        ({"refresh": ["a", "b"], "full": ["a"]}, OVERLAP),
    ],
)
def test_invalid_selections(refresh, options, message):
    with pytest.raises(IllegalArgumentException, match=message):
        refresh.run(**options)


def test_a_qualified_and_a_bare_name_are_the_same_table(refresh):
    with pytest.raises(IllegalArgumentException, match=OVERLAP):
        refresh.run(refresh=[refresh.name("a")], full=["a"])


def run_events(spark, schema, storage, sql, dry=False, refresh=None):
    graph = create_dataflow_graph(spark, spark.catalog.currentCatalog(), schema, None)
    registry = SparkConnectGraphElementRegistry(spark, graph)
    registry.register_sql(sql, Path("pipeline.sql"))
    events = []
    error = None
    try:
        for item in start_run(spark, graph, None, False, refresh, dry, storage):
            if "pipeline_event_result" in item:
                events.append(item["pipeline_event_result"].event.message)
    except Exception as caught:  # noqa: BLE001 the failure is an observation
        error = caught
    return events, error


def statuses(refresh, events, table):
    """A flow's lifecycle words, as FlowProgressEventLogger reports them."""
    name = refresh.name(table)
    words = []
    for event in events:
        if event.startswith((f"Flow {name} ", f"Flow '{name}' ")):
            words.append(event.split("\n")[0].rstrip(".").split(" ")[-1])
    return words


def test_a_batch_flow_reports_planning_and_a_streaming_flow_does_not(refresh):
    events = refresh.run()
    assert statuses(refresh, events, "mv") == ["QUEUED", "PLANNING", "STARTING", "RUNNING", "COMPLETED"]
    assert statuses(refresh, events, "a") == ["QUEUED", "STARTING", "RUNNING", "COMPLETED"]
    assert events[-1] == "Run is COMPLETED."


def test_a_dry_run_reports_only_its_end(refresh, spark, tmp_path):
    events, error = run_events(
        spark, refresh.schema, (tmp_path / "dry").as_uri(), "CREATE MATERIALIZED VIEW m AS SELECT 1 AS x", dry=True
    )
    assert error is None
    assert events == ["Run is COMPLETED."]


def test_a_failed_flow_retries_skips_its_readers_and_spares_the_rest(refresh, spark, tmp_path):
    events, error = run_events(
        spark,
        refresh.schema,
        (tmp_path / "fail").as_uri(),
        "CREATE MATERIALIZED VIEW f1 AS SELECT 1 / (id - 1) AS d FROM source_data;"
        "CREATE TEMPORARY VIEW v AS SELECT d FROM f1;"
        "CREATE MATERIALIZED VIEW f2 AS SELECT d FROM v;"
        "CREATE MATERIALIZED VIEW f3 AS SELECT id FROM source_data",
    )
    assert error is not None
    assert statuses(refresh, events, "f1").count("FAILED") == 3
    assert statuses(refresh, events, "f2") == ["QUEUED", "failure(s)"]
    assert statuses(refresh, events, "f3")[-1] == "COMPLETED"


def test_retries_follow_the_flow_configuration(refresh, spark, tmp_path):
    events, error = run_events(
        spark,
        refresh.schema,
        (tmp_path / "once").as_uri(),
        "SET spark.sql.pipelines.maxFlowRetryAttempts = 0;"
        "CREATE MATERIALIZED VIEW f1 AS SELECT 1 / (id - 1) AS d FROM source_data",
    )
    assert error is not None
    assert statuses(refresh, events, "f1").count("FAILED") == 1
