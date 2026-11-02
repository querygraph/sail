"""Streaming tables that read an Apache Iggy topic directly, through a real broker.

Spark has no Iggy source, so these state Sail's own contract, which follows a Spark streaming
source: a first run reads from the start of the topic, and later runs read only what is new.
"""

import json
from pathlib import Path

import pyspark.pipelines as dp
import pyspark.sql.functions as sf
import pytest
from pyspark.errors import PySparkException
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from pyspark.pipelines.graph_element_registry import graph_element_registration_context

from pysail.tests.spark.pipelines.test_autocdc_iggy import STREAM, TOPIC, broker  # noqa: F401
from pysail.testing.spark.utils.common import is_jvm_spark

pytestmark = pytest.mark.skipif(is_jvm_spark(), reason="Spark has no Iggy source")

SCHEMA = "id INT, name STRING, op STRING, seq INT"
FAILED = (PySparkException, SparkConnectGrpcException)


def row(key, name, seq=1):
    return {"id": key, "name": name, "op": "U", "seq": seq}


def read(spark, url):
    return (
        spark.readStream.format("iggy")
        .option("url", url)
        .option("stream", STREAM)
        .option("topic", TOPIC)
        .option("timeout_ms", "5000")
        .schema(SCHEMA)
        .load()
    )


def direct(spark, url):
    @dp.table
    def target():
        return read(spark, url).select("id", "name")


def through_a_view(spark, url):
    @dp.temporary_view
    def changes():
        return read(spark, url)

    @dp.table
    def target():
        return spark.readStream.table("changes").select("id", "name")


@pytest.fixture
def run(pipeline_spark, pipeline_factory, broker):  # noqa: F811
    def run(define=direct, **options):
        _, registry, start = pipeline_factory()
        with graph_element_registration_context(registry):
            define(pipeline_spark, broker.url)
        start(**options)

    return run


def rows(spark):
    return sorted(tuple(item) for item in spark.table("target").collect())


@pytest.mark.parametrize("define", [direct, through_a_view], ids=["direct", "view"])
def test_a_table_reads_a_topic_and_later_runs_read_what_is_new(pipeline_spark, broker, run, define):  # noqa: F811
    broker.send(row(1, "a"), row(2, "b"))
    run(define)
    assert rows(pipeline_spark) == [(1, "a"), (2, "b")]
    broker.send(row(3, "c"))
    run(define)
    assert rows(pipeline_spark) == [(1, "a"), (2, "b"), (3, "c")]
    run(define)
    assert rows(pipeline_spark) == [(1, "a"), (2, "b"), (3, "c")]


def test_an_empty_topic_is_a_start(pipeline_spark, broker, run):  # noqa: F811
    run()
    assert rows(pipeline_spark) == []
    broker.send(row(1, "a"))
    run()
    assert rows(pipeline_spark) == [(1, "a")]


def test_a_query_filters_and_joins_a_batch_table(pipeline_spark, broker, run):  # noqa: F811
    pipeline_spark.sql("CREATE TABLE names (id INT, label STRING) USING parquet")
    pipeline_spark.sql("INSERT INTO names VALUES (1, 'x'), (2, 'y')")

    def joined(spark, url):
        @dp.table
        def target():
            return read(spark, url).where("id > 1").join(spark.read.table("names"), "id").select("id", "label")

    broker.send(row(1, "a"), row(2, "b"))
    run(joined)
    assert rows(pipeline_spark) == [(2, "y")]


def test_a_recreated_topic_stops_the_run_and_leaves_the_table(pipeline_spark, broker, run):  # noqa: F811
    broker.send(row(1, "a"))
    run()
    broker.delete_topic()
    broker._request("POST", f"/streams/{STREAM}/topics", {"name": TOPIC, "partitions_count": 1})  # noqa: SLF001
    broker.send(row(9, "other history"))
    with pytest.raises(FAILED, match="recreated"):
        run()
    assert rows(pipeline_spark) == [(1, "a")]


def test_a_full_refresh_recovers_a_recreated_topic(pipeline_spark, broker, run):  # noqa: F811
    broker.send(row(1, "a"))
    run()
    broker.delete_topic()
    broker._request("POST", f"/streams/{STREAM}/topics", {"name": TOPIC, "partitions_count": 1})  # noqa: SLF001
    broker.send(row(9, "other history"))
    run(full_refresh_all=True)
    assert rows(pipeline_spark) == [(9, "other history")]


def test_a_missing_topic_is_refused(pipeline_spark, broker, pipeline_factory):  # noqa: F811
    _, registry, start = pipeline_factory()
    url = broker.url
    with graph_element_registration_context(registry):

        @dp.table
        def target():
            return (
                pipeline_spark.readStream.format("iggy")
                .option("url", url)
                .option("stream", STREAM)
                .option("topic", "absent")
                .option("timeout_ms", "5000")
                .schema(SCHEMA)
                .load()
            )

    with pytest.raises(FAILED, match="absent"):
        start()


def test_the_checkpoint_holds_no_credentials(pipeline_spark, broker, run, tmp_path):  # noqa: F811
    broker.send(row(1, "a"))
    run()
    states = list(Path(tmp_path).rglob("state.json"))
    assert states
    text = "".join(state.read_text() for state in states)
    assert "iggy:" not in text.replace("iggy-", "")
    assert json.loads(states[0].read_text())["bounded"]


def test_a_watermarked_window_over_a_topic(pipeline_spark, broker, run):  # noqa: F811
    def windowed(spark, url):
        @dp.table
        def target():
            return (
                spark.readStream.format("iggy")
                .option("url", url)
                .option("stream", STREAM)
                .option("topic", TOPIC)
                .option("timeout_ms", "5000")
                .schema("ts TIMESTAMP, id INT")
                .load()
                .withWatermark("ts", "1 second")
                .groupBy(sf.window("ts", "2 seconds"))
                .count()
            )

    moment = "2026-01-01T00:00:0{}Z"
    broker.send({"ts": moment.format(1), "id": 1}, {"ts": moment.format(2), "id": 2})
    run(windowed)
    assert pipeline_spark.table("target").count() == 0
    broker.send({"ts": moment.format(9), "id": 9})
    run(windowed)
    assert sorted(row["count"] for row in pipeline_spark.table("target").collect()) == [1, 1]
