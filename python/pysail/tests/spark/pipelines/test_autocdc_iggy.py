"""A pipeline reading Change Data Capture rows from a real Apache Iggy broker.

Needs `SAIL_IGGY_SERVER_BINARY` to name an `iggy-server` built at the pinned
revision; the workspace does not build the broker.
"""

from __future__ import annotations

import base64
import json
import os
import socket
import struct
import subprocess
import time
import urllib.error
import urllib.request

import pytest
from pyspark.errors.exceptions.base import PySparkException
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from pyspark.sql import functions as sf

try:
    from pyspark import pipelines as dp
    from pyspark.pipelines.graph_element_registry import graph_element_registration_context
except ImportError:
    pytest.skip("Declarative pipelines require Spark 4.1+", allow_module_level=True)

PASSWORD = "sail-test-password"  # noqa: S105
STREAM = "cdc"
TOPIC = "events"
SCHEMA = "id BIGINT, name STRING, op STRING, seq BIGINT"


def _free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class Broker:
    def __init__(self, tcp_port, http_port):
        self.url = f"iggy://iggy:{PASSWORD}@127.0.0.1:{tcp_port}"
        self._http = f"http://127.0.0.1:{http_port}"
        self._token = None

    def _request(self, method, path, body=None):
        request = urllib.request.Request(  # noqa: S310
            self._http + path,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        if self._token is not None:
            request.add_header("Authorization", f"Bearer {self._token}")
        with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
            content = response.read()
        return json.loads(content) if content else None

    def login(self):
        reply = self._request("POST", "/users/login", {"username": "iggy", "password": PASSWORD})
        self._token = reply["access_token"]["token"]

    def create_topic(self):
        self._request("POST", "/streams", {"name": STREAM})
        self._request("POST", f"/streams/{STREAM}/topics", {"name": TOPIC, "partitions_count": 1})

    def delete_topic(self):
        self._request("DELETE", f"/streams/{STREAM}/topics/{TOPIC}")

    def send(self, *rows):
        partition = base64.b64encode(struct.pack("<I", 0)).decode()
        messages = [{"payload": base64.b64encode(json.dumps(row).encode()).decode()} for row in rows]
        self._request(
            "POST",
            f"/streams/{STREAM}/topics/{TOPIC}/messages",
            {"partitioning": {"kind": "partition_id", "value": partition}, "messages": messages},
        )


@pytest.fixture
def broker(tmp_path):
    binary = os.environ.get("SAIL_IGGY_SERVER_BINARY")
    if binary is None:
        pytest.skip("SAIL_IGGY_SERVER_BINARY is not set")
    tcp_port, http_port = _free_port(), _free_port()
    environment = {
        **os.environ,
        "IGGY_PATH": str(tmp_path / "iggy"),
        "IGGY_TCP_ADDRESS": f"127.0.0.1:{tcp_port}",
        "IGGY_HTTP_ADDRESS": f"127.0.0.1:{http_port}",
        "IGGY_ROOT_USERNAME": "iggy",
        "IGGY_ROOT_PASSWORD": PASSWORD,
        "IGGY_QUIC_ENABLED": "false",
        "IGGY_WEBSOCKET_ENABLED": "false",
        "IGGY_SHARDING_CPU_ALLOCATION": "0..2",
        "IGGY_SHARDING_PIN_CORES": "false",
    }
    with (tmp_path / "iggy.log").open("wb") as log:
        process = subprocess.Popen([binary], env=environment, cwd=tmp_path, stdout=log, stderr=log)  # noqa: S603
    try:
        broker = Broker(tcp_port, http_port)
        deadline = time.monotonic() + 60
        while True:
            try:
                broker.login()
                break
            except (urllib.error.URLError, ConnectionError):
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.1)
        broker.create_topic()
        yield broker
    finally:
        process.kill()
        process.wait()


def _define(pipeline_spark, registry, url, topic=TOPIC):
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            return (
                pipeline_spark.readStream.format("iggy")
                .option("url", url)
                .option("stream", STREAM)
                .option("topic", topic)
                .option("timeout_ms", "5000")
                .schema(SCHEMA)
                .load()
            )

        dp.create_streaming_table("target", format="delta")
        dp.create_auto_cdc_flow(
            target="target",
            source="changes",
            keys=["id"],
            sequence_by="seq",
            apply_as_deletes=sf.expr("op = 'D'"),
            except_column_list=["op"],
        )


def _rows(pipeline_spark):
    rows = pipeline_spark.table("target").select("id", "name", "seq").orderBy("id").collect()
    return [(row.id, row.name, row.seq) for row in rows]


def _change(key, name, seq, op="U"):
    return {"id": key, "name": name, "op": op, "seq": seq}


def test_a_pipeline_applies_a_topic_and_later_runs_apply_only_what_is_new(pipeline_spark, pipeline_factory, broker):
    broker.send(_change(1, "alice", 1), _change(2, "bob", 1), _change(1, "alicia", 2))

    def run():
        _, registry, start = pipeline_factory()
        _define(pipeline_spark, registry, broker.url)
        start()

    run()
    assert _rows(pipeline_spark) == [(1, "alicia", 2), (2, "bob", 1)]

    broker.send(_change(2, "", 2, op="D"), _change(3, "carol", 1), _change(1, "stale", 1))
    run()
    assert _rows(pipeline_spark) == [(1, "alicia", 2), (3, "carol", 1)]

    run()
    assert _rows(pipeline_spark) == [(1, "alicia", 2), (3, "carol", 1)]


def test_a_recreated_topic_stops_the_run_and_leaves_the_target(pipeline_spark, pipeline_factory, broker):
    broker.send(_change(1, "alice", 1))

    def run():
        _, registry, start = pipeline_factory()
        _define(pipeline_spark, registry, broker.url)
        start()

    run()
    broker.delete_topic()
    broker._request("POST", f"/streams/{STREAM}/topics", {"name": TOPIC, "partitions_count": 1})  # noqa: SLF001
    broker.send(_change(9, "other history", 1))
    with pytest.raises((PySparkException, SparkConnectGrpcException)):
        run()
    assert _rows(pipeline_spark) == [(1, "alice", 1)]


def test_a_missing_topic_is_refused_before_the_first_write(pipeline_spark, pipeline_factory, broker):
    _, registry, start = pipeline_factory()
    _define(pipeline_spark, registry, broker.url, topic="absent")
    with pytest.raises((PySparkException, SparkConnectGrpcException), match="absent"):
        start()
    assert not pipeline_spark.catalog.tableExists("target")


def test_an_iggy_topic_cannot_be_read_as_a_batch(pipeline_spark, broker):
    frame = (
        pipeline_spark.read.format("iggy")
        .option("url", broker.url)
        .option("stream", STREAM)
        .option("topic", TOPIC)
        .schema(SCHEMA)
        .load()
    )
    with pytest.raises((PySparkException, SparkConnectGrpcException), match="pipeline Auto CDC flows"):
        frame.collect()


def test_an_iggy_view_read_by_an_ordinary_flow_is_not_executed(pipeline_spark, pipeline_factory, broker):
    _, registry, start = pipeline_factory()
    url = broker.url
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            return (
                pipeline_spark.readStream.format("iggy")
                .option("url", url)
                .option("stream", STREAM)
                .option("topic", TOPIC)
                .schema(SCHEMA)
                .load()
            )

        @dp.table
        def target():
            return pipeline_spark.readStream.table("changes")

    with pytest.raises((PySparkException, SparkConnectGrpcException), match="incremental pipeline execution"):
        start()
