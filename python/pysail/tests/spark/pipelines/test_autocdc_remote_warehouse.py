"""AutoCDC with its tables in an S3 warehouse and its state on this machine."""

from __future__ import annotations

import json
import os
import shutil
import uuid

import pytest
from pyspark.errors.exceptions.base import PySparkException
from pyspark.errors.exceptions.connect import SparkConnectGrpcException

try:
    from pyspark import pipelines as dp
    from pyspark.pipelines.graph_element_registry import graph_element_registration_context
except ImportError:
    pytest.skip("Declarative pipelines require Spark 4.1+", allow_module_level=True)

from pysail.testing.spark.utils.common import is_jvm_spark

if is_jvm_spark() or os.environ.get("SPARK_REMOTE"):
    pytest.skip("the warehouse is a Sail session setting", allow_module_level=True)

boto3 = pytest.importorskip("boto3")
ThreadedMotoServer = pytest.importorskip("moto.server").ThreadedMotoServer

FAILURE = (PySparkException, SparkConnectGrpcException)
SCHEMA = "id INT, name STRING, seq BIGINT"


@pytest.fixture
def warehouse(monkeypatch, tmp_path):
    server = ThreadedMotoServer(port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    for key, value in {
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_REGION": "us-east-1",
        "AWS_ENDPOINT": endpoint,
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ALLOW_HTTP": "true",
        "SAIL_AUTOCDC_STATE_DIR": str(tmp_path / "state"),
    }.items():
        monkeypatch.setenv(key, value)
    bucket = f"wh-{uuid.uuid4().hex[:12]}"
    boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1").create_bucket(Bucket=bucket)
    yield f"s3://{bucket}/warehouse", tmp_path / "state"
    server.stop()


def run(spark, factory, landing, **options):
    _, registry, start = factory()
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():
            return spark.readStream.format("json").schema(SCHEMA).load(landing)

        dp.create_streaming_table("target", format="delta")
        dp.create_auto_cdc_flow(target="target", source="changes", keys=["id"], sequence_by="seq")
    start(**options)


def test_a_remote_warehouse_runs_and_a_lost_state_stops_it(pipeline_spark, pipeline_factory, warehouse, tmp_path):
    spark = pipeline_spark
    url, state = warehouse
    spark.conf.set("spark.sql.warehouse.dir", url)
    landing = tmp_path / "landing"
    landing.mkdir()

    (landing / "a.json").write_text(json.dumps({"id": 1, "name": "alice", "seq": 1}) + "\n")
    run(spark, pipeline_factory, str(landing))
    assert [tuple(row) for row in spark.table("target").select("id", "name").collect()] == [(1, "alice")]

    (landing / "b.json").write_text(json.dumps({"id": 1, "name": "alicia", "seq": 2}) + "\n")
    run(spark, pipeline_factory, str(landing))
    assert [tuple(row) for row in spark.table("target").select("id", "name").collect()] == [(1, "alicia")]

    shutil.rmtree(state)
    with pytest.raises(FAILURE, match="does not hold it"):
        run(spark, pipeline_factory, str(landing))
