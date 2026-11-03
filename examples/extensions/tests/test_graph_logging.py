"""Run capabilities must not be disclosed by Connect request debug logging."""
from pathlib import Path

from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.session import SparkSession
from pyspark_pecan import GraphUtils
from pyspark_pecan import utils_pb2 as wire
from pyspark_pecan.utils import _UtilsRelation

from conftest import start_server


def test_utils_request_capability_is_absent_from_connect_debug_logs(request, tmp_path):
    root = tmp_path / "owned-staging"
    root.mkdir()
    server = tmp_path / "server"
    binary = str(Path(request.config.getoption("--sail-binary")).resolve())
    with start_server(
        binary,
        server,
        mode=request.config.getoption("--execution-mode"),
        extra_env={
            "SAIL_GRAPH_UTILS_ROOT": root.as_uri(),
            "RUST_LOG": "sail_spark_connect=debug",
        },
    ) as endpoint:
        spark = SparkSession.builder.remote(endpoint).create()
        try:
            utils = GraphUtils(spark)
            run, token = utils.allocate()
            relation = DataFrame(
                _UtilsRelation(wire.Request(exists=wire.Exists(path=run, token=token))),
                spark,
            )
            # Both AnalyzePlan variants and ExecutePlan receive the capability.
            assert "value" in relation.columns
            relation.explain()
            assert relation.collect()[0].value
            utils.remove(run, token)
        finally:
            spark.stop()
    log = (server / "server.log").read_text()
    encoded_token = ", ".join(str(byte) for byte in token.encode())
    if token in log or encoded_token in log:
        raise AssertionError("run capability leaked through Connect debug logging")
    # Reject a vacuous pass caused by disabled logging or unexercised routes.
    assert "ExecutePlan session_id=" in log
    assert "AnalyzePlan session_id=" in log
