"""Storage ownership through the real Connect boundary; no mocked filesystem API."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.session import SparkSession
from pyspark_pecan import GraphUtils
from pyspark_pecan.utils import _UtilsRelation
from pyspark_pecan import utils_pb2 as wire

from conftest import start_server


@pytest.fixture
def utils_server(request, tmp_path):
    root = tmp_path / "owned-staging"
    root.mkdir()
    binary = str(Path(request.config.getoption("--sail-binary")).resolve())
    mode = request.config.getoption("--execution-mode")
    with start_server(binary, tmp_path / "server", mode=mode,
                      extra_env={"SAIL_GRAPH_UTILS_ROOT": root.as_uri()}) as endpoint:
        spark = SparkSession.builder.remote(endpoint).create()
        try:
            yield spark, root, endpoint
        finally:
            spark.stop()


def test_analysis_is_pure_and_allocation_is_retry_safe(utils_server):
    spark, root, _ = utils_server
    utils = GraphUtils(spark)
    request_id = "2524c686-67f0-4b6b-b5cd-0b18e267180f"
    relation = DataFrame(_UtilsRelation(wire.Request(mkdir=wire.Mkdir(request_id=request_id))), spark)
    before = sorted(str(p.relative_to(root)) for p in root.rglob("*"))
    assert "token" in relation.columns
    relation.explain()
    assert sorted(str(p.relative_to(root)) for p in root.rglob("*")) == before
    first = relation.collect()[0]
    second = utils.allocate(request_id=request_id)
    assert (first.path, first.token) == second
    utils.remove(first.path, first.token)
    assert utils.remove(first.path, first.token) == 0
    with pytest.raises(Exception):
        utils.allocate(request_id=request_id)


def test_run_boundaries_listing_and_driver_worker_scalar(utils_server):
    spark, root, endpoint = utils_server
    utils = GraphUtils(spark)
    run, token = utils.allocate()
    sibling, sibling_token = utils.allocate()
    other = SparkSession.builder.remote(endpoint).create()
    try:
        # There is no direct client access in production; the test separately
        # observes the local store to catch forbidden filesystem side effects.
        for forbidden, capability in [(root.as_uri(), token), (sibling, token),
                                      (run + "/../escape", token), (run, "wrong")]:
            with pytest.raises(Exception):
                utils.remove(forbidden, capability)
        with pytest.raises(Exception):
            GraphUtils(other).remove(run, token)
        path = run + "/data"
        # Independent nonempty writes guarantee at least two committed files.
        # A requested partition count does not guarantee an output file count.
        spark.range(32, numPartitions=4).repartition(4).write.parquet(path + "/first")
        spark.range(32, numPartitions=4).repartition(4).write.parquet(path + "/second")
        assert utils.exists(path, token)
        listing = utils.ls(path, token, limit=1)
        entries = [row for row in listing if row.kind == "entry"]
        summary = [row for row in listing if row.kind == "ls"]
        assert len(entries) == 1
        assert len(summary) == 1 and summary[0].truncated
        assert all(row.path.startswith(path + "/") and row.size > 0 for row in entries)
        # Multiply by x in GF(2^64), including high-bit reduction. These inputs
        # distinguish polynomial arithmetic from integer multiplication/modulo.
        got = spark.createDataFrame([(1, -1, 0), (2, -(1 << 63), 0), (0, 42, -7)],
                                    "a long,x long,b long").repartition(4).selectExpr(
                                        "a", "gf_axpb(a,x,b) AS result").collect()
        assert {r.a: r.result for r in got} == {1: -1, 2: 27, 0: -7}
    finally:
        other.stop()
        utils.remove(run, token)
        utils.remove(sibling, sibling_token)


def test_killed_client_staging_is_removed_after_session_expiry(request, tmp_path):
    root = tmp_path / "orphan-staging"
    root.mkdir()
    binary = str(Path(request.config.getoption("--sail-binary")).resolve())
    mode = request.config.getoption("--execution-mode")
    child = None
    program = '''
import json, sys, time
from pyspark.sql.connect.session import SparkSession
from pyspark_pecan import GraphUtils
spark = SparkSession.builder.remote(sys.argv[1]).create()
utils = GraphUtils(spark)
path, token = utils.allocate()
spark.range(10, numPartitions=2).write.parquet(path + "/orphan")
print(json.dumps({"path": path}), flush=True)
time.sleep(120)
'''
    with start_server(binary, tmp_path / "server", mode=mode, extra_env={
        "SAIL_GRAPH_UTILS_ROOT": root.as_uri(), "SAIL_SPARK__SESSION_TIMEOUT_SECS": "10",
    }) as endpoint:
        try:
            with (tmp_path / "client.err").open("w") as error:
                child = subprocess.Popen([sys.executable, "-c", program, endpoint],
                                         stdout=subprocess.PIPE, stderr=error, text=True,
                                         env=dict(os.environ))
                # Bound waiting without blocking on a dead child's stdout.
                import selectors
                with selectors.DefaultSelector() as ready:
                    ready.register(child.stdout, selectors.EVENT_READ)
                    assert ready.select(timeout=90), "orphan client did not finish its write"
                    line = child.stdout.readline()
                assert line, (tmp_path / "client.err").read_text()
                path = json.loads(line)["path"]
                from urllib.parse import unquote, urlparse
                directory = Path(unquote(urlparse(path).path))
                assert list(directory.rglob("*.parquet")), "fixture must create real stage files"
                child.kill()
                child.wait(timeout=10)
                deadline = time.monotonic() + 60
                while list(directory.rglob("*.parquet")) and time.monotonic() < deadline:
                    time.sleep(0.1)
                assert not list(directory.rglob("*.parquet")), "session expiry left orphan stage files"
        finally:
            if child is not None and child.poll() is None:
                child.kill()
                child.wait(timeout=10)


def test_cancel_active_stage_writer_defers_deletion_until_session_close(utils_server):
    from concurrent.futures import ThreadPoolExecutor
    from pyspark_pecan import CancellationToken, GraphAlgorithms

    spark, root, _ = utils_server
    token = CancellationToken()
    graph = GraphAlgorithms(spark)
    # This input cannot finish materializing within the watchdog on any test
    # host: a quadrillion BIGINTs alone is 8 PB before file encoding. Wait for an
    # actual nonempty output file, rather than racing a fixed startup sleep.
    vertices = spark.range(1_000_000_000_000_000, numPartitions=4)
    edges = spark.createDataFrame([], "src long,dst long")
    with ThreadPoolExecutor(max_workers=1) as executor:
        running = executor.submit(graph.pagerank, vertices, edges,
                                  max_iterations=1, cancellation=token)
        try:
            deadline = time.monotonic() + 45
            while True:
                if running.done():
                    running.result()  # Surface a real setup failure.
                    pytest.fail("large snapshot unexpectedly completed")
                files = [p for p in root.rglob("*") if p.is_file()
                         and p.name != "_sail_graph_run" and p.stat().st_size > 0]
                if files:
                    break
                assert time.monotonic() < deadline, "snapshot writer produced no bytes"
                time.sleep(0.025)
            token.cancel()
            with pytest.raises(Exception, match="(?i)cancel|interrupt|abort") as cancelled:
                running.result(timeout=45)
            # Interrupt acknowledgment is not a writer-termination barrier.
            # The client must retain ownership instead of racing a live writer
            # with Rm. This is a different guarantee from normal result.close().
            assert cancelled.value.cleanup_deferred is True
            assert cancelled.value.run_path.startswith("file://")
            assert list(root.rglob("_sail_graph_run")), "interrupted write lost its ownership marker"
            assert spark.range(1).count() == 1, "cancellation must not stop unrelated session work"
            spark.stop()
            deadline = time.monotonic() + 45
            while [p for p in root.rglob("*") if p.is_file()] and time.monotonic() < deadline:
                time.sleep(0.05)
            assert not [p for p in root.rglob("*") if p.is_file()], "session cleanup left cancelled staging"
        finally:
            if not running.done():
                token.cancel()
                spark.stop()
