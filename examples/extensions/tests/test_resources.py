"""Host/native admission and lifecycle through real Spark Connect operations."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import signal
import time

import pytest
from pyspark.sql import functions as F
from pyspark.sql.connect.session import SparkSession
from sail_nutmeg.client import Nutmeg

from conftest import start_server
from test_nutmeg import cycle


def status(nutmeg):
    return json.loads(nutmeg._relation("diagnostics", "__session__").collect()[0].status)


def wait_status(nutmeg, predicate, *, timeout=60):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        latest = status(nutmeg)
        if predicate(latest):
            return latest
        time.sleep(0.025)
    raise AssertionError(f"native lifecycle did not reach the required state: {latest}")


def admitted_session(endpoint, *, timeout=60):
    deadline = time.monotonic() + timeout
    while True:
        candidate = SparkSession.builder.remote(endpoint).create()
        try:
            assert candidate.range(1).collect()[0].id == 0
        except Exception as error:
            candidate.stop()
            if "host memory admission" not in str(error) or time.monotonic() >= deadline:
                raise
            # Release acknowledges before asynchronous worker shutdown ends.
            # Failed session creation is terminal, so use a fresh incarnation.
            time.sleep(0.05)
        else:
            return candidate


def test_host_quota_contends_between_sessions_and_releases_on_session_stop(request, tmp_path):
    binary = str(Path(request.config.getoption("--sail-binary")).resolve())
    mode = request.config.getoption("--execution-mode")
    with start_server(binary, tmp_path / "quota", mode=mode, extra_env={
        "SAIL_NUTMEG_MEMORY_BYTES": str(4 << 20),
        "SAIL_RUNTIME__MEMORY_POOL__TYPE": "greedy",
        "SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE": str(6 << 20),
    }) as endpoint:
        first = SparkSession.builder.remote(endpoint).create()
        second = SparkSession.builder.remote(endpoint).create()
        third = None
        try:
            nm = Nutmeg(first)
            assert nm.stage("quota", *cycle(first)).revision == 1
            assert status(nm)["memory"]["limit_bytes"] == 4 << 20
            with pytest.raises(Exception, match="host memory admission.*refused"):
                second.range(1).collect()
            # Closing the first session drops its graph and host quota. A new
            # session can then admit the same cap without inheriting graph state.
            first.stop()
            third = admitted_session(endpoint)
            assert status(Nutmeg(third))["graphs"] == []
        finally:
            if third is not None:
                third.stop()
            second.stop()
            first.stop()


def test_actual_commit_is_not_replayed_after_client_receipt_failure(spark, request):
    nm = Nutmeg(spark)
    stage = nm._relation("stage", "receipt-loss", inputs=cycle(spark))
    # This expression depends on the committed revision, so it cannot fail
    # during planning or before native publication. It discards the receipt.
    fault = stage.select(F.raise_error(F.concat(F.lit("receipt-loss:"), F.col("revision").cast("string"))))
    pattern = "receipt-loss:1"
    if request.config.getoption("--execution-mode") != "local":
        pattern = "automatic retry disabled.*indeterminate.*receipt-loss:1"
    with pytest.raises(Exception, match=pattern):
        fault.collect()
    state = status(nm)
    graph = next(graph for graph in state["graphs"] if graph["name"] == "receipt-loss")
    assert (graph["revision"], graph["node_count"], graph["edge_count"]) == (1, 3, 3)
    assert nm.run("receipt-loss", "degree").count() == 3
    assert nm.stage("receipt-loss", *cycle(spark)).revision == 2
    if request.config.getoption("--execution-mode") != "local":
        tasks = spark.table("system.execution.tasks").where(F.col("session_id") == spark.session_id).selectExpr("CAST(job_id AS BIGINT) AS job_id", "CAST(attempt AS BIGINT) AS attempt", "status").collect()
        failed_jobs = {task.job_id for task in tasks if task.status == "FAILED"}
        assert failed_jobs, tasks
        assert all(task.attempt == 0 for task in tasks if task.job_id in failed_jobs), tasks


def test_spark_limit_early_stop_and_interrupt_end_kernels_and_release_memory(spark):
    nm = Nutmeg(spark)
    # The directed cycle has N*N reachable pairs. At N=65,536 the interrupted
    # count would require 4,294,967,296 rows through a single native APSP cursor,
    # while its admitted workspace remains linear in N. There is no floating
    # convergence threshold or thread-width-dependent termination shortcut.
    count = 65536
    nodes = spark.range(count, numPartitions=4).select(F.col("id").cast("string").alias("node_id"))
    edges = spark.range(count, numPartitions=4).select(F.col("id").cast("string").alias("source"), ((F.col("id") + 1) % count).cast("string").alias("target"))
    nm.stage("lifecycle", nodes, edges)
    assert len(nm.run("lifecycle", "degree").limit(1).collect()) == 1
    limited = wait_status(nm, lambda s: s["reads"] and s["reads"][-1]["state"] == "cancelled")
    assert limited["reads"][-1]["rows"] < count

    previous = limited["reads"][-1]["id"]
    iterator = nm.run("lifecycle", "allPairsShortestPaths").toLocalIterator()
    try:
        assert next(iterator) is not None
    finally:
        iterator.close()
    stopped = wait_status(nm, lambda s: any(r["id"] > previous and r["state"] == "cancelled" for r in s["reads"]))
    previous = max(r["id"] for r in stopped["reads"])

    def run_count():
        spark.addTag("native-resource-interrupt")
        try:
            return nm.run("lifecycle", "allPairsShortestPaths").count()
        finally:
            spark.removeTag("native-resource-interrupt")

    with ThreadPoolExecutor(max_workers=1) as executor:
        running = executor.submit(run_count)
        try:
            observed = wait_status(nm, lambda s: any(r["id"] > previous and r["state"] == "running" and r["batches"] > 0 for r in s["reads"]))
            read_id = max(r["id"] for r in observed["reads"])
            assert spark.interruptTag("native-resource-interrupt"), "Spark must identify the interrupted operation"
            with pytest.raises(Exception, match="(?i)cancel|interrupt|abort"):
                running.result(timeout=60)
            ended = wait_status(nm, lambda s: any(r["id"] == read_id and r["state"] == "cancelled" for r in s["reads"]))
            assert next(r for r in ended["reads"] if r["id"] == read_id)["rows"] < count * count
        finally:
            # Keep failures bounded without claiming cleanup is test evidence.
            if not running.done():
                spark.interruptTag("native-resource-interrupt")
    assert nm.drop("lifecycle").dropped
    cleared = wait_status(nm, lambda s: s["memory"]["used_bytes"] == 0)
    assert cleared["graphs"] == []
    assert all(read["state"] != "running" for read in cleared["reads"])


@pytest.mark.parametrize("termination", ["session-stop", "idle-expiry", "driver-shutdown", "session-stop-then-driver-shutdown"])
def test_active_native_kernel_releases_its_host_lease_on_teardown(request, tmp_path, termination):
    binary = str(Path(request.config.getoption("--sail-binary")).resolve())
    mode = request.config.getoption("--execution-mode")
    audit = tmp_path / "resource-audit.jsonl"
    environment = {
        "SAIL_NUTMEG_MEMORY_BYTES": str(128 << 20),
        "SAIL_RUNTIME__MEMORY_POOL__TYPE": "greedy",
        "SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE": str(192 << 20),
        "SAIL_NATIVE_RESOURCE_AUDIT": str(audit),
    }
    if termination == "idle-expiry":
        environment["SAIL_SPARK__SESSION_TIMEOUT_SECS"] = "10"
        # A heartbeat response normally causes PySpark to send ReleaseExecute,
        # which refreshes activity. Place it beyond the entire test watchdog so
        # this case exercises actual idle expiry while the kernel is running.
        environment["SAIL_SPARK__EXECUTION_HEARTBEAT_INTERVAL_SECS"] = "300"
    with start_server(binary, tmp_path / termination, mode=mode, extra_env=environment) as endpoint:
        spark = SparkSession.builder.remote(endpoint).create()
        replacement = None
        executor = ThreadPoolExecutor(max_workers=1)
        running = None
        try:
            nm = Nutmeg(spark)
            count = 65536
            nodes = spark.range(count, numPartitions=4).select(F.col("id").cast("string").alias("node_id"))
            edges = spark.range(count, numPartitions=4).select(F.col("id").cast("string").alias("source"), ((F.col("id") + 1) % count).cast("string").alias("target"))
            nm.stage("teardown", nodes, edges)
            running = executor.submit(lambda: nm.run("teardown", "allPairsShortestPaths").count())
            observed = wait_status(nm, lambda s: any(r["state"] == "running" and r["batches"] > 0 for r in s["reads"]))
            assert observed["memory"]["used_bytes"] > 0
            issued = [json.loads(line) for line in audit.read_text().splitlines() if line]
            admitted = [event for event in issued if event["event"] == "admitted"]
            assert len(admitted) == 1, issued
            lease = admitted[0]
            if termination in {"session-stop", "session-stop-then-driver-shutdown"}:
                spark.stop()
            if termination in {"driver-shutdown", "session-stop-then-driver-shutdown"}:
                os.kill(lease["pid"], signal.SIGINT)
            # Idle expiry deliberately sends no more RPCs to the first session.
            # The lifecycle assertion is the final lease drop below, not timing
            # a client acknowledgement against native execution.
            with pytest.raises(Exception) as stopped:
                running.result(timeout=90)
            assert not isinstance(stopped.value, TimeoutError), "client operation outlived teardown watchdog"
            deadline = time.monotonic() + 60
            while True:
                events = [json.loads(line) for line in audit.read_text().splitlines() if line]
                released = [event for event in events if event["event"] == "released" and event["pid"] == lease["pid"] and event["id"] == lease["id"]]
                if released:
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError(f"native producer/plan/output owners retained the lease after {termination}: {events}")
                time.sleep(0.025)
            assert len(released) == 1
            assert released[0]["bytes"] == 128 << 20
            assert released[0]["pool_reserved"] == 0
            if termination not in {"driver-shutdown", "session-stop-then-driver-shutdown"}:
                replacement = admitted_session(endpoint)
                assert status(Nutmeg(replacement))["graphs"] == []
        finally:
            if running is not None and not running.done():
                try:
                    spark.interruptAll()
                except Exception:
                    pass
            if replacement is not None:
                replacement.stop()
            spark.stop()
            executor.shutdown(wait=False, cancel_futures=True)
