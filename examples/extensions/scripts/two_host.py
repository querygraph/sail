#!/usr/bin/env python3
"""Run an exact-artifact, two-host native extension qualification.

Run with the client environment's Python:
  python two_host.py --config two-host.json --output evidence/two-host

Config contains a driver target and a list of worker targets. Each target has
repo, python, sail, advertise; optional ssh selects a remote host. Driver adds
connect_port and gateway_port. Workers optionally set port (default 0 lets Sail
advertise its actual OS-assigned port). Use identical native binary/wheels on
both hosts; x86_64 macOS artifacts may run under Rosetta on an arm64 host.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import socket
import subprocess
import time
import traceback

from two_host_worker import clean_python_environment, launch, stop

INVENTORY = r'''
import hashlib, importlib.metadata, json, platform, runpy, subprocess, sys
from pathlib import Path
repo, binary = map(Path, sys.argv[1:])
identity = runpy.run_path(str(repo / "crates/sail-session/src/extensions/package_identity.py"))["identity"]
packages = {}
for entry in importlib.metadata.entry_points(group="pysail.extensions"):
    factory = entry.load()
    factory = factory() if callable(factory) else factory
    manifest = factory.manifest()
    packages[manifest["name"]] = identity(entry, manifest)
with binary.open("rb") as stream:
    binary_hash = hashlib.file_digest(stream, "sha256").hexdigest()
print(json.dumps(dict(host=platform.node(), platform=platform.platform(), architecture=platform.machine(),
                     binary_sha256=binary_hash, packages=packages, python=sys.version,
                     source_commit=subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip(),
                     source_dirty=bool(subprocess.check_output(["git", "-C", str(repo), "status", "--porcelain"], text=True)))))
'''


def target_python(target, program, *arguments):
    argv = ["env", "-u", "PYTHONHOME", "-u", "PYTHONPATH", "-u", "DYLD_LIBRARY_PATH",
            target["python"], "-c", program, *arguments]
    if target.get("ssh"):
        argv = ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                target["ssh"], shlex.join(argv)]
    return json.loads(subprocess.check_output(argv, text=True,
                     env=clean_python_environment(), timeout=120))


def inventory(target):
    return target_python(target, INVENTORY, target["repo"], target["sail"])


def completed_worker_tasks(log):
    pattern = (r"worker_task_status worker_id=(\d+) job_id=(\d+) stage=(\d+) "
               r"partition=(\d+) attempt=(\d+) status=SUCCEEDED\b")
    fields = ("worker_id", "job_id", "stage", "partition", "attempt")
    return [dict(zip(fields, map(int, match))) for match in re.findall(pattern, log)]


def process_cleanup(targets, inventories, log_path):
    """Check the actual Sail PIDs on each host after foreground supervision ends."""
    hosts = {item["host"]: target for target, item in zip(targets, inventories)}
    started = {}
    for line in log_path.read_text().splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if event.get("event") == "sail_remote_started":
            started.setdefault(event["hostname"], []).append(event["pid"])
    assert started, "missing process supervision evidence"
    program = """
import json, os, sys
result = []
for pid in json.loads(sys.argv[1]):
    try:
        os.kill(pid, 0)
        alive = True
    except ProcessLookupError:
        alive = False
    result.append(dict(pid=pid, alive=alive))
print(json.dumps(result))
"""
    deadline = time.monotonic() + 30
    while True:
        result = {host: target_python(hosts[host], program, json.dumps(pids))
                  for host, pids in started.items()}
        if not any(row["alive"] for rows in result.values() for row in rows) or time.monotonic() >= deadline:
            return result
        time.sleep(1)


def wait_server(process, host, port):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"driver exited with status {process.returncode}")
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError("driver Spark Connect listener did not become ready")


def exercise(endpoint, worker_hosts, evidence):
    os.environ.setdefault("SPARK_CONNECT_MODE_ENABLED", "1")
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.connect.client.retries import DefaultPolicy
    from sail_nutmeg.client import Nutmeg
    from shapely import from_wkt

    spark = SparkSession.builder.remote(endpoint).create()
    spark.client.set_retry_policies([
        DefaultPolicy(max_retries=1, initial_backoff=100, max_backoff=100, jitter=0)
    ])
    try:
        def rows(name, frame):
            value = [row.asDict(recursive=True) for row in frame.collect()]
            evidence[name] = value
            return value

        source = spark.range(0, 17, numPartitions=4)
        geometries = source.select("id", F.expr(
            "ST_GeomFromWKB(ST_AsBinary(ST_Point(CAST(id AS DOUBLE),2.0)))"
        ).alias("geom")).repartition(4, "id")
        points = rows("sedona_worker_shuffle", geometries.select("id", F.expr("ST_AsText(geom)").alias("wkt")).orderBy("id"))
        assert len(points) == 17
        assert all(from_wkt(row["wkt"]).equals(from_wkt(f"POINT({i} 2)"))
                   for i, row in enumerate(points))

        nodes = spark.range(0, 5, numPartitions=4).select(F.col("id").alias("node_id"))
        edges = spark.createDataFrame([(0, 1), (0, 1), (1, 2), (2, 0)], "source long,target long").repartition(4)
        nm = Nutmeg(spark)
        graph = nm.tables(nodes, edges).validate()
        degree = rows("direct_dataframe_degree", graph.out_degrees().orderBy("nodeId"))
        assert [(row["nodeId"], row["outDegree"]) for row in degree] == [(0, 2), (1, 1), (2, 1), (3, 0), (4, 0)]
        walks = rows("direct_dataframe_two_hop", graph.walks(2).groupBy("source", "target").count().orderBy("source", "target"))
        assert [(row["source"], row["target"], row["count"]) for row in walks] == [(0, 2, 2), (1, 0, 1), (2, 1, 2)]

        # The spatial predicate executes on worker inputs; native staging and
        # CSR algorithms execute in the driver, with results feeding a shuffle.
        spatial_edges = edges.filter(F.expr(
            "ST_Distance(ST_Point(CAST(source AS DOUBLE),0.0),ST_Point(CAST(target AS DOUBLE),0.0)) <= 2.0"
        ))
        staged = nm.stage("two_host", nodes.select(F.col("node_id").cast("string").alias("node_id")),
                          spatial_edges.select(F.col("source").cast("string").alias("source"),
                                               F.col("target").cast("string").alias("target")))
        evidence["native_stage"] = staged.asDict()
        assert (staged.nodeCount, staged.edgeCount) == (5, 4)
        native = rows("native_degree_then_shuffle", nm.run("two_host", "degree").repartition(4, "nodeId").orderBy("nodeId"))
        assert [(row["nodeId"], row["degree"]) for row in native] == [("0", 2), ("1", 1), ("2", 1), ("3", 0), ("4", 0)]
        assert nm.nodes("two_host").count() == 5
        assert nm.edges("two_host").count() == 4
        evidence["native_status"] = nm.status()

        workers = rows("worker_endpoints", spark.sql("SELECT CAST(worker_id AS BIGINT) AS worker_id, host, CAST(port AS INT) AS port, status FROM system.cluster.workers"))
        running = [row for row in workers if row["host"] in worker_hosts
                   and row["port"] > 0 and row["status"] == "RUNNING"]
        assert len(running) >= 2, workers
        assert worker_hosts.issubset({row["host"] for row in running}), workers
        stages = rows("stage_placement", spark.sql("SELECT CAST(job_id AS BIGINT) AS job_id, CAST(stage AS BIGINT) AS stage, CAST(partitions AS BIGINT) AS partitions, placement FROM system.execution.stages"))
        assert any(row["placement"] == "Worker" and row["partitions"] >= 4 for row in stages), stages
        assert any(row["placement"] == "Driver" for row in stages), stages
        # Sail's current task table does not expose a worker assignment. Keep
        # every task row and its completion state without inventing that mapping.
        rows("task_attempts", spark.sql("""
            SELECT session_id, CAST(job_id AS BIGINT) AS job_id,
                   CAST(stage AS BIGINT) AS stage, CAST(partition AS BIGINT) AS partition,
                   CAST(attempt AS BIGINT) AS attempt, status, created_at, stopped_at
            FROM system.execution.tasks
        """))
        assert nm.drop("two_host").dropped
        return evidence
    finally:
        spark.stop()


def positive_int(value):
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--exercise", choices=["extensions", "portable-graphs"], default="extensions")
    parser.add_argument("--pagerank-method", choices=["power", "delta"], default="power")
    parser.add_argument("--wcc-method", choices=["min_label", "randomized", "randomized_fused"], default="min_label")
    parser.add_argument("--pagerank-iterations", type=int, help="portable graphs: power default 3, delta default 1000")
    parser.add_argument("--tolerance", type=float, help="portable graphs: power default fixed steps, delta default 1e-8")
    parser.add_argument("--wcc-iterations", type=int, help="portable graphs: min_label default 10, randomized default 100")
    parser.add_argument("--seed", type=int, default=42, help="portable graphs: randomized WCC seed")
    parser.add_argument("--worker-task-slots", type=positive_int, default=2,
                        help="asynchronous task slots per worker (default: 2; fused WCC needs at least 4)")
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    driver, workers = config["driver"], config["workers"]
    args.output.mkdir(parents=True, exist_ok=False)
    receipt = dict(started_utc=datetime.now(timezone.utc).isoformat(), config=config, exercise=args.exercise,
                   worker_task_slots=args.worker_task_slots,
                   controller_host=platform.node(), controller_architecture=platform.machine(),
                   boundary="Two physical hosts; functional checks only. Rosetta/emulation is not a performance measurement.")
    snapshots = args.output / "harness"
    snapshots.mkdir()
    receipt["harness_sha256"] = {}
    for name in ("two_host.py", "two_host_worker.py", "two_host_remote.py", "two_host_graphs.py"):
        data = Path(__file__).with_name(name).read_bytes()
        (snapshots / name).write_bytes(data)
        receipt["harness_sha256"][name] = hashlib.sha256(data).hexdigest()
    process = None
    try:
        inventories = receipt["inventories"] = []
        for target in [driver, *workers]:
            inventories.append(inventory(target))
        reference = inventories[0]
        assert all(item["source_commit"] == reference["source_commit"] and not item["source_dirty"]
                   for item in inventories), "driver/worker sources must be clean at the same commit"
        assert {"sedona", "nutmeg"}.issubset(reference["packages"])
        assert all(item["binary_sha256"] == reference["binary_sha256"] and item["packages"] == reference["packages"]
                   for item in inventories), "driver/worker executable or native package identity mismatch"
        assert len({item["host"] for item in inventories}) >= 2, "two distinct hostnames required"
        launcher = [driver["python"], str(Path(driver["repo"]) / "examples/extensions/scripts/two_host_worker.py"),
                    "--targets-json", json.dumps(workers)]
        env = dict(SAIL_EXPERIMENTAL_EXTENSIONS="1", SAIL_MODE="local-cluster",
                   SAIL_EXPERIMENTAL_PROCESS_WORKERS="1", SAIL_EXPERIMENTAL_WORKER_COMMAND=json.dumps(launcher),
                   RUST_LOG="info,sail_execution::task_runner::actor::handler=debug",
                   SAIL_CLUSTER__DRIVER_LISTEN_HOST="0.0.0.0",
                   SAIL_CLUSTER__DRIVER_LISTEN_PORT=str(driver["gateway_port"]),
                   SAIL_CLUSTER__DRIVER_EXTERNAL_HOST=driver["advertise"],
                   SAIL_CLUSTER__DRIVER_EXTERNAL_PORT=str(driver["gateway_port"]),
                   SAIL_CLUSTER__WORKER_INITIAL_COUNT="2", SAIL_CLUSTER__WORKER_MAX_COUNT="2",
                   SAIL_CLUSTER__WORKER_TASK_SLOTS=str(args.worker_task_slots),
                   SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS="600", SAIL_CLUSTER__TASK_MAX_ATTEMPTS="3",
                   SAIL_EXECUTION__DEFAULT_PARALLELISM="4")
        with (args.output / "server-and-workers.log").open("w") as log:
            process = launch(driver, [driver["sail"], "spark", "server", "--ip", "0.0.0.0",
                                     "--port", str(driver["connect_port"])], env, stdout=log)
            try:
                wait_server(process, driver["advertise"], driver["connect_port"])
                receipt["checks"] = {}
                run_exercise = exercise
                graph_options = {}
                if args.exercise == "portable-graphs":
                    from two_host_graphs import exercise as run_exercise
                    graph_options = dict(pagerank_method=args.pagerank_method, wcc_method=args.wcc_method,
                                         pagerank_iterations=args.pagerank_iterations, tolerance=args.tolerance,
                                         wcc_iterations=args.wcc_iterations, seed=args.seed)
                run_exercise(f"sc://{driver['advertise']}:{driver['connect_port']}",
                         {target["advertise"] for target in workers}, receipt["checks"], **graph_options)
            finally:
                stop(process)
                receipt["driver_supervisor_returncode"] = process.returncode
        completed = receipt["completed_worker_tasks"] = completed_worker_tasks(
            (args.output / "server-and-workers.log").read_text())
        required_workers = {row["worker_id"] for row in receipt["checks"]["worker_endpoints"]
                            if row["host"] in {target["advertise"] for target in workers}}
        assert required_workers.issubset({row["worker_id"] for row in completed}), \
            "missing directly observed successful tasks from a registered worker"
        if args.exercise == "portable-graphs":
            # Match worker completion logs specifically to iteration stages;
            # initial graph ingestion does not qualify algorithm distribution.
            windows = receipt["checks"]["iteration_windows"]
            for algorithm in {window["algorithm"] for window in windows}:
                keys = {(stage["job_id"], stage["stage"])
                        for window in windows if window["algorithm"] == algorithm
                        for stage in window["stages"]}
                observed = {row["worker_id"] for row in completed
                            if (row["job_id"], row["stage"]) in keys}
                assert required_workers <= observed, \
                    f"{algorithm} iteration stages did not complete on every host worker"
        cleanup = receipt["process_cleanup"] = process_cleanup(
            [driver, *workers], inventories, args.output / "server-and-workers.log")
        assert not any(row["alive"] for rows in cleanup.values() for row in rows), \
            "a supervised Sail process remained alive after launcher shutdown"
        assert receipt["driver_supervisor_returncode"] == 0, \
            f"driver supervisor failed during shutdown: {receipt['driver_supervisor_returncode']}"
        receipt["outcome"] = "passed"
    except BaseException:
        receipt["outcome"] = "failed"
        receipt["error"] = traceback.format_exc()
        raise
    finally:
        receipt["finished_utc"] = datetime.now(timezone.utc).isoformat()
        (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2, default=str) + "\n")


if __name__ == "__main__":
    main()
