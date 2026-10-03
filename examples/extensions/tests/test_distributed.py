"""Process isolation and package compatibility checks for distributed extensions."""
import contextlib
from pathlib import Path
import re
import shutil
import sysconfig

import pytest
from pyspark.sql.connect.session import SparkSession

from conftest import start_server
from test_protocol import manifest, read_events, write_loader_fixture


def test_nutmeg_stage_has_worker_inputs_and_a_driver_barrier(spark, request):
    if request.config.getoption("--execution-mode") == "local":
        pytest.skip("stage topology exists only in cluster modes")
    from sail_nutmeg.client import Nutmeg
    from test_nutmeg import cycle
    nm = Nutmeg(spark)
    result = nm.stage("placement", *cycle(spark))
    assert (result.nodeCount, result.edgeCount) == (3, 3)
    stages = spark.sql("SELECT CAST(job_id AS BIGINT) AS job_id, CAST(partitions AS BIGINT) AS partitions, size(inputs) AS input_count, placement FROM system.execution.stages").collect()
    jobs = {}
    for stage in stages:
        jobs.setdefault(stage.job_id, []).append(stage)
    assert any(
        any(stage.placement == "Driver" and stage.partitions == 1 and stage.input_count >= 2 for stage in group)
        and sum(stage.partitions for stage in group if stage.placement == "Worker") >= 8
        for group in jobs.values()
    ), stages


@pytest.mark.parametrize("worker_package", ["missing", "different-content"])
def test_separate_worker_rejects_unavailable_or_changed_package(request, tmp_path, worker_package):
    binary = str(Path(request.config.getoption("--sail-binary")).resolve())
    worker_path = tmp_path / "worker-site"
    worker_path.mkdir()
    if worker_package == "different-content":
        site = Path(sysconfig.get_paths()["purelib"])
        for item in site.iterdir():
            destination = worker_path / item.name
            if item.name == "sail_sedona":
                shutil.copytree(item, destination)
                with (destination / "__init__.py").open("a") as output:
                    output.write("\n# Independently rebuilt worker wheel; same advertised version.\n")
            else:
                destination.symlink_to(item, target_is_directory=item.is_dir())
    directory = tmp_path / "server"
    with start_server(binary, directory, mode="process-cluster", extra_env={
        "SAIL_EXPERIMENTAL_WORKER_PYTHONPATH": str(worker_path),
    }) as endpoint:
        spark = SparkSession.builder.remote(endpoint).create()
        try:
            with pytest.raises(Exception, match="native extension unavailable or package/configuration mismatch on worker"):
                spark.sql("SELECT ST_AsText(ST_Point(CAST(id AS DOUBLE), 2.0)) FROM range(17)").collect()
        finally:
            spark.stop()
    log = (directory / "server.log").read_text()
    workers = re.findall(r"extension process worker \d+: pid=Some\((\d+)\), driver_pid=(\d+)", log)
    assert len(workers) >= 2, log
    assert all(worker != driver for worker, driver in workers)
    assert len({worker for worker, _ in workers}) >= 2


def test_process_workers_reject_static_mismatch_before_import(request, tmp_path):
    binary = str(Path(request.config.getoption("--sail-binary")).resolve())
    # Only workers see this distribution. The driver still has its ordinary,
    # valid installed packages and must reach actual worker startup.
    worker_path = tmp_path / "worker-site"
    events = write_loader_fixture(worker_path, [
        dict(manifest=manifest("worker-compatible-first", placement="any")),
        dict(
            manifest=manifest("worker-poison", placement="any", datafusion_version="54.1.0"),
            import_error="WORKER_IMPORT_MUST_NOT_RUN",
        ),
    ])
    directory = tmp_path / "server"
    with start_server(binary, directory, mode="process-cluster", extra_env={
        "SAIL_EXPERIMENTAL_WORKER_PYTHONPATH": str(worker_path),
        # Rejection happens before worker registration. Bound the scheduler's
        # wait for that registration, and avoid retrying an intentional failure.
        "SAIL_CLUSTER__WORKER_LAUNCH_TIMEOUT_SECS": "10",
        "SAIL_CLUSTER__WORKER_LAUNCH_RETRY_STRATEGY__TYPE": "fixed",
        "SAIL_CLUSTER__WORKER_LAUNCH_RETRY_STRATEGY__FIXED__MAX_COUNT": "0",
        "SAIL_CLUSTER__TASK_LAUNCH_TIMEOUT_SECS": "15",
        "SAIL_CLUSTER__TASK_STREAM_CREATION_TIMEOUT_SECS": "15",
        "SAIL_CLUSTER__TASK_MAX_ATTEMPTS": "1",
    }) as endpoint:
        spark = SparkSession.builder.remote(endpoint).create()
        try:
            # A startup failure can reach the client as a task or worker error;
            # assert its exact preflight cause from the worker log below.
            with pytest.raises(Exception):
                spark.sql("SELECT ST_AsText(ST_Point(CAST(id AS DOUBLE), 2.0)) FROM range(17)").collect()
        finally:
            with contextlib.suppress(Exception):
                spark.stop()
    assert read_events(events) == [], "worker rejection must precede import, factory, manifest and bind"
    log = (directory / "server.log").read_text()
    assert re.search(r"worker-poison build mismatch.*55\.1\.0.*54\.1\.0", log), log
    assert "WORKER_IMPORT_MUST_NOT_RUN" not in log
    workers = re.findall(r"extension process worker \d+: pid=Some\((\d+)\), driver_pid=(\d+)", log)
    assert len(workers) >= 2, log
    assert all(worker != driver for worker, driver in workers)
    assert len({worker for worker, _ in workers}) >= 2
