"""Run qualification against an explicitly selected native Sail executable."""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import sysconfig
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql.connect.session import SparkSession


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--sail-binary", help="Native release Sail executable")
    parser.addoption("--execution-mode", default="local", choices=["local", "process-cluster"])
    parser.addoption("--evidence-dir", default="target/second-string-evidence")


@pytest.fixture(scope="session")
def spark(request: pytest.FixtureRequest) -> Iterator[SparkSession]:
    binary_option = request.config.getoption("--sail-binary")
    if not isinstance(binary_option, str):
        pytest.fail("Select a native release binary with --sail-binary")
    binary = Path(binary_option).resolve()
    if not binary.is_file():
        pytest.fail(f"Missing Sail executable: {binary}")
    evidence = Path(request.config.getoption("--evidence-dir"))
    evidence.mkdir(parents=True, exist_ok=True)
    mode = request.config.getoption("--execution-mode")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    env = dict(os.environ)
    env["PYTHONHOME"] = sys.base_prefix
    env["PYTHONPATH"] = sysconfig.get_paths()["purelib"]
    env["DYLD_LIBRARY_PATH"] = str(sysconfig.get_config_var("LIBDIR") or "")
    env["SAIL_EXPERIMENTAL_EXTENSIONS"] = "1"
    env["SAIL_MODE"] = "local-cluster" if mode == "process-cluster" else "local"
    env["TOKIO_WORKER_THREADS"] = "2"
    env["RAYON_NUM_THREADS"] = "2"
    env["SAIL_CLUSTER__WORKER_INITIAL_COUNT"] = "2"
    env["SAIL_CLUSTER__WORKER_MAX_COUNT"] = "2"
    # Slots hold streaming tasks; they are separate from the two CPU threads.
    env["SAIL_CLUSTER__WORKER_TASK_SLOTS"] = "32"
    env["LD_LIBRARY_PATH"] = env["DYLD_LIBRARY_PATH"]
    if mode == "process-cluster":
        env["SAIL_EXPERIMENTAL_PROCESS_WORKERS"] = "1"
    log_path = evidence / f"server-{mode}.log"
    session: SparkSession | None = None
    with log_path.open("wb") as log:
        process = subprocess.Popen(
            [str(binary), "spark", "server", "--ip", "127.0.0.1", "--port", str(port)],
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            session = SparkSession.builder.remote(f"sc://127.0.0.1:{port}").create()
            deadline = time.monotonic() + 30
            while True:
                if process.poll() is not None:
                    pytest.fail(f"Sail exited {process.returncode}; see {log_path}")
                try:
                    assert session.sql("SELECT 1 AS ready").collect()[0].ready == 1
                    break
                except Exception:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.2)
            yield session
        finally:
            if session is not None:
                session.stop()
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
