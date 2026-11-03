"""Run each integration session against the actual Sail executable."""
import contextlib
import functools
import gc
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import sysconfig
import time

os.environ.setdefault("SPARK_CONNECT_MODE_ENABLED", "1")

import pytest
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect.client.core import SparkConnectClient
from pyspark.sql.connect.client.retries import DefaultPolicy


def pytest_addoption(parser):
    parser.addoption("--sail-binary", required=True)
    parser.addoption("--execution-mode", default="local", choices=["local", "local-cluster", "process-cluster"])


@pytest.fixture(scope="session", autouse=True)
def retained_spark_sessions():
    """Defer stale session finalizers until every integration test is finished.

    PySpark 4.0.1's SparkSession.__del__ calls client.close() even after stop().
    That closes the class-global Connect release pool used by other sessions.
    Retain sessions across tests so cyclic GC cannot perform that second close
    during an active query. Explicit stop() and reattach/release RPCs still run.
    """
    sessions = []
    initialize = SparkSession.__init__

    @functools.wraps(initialize)
    def initialize_and_retain(session, *args, **kwargs):
        initialize(session, *args, **kwargs)
        sessions.append(session)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(SparkSession, "__init__", initialize_and_retain)
        yield sessions
    # The session-scoped autouse fixture outlives all function-scoped clients
    # and server fixtures. Finalizers are safe once no test is executing RPCs.
    sessions.clear()
    gc.collect()


@pytest.fixture(autouse=True)
def bounded_connect_retries(monkeypatch):
    """Apply to every client, including sessions constructed inside a test."""
    initialize = SparkConnectClient.__init__

    @functools.wraps(initialize)
    def initialize_with_bounded_retries(client, *args, **kwargs):
        initialize(client, *args, **kwargs)
        # PySpark RetryPolicyState.next_attempt returns milliseconds. A dead
        # server must fail promptly instead of using its ten-minute default.
        client.set_retry_policies([
            DefaultPolicy(max_retries=1, initial_backoff=100, max_backoff=100, jitter=0)
        ])

    monkeypatch.setattr(SparkConnectClient, "__init__", initialize_with_bounded_retries)


@contextlib.contextmanager
def start_server(binary, directory, mode="local", extra_pythonpath=None, extra_env=None):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    env = dict(os.environ)
    paths = [sysconfig.get_paths()["purelib"]]
    if extra_pythonpath:
        paths.insert(0, str(extra_pythonpath))
    env.update(PYTHONHOME=sys.base_prefix, DYLD_LIBRARY_PATH=sysconfig.get_config_var("LIBDIR") or "", LD_LIBRARY_PATH=sysconfig.get_config_var("LIBDIR") or "", PYTHONPATH=os.pathsep.join(paths), SAIL_EXPERIMENTAL_EXTENSIONS="1", SAIL_MODE="local-cluster" if mode == "process-cluster" else mode,
               SAIL_EXECUTION__DEFAULT_PARALLELISM="4")
    env.update(SAIL_EXPERIMENTAL_PROCESS_WORKERS="1" if mode == "process-cluster" else "0",
               SAIL_CLUSTER__WORKER_INITIAL_COUNT="2", SAIL_CLUSTER__WORKER_MAX_COUNT="2",
               SAIL_CLUSTER__TASK_MAX_ATTEMPTS="3")
    if extra_env:
        env.update(extra_env)
    env.pop("SAIL_INTERNAL__RUN_PYTHON", None)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "server.log").open("w") as log:
        process = subprocess.Popen([binary, "spark", "server", "--ip", "127.0.0.1", "--port", str(port)],
                                   cwd=directory, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError((directory / "server.log").read_text())
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise TimeoutError("Sail did not start within 60 seconds")
            yield f"sc://127.0.0.1:{port}"
        finally:
            process.send_signal(signal.SIGINT) if process.poll() is None else None
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@pytest.fixture(scope="session")
def endpoint(request, tmp_path_factory):
    binary = str(Path(request.config.getoption("--sail-binary")).resolve())
    with start_server(binary, tmp_path_factory.mktemp("sail-native-extensions"), mode=request.config.getoption("--execution-mode")) as uri:
        yield uri


@pytest.fixture
def spark(endpoint):
    session = SparkSession.builder.remote(endpoint).create()
    try:
        yield session
    finally:
        session.stop()
