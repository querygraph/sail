"""A Connect relation extension as a pipeline dataset source.

A native region is a batch source: a materialized view may read an extension
relation, and the pipeline plans it through the session's extension registry
each time the view is computed. A streaming table may not: an extension whose
input reads a stream is refused by name at analysis.

The extension-enabled server runs from a Sail binary built from this tree with
`SAIL_EXPERIMENTAL_EXTENSIONS=1`, and finds the extensions through the Python
it embeds, which must be a separate environment holding the extension wheels
(`examples/extensions/requirements.lock` pins its own PySpark, which must not
reach this client's). Two variables name them; without both the suite skips.

    SAIL_EXTENSIONS_SAIL_BINARY   the `sail` binary (`cargo build -p sail-cli`)
    SAIL_EXTENSIONS_PYTHON        that environment's `python`, with `sail_nutmeg`
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

# PySpark's functions dispatch to Connect only under this flag, which the shared
# test session sets for itself; this module brings its own session.
os.environ.setdefault("SPARK_CONNECT_MODE_ENABLED", "1")

from pysail.testing.spark.utils.common import is_jvm_spark
from pyspark.sql import functions as F
from pyspark.sql.connect.session import SparkSession

if is_jvm_spark():
    pytest.skip("Sail's Connect relation extensions", allow_module_level=True)

pytest.importorskip("pyspark.pipelines.spark_connect_pipeline")
registry_module = pytest.importorskip("pyspark.pipelines.spark_connect_graph_element_registry")
client_module = pytest.importorskip("pyspark.pipelines.spark_connect_pipeline")

BINARY = os.environ.get("SAIL_EXTENSIONS_SAIL_BINARY")
PYTHON = os.environ.get("SAIL_EXTENSIONS_PYTHON")
if not BINARY or not PYTHON:
    pytest.skip(
        "SAIL_EXTENSIONS_SAIL_BINARY and SAIL_EXTENSIONS_PYTHON name the extension-enabled server",
        allow_module_level=True,
    )
if not Path(BINARY).is_file():
    pytest.skip(f"no Sail binary at {BINARY}", allow_module_level=True)

NUTMEG_SOURCE = Path(__file__).resolve().parents[5] / "examples" / "extensions" / "nutmeg" / "python"


def _environment_paths() -> dict[str, str]:
    """The embedded Python's home, site-packages and library directory, from that Python itself."""
    script = (
        "import sys, sysconfig, json; import sail_nutmeg; "
        "print(json.dumps({'home': sys.base_prefix, "
        "'purelib': sysconfig.get_paths()['purelib'], "
        "'libdir': sysconfig.get_config_var('LIBDIR') or ''}))"
    )
    probe = subprocess.run([PYTHON, "-c", script], capture_output=True, text=True, check=False)
    if probe.returncode != 0:
        pytest.skip(f"{PYTHON} cannot import sail_nutmeg: {probe.stderr.strip()[-200:]}", allow_module_level=True)
    import json

    return json.loads(probe.stdout)


PATHS = _environment_paths()


@pytest.fixture(scope="module")
def extension_server(tmp_path_factory):
    """A Sail server with the extensions loaded, as `examples/extensions/tests` starts one."""
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    env = dict(os.environ)
    env.pop("SAIL_INTERNAL__RUN_PYTHON", None)
    env.update(
        PYTHONHOME=PATHS["home"],
        PYTHONPATH=PATHS["purelib"],
        DYLD_LIBRARY_PATH=PATHS["libdir"],
        LD_LIBRARY_PATH=PATHS["libdir"],
        SAIL_EXPERIMENTAL_EXTENSIONS="1",
        SAIL_EXPERIMENTAL_PROCESS_WORKERS="0",
        SAIL_EXECUTION__DEFAULT_PARALLELISM="4",
    )
    directory = tmp_path_factory.mktemp("extension-server")
    with (directory / "server.log").open("w") as log:
        process = subprocess.Popen(
            [BINARY, "spark", "server", "--ip", "127.0.0.1", "--port", str(port)],
            cwd=directory,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
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
                raise TimeoutError("the extension server did not start within 60 seconds")
            yield f"sc://127.0.0.1:{port}"
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


@pytest.fixture(scope="module")
def spark(extension_server, tmp_path_factory):
    """Module-scoped, as the package's `spark` is, so the package's autouse doctest fixture can take it."""
    session = SparkSession.builder.remote(extension_server).create()
    session.conf.set("spark.sql.warehouse.dir", (tmp_path_factory.mktemp("warehouse")).as_uri())
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def nutmeg(spark):
    """The Nutmeg Connect client from the examples' source tree; it is pure Python over PySpark."""
    if str(NUTMEG_SOURCE) not in sys.path:
        sys.path.insert(0, str(NUTMEG_SOURCE))
    from sail_nutmeg.client import Nutmeg

    return Nutmeg(spark)


@pytest.fixture
def pipeline(spark, tmp_path):
    """Run a set of definitions as the pipelines CLI does. As in Spark, a graph is started once:
    every run, dry or not, registers the definitions on a fresh graph."""

    def run(define, dry=False, full_refresh_all=False):
        from pyspark import pipelines as dp
        from pyspark.pipelines import graph_element_registry

        graph_id = client_module.create_dataflow_graph(spark, None, None, None)
        registry = registry_module.SparkConnectGraphElementRegistry(spark, graph_id)
        with graph_element_registry.graph_element_registration_context(registry):
            define(dp)
        client_module.handle_pipeline_events(
            client_module.start_run(spark, graph_id, None, full_refresh_all, None, dry, (tmp_path / "state").as_uri())
        )

    return run


def cycle(spark):
    nodes = spark.range(0, 3).select(F.col("id").cast("string").alias("node_id"))
    edges = spark.range(0, 3).select(
        F.col("id").cast("string").alias("source"), ((F.col("id") + 1) % 3).cast("string").alias("target")
    )
    return nodes, edges


def test_a_materialized_view_reads_a_graph_table(spark, nutmeg, pipeline):
    """`nm.tables(nodes, edges).degrees()` as a materialized view: dry run, run, full refresh.

    The graph tables are built outside the query function: `GraphTables` checks
    the two schemas when it is constructed, which is analysis, and Spark forbids
    analysis inside a pipeline query function (`ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION`).
    """
    nodes, edges = cycle(spark)
    graph = nutmeg.tables(nodes, edges)

    def define(dp):
        @dp.materialized_view
        def degrees():
            return graph.degrees()

    pipeline(define, dry=True)
    pipeline(define)
    rows = spark.table("degrees").orderBy("nodeId").collect()
    assert [tuple(row) for row in rows] == [("0", 1, 1, 2), ("1", 1, 1, 2), ("2", 1, 1, 2)]
    pipeline(define, full_refresh_all=True)
    assert spark.table("degrees").count() == 3


def test_a_materialized_view_reads_a_native_extension_relation(spark, nutmeg, pipeline):
    """A staged graph's native degree relation as the view's source: dry run, run, full refresh."""
    nodes, edges = cycle(spark)
    staged = nutmeg.stage("ring", nodes, edges)
    assert staged.nodeCount == 3 and staged.edgeCount == 3

    def define(dp):
        @dp.materialized_view
        def native_degrees():
            return nutmeg.run("ring", "degree")

    pipeline(define, dry=True)
    pipeline(define)
    rows = spark.table("native_degrees").orderBy("nodeId").collect()
    assert [(row.nodeId, row.degree) for row in rows] == [("0", 1), ("1", 1), ("2", 1)]
    pipeline(define, full_refresh_all=True)
    assert spark.table("native_degrees").count() == 3
    assert nutmeg.drop("ring").dropped


def test_an_extension_over_a_stream_is_refused_by_name(spark, nutmeg, pipeline, capsys):
    """A streaming table fed by an extension whose input reads a stream is refused at analysis, by name.

    As on Spark, the run fails with the resolution summary naming the flow; the flow's
    own reason arrives as a pipeline event, which the CLI prints.
    """

    def define(dp):
        @dp.materialized_view
        def nodes():
            return cycle(spark)[0]

        @dp.materialized_view
        def edges():
            return cycle(spark)[1]

        dp.create_streaming_table("staged")

        @dp.append_flow(target="staged")
        def stage_live():
            return nutmeg._relation(
                "stage", "live", inputs=(spark.readStream.table("nodes"), spark.read.table("edges"))
            )

    with pytest.raises(Exception, match=r"Flows with errors: sail\.default\.stage_live"):
        pipeline(define, dry=True)
    events = capsys.readouterr().out
    assert "Failed to resolve flow: 'sail.default.stage_live'" in events
    assert "relation extension cannot be read by a streaming flow; a native region is a batch source" in events
