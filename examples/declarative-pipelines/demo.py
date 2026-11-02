"""Sail Declarative Pipelines demo.

Starts a Sail Spark Connect server, runs pipeline projects through the unchanged Spark 4.2.1
`spark-pipelines` CLI, and checks every result through a Spark Connect session. Each check
prints PASS or FAIL; the exit status is 0 only when every check passed.

Usage: python demo.py --sail PATH/TO/sail [--iggy PATH/TO/iggy-server] [--workdir DIR] [--keep]
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.request
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

HERE = Path(__file__).resolve().parent
CLI = Path(pyspark.__file__).parent / "pipelines" / "cli.py"
IGGY_PASSWORD = "sail-demo-password"  # noqa: S105

GREEN, RED, BOLD, DIM, RESET = "\033[32m", "\033[31m", "\033[1m", "\033[2m", "\033[0m"
if not sys.stdout.isatty():
    GREEN = RED = BOLD = DIM = RESET = ""


class Checks:
    def __init__(self):
        self.results: list[tuple[str, str, bool, str]] = []
        self.section = ""

    def start(self, title: str):
        self.section = title
        print(f"\n{BOLD}== {title}{RESET}")

    def check(self, name: str, ok: bool, detail: str = ""):
        self.results.append((self.section, name, ok, detail))
        mark = f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"
        print(f"  {mark}  {name}" + (f"\n        {DIM}{detail}{RESET}" if detail and not ok else ""))
        return ok

    def equal(self, name: str, actual, expected):
        return self.check(name, actual == expected, f"expected {expected!r}, got {actual!r}")

    def skip(self, name: str, reason: str):
        self.results.append((self.section, name, None, reason))
        print(f"  SKIP  {name}\n        {DIM}{reason}{RESET}")

    def summary(self) -> bool:
        passed = sum(1 for r in self.results if r[2] is True)
        failed = [r for r in self.results if r[2] is False]
        skipped = [r for r in self.results if r[2] is None]
        print(f"\n{BOLD}== Summary{RESET}")
        print(f"  {passed} passed, {len(failed)} failed, {len(skipped)} skipped")
        for section, name, _, detail in failed:
            print(f"  {RED}FAIL{RESET}  {section}: {name}\n        {detail}")
        for section, name, _, reason in skipped:
            print(f"  SKIP  {section}: {name} ({reason})")
        return not failed


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def wait_for_port(port: int, process: subprocess.Popen, what: str, timeout: float = 120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"{what} exited with status {process.returncode}")
        with socket.socket() as probe:
            probe.settimeout(0.2)
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.1)
    raise RuntimeError(f"{what} did not listen on port {port} within {timeout:.0f}s")


class Server:
    """A Sail Spark Connect server whose sessions share one in-memory catalog, as Spark's do."""

    def __init__(self, binary: Path, workdir: Path):
        self.port = free_port()
        self.log = workdir / "sail-server.log"
        environment = {
            **os.environ,
            "SAIL_CATALOG__SHARED_MEMORY": "true",
            "SAIL_RUNTIME__STACK_SIZE": "16777216",
            "SAIL_EXECUTION__DEFAULT_PARALLELISM": "4",
        }
        with self.log.open("wb") as log:
            self.process = subprocess.Popen(  # noqa: S603 the binary the user named
                [str(binary), "spark", "server", "--port", str(self.port)],
                cwd=workdir,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        wait_for_port(self.port, self.process, "the Sail server")
        self.url = f"sc://localhost:{self.port}"

    def stop(self) -> int | None:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                return None
        return self.process.returncode


class Broker:
    """An Apache Iggy broker, driven over its HTTP API."""

    def __init__(self, binary: Path, workdir: Path):
        tcp, http = free_port(), free_port()
        self.url = f"iggy://iggy:{IGGY_PASSWORD}@127.0.0.1:{tcp}"
        self.http = f"http://127.0.0.1:{http}"
        self.token = None
        directory = workdir / "iggy"
        directory.mkdir()
        environment = {
            **os.environ,
            "IGGY_PATH": str(directory / "data"),
            "IGGY_TCP_ADDRESS": f"127.0.0.1:{tcp}",
            "IGGY_HTTP_ADDRESS": f"127.0.0.1:{http}",
            "IGGY_ROOT_USERNAME": "iggy",
            "IGGY_ROOT_PASSWORD": IGGY_PASSWORD,
            "IGGY_QUIC_ENABLED": "false",
            "IGGY_WEBSOCKET_ENABLED": "false",
            "IGGY_SHARDING_CPU_ALLOCATION": "0..2",
            "IGGY_SHARDING_PIN_CORES": "false",
        }
        with (directory / "iggy.log").open("wb") as log:
            self.process = subprocess.Popen(  # noqa: S603 the binary the user named
                [str(binary)], env=environment, cwd=directory, stdout=log, stderr=log
            )
        deadline = time.monotonic() + 60
        while True:
            try:
                reply = self.request("POST", "/users/login", {"username": "iggy", "password": IGGY_PASSWORD})
                self.token = reply["access_token"]["token"]
                break
            except (urllib.error.URLError, ConnectionError):
                if self.process.poll() is not None or time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
        self.request("POST", "/streams", {"name": "demo"})
        self.request("POST", "/streams/demo/topics", {"name": "customers", "partitions_count": 1})

    def request(self, method, path, body=None):
        request = urllib.request.Request(  # noqa: S310 a local broker
            self.http + path,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")
        with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
            content = response.read()
        return json.loads(content) if content else None

    def send(self, *rows):
        partition = base64.b64encode(struct.pack("<I", 0)).decode()
        messages = [{"payload": base64.b64encode(json.dumps(row).encode()).decode()} for row in rows]
        self.request(
            "POST",
            "/streams/demo/topics/customers/messages",
            {"partitioning": {"kind": "partition_id", "value": partition}, "messages": messages},
        )

    def stop(self):
        self.process.kill()
        self.process.wait()


class Project:
    """A pipeline project directory, copied from `projects/<name>` with its placeholders filled in."""

    def __init__(self, name: str, workdir: Path, server: Server, values: dict[str, str]):
        self.root = workdir / "projects" / name
        shutil.copytree(HERE / "projects" / name, self.root)
        for path in self.root.rglob("*"):
            if path.is_file():
                text = path.read_text()
                for key, value in values.items():
                    text = text.replace("{{" + key + "}}", value)
                path.write_text(text)
        self.spec = self.root / "pipeline.yaml"
        self.spec.write_text(
            f"""name: {name}
catalog: sail
database: demo
storage: "{(workdir / "storage" / name).as_uri()}"
configuration:
  "spark.remote": "{server.url}"
libraries:
  - glob:
      include: "transformations/**"
"""
        )
        self.server = server
        self.log = workdir / f"cli-{name}.log"

    def cli(self, command: str, *flags: str) -> tuple[int, str]:
        environment = {**os.environ, "SPARK_REMOTE": self.server.url}
        process = subprocess.run(  # noqa: S603 the PySpark CLI
            [sys.executable, str(CLI), command, *flags, "--spec", str(self.spec)],
            capture_output=True,
            text=True,
            timeout=600,
            env=environment,
            check=False,
        )
        output = process.stdout + process.stderr
        with self.log.open("a") as log:
            log.write(f"\n$ spark-pipelines {command} {' '.join(flags)}\n{output}")
        return process.returncode, output


def rows(spark: SparkSession, table: str, *columns: str) -> list[tuple]:
    """The rows of a table, sorted. The CLI wrote through its own session, so drop cached listings first."""
    spark.catalog.refreshTable(table)
    frame = spark.table(table)
    if columns:
        frame = frame.select(*columns)
    return sorted(tuple(row) for row in frame.collect())


def ran(checks: Checks, name: str, result: tuple[int, str]) -> bool:
    code, output = result
    tail = "\n        ".join(output.strip().splitlines()[-15:])
    return checks.check(name, code == 0 and "Run is COMPLETED" in output, f"exit {code}\n        {tail}")


def write_json(directory: Path, name: str, records: list[dict]):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text("".join(json.dumps(r) + "\n" for r in records))


def change(key, name, country, op, seq):
    return {"id": key, "name": name, "country": country, "op": op, "seq": seq}


def retail(checks: Checks, spark: SparkSession, server: Server, workdir: Path):
    data = workdir / "data"
    clicks, changes, delta_source = data / "clicks", data / "changes", data / "delta_source"

    checks.start("Setup: source tables, files and a Delta table")
    spark.sql("CREATE SCHEMA IF NOT EXISTS demo")
    spark.sql("CREATE TABLE demo.orders_raw (order_id INT, day STRING, customer_id INT, amount DOUBLE) USING parquet")
    spark.sql(
        "INSERT INTO demo.orders_raw VALUES "
        "(1, '2026-11-01', 10, 50.0), (2, '2026-11-01', 11, 150.0), (3, '2026-11-02', 10, 120.0)"
    )
    spark.sql("CREATE TABLE demo.customers_raw (id INT, name STRING, country STRING) USING parquet")
    spark.sql("INSERT INTO demo.customers_raw VALUES (10, 'Ada', 'UK'), (11, 'Linus', 'FI')")
    write_json(clicks, "1.json", [{"user": 10, "page": "home"}, {"user": 11, "page": "cart"}])
    write_json(
        changes,
        "1.json",
        [change(1, "alice", "US", "I", 1), change(2, "bob", "US", "I", 1), change(1, "alicia", "US", "U", 2)],
    )
    spark.range(3).write.format("delta").save(str(delta_source))
    checks.check("source table, JSON landing directories and Delta table created", True)

    project = Project(
        "retail",
        workdir,
        server,
        {"CLICKS": str(clicks), "CHANGES": str(changes), "DELTA_SOURCE": str(delta_source)},
    )
    outputs = ["orders", "daily_revenue", "large_order_count", "revenue_report", "customers_orc", "clicks"]
    outputs += ["delta_events", "all_activity", "customers"]

    checks.start("Dry run: the graph is analyzed and nothing is written")
    ran(checks, "spark-pipelines dry-run completes", project.cli("dry-run"))
    created = [name for name in outputs if spark.catalog.tableExists(f"demo.{name}")]
    checks.equal("no dataset was created", created, [])

    checks.start("First run: SQL and Python datasets, file, Delta and table sources, ORC, AutoCDC")
    ran(checks, "spark-pipelines run completes", project.cli("run"))
    checks.equal("streaming table `orders` read the source", len(rows(spark, "demo.orders")), 3)
    checks.equal(
        "materialized view `daily_revenue` aggregates it",
        rows(spark, "demo.daily_revenue"),
        [("2026-11-01", 200.0, 2), ("2026-11-02", 120.0, 1)],
    )
    checks.equal("a temporary view feeds `large_order_count`", rows(spark, "demo.large_order_count"), [(2,)])
    checks.check(
        "the temporary view is not published", not spark.catalog.tableExists("demo.large_orders"), "it exists"
    )
    checks.equal(
        "persisted view `revenue_report` is queryable",
        rows(spark, "demo.revenue_report"),
        [("2026-11-01", 200.0), ("2026-11-02", 120.0)],
    )
    checks.equal(
        "dataset comments are published",
        spark.catalog.getTable("demo.daily_revenue").description,
        "Revenue and order count per day",
    )
    orc_files = list((workdir / "spark-warehouse").rglob("*.orc"))
    checks.check("`customers_orc` is stored as ORC files", bool(orc_files), "no .orc file in the warehouse")
    checks.equal("`customers_orc` reads back", rows(spark, "demo.customers_orc"), [(10, "Ada", "UK"), (11, "Linus", "FI")])
    checks.equal("`clicks` read the JSON files", rows(spark, "demo.clicks"), [(10, "home"), (11, "cart")])
    checks.equal("`delta_events` read the Delta table", rows(spark, "demo.delta_events"), [(0,), (1,), (2,)])
    checks.equal(
        "two append flows fill `all_activity`",
        rows(spark, "demo.all_activity", "kind"),
        [("click",), ("click",), ("order",), ("order",), ("order",)],
    )
    checks.equal(
        "AutoCDC applied the change feed (later sequence wins)",
        rows(spark, "demo.customers", "id", "name", "seq"),
        [(1, "alicia", 2), (2, "bob", 1)],
    )

    checks.start("Second run: streaming tables read only what is new")
    spark.sql("INSERT INTO demo.orders_raw VALUES (4, '2026-11-02', 11, 30.0)")
    write_json(clicks, "2.json", [{"user": 10, "page": "checkout"}])
    spark.range(3, 5).write.format("delta").mode("append").save(str(delta_source))
    write_json(
        changes,
        "2.json",
        [change(2, None, None, "D", 2), change(3, "carol", "DE", "I", 1), change(1, "stale", "US", "U", 1)],
    )
    ran(checks, "spark-pipelines run completes", project.cli("run"))
    checks.equal("`orders` appended only the new order", rows(spark, "demo.orders", "order_id"), [(1,), (2,), (3,), (4,)])
    checks.equal(
        "`daily_revenue` was recomputed",
        rows(spark, "demo.daily_revenue"),
        [("2026-11-01", 200.0, 2), ("2026-11-02", 150.0, 2)],
    )
    checks.equal("`clicks` appended only the new file", len(rows(spark, "demo.clicks")), 3)
    checks.equal("`delta_events` appended only the new Delta commit", len(rows(spark, "demo.delta_events")), 5)
    checks.equal("`all_activity` has 4 orders and 3 clicks", len(rows(spark, "demo.all_activity")), 7)
    checks.equal(
        "AutoCDC applied a delete, an insert, and ignored an out-of-order update",
        rows(spark, "demo.customers", "id", "name", "seq"),
        [(1, "alicia", 2), (3, "carol", 1)],
    )

    checks.start("Third run with no new data: nothing is read twice")
    before = {name: rows(spark, f"demo.{name}") for name in ["orders", "clicks", "delta_events", "all_activity", "customers"]}
    ran(checks, "spark-pipelines run completes", project.cli("run"))
    after = {name: rows(spark, f"demo.{name}") for name in before}
    checks.equal("every streaming table and the AutoCDC target are unchanged", after, before)

    checks.start("Selective refresh: only the named dataset runs")
    spark.sql("INSERT INTO demo.orders_raw VALUES (5, '2026-11-03', 10, 10.0)")
    ran(checks, "spark-pipelines run --refresh daily_revenue completes", project.cli("run", "--refresh", "daily_revenue"))
    checks.equal("`orders` was not refreshed", len(rows(spark, "demo.orders")), 4)
    checks.equal(
        "`daily_revenue` was recomputed from the current `orders`",
        [r[0] for r in rows(spark, "demo.daily_revenue")],
        ["2026-11-01", "2026-11-02"],
    )
    ran(checks, "a normal run then picks up the new order", project.cli("run"))
    checks.equal("`orders` now has the new order", len(rows(spark, "demo.orders")), 5)

    checks.start("Full refresh: streaming tables and the AutoCDC target are rebuilt from their sources")
    spark.sql("INSERT OVERWRITE TABLE demo.orders_raw VALUES (9, '2026-11-04', 11, 99.0)")
    ran(checks, "spark-pipelines run --full-refresh-all completes", project.cli("run", "--full-refresh-all"))
    checks.equal("`orders` holds only the current source", rows(spark, "demo.orders", "order_id"), [(9,)])
    checks.equal("`daily_revenue` follows", rows(spark, "demo.daily_revenue"), [("2026-11-04", 99.0, 1)])
    checks.equal("`clicks` re-read every file", len(rows(spark, "demo.clicks")), 3)
    checks.equal("`all_activity` was rebuilt: 1 order and 3 clicks", len(rows(spark, "demo.all_activity")), 4)
    checks.equal(
        "AutoCDC replayed the whole change feed to the same state",
        rows(spark, "demo.customers", "id", "name", "seq"),
        [(1, "alicia", 2), (3, "carol", 1)],
    )


def broken(checks: Checks, spark: SparkSession, server: Server, workdir: Path):
    checks.start("Errors: a broken graph fails before anything is written")
    project = Project("broken", workdir, server, {})
    code, output = project.cli("run")
    checks.check("the run fails", code != 0, f"exit {code}")
    checks.check(
        "the error names Spark's condition and the column",
        "UNRESOLVED_COLUMN" in output and "no_such_column" in output,
        output.strip()[-600:],
    )
    checks.check(
        "the valid dataset in the same graph was not created either",
        not spark.catalog.tableExists("demo.fine"),
        "demo.fine exists",
    )


def iggy(checks: Checks, spark: SparkSession, server: Server, workdir: Path, binary: Path | None):
    checks.start("Apache Iggy: a streaming table and an AutoCDC target read a topic")
    if binary is None:
        checks.skip("Iggy streaming", "no iggy-server binary given (see README: --iggy or IGGY_SERVER_BINARY)")
        return
    broker = Broker(binary, workdir)
    try:
        project = Project("iggy", workdir, server, {"IGGY_URL": broker.url})

        def message(key, name, op, seq):
            return {"id": key, "name": name, "op": op, "seq": seq}

        broker.send(message(1, "alice", "I", 1), message(2, "bob", "I", 1), message(1, "alicia", "U", 2))
        ran(checks, "first run completes", project.cli("run"))
        checks.equal("`iggy_raw` read the topic", len(rows(spark, "demo.iggy_raw")), 3)
        checks.equal(
            "`iggy_customers` applied the changes",
            rows(spark, "demo.iggy_customers", "id", "name", "seq"),
            [(1, "alicia", 2), (2, "bob", 1)],
        )
        broker.send(message(2, "", "D", 2), message(3, "carol", "I", 1), message(1, "stale", "U", 1))
        ran(checks, "second run completes", project.cli("run"))
        checks.equal("`iggy_raw` appended only the new messages", len(rows(spark, "demo.iggy_raw")), 6)
        checks.equal(
            "`iggy_customers` applied a delete and an insert, and ignored a stale update",
            rows(spark, "demo.iggy_customers", "id", "name", "seq"),
            [(1, "alicia", 2), (3, "carol", 1)],
        )
        ran(checks, "a run with no new messages completes", project.cli("run"))
        checks.equal("nothing was read twice", len(rows(spark, "demo.iggy_raw")), 6)
    finally:
        broker.stop()


def show(spark: SparkSession):
    print(f"\n{BOLD}== The datasets, as the last run left them{RESET}")
    for name in ["orders", "daily_revenue", "revenue_report", "customers_orc", "all_activity", "customers"]:
        print(f"\n  demo.{name}")
        spark.catalog.refreshTable(f"demo.{name}")
        frame = spark.table(f"demo.{name}")
        print("  " + " | ".join(frame.columns))
        for row in sorted(tuple(r) for r in frame.collect())[:8]:
            print("  " + " | ".join(str(v) for v in row))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sail", required=True, type=Path, help="the sail binary built from this branch")
    parser.add_argument("--iggy", type=Path, default=os.environ.get("IGGY_SERVER_BINARY"), help="an iggy-server binary")
    parser.add_argument("--workdir", type=Path, help="work directory (default: a new temporary directory)")
    parser.add_argument("--keep", action="store_true", help="keep the work directory")
    arguments = parser.parse_args()

    workdir = arguments.workdir or Path(tempfile.mkdtemp(prefix="sail-sdp-demo-"))
    workdir.mkdir(parents=True, exist_ok=True)
    print(f"{BOLD}Sail Declarative Pipelines demo{RESET}")
    print(f"  sail:    {arguments.sail}")
    print(f"  pyspark: {pyspark.__version__}")
    print(f"  iggy:    {arguments.iggy or 'not given'}")
    print(f"  workdir: {workdir}")

    checks = Checks()
    server = Server(arguments.sail.resolve(), workdir)
    spark = SparkSession.builder.remote(server.url).getOrCreate()
    try:
        for step in (retail, broken):
            try:
                step(checks, spark, server, workdir)
            except Exception:  # noqa: BLE001 report and continue with the next part
                checks.check(f"{step.__name__} completed without an unexpected error", False, traceback.format_exc())
        try:
            iggy(checks, spark, server, workdir, arguments.iggy)
        except Exception:  # noqa: BLE001
            checks.check("iggy completed without an unexpected error", False, traceback.format_exc())
        try:
            show(spark)
        except Exception:  # noqa: BLE001 display only
            pass
    finally:
        spark.stop()
        checks.start("Shutdown")
        code = server.stop()
        checks.check("the server stops cleanly on SIGINT", code == 0, f"exit {code}; see {server.log}")

    ok = checks.summary()
    print(f"\n  logs: {workdir}/sail-server.log and {workdir}/cli-*.log")
    if not arguments.keep and arguments.workdir is None and ok:
        shutil.rmtree(workdir, ignore_errors=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
