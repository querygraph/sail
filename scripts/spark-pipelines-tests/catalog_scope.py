"""Port Spark 4.2.1 SqlPipelineSuite:634, 'Use database and set catalog works'.

Both catalogs must already be configured. Adapter changes are explicit storage
providers, unique namespaces and public Connect graph registration. The two SQL
files, STREAM(mv2) CTE reference and five output assertions are retained. This is
an assertion port, not execution of the original Scala suite.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

import pyspark
from pyspark.pipelines import spark_connect_pipeline as client
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.sql import SparkSession
from pyspark.sql.connect import proto as pb

SPARK_VERSION = "4.2.1"


@dataclass(frozen=True)
class Output:
    catalog: str
    database: str
    table: str
    provider: str
    expected: tuple[int, ...]
    upstream_assertion: bool

    @property
    def name(self) -> str:
        return quoted(self.catalog, self.database, self.table)


def quoted(*parts: str) -> str:
    return ".".join("`" + part.replace("`", "``") + "`" for part in parts)


def execute(args: argparse.Namespace, report: dict[str, Any]) -> bool:
    spark = SparkSession.builder.remote(args.remote).getOrCreate()
    prefix = "sdp_scope_" + uuid4().hex
    created: list[str] = []
    owned: list[str] = []
    private_names: list[str] = []
    graph_id: str | None = None
    original_catalog: str | None = None
    original_database: str | None = None
    success = False
    stage = "setup"
    try:
        if spark.version != SPARK_VERSION:
            raise RuntimeError(f"Expected server Spark {SPARK_VERSION}, got {spark.version}")
        report["server_spark_version"] = spark.version
        original_catalog = spark.catalog.currentCatalog()
        original_database = spark.catalog.currentDatabase()
        if args.warehouse:
            spark.conf.set("spark.sql.warehouse.dir", args.warehouse)
        pipeline_database = args.database or prefix + "_pipeline"
        other_database = prefix + "_other"
        secondary_database = prefix + "_secondary"
        namespaces = {
            "pipeline": (args.primary_catalog, pipeline_database),
            "database": (args.primary_catalog, other_database),
            "catalog": (args.secondary_catalog, secondary_database),
        }
        report["namespaces"] = namespaces
        for scope, parts in namespaces.items():
            namespace = quoted(*parts)
            if scope == "pipeline" and args.database:
                assert spark.catalog.databaseExists(namespace), "requested pipeline namespace does not exist"
            else:
                assert not spark.catalog.databaseExists(namespace), "generated namespace already exists"
                created.append(namespace)
                spark.sql(f"CREATE DATABASE {namespace}").collect()
        spark.catalog.setCurrentCatalog(args.primary_catalog)
        spark.catalog.setCurrentDatabase(pipeline_database)
        outputs = [
            Output(args.primary_catalog, pipeline_database, "mv", args.primary_provider, (0, 1, 2), False),
            Output(args.primary_catalog, other_database, "mv2", args.primary_provider, (1, 2, 3, 4), False),
            Output(args.primary_catalog, other_database, "mv3", args.primary_provider, (1, 2), True),
            Output(args.secondary_catalog, secondary_database, "mv4", args.secondary_provider, (2, 4), True),
            Output(args.primary_catalog, other_database, "mv5", args.primary_provider, (1, 2, 3, 4), True),
            Output(args.primary_catalog, other_database, "mv6", args.primary_provider, (0, 1, 2), True),
            Output(args.primary_catalog, pipeline_database, "mv6", args.primary_provider, (1, 2, 3, 4), True),
        ]
        for output in outputs:
            assert not spark.catalog.tableExists(output.name), f"fixture output already exists: {output.name}"
            owned.append(output.name)
        for parts in namespaces.values():
            name = quoted(*parts, "tv")
            assert not spark.catalog.tableExists(name), f"fixture view already exists: {name}"
            private_names.append(name)
        assert not spark.catalog.tableExists("tv"), "fixture temporary view already exists"
        secondary = quoted(args.secondary_catalog)
        pipeline_ns = quoted(args.primary_catalog, pipeline_database)
        other_ns = quoted(args.primary_catalog, other_database)
        files = [
            (
                "file1.sql",
                f"""
CREATE MATERIALIZED VIEW mv USING {args.primary_provider} AS SELECT * FROM RANGE(3);
USE DATABASE {quoted(other_database)};
CREATE MATERIALIZED VIEW mv2 USING {args.primary_provider} AS SELECT * FROM RANGE(1, 5);
CREATE MATERIALIZED VIEW mv3 USING {args.primary_provider} AS
WITH mv AS (SELECT * FROM {pipeline_ns}.mv)
SELECT mv2.id FROM mv JOIN mv2 ON mv.id=mv2.id;
SET CATALOG {secondary};
USE DATABASE {quoted(secondary_database)};
CREATE TEMPORARY VIEW tv AS SELECT * FROM {pipeline_ns}.mv;
CREATE MATERIALIZED VIEW mv4 USING {args.secondary_provider} AS
WITH mv2 AS (SELECT * FROM {other_ns}.mv2)
SELECT * FROM STREAM(mv2) WHERE mv2.id % 2 == 0;
USE NAMESPACE {other_ns};
CREATE MATERIALIZED VIEW mv5 USING {args.primary_provider} AS SELECT * FROM mv2;
CREATE MATERIALIZED VIEW mv6 USING {args.primary_provider} AS SELECT * FROM tv;
""",
            ),
            (
                "file2.sql",
                f"CREATE MATERIALIZED VIEW mv6 USING {args.primary_provider} AS SELECT * FROM {other_ns}.mv5;",
            ),
        ]
        report["files"] = [{"path": path, "sql": sql} for path, sql in files]
        graph_id = client.create_dataflow_graph(spark, args.primary_catalog, pipeline_database, None)
        registry = SparkConnectGraphElementRegistry(spark, graph_id)
        stage = "registration"
        for path, sql in files:
            registry.register_sql(sql, Path(path))
        assert spark.catalog.currentCatalog() == args.primary_catalog
        assert spark.catalog.currentDatabase() == pipeline_database
        stage = "run"
        client.handle_pipeline_events(
            client.start_run(spark, graph_id, None, False, None, False, args.storage.rstrip("/") + "/" + prefix)
        )
        stage = "assertions"
        success = True
        for output in outputs:
            result: dict[str, Any] = {
                "name": output.name,
                "expected": list(output.expected),
                "upstream_assertion": output.upstream_assertion,
                "provider": output.provider,
            }
            try:
                # Preserve the upstream two-part reads for mv5 and mv6, so
                # session catalog resolution remains part of the assertion.
                read_name = (
                    quoted(output.database, output.table)
                    if output.database == other_database and output.table in {"mv5", "mv6"}
                    else output.name
                )
                result["query"] = f"SELECT * FROM {read_name}"
                frame = spark.sql(result["query"])
                rows = [row.id for row in frame.orderBy("id").collect()]
                result["rows"] = rows
                assert rows == list(output.expected), rows
                assert frame.schema.simpleString() == "struct<id:bigint>", frame.schema
                provider = spark.sql(f"DESCRIBE EXTENDED {output.name}").where("col_name = 'Provider'").first()
                assert provider is not None and provider.data_type == output.provider, provider
                result["outcome"] = "passed"
            except Exception as error:  # noqa: BLE001
                result.update(outcome="failed", error_type=type(error).__name__, error=str(error))
                success = False
            report["outputs"].append(result)
        for name in ["tv", *private_names]:
            assert not spark.catalog.tableExists(name), f"private view leaked to catalog: {name}"
        assert spark.catalog.currentCatalog() == args.primary_catalog
        assert spark.catalog.currentDatabase() == pipeline_database
        report["private_view_hidden"] = True
        report["session_defaults_preserved"] = True
    except Exception as error:  # noqa: BLE001
        report["failure"] = {"stage": stage, "error_type": type(error).__name__, "error": str(error)}
        success = False
    finally:
        cleanup_errors = []
        if graph_id is not None:
            try:
                command = pb.Command()
                command.pipeline_command.drop_dataflow_graph.dataflow_graph_id = graph_id
                spark.client.execute_command(command)
            except Exception as error:  # noqa: BLE001
                cleanup_errors.append(f"graph {graph_id}: {error}")
        for table in reversed(owned):
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table}").collect()
                assert not spark.catalog.tableExists(table), "fixture table survived cleanup"
            except Exception as error:  # noqa: BLE001
                cleanup_errors.append(f"{table}: {error}")
        for name in private_names:
            try:
                if spark.catalog.tableExists(name):
                    spark.sql(f"DROP VIEW {name}").collect()
                assert not spark.catalog.tableExists(name), "fixture view survived cleanup"
            except Exception as error:  # noqa: BLE001
                cleanup_errors.append(f"{name}: {error}")
        if original_catalog is not None and original_database is not None:
            try:
                spark.catalog.setCurrentCatalog(original_catalog)
                spark.catalog.setCurrentDatabase(original_database)
            except Exception as error:  # noqa: BLE001
                cleanup_errors.append(f"restore session defaults: {error}")
        for namespace in reversed(created):
            try:
                spark.sql(f"DROP DATABASE IF EXISTS {namespace}").collect()
                assert not spark.catalog.databaseExists(namespace), "fixture namespace survived cleanup"
            except Exception as error:  # noqa: BLE001
                cleanup_errors.append(f"{namespace}: {error}")
        try:
            spark.stop()
        except Exception as error:  # noqa: BLE001
            cleanup_errors.append(f"stop session: {error}")
        report["cleanup"] = {"outcome": "failed" if cleanup_errors else "passed", "errors": cleanup_errors}
        success = success and not cleanup_errors
    return success


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--primary-catalog", required=True)
    parser.add_argument("--secondary-catalog", required=True)
    parser.add_argument("--primary-provider", choices=["parquet", "iceberg", "delta"], default="parquet")
    parser.add_argument("--secondary-provider", choices=["parquet", "iceberg", "delta"], default="parquet")
    parser.add_argument("--database", help="Existing pipeline namespace; default creates a unique namespace")
    parser.add_argument("--warehouse", help="Optional Sail session warehouse override")
    parser.add_argument("--storage", required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if not __debug__:
        parser.error("Optimized Python disables acceptance assertions")
    if args.primary_catalog == args.secondary_catalog:
        parser.error("The combined catalog fixture requires two distinct catalogs")
    if not urlparse(args.storage).scheme:
        parser.error("Storage must have a URI scheme")
    if pyspark.__version__ != SPARK_VERSION:
        parser.error(f"Expected PySpark {SPARK_VERSION}, got {pyspark.__version__}")
    commit = subprocess.run(
        ["git", "-C", str(Path(__file__).resolve().parents[2]), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    report: dict[str, Any] = {
        "runner_commit": commit.stdout.strip() or None,
        "upstream": "SqlPipelineSuite:634:Use database and set catalog works",
        "scope": "Assertion port through public Connect, not original Scala suite execution",
        "adapter_changes": ["unique namespaces", "preconfigured catalogs", "explicit storage providers"],
        "outputs": [],
    }
    try:
        success = execute(args, report)
    except Exception as error:  # noqa: BLE001
        report["runner_error"] = str(error)
        success = False
    report["outcome"] = "passed" if success else "failed"
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
