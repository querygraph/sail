"""Check the six batch namespace pairs from Spark SqlPipelineSuite:759.

Both catalogs must already be configured on the server. The runner creates two
unique namespaces and unique tables, checks data/provider identity, then removes
its catalog objects. Requires PySpark[connect,pipelines] 4.2.1.
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
class Case:
    name: str
    upstream: str
    downstream: str


CASES = (
    Case("unqualified", "default", "default"),
    Case("unqualified_to_database", "default", "database"),
    Case("unqualified_to_catalog", "default", "catalog"),
    Case("database_to_unqualified", "database", "default"),
    Case("catalog_to_unqualified", "catalog", "default"),
    Case("catalog_to_database", "catalog", "database"),
)


def quoted(*parts: str) -> str:
    return ".".join("`" + part.replace("`", "``") + "`" for part in parts)


def check_case(
    spark: SparkSession,
    args: argparse.Namespace,
    case: Case,
    index: int,
    prefix: str,
    namespaces: dict[str, tuple[str, str]],
    owned: list[str],
) -> dict[str, Any]:
    names = []
    qualified = []
    providers = []
    for scope, label in [(case.upstream, "upstream"), (case.downstream, "downstream")]:
        catalog, database = namespaces[scope]
        table = f"{prefix}_{label}_{index}"
        full_name = quoted(catalog, database, table)
        assert not spark.catalog.tableExists(full_name), "generated table name already exists"
        owned.append(full_name)
        qualified.append(full_name)
        names.append(
            quoted(table) if scope == "default" else quoted(database, table) if scope == "database" else full_name
        )
        providers.append(args.secondary_provider if scope == "catalog" else args.primary_provider)
    graph_id = client.create_dataflow_graph(spark, args.primary_catalog, namespaces["default"][1], None)
    try:
        registry = SparkConnectGraphElementRegistry(spark, graph_id)
        sql = (
            f"CREATE MATERIALIZED VIEW {names[0]} (id BIGINT) USING {providers[0]} AS SELECT * FROM RANGE(10);\n"
            f"CREATE MATERIALIZED VIEW {names[1]} USING {providers[1]} AS SELECT id FROM {names[0]} WHERE (id%2)=0;"
        )
        registry.register_sql(sql, Path(f"{case.name}.sql"))
        client.handle_pipeline_events(
            client.start_run(
                spark, graph_id, None, False, None, False, args.storage.rstrip("/") + "/" + prefix + f"/{index}"
            )
        )
        rows = [row.id for row in spark.table(names[1]).orderBy("id").collect()]
        assert rows == [0, 2, 4, 6, 8], rows
        assert [row.id for row in spark.table(names[0]).orderBy("id").collect()] == list(range(10))
        schemas = []
        for table, provider in zip(qualified, providers, strict=True):
            frame = spark.table(table)
            assert frame.schema.simpleString() == "struct<id:bigint>", frame.schema
            row = spark.sql(f"DESCRIBE EXTENDED {table}").where("col_name = 'Provider'").first()
            assert row is not None and row.data_type == provider, row
            schemas.append(frame.schema.jsonValue())
        return {
            "name": case.name,
            "outcome": "passed",
            "sql": sql,
            "tables": qualified,
            "providers": providers,
            "schemas": schemas,
            "rows": rows,
        }
    finally:
        command = pb.Command()
        command.pipeline_command.drop_dataflow_graph.dataflow_graph_id = graph_id
        spark.client.execute_command(command)


def execute(args: argparse.Namespace, report: dict[str, Any]) -> bool:
    spark = SparkSession.builder.remote(args.remote).getOrCreate()
    prefix = "sdp_namespaces_" + uuid4().hex
    owned: list[str] = []
    created: list[str] = []
    success = True
    try:
        if spark.version != SPARK_VERSION:
            raise RuntimeError(f"Expected server Spark {SPARK_VERSION}, got {spark.version}")
        report["server_spark_version"] = spark.version
        spark.catalog.setCurrentCatalog(args.primary_catalog)
        if args.database:
            spark.catalog.setCurrentDatabase(args.database)
        database = spark.catalog.currentDatabase()
        if args.warehouse:
            spark.conf.set("spark.sql.warehouse.dir", args.warehouse)
        namespaces = {
            "default": (args.primary_catalog, database),
            "database": (args.primary_catalog, prefix + "_db"),
            "catalog": (args.secondary_catalog, prefix + "_db"),
        }
        report["namespaces"] = namespaces
        for scope in ["database", "catalog"]:
            name = quoted(*namespaces[scope])
            assert not spark.catalog.databaseExists(name), "generated namespace already exists"
            spark.sql(f"CREATE DATABASE {name}").collect()
            created.append(name)
        for index, case in enumerate(CASES):
            try:
                report["cases"].append(check_case(spark, args, case, index, prefix, namespaces, owned))
            except Exception as error:  # noqa: BLE001
                report["cases"].append(
                    {"name": case.name, "outcome": "failed", "error_type": type(error).__name__, "error": str(error)}
                )
                success = False
                break
    except Exception as error:  # noqa: BLE001
        report["setup_error"] = str(error)
        success = False
    finally:
        cleanup_errors = []
        for table in reversed(owned):
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table}").collect()
                assert not spark.catalog.tableExists(table), "temporary table survived cleanup"
            except Exception as error:  # noqa: BLE001
                cleanup_errors.append(f"{table}: {error}")
        for namespace in reversed(created):
            try:
                spark.sql(f"DROP DATABASE {namespace}").collect()
                assert not spark.catalog.databaseExists(namespace), "temporary namespace survived cleanup"
            except Exception as error:  # noqa: BLE001
                cleanup_errors.append(f"{namespace}: {error}")
        report["cleanup"] = {"outcome": "failed" if cleanup_errors else "passed", "errors": cleanup_errors}
        success = success and not cleanup_errors
        spark.stop()
    return success


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--primary-catalog", required=True)
    parser.add_argument("--secondary-catalog", required=True)
    parser.add_argument("--primary-provider", choices=["parquet", "iceberg", "delta"], default="parquet")
    parser.add_argument("--secondary-provider", choices=["parquet", "iceberg", "delta"], default="parquet")
    parser.add_argument("--database")
    parser.add_argument("--warehouse", help="Optional Sail session warehouse override")
    parser.add_argument("--storage", required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if not __debug__:
        parser.error("Optimized Python disables acceptance assertions")
    if args.primary_catalog == args.secondary_catalog:
        parser.error("The namespace matrix requires two distinct catalogs")
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
    report: dict[str, Any] = {"runner_commit": commit.stdout.strip() or None, "cases": []}
    try:
        success = execute(args, report)
    except Exception as error:  # noqa: BLE001
        report["runner_error"] = str(error)
        success = False
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
