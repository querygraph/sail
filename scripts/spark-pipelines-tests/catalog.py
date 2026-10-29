"""Check batch pipeline table publication through a configured catalog.

Requires PySpark[connect,pipelines] 4.2.1. Creates a unique table, refreshes its
rows/comments/columns, verifies its location, and drops it. This is a focused
catalog acceptance runner; it does not run Spark's Scala suites or test views.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pyspark
from pyspark.pipelines import spark_connect_pipeline as client
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.sql import SparkSession

SPARK_VERSION = "4.2.1"


@dataclass(frozen=True)
class Case:
    name: str
    definition: str
    column: str
    data_type: str
    rows: list[int | str]
    comment: str | None = None


CASES = (
    Case("initial", "AS SELECT id AS value FROM range(3)", "value", "bigint", [0, 1, 2]),
    Case("refresh", "AS SELECT id AS value FROM range(2, 4)", "value", "bigint", [2, 3]),
    Case(
        "column_comment",
        "(value BIGINT COMMENT 'refreshed') {using} AS SELECT id AS value FROM range(4, 5)",
        "value",
        "bigint",
        [4],
        "refreshed",
    ),
    Case("replace_columns", "AS SELECT 'new' AS other", "other", "string", ["new"]),
)


def check_case(spark: SparkSession, case: Case, table: str, provider: str, storage: str) -> dict[str, Any]:
    graph_id = client.create_dataflow_graph(spark, None, None, None)
    registry = SparkConnectGraphElementRegistry(spark, graph_id)
    using = f"USING {provider}"
    definition = case.definition.format(using=using)
    if "{using}" not in case.definition:
        definition = f"{using} {definition}"
    sql = f"CREATE MATERIALIZED VIEW `{table}` {definition};"
    registry.register_sql(sql, Path(f"{case.name}.sql"))
    client.handle_pipeline_events(client.start_run(spark, graph_id, None, False, None, False, storage))
    frame = spark.table(table)
    assert frame.columns == [case.column], frame.columns
    field = frame.schema[case.column]
    assert field.dataType.simpleString() == case.data_type, field
    assert field.nullable, field
    assert field.metadata.get("comment") == case.comment, field.metadata
    rows = [row[0] for row in frame.orderBy(case.column).collect()]
    assert rows == case.rows, (rows, case.rows)
    location_row = spark.sql(f"DESCRIBE EXTENDED `{table}`").where("col_name = 'Location'").first()
    assert location_row is not None and location_row.data_type, "missing table location"
    return {
        "name": case.name,
        "outcome": "passed",
        "rows": rows,
        "schema": frame.schema.jsonValue(),
        "location": location_row.data_type,
        "sql": sql,
    }


def execute(args: argparse.Namespace, report: dict[str, Any]) -> bool:
    spark = SparkSession.builder.remote(args.remote).getOrCreate()
    table = f"sail_pipeline_catalog_{uuid.uuid4().hex}"
    report["table"] = table
    success = True
    owns_name = False
    try:
        report["server_spark_version"] = spark.version
        if spark.version != SPARK_VERSION:
            raise RuntimeError(f"Expected server Spark {SPARK_VERSION}, got {spark.version}")
        if args.catalog:
            spark.catalog.setCurrentCatalog(args.catalog)
        if args.database:
            spark.catalog.setCurrentDatabase(args.database)
        if args.warehouse:
            spark.conf.set("spark.sql.warehouse.dir", args.warehouse)
        report.update(catalog=spark.catalog.currentCatalog(), database=spark.catalog.currentDatabase())
        assert not spark.catalog.tableExists(table), "generated table name already exists"
        owns_name = True
        location = None
        for case in CASES:
            try:
                result = check_case(spark, case, table, args.provider, args.storage.rstrip("/") + "/" + table)
                if location is None:
                    location = result["location"]
                assert result["location"] == location, "refresh replaced table location"
                report["cases"].append(result)
            except Exception as error:  # noqa: BLE001
                report["cases"].append({"name": case.name, "outcome": "failed", "error": str(error)})
                success = False
                break
    except Exception as error:  # noqa: BLE001
        report["setup_error"] = str(error)
        success = False
    finally:
        try:
            if owns_name:
                spark.sql(f"DROP TABLE IF EXISTS `{table}`").collect()
                assert not spark.catalog.tableExists(table), "temporary table survived cleanup"
                report["cleanup"] = "passed"
            else:
                report["cleanup"] = "not-needed"
        except Exception as error:  # noqa: BLE001
            report["cleanup"] = str(error)
            success = False
        spark.stop()
    return success


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", required=True)
    parser.add_argument("--provider", choices=("parquet", "iceberg", "delta"), required=True)
    parser.add_argument("--storage", required=True, help="owned pipeline storage URI")
    parser.add_argument("--warehouse", help="optional Sail session warehouse URI")
    parser.add_argument("--catalog")
    parser.add_argument("--database")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if not __debug__:
        parser.error("Run without -O so acceptance assertions are enabled")
    if not urlparse(args.storage).scheme:
        parser.error("Supply an owned storage URI including its scheme")
    if pyspark.__version__ != SPARK_VERSION:
        parser.error(f"Expected PySpark {SPARK_VERSION}, got {pyspark.__version__}")
    root = Path(__file__).resolve().parents[2]
    commit = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    report: dict[str, Any] = {
        "runner_commit": commit,
        "client_spark_version": pyspark.__version__,
        "provider": args.provider,
        "cases": [],
        "case_specs": [asdict(c) for c in CASES],
    }
    try:
        success = execute(args, report)
    except Exception as error:  # noqa: BLE001
        report["runner_error"] = str(error)
        success = False
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    if not success:
        raise SystemExit(1)
    print(f"Passed {len(CASES)} catalog cases and cleanup; report: {args.report}")


if __name__ == "__main__":
    main()
