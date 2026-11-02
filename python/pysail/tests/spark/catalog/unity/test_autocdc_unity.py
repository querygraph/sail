"""AutoCDC into a Delta table that Unity Catalog manages.

Unity Catalog ratifies the table's commits, so the flow takes the plain path:
progress is the pipeline checkpoint, and a replayed merge is safe because rows
win by sequence.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

try:
    from pyspark import pipelines as dp
    from pyspark.pipelines import spark_connect_pipeline as client
    from pyspark.pipelines.graph_element_registry import graph_element_registration_context
    from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
except ImportError:
    pytest.skip("Declarative pipelines require Spark 4.1+", allow_module_level=True)

from pysail.testing.containers.unity_defaults import DEFAULT_CATALOG

SCHEMA = "id INT, name STRING, seq BIGINT, is_delete BOOLEAN"
TARGET = f"{DEFAULT_CATALOG}.default.autocdc_target"


def run(spark: SparkSession, landing: Path, storage: Path, **options: object) -> None:
    graph = client.create_dataflow_graph(spark, None, None, None)
    registry = SparkConnectGraphElementRegistry(spark, graph)
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def changes():  # noqa: ANN202
            return spark.readStream.format("json").schema(SCHEMA).load(str(landing))

        dp.create_streaming_table(TARGET, format="delta")
        dp.create_auto_cdc_flow(
            target=TARGET,
            source="changes",
            keys=["id"],
            sequence_by="seq",
            apply_as_deletes=sf.expr("is_delete"),
            except_column_list=["is_delete"],
        )
    client.handle_pipeline_events(
        client.start_run(
            spark,
            graph,
            None,
            options.get("full_refresh_all", False),
            None,
            False,  # noqa: FBT003
            storage.as_uri(),
        )
    )


def send(landing: Path, name: str, *rows: tuple) -> None:
    keys = ["id", "name", "seq", "is_delete"]
    (landing / f"{name}.json").write_text("".join(json.dumps(dict(zip(keys, row))) + "\n" for row in rows))


def test_autocdc_runs_into_a_unity_managed_delta_table(spark: SparkSession, tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    storage = tmp_path / "storage"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DEFAULT_CATALOG}.default")
    spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
    try:
        send(landing, "a", (1, "alice", 1, False), (2, "bob", 1, False))
        run(spark, landing, storage)
        send(landing, "b", (1, "alicia", 2, False), (2, "", 3, True), (1, "stale", 1, False))
        run(spark, landing, storage)
        rows = sorted(tuple(row) for row in spark.table(TARGET).select("id", "name", "seq").collect())
        assert rows == [(1, "alicia", 2)]
        run(spark, landing, storage)
        assert sorted(tuple(row) for row in spark.table(TARGET).select("id", "name").collect()) == [(1, "alicia")]
        run(spark, landing, storage, full_refresh_all=True)
        assert sorted(tuple(row) for row in spark.table(TARGET).select("id", "name").collect()) == [(1, "alicia")]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
