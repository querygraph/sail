"""SQL file-local catalog and namespace scope in batch pipelines."""

from pathlib import Path
from uuid import uuid4

import pytest
from pyspark.sql.connect import proto as pb

pytest.importorskip("pyspark.pipelines")


@pytest.mark.parametrize("mode", ["explicit-namespace", "use-after-set", "file-reset", "private-view"])
def test_catalog_scope_is_local_to_sql_file(pipeline_spark, pipeline, tmp_path, mode):
    _, registry, run = pipeline
    catalog = pipeline_spark.catalog.currentCatalog()
    database = pipeline_spark.catalog.currentDatabase()
    other = "scope_" + uuid4().hex
    output = "scope_output"
    original_output = f"`{catalog}`.`{database}`.{output}"
    other_output = f"`{catalog}`.{other}.{output}"
    assert not pipeline_spark.catalog.tableExists(original_output)
    pipeline_spark.sql(f"CREATE DATABASE {other}")
    expected = {other_output: [7]}
    try:
        set_catalog = f"SET CATALOG `{catalog}`;"
        if mode == "explicit-namespace":
            files = [f"{set_catalog} CREATE MATERIALIZED VIEW {other}.{output} AS SELECT 7 AS value"]
        elif mode == "use-after-set":
            files = [f"{set_catalog} USE DATABASE {other}; CREATE MATERIALIZED VIEW {output} AS SELECT 7 AS value"]
        elif mode == "file-reset":
            files = [
                f"{set_catalog} USE DATABASE {other}; CREATE MATERIALIZED VIEW {output} AS SELECT 7 AS value",
                f"CREATE MATERIALIZED VIEW {output} AS SELECT 8 AS value",
            ]
            expected[original_output] = [8]
        else:
            files = [
                (
                    f"{set_catalog} CREATE TEMPORARY VIEW scope_private AS SELECT 7 AS value; "
                    f"USE DATABASE {other}; CREATE MATERIALIZED VIEW {output} AS SELECT * FROM scope_private"
                )
            ]
        for index, sql in enumerate(files):
            registry.register_sql(sql, tmp_path / f"scope_{index}.sql")
        assert pipeline_spark.catalog.currentCatalog() == catalog
        assert pipeline_spark.catalog.currentDatabase() == database
        run()
        for name, rows in expected.items():
            assert [row.value for row in pipeline_spark.table(name).collect()] == rows
        assert not pipeline_spark.catalog.tableExists("scope_private")
        if mode != "file-reset":
            assert not pipeline_spark.catalog.tableExists(original_output)
    finally:
        for name in expected:
            pipeline_spark.sql(f"DROP TABLE IF EXISTS {name}")
        pipeline_spark.sql(f"DROP DATABASE {other}")


def test_multipart_outputs_across_two_namespaces(pipeline_spark, pipeline):
    """SqlPipelineSuite:562: both qualified outputs return Row(1)."""
    graph_id, registry, run = pipeline
    suffix = uuid4().hex
    first = "multipart_first_" + suffix
    second = "multipart_second_" + suffix
    outputs = [f"{first}.mv1", f"{second}.mv2"]
    created = []
    try:
        for database in [first, second]:
            assert not pipeline_spark.catalog.databaseExists(database)
            pipeline_spark.sql(f"CREATE DATABASE {database}").collect()
            created.append(database)
        registry.register_sql(
            f"CREATE MATERIALIZED VIEW {first}.mv1 AS SELECT 1; "
            f"CREATE MATERIALIZED VIEW {second}.mv2 AS SELECT * FROM {first}.mv1;",
            Path("multipart.sql"),
        )
        run()
        for output in outputs:
            assert [tuple(row) for row in pipeline_spark.sql(f"SELECT * FROM {output}").collect()] == [(1,)]
    finally:
        command = pb.Command()
        command.pipeline_command.drop_dataflow_graph.dataflow_graph_id = graph_id
        pipeline_spark.client.execute_command(command)
        for output in reversed(outputs):
            pipeline_spark.sql(f"DROP TABLE IF EXISTS {output}").collect()
            assert not pipeline_spark.catalog.tableExists(output)
        for database in reversed(created):
            pipeline_spark.sql(f"DROP DATABASE {database}").collect()
            assert not pipeline_spark.catalog.databaseExists(database)
