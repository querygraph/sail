"""Structured batch graph-registration errors from Spark's SQL pipeline suite."""

import pytest
from pyspark.errors.exceptions.connect import AnalysisException

pytest.importorskip("pyspark.pipelines")


@pytest.mark.parametrize("dry", [False, True])
@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("", id="empty-file"),
        pytest.param("CREATE TEMPORARY VIEW a AS SELECT id FROM range(1, 3)", id="temporary-only"),
        pytest.param("CREATE FLOW f AS INSERT INTO a BY NAME SELECT 1", id="flow-only"),
    ],
)
def test_sql_without_persisted_output_has_structured_error(pipeline_spark, pipeline, tmp_path, sql, dry):
    _, registry, run = pipeline
    registry.register_sql(sql, tmp_path / "empty.sql")
    with pytest.raises(AnalysisException) as error:
        run(dry=dry)
    assert error.value.getCondition() == "RUN_EMPTY_PIPELINE"
    assert error.value.getMessageParameters() == {}
    assert error.value.getSqlState() == "42617"
    assert not pipeline_spark.catalog.tableExists("a")


@pytest.mark.parametrize("dry", [False, True])
@pytest.mark.parametrize(
    ("definitions", "private", "types"),
    [
        pytest.param(
            ["CREATE STREAMING TABLE duplicate", "CREATE VIEW duplicate AS SELECT 1 AS value"],
            False,
            ["TABLE", "VIEW"],
            id="table-view",
        ),
        pytest.param(
            ["CREATE VIEW duplicate AS SELECT 1 AS value", "CREATE STREAMING TABLE duplicate"],
            False,
            ["TABLE", "VIEW"],
            id="view-table",
        ),
        pytest.param(
            ["CREATE MATERIALIZED VIEW duplicate AS SELECT 1 AS value"] * 2,
            False,
            ["TABLE", "TABLE"],
            id="materialized-materialized",
        ),
        pytest.param(
            ["CREATE STREAMING TABLE duplicate", "CREATE MATERIALIZED VIEW duplicate AS SELECT 1 AS value"],
            False,
            ["TABLE", "TABLE"],
            id="streaming-materialized",
        ),
        pytest.param(
            ["CREATE VIEW duplicate AS SELECT 1 AS value"] * 2,
            False,
            ["VIEW", "VIEW"],
            id="persisted-persisted",
        ),
        pytest.param(
            ["CREATE TEMPORARY VIEW duplicate AS SELECT 1 AS value"] * 2,
            True,
            ["VIEW", "VIEW"],
            id="temporary-temporary",
        ),
        pytest.param(
            ["CREATE VIEW duplicate AS SELECT 1 AS value"] * 2 + ["CREATE STREAMING TABLE duplicate"],
            False,
            ["TABLE", "VIEW"],
            id="views-before-table",
        ),
    ],
)
def test_duplicate_outputs_have_structured_types(pipeline_spark, pipeline, tmp_path, definitions, private, types, dry):
    _, registry, run = pipeline
    registry.register_sql("CREATE MATERIALIZED VIEW valid_output AS SELECT 1 AS value", tmp_path / "valid.sql")
    for index, definition in enumerate(definitions):
        registry.register_sql(definition, tmp_path / f"duplicate_{index}.sql")
    with pytest.raises(AnalysisException) as error:
        run(dry=dry)
    identifier = "`duplicate`"
    if not private:
        identifier = (
            f"`{pipeline_spark.catalog.currentCatalog()}`.`{pipeline_spark.catalog.currentDatabase()}`.{identifier}"
        )
    assert error.value.getCondition() == "PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT"
    assert error.value.getSqlState() == "42710"
    assert error.value.getMessageParameters() == {
        "outputName": identifier,
        "outputType1": types[0],
        "outputType2": types[1],
    }
    assert not pipeline_spark.catalog.tableExists("valid_output")
    assert not pipeline_spark.catalog.tableExists("duplicate")
