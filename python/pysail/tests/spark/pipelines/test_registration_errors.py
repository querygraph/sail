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
