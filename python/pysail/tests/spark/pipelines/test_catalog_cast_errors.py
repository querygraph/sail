"""Catalog constant evaluation keeps native ANSI cast execution errors."""

import pytest
from pyspark.errors.exceptions.connect import NumberFormatException

pytest.importorskip("pyspark.pipelines")


@pytest.mark.parametrize("file_ansi", ["true", "false"])
def test_catalog_cast_failure_uses_session_ansi_and_native_metadata(pipeline_spark, pipeline, tmp_path, file_ansi):
    _, registry, _ = pipeline
    pipeline_spark.conf.set("spark.sql.ansi.enabled", "true")
    catalog = pipeline_spark.catalog.currentCatalog()
    database = pipeline_spark.catalog.currentDatabase()
    sql = f"SET spark.sql.ansi.enabled={file_ansi};\nSET CATALOG CAST(CAST('bad' AS INT) AS STRING)"
    with pytest.raises(NumberFormatException) as error:
        registry.register_sql(sql, tmp_path / "ansi.sql")
    assert error.value.getCondition() == "CAST_INVALID_INPUT"
    assert error.value.getSqlState() == "22018"
    assert error.value.getMessageParameters() == {
        "expression": "'bad'",
        "sourceType": '"STRING"',
        "targetType": '"INT"',
        "ansiConfig": '"spark.sql.ansi.enabled"',
    }
    assert pipeline_spark.catalog.currentCatalog() == catalog
    assert pipeline_spark.catalog.currentDatabase() == database
