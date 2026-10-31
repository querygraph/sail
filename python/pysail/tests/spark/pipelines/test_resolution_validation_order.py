"""Resolution failures precede persisted-view source validation in Spark 4.2.1."""

import pytest
from pyspark.errors.exceptions.connect import AnalysisException


@pytest.mark.parametrize("dry", [False, True])
@pytest.mark.parametrize("missing_first", [False, True])
def test_missing_query_precedes_persisted_view_temporary_source(pipeline_spark, pipeline, tmp_path, dry, missing_first):
    _, registry, run = pipeline
    view = "CREATE TEMPORARY VIEW temporary AS SELECT 1 AS id; CREATE VIEW persisted AS SELECT * FROM temporary"
    missing = "CREATE MATERIALIZED VIEW missing AS SELECT * FROM missing_source"
    text = f"{missing};{view}" if missing_first else f"{view};{missing}"
    registry.register_sql(text, tmp_path / "validation.sql")
    with pytest.raises(AnalysisException) as error:
        run(dry=dry)
    namespace = f"{pipeline_spark.catalog.currentCatalog()}.{pipeline_spark.catalog.currentDatabase()}"
    assert error.value.getCondition() is None
    assert f"Flows with errors: {namespace}.missing\n" in error.value.getMessage()
    assert "INVALID_TEMP_OBJ_REFERENCE" not in error.value.getMessage()
    assert not pipeline_spark.catalog.tableExists("persisted")
    assert not pipeline_spark.catalog.tableExists("missing")
