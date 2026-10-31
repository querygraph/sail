"""Original DDLSuite pipeline declarations reject outside pipeline execution."""

import pytest
from pyspark.errors.exceptions.connect import UnsupportedOperationException


@pytest.mark.parametrize("kind", ["MATERIALIZED VIEW", "STREAMING TABLE"])
def test_pipeline_dataset_direct_execution_has_spark_diagnostic(spark, kind):
    assert not spark.catalog.tableExists("table1")
    with pytest.raises(UnsupportedOperationException) as captured:
        spark.sql(f"CREATE {kind} table1 AS SELECT 1").collect()
    error = captured.value
    assert error.getCondition() == "UNSUPPORTED_FEATURE.CREATE_PIPELINE_DATASET_QUERY_EXECUTION"
    assert error.getSqlState() == "0A000"
    assert error.getMessageParameters() == {"pipelineDatasetType": kind}
    assert not spark.catalog.tableExists("table1")


def test_flow_direct_execution_has_spark_diagnostic(spark):
    try:
        spark.sql("CREATE TABLE table1 AS SELECT 1").collect()
        spark.sql("CREATE TABLE table2 AS SELECT 2").collect()
        with pytest.raises(UnsupportedOperationException) as captured:
            spark.sql("CREATE FLOW f AS INSERT INTO table2 SELECT * FROM table1").collect()
        error = captured.value
        assert error.getCondition() == "UNSUPPORTED_FEATURE.CREATE_FLOW_QUERY_EXECUTION"
        assert error.getSqlState() == "0A000"
        assert error.getMessageParameters() == {}
    finally:
        spark.sql("DROP TABLE IF EXISTS table1").collect()
        spark.sql("DROP TABLE IF EXISTS table2").collect()
