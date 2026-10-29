import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.sql.types import LongType, StructField, StructType

try:
    from pyspark import pipelines as dp
    from pyspark.pipelines.graph_element_registry import graph_element_registration_context
except ImportError:
    pytest.skip("Declarative pipelines require Spark 4.1+", allow_module_level=True)


@pytest.mark.parametrize(
    "schema",
    ["id BIGINT", StructType([StructField("id", LongType(), metadata={"comment": "identifier"})])],
)
def test_declared_schema_validation(pipeline_spark, pipeline, schema):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view(schema=schema)
        def output():
            return pipeline_spark.range(3)

    run(dry=True)
    assert not pipeline_spark.catalog.tableExists("output")


def test_incompatible_declared_schema_prevents_all_writes(pipeline_spark, pipeline):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def valid():
            return pipeline_spark.range(1)

        @dp.materialized_view(schema="id STRING")
        def invalid():
            return pipeline_spark.range(1)

    with pytest.raises(AnalysisException, match="USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE"):
        run()
    assert not pipeline_spark.catalog.tableExists("valid")
    assert not pipeline_spark.catalog.tableExists("invalid")
