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
    [
        "id BIGINT",
        "id BIGINT, optional STRING",
        StructType([StructField("id", LongType(), metadata={"comment": "identifier"})]),
    ],
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


def test_inferred_columns_must_be_declared(pipeline_spark, pipeline):
    """ConnectInvalidPipelineSuite: inferred schema is not a subset of the declaration."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view(schema="expected BIGINT")
        def output():
            return pipeline_spark.range(2).selectExpr("id AS `incorrect-col-name`")

    with pytest.raises(AnalysisException, match="USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE") as error:
        run(dry=True)
    assert "incorrect-col-name" in str(error.value)
    assert not pipeline_spark.catalog.tableExists("output")


def test_declared_nonnullable_field_cannot_receive_nullable_data(pipeline_spark, pipeline):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view(schema=StructType([StructField("id", LongType(), nullable=False)]))
        def output():
            return pipeline_spark.createDataFrame([(1,), (None,)], "id BIGINT")

    with pytest.raises(AnalysisException, match="USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE"):
        run(dry=True)
    assert not pipeline_spark.catalog.tableExists("output")
