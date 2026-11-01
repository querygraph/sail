import pytest
from pyspark.errors.exceptions.connect import IllegalArgumentException

try:
    from pyspark import pipelines as dp
    from pyspark.pipelines.graph_element_registry import graph_element_registration_context
except ImportError:
    pytest.skip("Declarative pipelines require Spark 4.1+", allow_module_level=True)


@pytest.fixture(autouse=True)
def drop_output(pipeline_spark):
    # Spark's catalog and warehouse outlive the session, and a leftover directory fails the next case.
    yield
    pipeline_spark.sql("DROP TABLE IF EXISTS output")


@pytest.mark.parametrize(
    "properties, dry, expected",
    [
        ({"pipelines.unknown": "x", "custom": "keep"}, False, {"custom": "keep"}),
        ({"PIPELINES.RESET.ALLOWED": "false"}, False, {"pipelines.reset.allowed": "false"}),
        ({"pipelines.reset.allowed": "maybe"}, False, None),
        ({"pipelines.reset.allowed": "maybe"}, True, None),
        ({"PIPELINES.RESET.ALLOWED": "maybe"}, False, {"pipelines.reset.allowed": "maybe"}),
        ({"pipelines.reset.allowed": " TRUE "}, False, None),
    ],
)
def test_pipeline_table_properties(pipeline_spark, pipeline, properties, dry, expected):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view(table_properties=properties)
        def output():
            return pipeline_spark.range(1)

    if expected is None:
        with pytest.raises(IllegalArgumentException, match="Could not parse value"):
            run(dry=dry)
        assert not pipeline_spark.catalog.tableExists("output")
    else:
        run(dry=dry)
        stored = {
            row.key: row.value
            for row in pipeline_spark.sql("SHOW TBLPROPERTIES output").collect()
            if row.key.lower().startswith("pipelines.") or row.key == "custom"
        }
        assert stored == expected
        assert pipeline_spark.table("output").count() == 1
