"""Query analysis precedes graph semantic validation (Spark 4.2.1)."""

import pytest
from pyspark import pipelines as dp
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.graph_element_registry import graph_element_registration_context


@pytest.mark.parametrize("missing", ["table", "column"])
def test_unresolved_query_precedes_batch_flow_for_streaming_table(pipeline_spark, pipeline_factory, missing):
    failures = []
    for define in (dp.materialized_view, dp.table):
        _, registry, run = pipeline_factory()
        with graph_element_registration_context(registry):

            @define(name="output")
            def output():
                if missing == "table":
                    return pipeline_spark.table("missing_source")
                return pipeline_spark.range(1).selectExpr("missing_column")

        with pytest.raises(AnalysisException) as error:
            run(dry=True)
        # Spark reports the underlying query through flow events and raises an
        # UnresolvedPipelineException. Preserve that failure when changing the
        # destination type; streamingness must not mask unsuccessful analysis.
        failures.append(
            (
                type(error.value),
                error.value.getCondition(),
                error.value.getSqlState(),
                error.value.getMessageParameters(),
            )
        )
        assert "BATCH_RELATION_FOR_STREAMING_TABLE" not in str(error.value)
        assert not pipeline_spark.catalog.tableExists("output")
    assert failures[0] == failures[1]
