"""Pipeline DataFrame API behavior against Spark Connect."""

import pytest
from pyspark.errors.exceptions.connect import AnalysisException, IllegalArgumentException
from pyspark.sql.connect import proto as pb

try:
    from pyspark import pipelines as dp
    from pyspark.pipelines.graph_element_registry import graph_element_registration_context
except ImportError:
    pytest.skip("Declarative pipelines require Spark 4.1+", allow_module_level=True)


def test_dry_run_does_not_publish(pipeline_spark, pipeline):
    """SparkConnectPipelinesTest.test_dry_run, with an explicit side-effect assertion."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def output():
            return pipeline_spark.range(4)

    run(dry=True)
    assert not pipeline_spark.catalog.tableExists("output")


def test_batch_dependencies_and_view_isolation(pipeline_spark, pipeline):
    """ConnectValidPipelineSuite: Dependencies; definitions may appear out of order."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def result():
            return pipeline_spark.table("intermediate").selectExpr("id * 2 AS doubled")

        @dp.temporary_view
        def intermediate():
            return pipeline_spark.table("source").where("id > 1")

        @dp.materialized_view
        def source():
            return pipeline_spark.range(4)

    run()
    assert [row.doubled for row in pipeline_spark.table("result").orderBy("doubled").collect()] == [4, 6]
    assert not pipeline_spark.catalog.tableExists("intermediate")
    assert pipeline_spark.table("source").count() == 4


def test_repeated_materialized_view_refresh(pipeline_spark, pipeline):
    _, registry, run = pipeline
    pipeline_spark.range(2).write.saveAsTable("external_input")
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def output():
            return pipeline_spark.table("external_input")

    run()
    pipeline_spark.range(2, 5).write.mode("append").saveAsTable("external_input")
    run()
    assert [row.id for row in pipeline_spark.table("output").orderBy("id").collect()] == list(range(5))


def test_materialized_view_comment_is_published(pipeline_spark, pipeline):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view(comment="daily summary")
        def output():
            return pipeline_spark.range(2)

    run(dry=True)
    assert not pipeline_spark.catalog.tableExists("output")
    run()
    assert pipeline_spark.catalog.getTable("output").description == "daily summary"
    assert pipeline_spark.table("output").count() == 2


def test_selective_refresh_reads_committed_excluded_producers(pipeline_spark, pipeline):
    """PipelineRefreshFunctionalSuite: selective refresh does not refresh dependencies."""
    _, registry, run = pipeline
    pipeline_spark.range(1).write.saveAsTable("external_input")
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def producer():
            return pipeline_spark.table("external_input")

        @dp.materialized_view
        def consumer():
            return pipeline_spark.table("producer")

    run()
    pipeline_spark.range(1, 3).write.mode("append").saveAsTable("external_input")
    run(refresh=["consumer"])
    assert pipeline_spark.table("producer").count() == 1
    assert pipeline_spark.table("consumer").count() == 1
    run(full_refresh=["producer"])
    assert pipeline_spark.table("producer").count() == 3
    assert pipeline_spark.table("consumer").count() == 1
    run(refresh=["consumer"])
    assert pipeline_spark.table("consumer").count() == 3


@pytest.mark.parametrize(
    "options, message",
    [
        ({"refresh": ["output"], "full_refresh_all": True}, "Cannot specify a subset to refresh"),
        (
            {"full_refresh": ["output"], "full_refresh_all": True},
            "Cannot specify a subset to full refresh",
        ),
        ({"refresh": ["output"], "full_refresh": ["default.output"]}, "cannot overlap"),
    ],
)
def test_invalid_refresh_selection_does_not_publish(pipeline_spark, pipeline, options, message):
    """PipelineRefreshFunctionalSuite: three conflicting selection validation cases."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def output():
            return pipeline_spark.range(4)

    with pytest.raises(IllegalArgumentException, match=message):
        run(**options)
    assert not pipeline_spark.catalog.tableExists("output")


def test_graph_validation_precedes_any_write(pipeline_spark, pipeline):
    """ConnectInvalidPipelineSuite: Missing attribute in the schema."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def valid():
            return pipeline_spark.range(4)

        @dp.materialized_view
        def invalid():
            return pipeline_spark.range(4).selectExpr("missing_column")

    with pytest.raises(AnalysisException):
        run()
    assert not pipeline_spark.catalog.tableExists("valid")


@pytest.mark.parametrize("dry", [True, False])
def test_cycle_does_not_read_old_catalog_output(pipeline_spark, pipeline, dry):
    """ConnectInvalidPipelineSuite: Cyclic graph with materialized nodes."""
    _, registry, run = pipeline
    pipeline_spark.range(1).write.saveAsTable("left_node")
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def left_node():
            return pipeline_spark.table("right_node")

        @dp.materialized_view
        def right_node():
            return pipeline_spark.table("left_node")

    with pytest.raises(AnalysisException, match="PIPELINE_CIRCULAR_DEPENDENCY"):
        run(dry=dry)
    assert not pipeline_spark.catalog.tableExists("right_node")
    assert pipeline_spark.table("left_node").count() == 1


def test_empty_pipeline(pipeline):
    """SqlPipelineSuite: No table defined pipeline fails with RUN_EMPTY_PIPELINE."""
    _, _, run = pipeline
    with pytest.raises(AnalysisException, match="RUN_EMPTY_PIPELINE"):
        run(dry=True)


def test_only_temporary_views(pipeline_spark, pipeline):
    """SqlPipelineSuite: Pipeline with only temp views fails with RUN_EMPTY_PIPELINE."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def intermediate():
            return pipeline_spark.range(1)

    with pytest.raises(AnalysisException, match="RUN_EMPTY_PIPELINE"):
        run(dry=True)


def test_duplicate_output(pipeline_spark, pipeline):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):
        dp.materialized_view(name="duplicate")(lambda: pipeline_spark.range(1))
        dp.materialized_view(name="duplicate")(lambda: pipeline_spark.range(2))
    with pytest.raises(AnalysisException, match="PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT"):
        run(dry=True)


def test_streaming_table_requires_stream(pipeline_spark, pipeline):
    """ConnectInvalidPipelineSuite: Streaming table backed by batch relation fails validation."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.table
        def output():
            return pipeline_spark.range(1)

    with pytest.raises(AnalysisException, match="BATCH_RELATION_FOR_STREAMING_TABLE"):
        run(dry=True)


def test_materialized_view_requires_batch(pipeline_spark, pipeline):
    """ConnectInvalidPipelineSuite: Materialized view backed by streaming relation fails validation."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def output():
            return pipeline_spark.readStream.format("rate").load()

    with pytest.raises(AnalysisException, match="STREAMING_RELATION_FOR_MATERIALIZED_VIEW"):
        run(dry=True)


def test_drop_graph_preserves_tables(pipeline_spark, pipeline):
    graph_id, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def output():
            return pipeline_spark.range(3)

    run()
    command = pb.Command()
    command.pipeline_command.drop_dataflow_graph.dataflow_graph_id = graph_id
    pipeline_spark.client.execute_command(command)
    assert pipeline_spark.table("output").count() == 3
    with pytest.raises(IllegalArgumentException, match="does not exist"):
        run()


def test_quoted_identifier_with_dot(pipeline_spark, pipeline):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):
        dp.materialized_view(name="`output.with.dot`")(lambda: pipeline_spark.range(2))
    run()
    assert pipeline_spark.table("`output.with.dot`").count() == 2
