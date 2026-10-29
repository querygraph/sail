"""Pipeline DataFrame API behavior against Spark Connect."""

import pytest
from pyspark.errors.exceptions import connect as errors
from pyspark.errors.exceptions.connect import (
    AnalysisException,
    IllegalArgumentException,
    UnknownException,
)
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


def test_repeated_materialized_view_refresh(pipeline_spark, pipeline_factory):
    pipeline_spark.range(2).write.saveAsTable("external_input")

    def refresh():
        _, registry, run = pipeline_factory()
        with graph_element_registration_context(registry):

            @dp.materialized_view
            def output():
                return pipeline_spark.table("external_input")

        run()

    refresh()
    pipeline_spark.range(2, 5).write.mode("append").saveAsTable("external_input")
    refresh()
    assert [row.id for row in pipeline_spark.table("output").orderBy("id").collect()] == list(range(5))


@pytest.mark.parametrize("dry", [True, False])
def test_materialized_view_comment_is_published(pipeline_spark, pipeline, dry):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view(comment="daily summary")
        def output():
            return pipeline_spark.range(2)

    run(dry=dry)
    if dry:
        assert not pipeline_spark.catalog.tableExists("output")
    else:
        assert pipeline_spark.catalog.getTable("output").description == "daily summary"
        assert pipeline_spark.table("output").count() == 2


def test_selective_refresh_reads_committed_excluded_producers(pipeline_spark, pipeline_factory):
    """PipelineRefreshFunctionalSuite: selective refresh does not refresh dependencies."""
    pipeline_spark.range(1).write.saveAsTable("external_input")

    def refresh(**options):
        _, registry, run = pipeline_factory()
        with graph_element_registration_context(registry):

            @dp.materialized_view
            def producer():
                return pipeline_spark.table("external_input")

            @dp.materialized_view
            def consumer():
                return pipeline_spark.table("producer")

        run(**options)

    refresh()
    pipeline_spark.range(1, 3).write.mode("append").saveAsTable("external_input")
    refresh(refresh=["consumer"])
    assert pipeline_spark.table("producer").count() == 1
    assert pipeline_spark.table("consumer").count() == 1
    refresh(full_refresh=["producer"])
    assert pipeline_spark.table("producer").count() == 3
    assert pipeline_spark.table("consumer").count() == 1
    refresh(refresh=["consumer"])
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
    # Invalid command options do not reserve the graph's execution entry.
    run(dry=True)


@pytest.mark.parametrize("dry", [True, False])
def test_graph_can_only_be_started_once(pipeline_spark, pipeline, dry):
    graph_id, registry, run = pipeline
    with graph_element_registration_context(registry):
        dp.materialized_view(name="output")(lambda: pipeline_spark.range(2))

    run(dry=dry)
    with pytest.raises(UnknownException) as error:
        run(dry=True)
    assert error.value.getCondition() == (
        "SPARK_CONNECT_ILLEGAL_STATE.EXECUTION_STATE_EXECUTE_HOLDER_ALREADY_EXISTS_GRAPH"
    )
    assert error.value.getMessageParameters() == {"graphId": graph_id}
    assert error.value.getSqlState() == "XXSC0"
    assert pipeline_spark.catalog.tableExists("output") is not dry
    if not dry:
        assert pipeline_spark.table("output").count() == 2


def test_failed_analysis_still_reserves_the_graph(pipeline_spark, pipeline):
    graph_id, registry, run = pipeline
    with graph_element_registration_context(registry):
        dp.materialized_view(name="output")(lambda: pipeline_spark.range(2).selectExpr("missing_column"))

    # Spark wraps the underlying unresolved column in UnresolvedPipelineException.
    # The failure still occurs after the execution entry has been reserved.
    with pytest.raises(AnalysisException):
        run(dry=True)
    with pytest.raises(UnknownException) as error:
        run(dry=True)
    assert error.value.getCondition() == (
        "SPARK_CONNECT_ILLEGAL_STATE.EXECUTION_STATE_EXECUTE_HOLDER_ALREADY_EXISTS_GRAPH"
    )
    assert error.value.getMessageParameters() == {"graphId": graph_id}


def test_empty_registration_can_be_corrected_before_start(pipeline_spark, pipeline):
    _, registry, run = pipeline
    with pytest.raises(AnalysisException) as error:
        run(dry=True)
    assert error.value.getCondition() == "RUN_EMPTY_PIPELINE"
    assert error.value.getMessageParameters() == {}
    assert error.value.getSqlState() == "42617"
    with graph_element_registration_context(registry):
        dp.materialized_view(name="output")(lambda: pipeline_spark.range(2))
    run(dry=True)
    assert not pipeline_spark.catalog.tableExists("output")


def test_duplicate_registration_does_not_reserve_the_graph(pipeline_spark, pipeline):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):
        dp.materialized_view(name="output")(lambda: pipeline_spark.range(2))
        dp.materialized_view(name="output")(lambda: pipeline_spark.range(3))
    for _ in range(2):
        with pytest.raises(AnalysisException, match="PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT"):
            run(dry=True)


def test_missing_storage_does_not_reserve_the_graph(pipeline_spark, pipeline):
    graph_id, registry, run = pipeline
    with graph_element_registration_context(registry):
        dp.materialized_view(name="output")(lambda: pipeline_spark.range(2))

    command = pb.Command()
    command.pipeline_command.start_run.dataflow_graph_id = graph_id
    command.pipeline_command.start_run.dry = True
    with pytest.raises(IllegalArgumentException, match="Storage must be specified"):
        pipeline_spark.client.execute_command(command)
    run(dry=True)
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
    with pytest.raises(AnalysisException) as error:
        run(dry=True)
    assert error.value.getCondition() == "RUN_EMPTY_PIPELINE"
    assert error.value.getMessageParameters() == {}
    assert error.value.getSqlState() == "42617"


def test_only_temporary_views(pipeline_spark, pipeline):
    """SqlPipelineSuite: Pipeline with only temp views fails with RUN_EMPTY_PIPELINE."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def intermediate():
            return pipeline_spark.range(1)

    with pytest.raises(AnalysisException) as error:
        run(dry=True)
    assert error.value.getCondition() == "RUN_EMPTY_PIPELINE"
    assert error.value.getMessageParameters() == {}
    assert error.value.getSqlState() == "42617"


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
    with pytest.raises(getattr(errors, "SparkException", UnknownException)) as error:
        run()
    assert error.value.getCondition() == "DATAFLOW_GRAPH_NOT_FOUND"
    assert error.value.getMessageParameters() == {"graphId": graph_id}
    assert error.value.getSqlState() == "KD011"


def test_quoted_identifier_with_dot(pipeline_spark, pipeline):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):
        dp.materialized_view(name="`output.with.dot`")(lambda: pipeline_spark.range(2))
    run()
    assert pipeline_spark.table("`output.with.dot`").count() == 2
