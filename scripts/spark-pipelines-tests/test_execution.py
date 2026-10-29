"""Server behavior beyond Spark's predominantly client-side Python SDP tests.

Run with SPARK_CONNECT_TESTING_REMOTE=sc://localhost:50051 pytest <this file>.
Upstream case names are recorded in test docstrings where the behavior is ported.
"""

import os

import pytest
from pyspark import pipelines as dp
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import (
    SparkConnectGraphElementRegistry,
)
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph,
    handle_pipeline_events,
    start_run,
)
from pyspark.sql import SparkSession
from pyspark.sql.connect import proto as pb


@pytest.fixture
def spark(tmp_path):
    session = SparkSession.builder.remote(os.environ["SPARK_CONNECT_TESTING_REMOTE"]).create()
    session.conf.set("spark.sql.warehouse.dir", (tmp_path / "warehouse").as_uri())
    try:
        yield session
    finally:
        session.stop()


@pytest.fixture
def pipeline(spark, tmp_path):
    graph_id = create_dataflow_graph(spark, None, None, None)
    registry = SparkConnectGraphElementRegistry(spark, graph_id)

    def run(dry=False, refresh=None, full_refresh=None, full_refresh_all=False):
        handle_pipeline_events(
            start_run(
                spark,
                graph_id,
                full_refresh,
                full_refresh_all,
                refresh,
                dry,
                (tmp_path / "state").as_uri(),
            )
        )

    return graph_id, registry, run


def test_dry_run_does_not_publish(spark, pipeline):
    """SparkConnectPipelinesTest.test_dry_run, with an explicit side-effect assertion."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def output():
            return spark.range(4)

    run(dry=True)
    assert not spark.catalog.tableExists("output")


def test_batch_dependencies_and_view_isolation(spark, pipeline):
    """ConnectValidPipelineSuite: Dependencies; definitions may appear out of order."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def result():
            return spark.table("intermediate").selectExpr("id * 2 AS doubled")

        @dp.temporary_view
        def intermediate():
            return spark.table("source").where("id > 1")

        @dp.materialized_view
        def source():
            return spark.range(4)

    run()
    assert [row.doubled for row in spark.table("result").orderBy("doubled").collect()] == [4, 6]
    assert not spark.catalog.tableExists("intermediate")
    assert spark.table("source").count() == 4


def test_repeated_materialized_view_refresh(spark, pipeline):
    _, registry, run = pipeline
    spark.range(2).write.saveAsTable("external_input")
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def output():
            return spark.table("external_input")

    run()
    spark.range(2, 5).write.mode("append").saveAsTable("external_input")
    run()
    assert [row.id for row in spark.table("output").orderBy("id").collect()] == list(range(5))


def test_selective_refresh_reads_committed_excluded_producers(spark, pipeline):
    """PipelineRefreshFunctionalSuite: selective refresh does not refresh dependencies."""
    _, registry, run = pipeline
    spark.range(1).write.saveAsTable("external_input")
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def producer():
            return spark.table("external_input")

        @dp.materialized_view
        def consumer():
            return spark.table("producer")

    run()
    spark.range(1, 3).write.mode("append").saveAsTable("external_input")
    run(refresh=["consumer"])
    assert spark.table("producer").count() == 1
    assert spark.table("consumer").count() == 1
    run(full_refresh=["producer"])
    assert spark.table("producer").count() == 3
    assert spark.table("consumer").count() == 1
    run(refresh=["consumer"])
    assert spark.table("consumer").count() == 3


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
def test_invalid_refresh_selection_does_not_publish(spark, pipeline, options, message):
    """PipelineRefreshFunctionalSuite: three conflicting selection validation cases."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def output():
            return spark.range(4)

    with pytest.raises(Exception, match=message):
        run(**options)
    assert not spark.catalog.tableExists("output")


def test_graph_validation_precedes_any_write(spark, pipeline):
    """ConnectInvalidPipelineSuite: Missing attribute in the schema."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def valid():
            return spark.range(4)

        @dp.materialized_view
        def invalid():
            return spark.range(4).selectExpr("missing_column")

    with pytest.raises(AnalysisException):
        run()
    assert not spark.catalog.tableExists("valid")


@pytest.mark.parametrize("dry", [True, False])
def test_cycle_does_not_read_old_catalog_output(spark, pipeline, dry):
    """ConnectInvalidPipelineSuite: Cyclic graph with materialized nodes."""
    _, registry, run = pipeline
    spark.range(1).write.saveAsTable("left_node")
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def left_node():
            return spark.table("right_node")

        @dp.materialized_view
        def right_node():
            return spark.table("left_node")

    with pytest.raises(AnalysisException, match="PIPELINE_CIRCULAR_DEPENDENCY"):
        run(dry=dry)
    assert not spark.catalog.tableExists("right_node")
    assert spark.table("left_node").count() == 1


def test_empty_pipeline(pipeline):
    """SqlPipelineSuite: No table defined pipeline fails with RUN_EMPTY_PIPELINE."""
    _, _, run = pipeline
    with pytest.raises(AnalysisException, match="RUN_EMPTY_PIPELINE"):
        run(dry=True)


def test_only_temporary_views(spark, pipeline):
    """SqlPipelineSuite: Pipeline with only temp views fails with RUN_EMPTY_PIPELINE."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.temporary_view
        def intermediate():
            return spark.range(1)

    with pytest.raises(AnalysisException, match="RUN_EMPTY_PIPELINE"):
        run(dry=True)


def test_duplicate_output(spark, pipeline):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):
        dp.materialized_view(name="duplicate")(lambda: spark.range(1))
        dp.materialized_view(name="duplicate")(lambda: spark.range(2))
    with pytest.raises(AnalysisException, match="PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT"):
        run(dry=True)


def test_streaming_table_requires_stream(spark, pipeline):
    """ConnectInvalidPipelineSuite: Streaming table backed by batch relation fails validation."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.table
        def output():
            return spark.range(1)

    with pytest.raises(AnalysisException, match="BATCH_RELATION_FOR_STREAMING_TABLE"):
        run(dry=True)


def test_materialized_view_requires_batch(spark, pipeline):
    """ConnectInvalidPipelineSuite: Materialized view backed by streaming relation fails validation."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def output():
            return spark.readStream.format("rate").load()

    with pytest.raises(AnalysisException, match="STREAMING_RELATION_FOR_MATERIALIZED_VIEW"):
        run(dry=True)


def test_drop_graph_preserves_tables(spark, pipeline):
    graph_id, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def output():
            return spark.range(3)

    run()
    command = pb.Command()
    command.pipeline_command.drop_dataflow_graph.dataflow_graph_id = graph_id
    spark.client.execute_command(command)
    assert spark.table("output").count() == 3
    with pytest.raises(Exception, match="does not exist"):
        run()


def test_quoted_identifier_with_dot(spark, pipeline):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):
        dp.materialized_view(name="`output.with.dot`")(lambda: spark.range(2))
    run()
    assert spark.table("`output.with.dot`").count() == 2
