"""Spark 4.2.1 read-mode checks against a view's analyzed streamingness."""

import pytest
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run

CHECK = "pipelines.incompatibleViewCheck.enabled"
SOURCE = "CREATE MATERIALIZED VIEW src AS SELECT 1 AS id;"
BATCH_VIEW = "CREATE TEMPORARY VIEW v AS SELECT * FROM src;"
STREAM_VIEW = "CREATE TEMPORARY VIEW v AS SELECT * FROM STREAM(src);"
UNION_VIEW = "CREATE TEMPORARY VIEW v AS SELECT * FROM src UNION ALL SELECT * FROM STREAM(src);"
CTE_VIEW = "CREATE TEMPORARY VIEW v AS WITH c AS (SELECT * FROM STREAM(src)) SELECT * FROM c;"
STREAMING_TABLE = "CREATE STREAMING TABLE consumer AS SELECT * FROM "
MATERIALIZED_VIEW = "CREATE MATERIALIZED VIEW consumer AS SELECT * FROM "
BATCH_READ = "INCOMPATIBLE_BATCH_VIEW_READ"
STREAM_READ = "INCOMPATIBLE_STREAMING_VIEW_READ"
BATCH_FLOW = "INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE"
STREAM_FLOW = "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW"


def dry_run(spark, tmp_path, sql):
    """Return the flow event messages and the terminal error text of a dry run."""
    graph = create_dataflow_graph(spark, None, None, None)
    SparkConnectGraphElementRegistry(spark, graph).register_sql(sql, tmp_path / "views.sql")
    messages = []
    terminal = None
    try:
        for result in start_run(spark, graph, None, False, None, True, (tmp_path / "state").as_uri()):
            if "pipeline_event_result" in result:
                messages.append(result["pipeline_event_result"].event.message)
    except Exception as error:  # noqa: BLE001 - the terminal outcome is the observation
        terminal = str(error)
    return messages, terminal


def assert_outcome(spark, tmp_path, sql, expected):
    messages, terminal = dry_run(spark, tmp_path, sql)
    if expected is None:
        assert terminal is None, (terminal, messages)
    else:
        assert terminal is not None
        assert f"[{expected}]" in " ".join([*messages, terminal]), (terminal, messages)


@pytest.mark.parametrize(
    ("view", "consumer", "default", "disabled"),
    [
        (BATCH_VIEW, STREAMING_TABLE + "STREAM(v)", BATCH_READ, BATCH_FLOW),
        (BATCH_VIEW, STREAMING_TABLE + "v", BATCH_FLOW, BATCH_FLOW),
        (BATCH_VIEW, MATERIALIZED_VIEW + "v", None, None),
        (BATCH_VIEW, MATERIALIZED_VIEW + "STREAM(v)", BATCH_READ, None),
        (STREAM_VIEW, STREAMING_TABLE + "STREAM(v)", None, None),
        (STREAM_VIEW, STREAMING_TABLE + "v", STREAM_READ, None),
        (STREAM_VIEW, MATERIALIZED_VIEW + "v", STREAM_READ, STREAM_FLOW),
        (STREAM_VIEW, MATERIALIZED_VIEW + "STREAM(v)", STREAM_FLOW, STREAM_FLOW),
    ],
)
@pytest.mark.parametrize("flag", [None, "true", "false"])
def test_read_mode_against_the_analyzed_view(pipeline_spark, tmp_path, view, consumer, default, disabled, flag):
    set_flag = f"SET {CHECK}={flag};" if flag else ""
    expected = disabled if flag == "false" else default
    assert_outcome(pipeline_spark, tmp_path, SOURCE + view + set_flag + consumer, expected)


@pytest.mark.parametrize(
    ("view", "consumer", "expected"),
    [
        (UNION_VIEW, STREAMING_TABLE + "STREAM(v)", None),
        (UNION_VIEW, STREAMING_TABLE + "v", STREAM_READ),
        (CTE_VIEW, STREAMING_TABLE + "STREAM(v)", None),
        (CTE_VIEW, STREAMING_TABLE + "v", STREAM_READ),
    ],
)
def test_streamingness_comes_from_the_whole_view(pipeline_spark, tmp_path, view, consumer, expected):
    assert_outcome(pipeline_spark, tmp_path, SOURCE + view + consumer, expected)


@pytest.mark.parametrize("consumer", ["w", "STREAM(w)"])
def test_streamingness_passes_through_a_batch_read_of_a_view(pipeline_spark, tmp_path, consumer):
    sql = (
        SOURCE
        + STREAM_VIEW
        + f"SET {CHECK}=false;CREATE TEMPORARY VIEW w AS SELECT * FROM v;"
        + STREAMING_TABLE
        + consumer
    )
    assert_outcome(pipeline_spark, tmp_path, sql, None)


def test_the_session_setting_disables_the_check(pipeline_spark, tmp_path):
    pipeline_spark.conf.set(CHECK, "false")
    try:
        assert_outcome(pipeline_spark, tmp_path, SOURCE + BATCH_VIEW + STREAMING_TABLE + "STREAM(v)", BATCH_FLOW)
    finally:
        pipeline_spark.conf.unset(CHECK)


def test_a_view_setting_does_not_exempt_its_own_first_read(pipeline_spark, tmp_path):
    sql = SOURCE + STREAMING_TABLE + f"STREAM(v);SET {CHECK}=false;" + BATCH_VIEW
    assert_outcome(pipeline_spark, tmp_path, sql, BATCH_READ)


def test_an_invalid_setting_fails_only_a_flow_reading_a_view(pipeline_spark, tmp_path):
    sql = f"{SOURCE}{BATCH_VIEW}SET {CHECK}=maybe;{STREAMING_TABLE}STREAM(v)"
    messages, terminal = dry_run(pipeline_spark, tmp_path, sql)
    assert terminal is not None
    assert any('For input string: "maybe"' in message for message in messages), messages


def test_error_identifies_the_view_and_the_setting(pipeline_spark, tmp_path):
    messages, _ = dry_run(pipeline_spark, tmp_path, SOURCE + STREAM_VIEW + MATERIALIZED_VIEW + "v")
    expected = (
        "[INCOMPATIBLE_STREAMING_VIEW_READ] View `v` is a streaming view and must be referenced using "
        "SparkSession#readStream. This check can be disabled by setting Spark conf "
        "pipelines.incompatibleViewCheck.enabled = false."
    )
    assert any(expected in message for message in messages), messages
