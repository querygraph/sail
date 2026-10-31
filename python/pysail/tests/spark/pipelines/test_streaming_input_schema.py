"""Spark 4.2.1 schemas exposed by streaming reads of materialized virtual inputs.

Spark reads these through a row encoder, so a STREAM read differs from a batch read.
A declared consumer observes the input type without failing the first flow call,
which an expression error would do before any merged second call can run.
"""

import pytest
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run

FRAMEWORK = "spark.sql.types.framework.enabled"
PRESERVE_CHAR = "spark.sql.preserveCharVarcharTypeInfo"
INCOMPATIBLE = "USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE"
TIME_SOURCE = "CREATE MATERIALIZED VIEW source AS SELECT CAST(NULL AS TIME(3)) AS id;"


@pytest.fixture
def session(pipeline_spark):
    pipeline_spark.conf.set("spark.sql.timeType.enabled", "true")
    yield pipeline_spark
    for key in ["spark.sql.timeType.enabled", FRAMEWORK, PRESERVE_CHAR]:
        pipeline_spark.conf.unset(key)


def dry_run(spark, tmp_path, sql, confs=None):
    """Return the flow failure messages and the terminal error text of a dry run."""
    graph = create_dataflow_graph(spark, None, None, confs)
    registry = SparkConnectGraphElementRegistry(spark, graph)
    registry.register_sql(sql, tmp_path / "streaming-input.sql")
    messages = []
    terminal = None
    try:
        for result in start_run(spark, graph, None, False, None, True, (tmp_path / "state").as_uri()):
            if "pipeline_event_result" in result:
                messages.append(result["pipeline_event_result"].event.message)
    except Exception as error:  # noqa: BLE001 - the terminal outcome is the observation
        terminal = str(error)
    return messages, terminal


def assert_declaration_outcome(spark, tmp_path, sql, *, compatible, confs=None):
    messages, terminal = dry_run(spark, tmp_path, sql, confs)
    if compatible:
        assert terminal is None, (terminal, messages)
    else:
        assert terminal is not None
        assert INCOMPATIBLE in terminal


@pytest.mark.parametrize(
    ("session_value", "graph_value", "flow_value", "effective"),
    [
        ("false", None, None, False),
        ("true", None, None, True),
        ("false", "true", None, True),
        ("true", "false", None, False),
        ("false", None, "true", True),
        ("true", None, "false", False),
        ("false", "false", "true", True),
        ("true", "true", "false", False),
    ],
)
@pytest.mark.parametrize("declared", [3, 6])
def test_time_follows_the_flow_effective_setting(
    session, tmp_path, session_value, graph_value, flow_value, effective, declared
):
    session.conf.set(FRAMEWORK, session_value)
    flow_set = f"SET {FRAMEWORK}={flow_value};" if flow_value else ""
    sql = (
        TIME_SOURCE
        + flow_set
        + f"CREATE STREAMING TABLE consumer (id TIME({declared})) AS SELECT id FROM STREAM(source)"
    )
    confs = {FRAMEWORK: graph_value} if graph_value else None
    assert_declaration_outcome(session, tmp_path, sql, compatible=(declared == 3) == effective, confs=confs)


@pytest.mark.parametrize("view_value", [False, True])
@pytest.mark.parametrize("session_value", ["false", "true"])
@pytest.mark.parametrize("declared", [3, 6])
def test_a_view_setting_reaches_the_read_on_the_second_call(session, tmp_path, view_value, session_value, declared):
    session.conf.set(FRAMEWORK, session_value)
    sql = (
        TIME_SOURCE
        + f"CREATE STREAMING TABLE consumer (id TIME({declared}), k INT) AS SELECT id, k FROM STREAM(source), v;"
        + f"SET {FRAMEWORK}={str(view_value).lower()};"
        + "CREATE TEMPORARY VIEW v AS SELECT 1 AS k"
    )
    assert_declaration_outcome(session, tmp_path, sql, compatible=(declared == 3) == view_value)


@pytest.mark.parametrize("framework", ["false", "true"])
def test_batch_reads_keep_the_input_type(session, tmp_path, framework):
    session.conf.set(FRAMEWORK, framework)
    sql = (
        "CREATE MATERIALIZED VIEW source AS SELECT CAST(NULL AS TIME(3)) AS t, CAST(NULL AS INTERVAL YEAR) AS i;"
        "CREATE MATERIALIZED VIEW consumer (t TIME(3), i INTERVAL YEAR) AS SELECT t, i FROM source"
    )
    assert_declaration_outcome(session, tmp_path, sql, compatible=True)


@pytest.mark.parametrize("framework", ["false", "true"])
@pytest.mark.parametrize(
    ("source", "exposed", "narrow"),
    [
        ("INTERVAL YEAR", "INTERVAL YEAR TO MONTH", "INTERVAL YEAR"),
        ("INTERVAL HOUR", "INTERVAL DAY TO SECOND", "INTERVAL HOUR"),
        ("ARRAY<INTERVAL MONTH>", "ARRAY<INTERVAL YEAR TO MONTH>", "ARRAY<INTERVAL MONTH>"),
        ("MAP<STRING, INTERVAL YEAR>", "MAP<STRING, INTERVAL YEAR TO MONTH>", "MAP<STRING, INTERVAL YEAR>"),
        ("MAP<INTERVAL HOUR, STRING>", "MAP<INTERVAL DAY TO SECOND, STRING>", "MAP<INTERVAL HOUR, STRING>"),
        ("STRUCT<x: INTERVAL HOUR>", "STRUCT<x: INTERVAL DAY TO SECOND>", "STRUCT<x: INTERVAL HOUR>"),
    ],
)
@pytest.mark.parametrize("input_declared", [False, True])
def test_intervals_widen_recursively(session, tmp_path, framework, source, exposed, narrow, input_declared):
    session.conf.set(FRAMEWORK, framework)
    head = f"source (id {source})" if input_declared else "source"
    prefix = f"CREATE MATERIALIZED VIEW {head} AS SELECT CAST(NULL AS {source}) AS id;"
    reader = "AS SELECT id FROM STREAM(source)"
    assert_declaration_outcome(
        session, tmp_path, f"{prefix}CREATE STREAMING TABLE consumer (id {exposed}) {reader}", compatible=True
    )
    assert_declaration_outcome(
        session, tmp_path, f"{prefix}CREATE STREAMING TABLE consumer (id {narrow}) {reader}", compatible=False
    )


@pytest.mark.parametrize(
    ("declared", "outcome"),
    [
        ("id INT NOT NULL", True),
        ("s STRUCT<x: INT NOT NULL>", True),
    ],
)
def test_streaming_reads_keep_nullability(session, tmp_path, declared, outcome):
    value = "1 AS id" if declared.startswith("id") else "struct(1 AS x) AS s"
    column = declared.split()[0]
    sql = (
        f"CREATE MATERIALIZED VIEW source ({declared}) AS SELECT {value};"
        f"CREATE STREAMING TABLE consumer ({declared}) AS SELECT {column} FROM STREAM(source)"
    )
    assert_declaration_outcome(session, tmp_path, sql, compatible=outcome)


def test_a_nullable_streaming_input_does_not_satisfy_not_null(session, tmp_path):
    sql = (
        "CREATE MATERIALIZED VIEW source AS SELECT CAST(NULL AS INT) AS id;"
        "CREATE STREAMING TABLE consumer (id INT NOT NULL) AS SELECT id FROM STREAM(source)"
    )
    assert_declaration_outcome(session, tmp_path, sql, compatible=False)


@pytest.mark.parametrize("name", ["CHAR(3)", "VARCHAR(3)"])
@pytest.mark.parametrize("wrapped", [False, True])
def test_declared_char_has_no_encoder_on_a_streaming_read(session, tmp_path, name, wrapped):
    data_type = f"ARRAY<{name}>" if wrapped else name
    sql = (
        f"CREATE MATERIALIZED VIEW source (id {data_type}) AS SELECT CAST(NULL AS {data_type}) AS id;"
        "CREATE STREAMING TABLE consumer AS SELECT id FROM STREAM(source)"
    )
    messages, terminal = dry_run(session, tmp_path, sql)
    assert terminal is not None
    assert any(f'[UNSUPPORTED_DATA_TYPE_FOR_ENCODER] Cannot create encoder for "{name}"' in m for m in messages)


def test_declared_char_is_read_when_its_type_information_is_preserved(session, tmp_path):
    session.conf.set(PRESERVE_CHAR, "true")
    sql = (
        "CREATE MATERIALIZED VIEW source (id CHAR(3)) AS SELECT CAST('a' AS CHAR(3)) AS id;"
        "CREATE STREAMING TABLE consumer AS SELECT id FROM STREAM(source)"
    )
    messages, _ = dry_run(session, tmp_path, sql)
    assert not any("UNSUPPORTED_DATA_TYPE_FOR_ENCODER" in message for message in messages)
