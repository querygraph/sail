"""Spark 4.2.1 Auto CDC flow definition validation through Spark Connect.

The cases mirror the server-side assertions of
`connect/pipelines/PythonPipelineSuite.scala` ("AutoCDC API: ..."),
`autocdc/ChangeArgsSuite.scala` and `graph/ConnectInvalidPipelineSuite.scala`
as observed by a PySpark 4.2.1 client. Nothing executes a CDC flow: a run with
a streaming table is still rejected before any write.
"""

from __future__ import annotations

import pytest
from pyspark.errors.exceptions import connect as errors
from pyspark.sql.connect import proto as pb

pytest.importorskip("pyspark.pipelines")
flow_module = pytest.importorskip("pyspark.pipelines.flow")
output_module = pytest.importorskip("pyspark.pipelines.output")
location_module = pytest.importorskip("pyspark.pipelines.source_code_location")
functions = pytest.importorskip("pyspark.sql.connect.functions")

if not hasattr(flow_module, "AutoCdcFlow"):
    pytest.skip("Auto CDC flows need PySpark 4.2.1 or later", allow_module_level=True)

LOCATION = location_module.SourceCodeLocation(filename="test_autocdc_definition.py", line_number=1)


def _column(value):
    return functions.col(value) if isinstance(value, str) else value


def _cdc_flow(target, source, keys, sequence_by, **arguments):
    """Build the client model `create_auto_cdc_flow` registers, with string arguments normalized."""
    return flow_module.AutoCdcFlow(
        name=arguments.get("name") or target,
        target=target,
        source=source,
        keys=[_column(key) for key in keys],
        sequence_by=functions.expr(sequence_by) if isinstance(sequence_by, str) else sequence_by,
        apply_as_deletes=arguments.get("apply_as_deletes"),
        column_list=arguments.get("column_list"),
        except_column_list=arguments.get("except_column_list"),
        stored_as_scd_type=arguments.get("stored_as_scd_type"),
        source_code_location=LOCATION,
    )


def _streaming_table(name):
    return output_module.StreamingTable(
        name=name,
        comment=None,
        source_code_location=LOCATION,
        table_properties={},
        partition_cols=None,
        cluster_by=None,
        schema=None,
        format=None,
    )


def _define_flow(pipeline_spark, graph_id, **fields):
    """Send a raw DefineFlow so missing fields and the once option can be exercised."""
    command = pb.Command()
    command.pipeline_command.define_flow.CopyFrom(pb.PipelineCommand.DefineFlow(dataflow_graph_id=graph_id, **fields))
    pipeline_spark.client.execute_command(command)


def _assert_error(error, condition, sql_state, parameters):
    assert error.getCondition() == condition
    assert error.getMessageParameters() == parameters
    assert error.getSqlState() == sql_state


def test_minimal_flow_registers_with_the_target_name(pipeline):
    _, registry, _ = pipeline
    registry.register_output(_streaming_table("target"))
    registry.register_auto_cdc_flow(_cdc_flow("target", "src", ["value"], "timestamp"))


def test_explicit_name_and_all_arguments_are_accepted(pipeline):
    _, registry, _ = pipeline
    registry.register_output(_streaming_table("tgt"))
    registry.register_auto_cdc_flow(
        _cdc_flow(
            "tgt",
            "src",
            ["id", "region"],
            "ts",
            apply_as_deletes=functions.expr("op = 'DELETE'"),
            column_list=[functions.col("id"), functions.col("val")],
            stored_as_scd_type=1,
            name="my_flow",
        )
    )


@pytest.mark.parametrize(
    ("argument", "name", "parameters"),
    [
        pytest.param("keys", "a.b", {"columnName": "a.b", "nameParts": "a, b"}, id="keys"),
        pytest.param(
            "column_list",
            "nested.field",
            {"columnName": "nested.field", "nameParts": "nested, field"},
            id="column-list",
        ),
        pytest.param("except_column_list", "x.y.z", {"columnName": "x.y.z", "nameParts": "x, y, z"}, id="except-list"),
    ],
)
def test_multipart_column_identifiers_are_rejected(pipeline, argument, name, parameters):
    _, registry, _ = pipeline
    registry.register_output(_streaming_table("target"))
    keys = [name] if argument == "keys" else ["value"]
    lists = {} if argument == "keys" else {argument: [functions.col(name)]}
    with pytest.raises(errors.AnalysisException) as error:
        registry.register_auto_cdc_flow(_cdc_flow("target", "src", keys, "timestamp", **lists))
    _assert_error(error.value, "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER", "22023", parameters)


def test_non_attribute_key_expression_is_rejected(pipeline):
    _, registry, _ = pipeline
    registry.register_output(_streaming_table("target"))
    with pytest.raises(errors.AnalysisException) as error:
        registry.register_auto_cdc_flow(_cdc_flow("target", "src", [functions.expr("value + 1")], "timestamp"))
    _assert_error(error.value, "AUTOCDC_NON_COLUMN_IDENTIFIER", "22023", {"expression": "(value + 1)"})


def test_both_column_lists_are_rejected(pipeline):
    _, registry, _ = pipeline
    registry.register_output(_streaming_table("target"))
    with pytest.raises(errors.AnalysisException) as error:
        registry.register_auto_cdc_flow(
            _cdc_flow(
                "target",
                "src",
                ["value"],
                "timestamp",
                column_list=[functions.col("value")],
                except_column_list=[functions.col("timestamp")],
            )
        )
    _assert_error(error.value, "AUTOCDC_BOTH_COLUMN_LIST_AND_EXCEPT_COLUMN_LIST", "42613", {})


def test_empty_keys_are_rejected(pipeline):
    _, registry, _ = pipeline
    registry.register_output(_streaming_table("target"))
    with pytest.raises(errors.AnalysisException) as error:
        registry.register_auto_cdc_flow(_cdc_flow("target", "src", [], "timestamp"))
    _assert_error(error.value, "AUTOCDC_EMPTY_KEYS", "22023", {})


@pytest.mark.parametrize(
    ("fields", "condition"),
    [
        pytest.param({}, "AUTOCDC_MISSING_SOURCE", id="source"),
        pytest.param({"source": "src"}, "AUTOCDC_MISSING_SEQUENCE_BY", id="sequence-by"),
    ],
)
def test_missing_required_fields_are_rejected(pipeline, pipeline_spark, fields, condition):
    graph_id, registry, _ = pipeline
    registry.register_output(_streaming_table("target"))
    details = pb.PipelineCommand.DefineFlow.AutoCdcFlowDetails(**fields)
    with pytest.raises(errors.AnalysisException) as error:
        _define_flow(
            pipeline_spark, graph_id, flow_name="target", target_dataset_name="target", auto_cdc_flow_details=details
        )
    _assert_error(error.value, condition, "22023", {})


def test_once_option_is_rejected_for_auto_cdc_flows(pipeline, pipeline_spark):
    # `AutoCdcFlow.once` is always false in Spark; the once option is rejected for every flow.
    graph_id, registry, _ = pipeline
    registry.register_output(_streaming_table("target"))
    details = pb.PipelineCommand.DefineFlow.AutoCdcFlowDetails(
        source="src",
        keys=[functions.col("id").to_plan(pipeline_spark.client)],
        sequence_by=functions.expr("seq").to_plan(pipeline_spark.client),
    )
    with pytest.raises(errors.AnalysisException) as error:
        _define_flow(
            pipeline_spark,
            graph_id,
            flow_name="target",
            target_dataset_name="target",
            auto_cdc_flow_details=details,
            once=True,
        )
    assert error.value.getCondition() == "DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED"


def test_auto_cdc_flow_targeting_a_temporary_view_fails_at_dry_run(pipeline, tmp_path):
    # As in Spark's test, the temporary view has no query of its own: the Auto CDC flow is its only flow.
    _, registry, run = pipeline
    registry.register_sql("CREATE MATERIALIZED VIEW events AS SELECT 1 AS id, 1 AS seq", tmp_path / "a.sql")
    registry.register_output(
        output_module.TemporaryView(name="target_view", comment=None, source_code_location=LOCATION)
    )
    registry.register_auto_cdc_flow(_cdc_flow("target_view", "events", ["id"], "seq"))
    with pytest.raises(errors.AnalysisException) as error:
        run(dry=True)
    # Sail keeps temporary view identities single-part; Spark reports the fully qualified identifier.
    _assert_error(
        error.value,
        "INVALID_FLOW_QUERY_TYPE.AUTOCDC_RELATION_FOR_TEMPORARY_VIEW",
        "42000",
        {"flowIdentifier": "`target_view`", "viewIdentifier": "`target_view`"},
    )


def test_auto_cdc_flow_targeting_a_materialized_view_fails_at_dry_run(pipeline, tmp_path):
    # Spark also checks the `flowIdentifier` and `tableIdentifier` parameters; Sail's existing
    # streamingness check reports the condition only. The persisted-view variant needs a view
    # without a query, which only the Rust port can construct.
    _, registry, run = pipeline
    registry.register_sql("CREATE MATERIALIZED VIEW events AS SELECT 1 AS id, 1 AS seq", tmp_path / "a.sql")
    registry.register_output(
        output_module.MaterializedView(
            name="target",
            comment=None,
            source_code_location=LOCATION,
            table_properties={},
            partition_cols=None,
            cluster_by=None,
            schema=None,
            format=None,
        )
    )
    registry.register_auto_cdc_flow(_cdc_flow("target", "events", ["id"], "seq", name="auto_cdc_flow"))
    with pytest.raises(errors.AnalysisException) as error:
        run(dry=True)
    assert "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW" in str(error.value)


def test_a_multiquery_table_cannot_have_an_auto_cdc_input(pipeline, pipeline_spark, tmp_path):
    _, registry, run = pipeline
    registry.register_sql("CREATE MATERIALIZED VIEW events AS SELECT 1 AS id, 1 AS seq", tmp_path / "a.sql")
    registry.register_output(_streaming_table("target"))
    registry.register_auto_cdc_flow(_cdc_flow("target", "events", ["id"], "seq", name="auto_cdc_flow"))
    registry.register_sql(
        "CREATE FLOW extra_flow AS INSERT INTO target BY NAME SELECT * FROM STREAM(events)", tmp_path / "b.sql"
    )
    catalog = pipeline_spark.catalog.currentCatalog()
    database = pipeline_spark.catalog.currentDatabase()
    with pytest.raises(errors.AnalysisException) as error:
        run(dry=True)
    _assert_error(
        error.value,
        "AUTOCDC_MULTIPLE_FLOWS_TO_TARGET",
        "42000",
        {
            "tableName": f"{catalog}.{database}.target",
            "flows": f"{catalog}.{database}.auto_cdc_flow, {catalog}.{database}.extra_flow",
        },
    )


@pytest.mark.parametrize("dry", [True, False])
def test_auto_cdc_flow_resolves_in_a_dry_run_and_needs_a_merge_target_to_run(
    pipeline_factory, pipeline_spark, tmp_path, dry
):
    # The same arguments as Spark's "registered flow survives graph resolution and validation" case.
    # Sail retains the execution entry after a run, so each run uses a fresh graph.
    _, registry, run = pipeline_factory()
    registry.register_sql("CREATE MATERIALIZED VIEW events AS SELECT 1 AS id, 1 AS seq", tmp_path / "a.sql")
    registry.register_output(_streaming_table("target"))
    registry.register_auto_cdc_flow(
        _cdc_flow(
            "target",
            "events",
            ["id"],
            "seq",
            apply_as_deletes=functions.expr("id % 2 = 0"),
            column_list=[functions.col("id"), functions.col("seq")],
        )
    )
    if dry:
        run(dry=True)
        assert not pipeline_spark.catalog.tableExists("target")
        return
    with pytest.raises(errors.AnalysisException, match="AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE"):
        run()


def test_four_part_source_is_rejected_before_key_checks(pipeline):
    """`GraphIdentifierManager.parseTableIdentifier` accepts one to three parts; a quoted name is one part."""
    _, registry, _ = pipeline
    registry.register_output(_streaming_table("target"))
    with pytest.raises(errors.SparkConnectGrpcException) as error:
        registry.register_auto_cdc_flow(_cdc_flow("target", "a.b.c.d", [], "timestamp"))
    assert "4+ part table identifier a.b.c.d is not supported." in str(error.value)
    registry.register_auto_cdc_flow(_cdc_flow("target", "`a.b.c.d`", ["value"], "timestamp"))


@pytest.mark.parametrize("argument", ["keys", "column_list", "except_column_list"])
@pytest.mark.parametrize("name", ["a..b", "`a", "`a`b"])
def test_malformed_attribute_names_are_rejected(pipeline, argument, name):
    """The planner's `UnresolvedAttribute.quotedString` parses every `col(...)` before the CDC checks."""
    _, registry, _ = pipeline
    registry.register_output(_streaming_table("target"))
    keys = [name] if argument == "keys" else ["value"]
    lists = {} if argument == "keys" else {argument: [functions.col(name)]}
    with pytest.raises(errors.AnalysisException) as error:
        registry.register_auto_cdc_flow(_cdc_flow("target", "src", keys, "timestamp", **lists))
    _assert_error(error.value, "INVALID_ATTRIBUTE_NAME_SYNTAX", "42601", {"name": name})


def test_unquoted_spaces_keep_their_part_and_dots_still_split(pipeline):
    _, registry, _ = pipeline
    registry.register_output(_streaming_table("target"))
    with pytest.raises(errors.AnalysisException) as error:
        registry.register_auto_cdc_flow(_cdc_flow("target", "src", ["a b.c"], "timestamp"))
    _assert_error(
        error.value, "AUTOCDC_MULTIPART_COLUMN_IDENTIFIER", "22023", {"columnName": "a b.c", "nameParts": "a b, c"}
    )


def test_quoted_dots_make_a_single_column_name(pipeline):
    _, registry, _ = pipeline
    registry.register_output(_streaming_table("target"))
    registry.register_auto_cdc_flow(
        _cdc_flow("target", "src", ["`a..b`"], "timestamp", column_list=[functions.col("`x.y`")])
    )


def _sink(name):
    return output_module.Sink(name=name, comment=None, source_code_location=LOCATION, format="memory", options={})


@pytest.mark.parametrize("dry", [True, False])
def test_auto_cdc_flow_into_a_sink_passes_a_dry_run_and_execution_is_still_rejected(
    pipeline_factory, pipeline_spark, tmp_path, dry
):
    """Spark rejects a sink destination in `FlowPlanner`, reached only when the flow is planned for execution;
    `dryRunPipeline` resolves and validates without it. Sail's public execution boundary is still the earlier
    rejection of every streaming-table pipeline, so the planner error itself is not publicly observable here."""
    _, registry, run = pipeline_factory()
    registry.register_sql("CREATE MATERIALIZED VIEW events AS SELECT 1 AS id, 1 AS seq", tmp_path / "a.sql")
    registry.register_output(_sink("out"))
    registry.register_auto_cdc_flow(_cdc_flow("out", "events", ["id"], "seq"))
    if dry:
        run(dry=True)
        return
    with pytest.raises(errors.SparkConnectGrpcException) as error:
        run()
    assert "incremental pipeline execution" in str(error.value)


def _batch_flow(pipeline_spark, name, target, source):
    return flow_module.Flow(
        name=name,
        target=target,
        spark_conf={},
        source_code_location=LOCATION,
        func=lambda: pipeline_spark.read.table(source),
    )


def _streaming_flow(pipeline_spark, name, target, source):
    return flow_module.Flow(
        name=name,
        target=target,
        spark_conf={},
        source_code_location=LOCATION,
        func=lambda: pipeline_spark.readStream.table(source),
    )


@pytest.mark.parametrize("cdc_first", [False, True])
def test_auto_cdc_multiflow_is_reported_before_materialized_view_multiflow(
    pipeline_factory, pipeline_spark, tmp_path, cdc_first
):
    """`validateMultiQueryTables` finds an Auto CDC multiflow target before any materialized-view multiflow,
    whatever the declaration order, and only after every flow has resolved."""
    _, registry, run = pipeline_factory()
    registry.register_sql("CREATE MATERIALIZED VIEW events AS SELECT 1 AS id, 1 AS seq", tmp_path / "a.sql")

    def define_mv():
        registry.register_output(
            output_module.MaterializedView(
                name="mv",
                comment=None,
                source_code_location=LOCATION,
                table_properties={},
                partition_cols=None,
                cluster_by=None,
                schema=None,
                format=None,
            )
        )
        registry.register_flow(_batch_flow(pipeline_spark, "mv_a", "mv", "events"))
        registry.register_flow(_batch_flow(pipeline_spark, "mv_b", "mv", "events"))

    def define_cdc():
        registry.register_output(_streaming_table("target"))
        registry.register_auto_cdc_flow(_cdc_flow("target", "events", ["id"], "seq", name="auto_cdc_flow"))
        registry.register_flow(_streaming_flow(pipeline_spark, "extra_flow", "target", "events"))

    if cdc_first:
        define_cdc()
        define_mv()
    else:
        define_mv()
        define_cdc()
    with pytest.raises(errors.AnalysisException) as error:
        run(dry=True)
    _assert_error(
        error.value,
        "AUTOCDC_MULTIPLE_FLOWS_TO_TARGET",
        "42000",
        {
            "tableName": "sail.default.target",
            "flows": "sail.default.auto_cdc_flow, sail.default.extra_flow",
        },
    )


def test_unresolved_source_is_reported_before_the_temporary_view_rule(pipeline_factory, tmp_path):
    """Spark's `validateSuccessfulFlowAnalysis` precedes the streamingness phase: a CDC flow reading a
    dataset nobody defines fails on resolution, not on its temporary-view target."""
    _, registry, run = pipeline_factory()
    registry.register_sql("CREATE MATERIALIZED VIEW unrelated AS SELECT 1 AS id", tmp_path / "a.sql")
    registry.register_output(
        output_module.TemporaryView(name="target_view", comment=None, source_code_location=LOCATION)
    )
    registry.register_auto_cdc_flow(_cdc_flow("target_view", "no_such_source", ["id"], "seq"))
    with pytest.raises(errors.AnalysisException) as error:
        run(dry=True)
    message = str(error.value)
    assert "AUTOCDC_RELATION_FOR_TEMPORARY_VIEW" not in message
    # The wrapper names the failed flow; its TABLE_OR_VIEW_NOT_FOUND cause is
    # retained in the report and not part of the wrapper message.
    assert "Flows with errors: target_view" in message


def _missing_source_flow(pipeline_spark, name, target):
    return _streaming_flow(pipeline_spark, name, target, "no_such_source")


@pytest.mark.parametrize("missing_first", [False, True])
def test_auto_cdc_multiflow_does_not_preempt_a_missing_source(
    pipeline_factory, pipeline_spark, tmp_path, missing_first
):
    """`validateSuccessfulFlowAnalysis` runs over the whole graph before `validateMultiQueryTables`: a resolvable
    Auto CDC multiflow target never wins over another output's unresolved source, in either declaration order."""
    _, registry, run = pipeline_factory()
    registry.register_sql("CREATE MATERIALIZED VIEW events AS SELECT 1 AS id, 1 AS seq", tmp_path / "a.sql")

    def define_cdc_multiflow():
        registry.register_output(_streaming_table("target"))
        registry.register_auto_cdc_flow(_cdc_flow("target", "events", ["id"], "seq", name="auto_cdc_flow"))
        registry.register_flow(_streaming_flow(pipeline_spark, "extra_flow", "target", "events"))

    def define_missing():
        registry.register_output(_streaming_table("late"))
        registry.register_flow(_missing_source_flow(pipeline_spark, "late", "late"))

    if missing_first:
        define_missing()
        define_cdc_multiflow()
    else:
        define_cdc_multiflow()
        define_missing()
    with pytest.raises(errors.AnalysisException) as error:
        run(dry=True)
    message = str(error.value)
    assert "AUTOCDC_MULTIPLE_FLOWS_TO_TARGET" not in message
    assert "Flows with errors: sail.default.late" in message


@pytest.mark.parametrize("missing_first", [False, True])
def test_streamingness_error_does_not_preempt_a_missing_source(
    pipeline_factory, pipeline_spark, tmp_path, missing_first
):
    """`validateFlowStreamingness` is a resolved-graph check: a batch flow into a streaming table is reported only
    after every flow resolved, so an unresolved source elsewhere comes first in either order."""
    _, registry, run = pipeline_factory()
    registry.register_sql("CREATE MATERIALIZED VIEW events AS SELECT 1 AS id, 1 AS seq", tmp_path / "a.sql")

    def define_batch_into_streaming_table():
        registry.register_output(_streaming_table("bad"))
        registry.register_flow(_batch_flow(pipeline_spark, "bad", "bad", "events"))

    def define_missing():
        registry.register_output(_streaming_table("late"))
        registry.register_flow(_missing_source_flow(pipeline_spark, "late", "late"))

    if missing_first:
        define_missing()
        define_batch_into_streaming_table()
    else:
        define_batch_into_streaming_table()
        define_missing()
    with pytest.raises(errors.AnalysisException) as error:
        run(dry=True)
    message = str(error.value)
    assert "BATCH_RELATION_FOR_STREAMING_TABLE" not in message
    assert "Flows with errors: sail.default.late" in message


def test_batch_into_streaming_table_is_still_rejected_once_everything_resolves(
    pipeline_factory, pipeline_spark, tmp_path
):
    _, registry, run = pipeline_factory()
    registry.register_sql("CREATE MATERIALIZED VIEW events AS SELECT 1 AS id, 1 AS seq", tmp_path / "a.sql")
    registry.register_output(_streaming_table("bad"))
    registry.register_flow(_batch_flow(pipeline_spark, "bad", "bad", "events"))
    with pytest.raises(errors.AnalysisException) as error:
        run(dry=True)
    assert "BATCH_RELATION_FOR_STREAMING_TABLE" in str(error.value)


@pytest.mark.parametrize("missing_first", [False, True])
def test_an_unread_table_is_not_inferred_while_a_source_is_unresolved(
    pipeline_factory, pipeline_spark, tmp_path, missing_first
):
    """`validateSuccessfulFlowAnalysis` precedes every inference phase: an output whose flows merge badly
    but which nobody reads is inferred only after resolution succeeds, so the unresolved source is
    reported in both declaration orders. (Earlier Sail merged inside the loop and reported whichever
    came first; that divergence is closed.)"""
    _, registry, run = pipeline_factory()
    registry.register_sql("CREATE MATERIALIZED VIEW events_int AS SELECT 1 AS id", tmp_path / "a.sql")
    registry.register_sql("CREATE MATERIALIZED VIEW events_str AS SELECT 'x' AS id", tmp_path / "b.sql")

    def define_incompatible_merge():
        registry.register_output(_streaming_table("merged"))
        registry.register_flow(_streaming_flow(pipeline_spark, "merged_int", "merged", "events_int"))
        registry.register_flow(_streaming_flow(pipeline_spark, "merged_str", "merged", "events_str"))

    def define_missing():
        registry.register_output(_streaming_table("late"))
        registry.register_flow(_missing_source_flow(pipeline_spark, "late", "late"))

    if missing_first:
        define_missing()
        define_incompatible_merge()
    else:
        define_incompatible_merge()
        define_missing()
    with pytest.raises(errors.AnalysisException) as error:
        run(dry=True)
    message = str(error.value)
    assert "Flows with errors: sail.default.late" in message
    assert "cannot merge incompatible pipeline types" not in message
