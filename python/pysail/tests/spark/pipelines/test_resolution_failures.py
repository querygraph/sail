"""Spark 4.2.1 unresolved-flow summaries and failure event ordering."""

import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.spark_connect_pipeline import start_run


def summary(direct, downstream):
    return (
        "\nFailed to resolve flows in the pipeline.\n\n"
        "A flow can fail to resolve because the flow itself contains errors or because it reads\n"
        "from an upstream flow which failed to resolve.\n\n"
        f"Flows with errors: {', '.join(sorted(direct))}\n"
        f"Flows that failed due to upstream errors: {', '.join(sorted(downstream))}\n\n"
        "To view the exceptions that were raised while resolving these flows, look for flow\n"
        "failures that precede this log."
    )


@pytest.mark.parametrize("dry", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("extra_query", [None, "SELECT 1 AS id", "SELECT * FROM missing_extra_source"])
def test_unresolved_flows_report_every_failure_before_terminal_error(
    pipeline_spark, pipeline, tmp_path, dry, reverse, extra_query
):
    graph_id, registry, _ = pipeline
    statements = [
        "CREATE MATERIALIZED VIEW healthy AS SELECT 1 AS id",
        "CREATE MATERIALIZED VIEW bad AS SELECT * FROM missing_source",
        "CREATE MATERIALIZED VIEW middle AS SELECT * FROM bad",
        "CREATE MATERIALIZED VIEW leaf AS SELECT * FROM middle",
        "CREATE MATERIALIZED VIEW other_bad AS SELECT missing_column FROM range(1)",
    ]
    if reverse:
        statements.reverse()
    if extra_query is not None:
        statements.append(f"CREATE FLOW secondary_flow AS INSERT INTO bad BY NAME {extra_query}")
    registry.register_sql(";".join(statements), tmp_path / "failures.sql")
    messages = []
    with pytest.raises(AnalysisException) as error:
        for result in start_run(pipeline_spark, graph_id, None, False, None, dry, (tmp_path / "state").as_uri()):
            if "pipeline_event_result" in result:
                messages.append(result["pipeline_event_result"].event.message)
    namespace = f"{pipeline_spark.catalog.currentCatalog()}.{pipeline_spark.catalog.currentDatabase()}"
    direct = {f"{namespace}.bad", f"{namespace}.other_bad"}
    downstream = {f"{namespace}.leaf", f"{namespace}.middle"}
    if extra_query == "SELECT * FROM missing_extra_source":
        direct.add(f"{namespace}.secondary_flow")
    assert error.value.getCondition() is None
    assert error.value.getSqlState() is None
    assert error.value.getMessageParameters() is None
    assert error.value.getMessage().split("\n\nJVM stacktrace:")[0] == summary(direct, downstream)
    failures = [message for message in messages if message.startswith("Failed to resolve flow")]
    assert len(failures) == len(direct) + len(downstream)
    assert all(message.startswith("Failed to resolve flow due to upstream failure:") for message in failures[:2])
    assert {message.split("'.", 1)[0].split("'", 1)[1] for message in failures[:2]} == downstream
    assert {message.split("'.", 1)[0].split("'", 1)[1] for message in failures[2:]} == direct
    assert any("missing_source" in message for message in failures[2:])
    assert any("missing_column" in message for message in failures[2:])
    assert all("Dataset is defined in the pipeline but could not be resolved." in message for message in failures[:2])
    for name in ["healthy", "bad", "middle", "leaf", "other_bad"]:
        assert not pipeline_spark.catalog.tableExists(name)
