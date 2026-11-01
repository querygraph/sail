"""ConnectInvalidPipelineSuite, through the public client.

The Scala suite calls `validate()` on a resolved graph and reads the exception's
fields. Here each graph is a dry run: its terminal error, and the flow events it
streams, are what a client sees. `Seq(...).toDF` becomes a VALUES query.

`MemoryStream` is replaced by a parquet table read as a file stream. Every case
that uses it only needs a relation that is streaming when the flow is analyzed,
so the substitution proves the same thing: nothing in this file reads rows.
"""

import re
from pathlib import Path

import pyspark.pipelines as dp
import pytest

from pysail.testing.spark.utils.common import is_jvm_spark
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.output import TemporaryView
from pyspark.pipelines.source_code_location import SourceCodeLocation
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run
from pyspark.sql.connect import proto as pb

LOCATION = SourceCodeLocation(filename="test_connect_invalid.py", line_number=1)
CIRCULAR = (
    r"The downstream table '(?P<downstream>[^']+)' is referenced when creating the upstream table or view "
    r"'(?P<upstream>[^']+)'\. Circular dependencies are not supported in a pipeline\."
)
HINT = "please full refresh"


class Suite:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.catalog = spark.catalog.currentCatalog()
        self.storage = storage

    def name(self, dataset):
        return f"{self.catalog}.{self.schema}.{dataset}"

    def quoted(self, dataset):
        return f"`{self.catalog}`.`{self.schema}`.`{dataset}`"

    def build(self, *sql, python=None):
        graph = create_dataflow_graph(self.spark, self.catalog, self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        for index, text in enumerate(sql):
            registry.register_sql(text, Path(f"pipeline{index}.sql"))
        if python is not None:
            scope = {"spark": self.spark, "dp": dp, "registry": registry}
            scope.update(TemporaryView=TemporaryView, LOCATION=LOCATION)
            with (
                add_pipeline_analysis_context(spark=self.spark, dataflow_graph_id=graph, flow_name=None),
                graph_element_registration_context(registry),
            ):
                exec(python, scope)  # noqa: S102 the graph definition under test
        return graph

    def dry(self, *sql, python=None):
        """A dry run's event messages and terminal error."""
        messages = []
        error = None
        try:
            for item in start_run(self.spark, self.build(*sql, python=python), None, False, None, True, self.storage):
                if "pipeline_event_result" in item:
                    messages.append(item["pipeline_event_result"].event.message)
        except AnalysisException as caught:
            error = caught
        return messages, error

    def rejects(self, *sql, python=None):
        messages, error = self.dry(*sql, python=python)
        assert error is not None, messages
        return messages, error

    def event(self, messages, text):
        found = [message for message in messages if text in message]
        assert found, (text, messages)
        return found[0]

    def source(self, ddl="value INT"):
        self.spark.sql(f"CREATE TABLE {self.schema}.src({ddl}) USING parquet")


@pytest.fixture
def suite(spark, tmp_path, request):
    schema = "ci_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Suite(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def assert_invalid_flow(error, condition, parameters):
    assert error.getCondition() == condition
    assert error.getMessageParameters() == parameters


def test_missing_source(suite):
    messages, error = suite.rejects("CREATE VIEW b AS SELECT * FROM a")
    assert "Failed to resolve flows in the pipeline" in str(error)
    assert "TABLE_OR_VIEW_NOT_FOUND" in suite.event(messages, f"Failed to resolve flow: '{suite.name('b')}'")


def test_upstream_and_downstream_errors_are_told_apart(suite):
    messages, error = suite.rejects(
        """
        CREATE VIEW a AS SELECT * FROM RANGE(5);
        CREATE VIEW b AS SELECT * FROM nonExistentFlow;
        CREATE VIEW c AS SELECT * FROM b;
        CREATE VIEW d AS SELECT * FROM RANGE(5);
        CREATE VIEW e AS SELECT nonExistentColumn FROM RANGE(5);
        CREATE VIEW f AS SELECT * FROM e
        """
    )
    text = str(error)
    assert "Failed to resolve flows in the pipeline" in text
    assert f"Flows with errors: {suite.name('b')}, {suite.name('e')}" in text
    assert f"Flows that failed due to upstream errors: {suite.name('c')}, {suite.name('f')}" in text
    assert "TABLE_OR_VIEW_NOT_FOUND" in suite.event(messages, f"Failed to resolve flow: '{suite.name('b')}'")
    assert "nonExistentColumn" in suite.event(messages, f"Failed to resolve flow: '{suite.name('e')}'")
    for flow, upstream in [("c", "b"), ("f", "e")]:
        event = suite.event(messages, f"Failed to resolve flow due to upstream failure: '{suite.name(flow)}'")
        assert (
            f"Failed to read dataset '{suite.name(upstream)}'. Dataset is defined in the pipeline "
            "but could not be resolved"
        ) in event


def test_direct_and_downstream_errors_of_a_multi_flow_table(suite):
    suite.source("id BIGINT")
    _, error = suite.rejects(
        """
        CREATE STREAMING TABLE a;
        CREATE FLOW a AS INSERT INTO a BY NAME SELECT id FROM STREAM(src);
        CREATE FLOW a_2 AS INSERT INTO a BY NAME SELECT non_existent_col FROM STREAM(src);
        CREATE STREAMING TABLE b AS SELECT * FROM STREAM(a)
        """
    )
    text = str(error)
    assert f"Flows with errors: {suite.name('a_2')}" in text
    assert f"Flows that failed due to upstream errors: {suite.name('b')}" in text


def test_missing_attribute_in_the_schema(suite):
    messages, _ = suite.rejects(
        """
        CREATE VIEW a AS SELECT * FROM VALUES (1), (2), (3) AS t(z);
        CREATE VIEW b AS SELECT x FROM a
        """
    )
    event = suite.event(messages, f"Failed to resolve flow: '{suite.name('b')}'")
    assert "UNRESOLVED_COLUMN.WITH_SUGGESTION" in event
    assert "`x`" in event
    assert "`z`" in event


@pytest.mark.xfail(not is_jvm_spark(), strict=True, reason="Sail reports UNRESOLVED_COLUMN for any unresolved USING column")
def test_joining_on_a_column_with_different_names(suite):
    messages, _ = suite.rejects(
        """
        CREATE VIEW a AS SELECT * FROM VALUES (1), (2), (3) AS t(x);
        CREATE VIEW b AS SELECT * FROM VALUES ('a'), ('b'), ('c') AS t(y);
        CREATE VIEW c AS SELECT * FROM a JOIN b USING (x)
        """
    )
    event = suite.event(messages, f"Failed to resolve flow: '{suite.name('c')}'")
    assert "USING column `x` cannot be resolved on the right side" in event


def test_union_of_incompatible_schemas(suite):
    messages, _ = suite.rejects(
        """
        CREATE VIEW a AS SELECT * FROM VALUES (1), (2), (3) AS t(x);
        CREATE VIEW b AS SELECT * FROM VALUES (true), (false) AS t(x);
        CREATE VIEW c AS SELECT x FROM a UNION SELECT x FROM b
        """
    )
    event = suite.event(messages, f"Failed to resolve flow: '{suite.name('c')}'")
    assert "compatible column types" in event or "Failed to merge incompatible data types" in event


def cycle_ends(error):
    """The ends of a CircularDependencyException, which Spark reports without an error condition."""
    found = re.search(CIRCULAR, str(error))
    assert found, str(error)
    return found.group("upstream"), found.group("downstream")


def assert_cycle(error, cycle):
    upstream, downstream = cycle_ends(error)
    assert upstream != downstream
    assert upstream in cycle
    assert downstream in cycle


def test_self_reference(suite):
    _, error = suite.rejects("CREATE VIEW a AS SELECT * FROM a")
    assert cycle_ends(error) == (suite.name("a"), suite.name("a"))


def test_simple_cycle(suite):
    _, error = suite.rejects("CREATE VIEW a AS SELECT * FROM b; CREATE VIEW b AS SELECT * FROM a")
    assert_cycle(error, {suite.name("a"), suite.name("b")})


def bcd(suite):
    return {suite.name("b"), suite.name("c"), suite.name("d")}


def test_cyclic_graph(suite):
    _, error = suite.rejects(
        """
        CREATE VIEW a AS SELECT * FROM VALUES (1), (2), (3) AS t(x);
        CREATE VIEW b AS SELECT * FROM a UNION SELECT * FROM d;
        CREATE VIEW c AS SELECT * FROM b;
        CREATE VIEW d AS SELECT * FROM c
        """
    )
    assert_cycle(error, bcd(suite))


def test_cyclic_graph_with_materialized_nodes(suite):
    _, error = suite.rejects(
        """
        CREATE STREAMING TABLE a AS SELECT * FROM VALUES (1), (2), (3) AS t(x);
        CREATE STREAMING TABLE b AS SELECT * FROM a UNION SELECT * FROM d;
        CREATE STREAMING TABLE c AS SELECT * FROM b;
        CREATE STREAMING TABLE d AS SELECT * FROM c
        """
    )
    assert_cycle(error, bcd(suite))


def test_second_query_makes_the_graph_cyclic(suite):
    _, error = suite.rejects(
        """
        CREATE STREAMING TABLE a AS SELECT * FROM VALUES (1), (2), (3) AS t(x);
        CREATE STREAMING TABLE b;
        CREATE FLOW b AS INSERT INTO b BY NAME SELECT * FROM a;
        CREATE FLOW b2 AS INSERT INTO b BY NAME SELECT * FROM d;
        CREATE STREAMING TABLE c AS SELECT * FROM b;
        CREATE STREAMING TABLE d AS SELECT * FROM c
        """
    )
    assert_cycle(error, bcd(suite))


def test_cycle_of_named_flows(suite):
    _, error = suite.rejects(
        """
        CREATE STREAMING TABLE a AS SELECT * FROM VALUES (1), (2), (3) AS t(x);
        CREATE STREAMING TABLE b;
        CREATE FLOW `b-name` AS INSERT INTO b BY NAME SELECT * FROM a UNION SELECT * FROM d;
        CREATE STREAMING TABLE c;
        CREATE FLOW `c-name` AS INSERT INTO c BY NAME SELECT * FROM b;
        CREATE STREAMING TABLE d;
        CREATE FLOW `d-name` AS INSERT INTO d BY NAME SELECT * FROM c
        """
    )
    assert_cycle(error, bcd(suite))


def test_view_table_conf_conflict(suite):
    # SET reaches a dataset per SQL file, as the Scala test's per-dataset sqlConf does.
    _, error = suite.rejects(
        "SET x = a-val; CREATE VIEW a AS SELECT 1 AS value",
        "SET x = b-val; CREATE STREAMING TABLE b AS SELECT * FROM a",
    )
    text = str(error)
    assert f"Found duplicate sql conf for dataset '{suite.name('b')}':" in text
    assert f"'x' is defined by both '{suite.name('a')}' and '{suite.name('b')}'" in text


def test_view_view_conf_conflict(suite):
    _, error = suite.rejects(
        "SET x = a-val; CREATE VIEW a AS SELECT 1 AS value",
        "SET x = b-val; CREATE VIEW b AS SELECT 1 AS value",
        "SET y = c-val; CREATE STREAMING TABLE c AS SELECT * FROM a UNION SELECT * FROM b",
    )
    text = str(error)
    assert f"Found duplicate sql conf for dataset '{suite.name('c')}':" in text
    assert f"'x' is defined by both '{suite.name('a')}' and '{suite.name('b')}'" in text


def test_reading_a_complete_view_incrementally(suite):
    messages, _ = suite.rejects(
        "CREATE VIEW a AS SELECT 1 AS value; CREATE STREAMING TABLE b AS SELECT * FROM STREAM(a)"
    )
    event = suite.event(messages, f"Failed to resolve flow: '{suite.name('b')}'")
    assert f"View {suite.quoted('a')} is a batch view and must be referenced using SparkSession#read." in event


def test_reading_an_incremental_view_completely(suite):
    suite.source()
    messages, _ = suite.rejects(
        "CREATE VIEW a AS SELECT * FROM STREAM(src); CREATE STREAMING TABLE b AS SELECT * FROM a"
    )
    event = suite.event(messages, f"Failed to resolve flow: '{suite.name('b')}'")
    assert f"View {suite.quoted('a')} is a streaming view and must be referenced using SparkSession#readStream" in event


def test_a_streaming_table_backed_by_a_batch_relation(suite):
    _, error = suite.rejects("CREATE STREAMING TABLE a AS SELECT * FROM VALUES (1), (2) AS t(value)")
    assert_invalid_flow(
        error,
        "INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE",
        {"flowIdentifier": suite.quoted("a"), "tableIdentifier": suite.quoted("a")},
    )


def test_a_materialized_view_backed_by_a_streaming_relation(suite):
    suite.source()
    _, error = suite.rejects("CREATE MATERIALIZED VIEW a AS SELECT * FROM STREAM(src)")
    assert_invalid_flow(
        error,
        "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW",
        {"flowIdentifier": suite.quoted("a"), "tableIdentifier": suite.quoted("a")},
    )


def test_a_once_flow_is_refused_at_registration(suite, spark):
    # The Scala test registers a once flow fed by a stream. Connect refuses `once` outright,
    # so INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_ONCE_FLOW cannot be reached by a client.
    graph = suite.build("CREATE STREAMING TABLE a")
    command = pb.Command()
    command.pipeline_command.define_flow.CopyFrom(
        pb.PipelineCommand.DefineFlow(
            dataflow_graph_id=graph,
            flow_name="once_flow",
            target_dataset_name="a",
            relation_flow_details=pb.PipelineCommand.DefineFlow.WriteRelationFlowDetails(
                relation=spark.range(1)._plan.plan(spark.client),
            ),
            once=True,
        )
    )
    with pytest.raises(AnalysisException) as error:
        spark.client.execute_command(command)
    assert error.value.getCondition() == "DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED"
    assert error.value.getMessageParameters() == {"flowName": "once_flow"}


def test_inferred_schema_is_not_a_subset_of_the_declared_schema(suite):
    _, error = suite.rejects(
        python="""
@dp.table(schema = "x INT")
def a():
  return spark.sql("SELECT * FROM VALUES (1), (2) AS t(`incorrect-col-name`)")
"""
    )
    text = str(error)
    assert f"'{suite.name('a')}' has a user-specified schema that is incompatible" in text
    assert "incorrect-col-name" in text
    assert HINT not in text


def test_inferred_streaming_schema_is_incompatible_with_the_declared_schema(suite):
    # The Scala test uses a once flow, which Connect refuses; a streaming flow takes the same branch.
    suite.source("x BOOLEAN")
    _, error = suite.rejects(
        python="""
@dp.table(schema = "x INT")
def a():
  return spark.readStream.table("src")
"""
    )
    text = str(error)
    assert f"'{suite.name('a')}' has a user-specified schema that is incompatible" in text
    assert "boolean" in text
    assert "integer" in text
    assert HINT in text


CDC_SOURCE = """
@dp.table
def events():
  return spark.readStream.table("src")
"""


def test_auto_cdc_flow_targeting_a_materialized_view(suite):
    suite.source("id INT, seq INT")
    _, error = suite.rejects(
        python=CDC_SOURCE
        + """
from pyspark.pipelines.output import MaterializedView
registry.register_output(MaterializedView(
    name="target", comment=None, source_code_location=LOCATION, table_properties={},
    partition_cols=None, cluster_by=None, schema=None, format="parquet"))
dp.create_auto_cdc_flow(target="target", source="events", keys=["id"], sequence_by="seq", name="auto_cdc_flow")
"""
    )
    assert_invalid_flow(
        error,
        "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_MATERIALIZED_VIEW",
        {"flowIdentifier": suite.quoted("auto_cdc_flow"), "tableIdentifier": suite.quoted("target")},
    )


@pytest.mark.skip(reason="a client cannot declare a persisted view without a query, and a second flow fails a require")
def test_auto_cdc_flow_targeting_a_persisted_view():
    pass


def test_auto_cdc_flow_targeting_a_temporary_view(suite):
    suite.source("id INT, seq INT")
    _, error = suite.rejects(
        python=CDC_SOURCE
        + """
registry.register_output(TemporaryView(name="target_view", comment=None, source_code_location=LOCATION))
dp.create_auto_cdc_flow(target="target_view", source="events", keys=["id"], sequence_by="seq")
"""
    )
    assert_invalid_flow(
        error,
        "INVALID_FLOW_QUERY_TYPE.AUTOCDC_RELATION_FOR_TEMPORARY_VIEW",
        {"flowIdentifier": "`target_view`", "viewIdentifier": "`target_view`"},
    )


def test_a_multiquery_table_cannot_have_an_auto_cdc_input(suite):
    suite.source("id INT, seq INT")
    _, error = suite.rejects(
        python=CDC_SOURCE
        + """
dp.create_streaming_table("target")
dp.create_auto_cdc_flow(target="target", source="events", keys=["id"], sequence_by="seq", name="auto_cdc_flow")

@dp.append_flow(target = "target", name = "extra_flow")
def extra():
  return spark.readStream.table("src")
"""
    )
    assert_invalid_flow(
        error,
        "AUTOCDC_MULTIPLE_FLOWS_TO_TARGET",
        {
            "tableName": suite.name("target"),
            "flows": f"{suite.name('auto_cdc_flow')}, {suite.name('extra_flow')}",
        },
    )
