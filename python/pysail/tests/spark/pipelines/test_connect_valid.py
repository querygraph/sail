"""ConnectValidPipelineSuite, through the public client.

The Scala suite reads resolved flows from the graph. A client cannot, so flow
schemas are read from what a real run publishes, and `DataflowGraph.resolved` is
a dry run that succeeds. `Flow.inputs` has no client-visible form and is dropped.
`Seq(...).toDF` becomes a VALUES query.

Flow `sqlConf` maps are not visible either. `spark.sql.session.timeZone` stands
in for the Scala test's keys: casting an instant to a string is resolved with a
flow's conf, so a table's rows show the zone that flow was analyzed with.

`MemoryStream` is replaced by a parquet table read as a file stream. A persisted
view cannot be streaming (Spark fails it in validation, which the Scala cases do
not reach), so the streaming views here are temporary views.
"""

from pathlib import Path

import pyspark.pipelines as dp
import pytest

from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run

ZONE = "SET spark.sql.session.timeZone = Asia/Tokyo;"
INSTANT = "CAST(TIMESTAMP '2020-01-01 00:00:00 UTC' AS STRING)"
IN_TOKYO = "2020-01-01 09:00:00"
ANOTHER_ZONE = "America/Lima"
IN_LIMA = "2019-12-31 19:00:00"


def row_order(row):
    return tuple((value is None, 0 if value is None else value) for value in row)


class Suite:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.catalog = spark.catalog.currentCatalog()
        self.storage = storage

    def name(self, dataset):
        return f"{self.catalog}.{self.schema}.{dataset}"

    def build(self, *sql, python=None, conf=None):
        graph = create_dataflow_graph(self.spark, self.catalog, self.schema, conf)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        for index, text in enumerate(sql):
            registry.register_sql(text, Path(f"pipeline{index}.sql"))
        if python is not None:
            with (
                add_pipeline_analysis_context(spark=self.spark, dataflow_graph_id=graph, flow_name=None),
                graph_element_registration_context(registry),
            ):
                exec(python, {"spark": self.spark, "dp": dp})  # noqa: S102 the graph definition under test
        return graph

    def messages(self, graph, dry):
        stream = start_run(self.spark, graph, None, False, None, dry, self.storage)
        return [item["pipeline_event_result"].event.message for item in stream if "pipeline_event_result" in item]

    def dry(self, *sql, python=None, conf=None):
        return self.messages(self.build(*sql, python=python, conf=conf), True)

    def run(self, *sql, python=None, conf=None):
        return self.messages(self.build(*sql, python=python, conf=conf), False)

    def frame(self, dataset):
        return self.spark.table(f"{self.schema}.{dataset}")

    def rows(self, dataset):
        return sorted((tuple(row) for row in self.frame(dataset).collect()), key=row_order)

    def shape(self, dataset):
        return self.frame(dataset).schema.simpleString()

    def source(self, table="src", values="(1), (2), (3), (4)"):
        self.spark.sql(f"CREATE TABLE {self.schema}.{table}(value INT) USING parquet")
        if values:
            self.spark.sql(f"INSERT INTO {self.schema}.{table} VALUES {values}")

    def statuses(self, messages, dataset):
        """A flow's lifecycle words, as FlowProgressEventLogger reports them."""
        name = self.name(dataset)
        words = []
        for message in messages:
            if message.startswith((f"Flow {name} ", f"Flow '{name}' ")):
                words.append(message.split("\n")[0].rstrip(".").split(" ")[-1])
        return words


@pytest.fixture
def suite(spark, tmp_path, request):
    schema = "cv_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Suite(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_extra_simple(suite):
    suite.run("CREATE VIEW b AS SELECT * FROM VALUES (1), (2), (3) AS t(y)")
    assert suite.shape("b") == "struct<y:int>"
    assert suite.rows("b") == [(1,), (2,), (3,)]


def test_simple(suite):
    suite.run(
        """
        CREATE VIEW a AS SELECT * FROM VALUES (1), (2), (3) AS t(x);
        CREATE VIEW b AS SELECT x AS y FROM a
        """
    )
    assert suite.shape("a") == "struct<x:int>"
    assert suite.shape("b") == "struct<y:int>"
    assert suite.rows("b") == [(1,), (2,), (3,)]


def test_dependencies(suite):
    suite.run(
        """
        CREATE VIEW a AS SELECT * FROM VALUES (1), (2), (3) AS t(x);
        CREATE VIEW c AS SELECT y AS z FROM b;
        CREATE VIEW b AS SELECT x AS y FROM a
        """
    )
    assert suite.shape("b") == "struct<y:int>"
    assert suite.shape("c") == "struct<z:int>"
    assert suite.rows("c") == [(1,), (2,), (3,)]


def test_multi_hop_schema_merging(suite):
    # `OUTER` is an alias of the VALUES relation here, so this is an inner join on false.
    suite.run(
        """
        CREATE VIEW b AS SELECT * FROM VALUES ((1)) OUTER JOIN d ON false;
        CREATE VIEW e AS SELECT * FROM b;
        CREATE VIEW d AS SELECT * FROM VALUES (1) AS t(y)
        """
    )
    assert suite.shape("b") == "struct<col1:int,y:int>"
    assert suite.shape("e") == "struct<col1:int,y:int>"
    assert suite.rows("e") == []


def test_cross_product_join_merges_schema(suite):
    suite.run(
        """
        CREATE VIEW a AS SELECT * FROM VALUES (1), (2), (3) AS t(x);
        CREATE VIEW b AS SELECT * FROM VALUES (4), (5), (6) AS t(y);
        CREATE VIEW c AS SELECT * FROM a CROSS JOIN b
        """
    )
    assert suite.shape("c") == "struct<x:int,y:int>"
    assert len(suite.rows("c")) == 9


def test_real_join_merges_schema(suite):
    suite.run(
        """
        CREATE VIEW a AS SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(x, y);
        CREATE VIEW b AS SELECT * FROM VALUES (2, 'm'), (3, 'n'), (4, 'o') AS t(x, z);
        CREATE VIEW c AS SELECT * FROM a JOIN b USING (x)
        """
    )
    assert suite.shape("c") == "struct<x:int,y:string,z:string>"
    assert suite.rows("c") == [(2, "b", "m"), (3, "c", "n")]


def test_union_of_streaming_and_batch_dataframes(suite):
    # Only resolution, as in Scala: Structured Streaming refuses this union when a query starts.
    suite.source()
    suite.dry(
        """
        CREATE TEMPORARY VIEW a AS SELECT * FROM STREAM(src);
        CREATE TEMPORARY VIEW b AS SELECT * FROM VALUES (1), (2), (3) AS t(value);
        CREATE TEMPORARY VIEW c AS SELECT * FROM STREAM(a) UNION ALL SELECT * FROM b;
        CREATE STREAMING TABLE d AS SELECT * FROM STREAM(c)
        """
    )


def test_union_of_two_streaming_dataframes(suite):
    suite.source("src1")
    suite.source("src2")
    suite.run(
        """
        CREATE TEMPORARY VIEW a AS SELECT * FROM STREAM(src1);
        CREATE TEMPORARY VIEW b AS SELECT * FROM STREAM(src2);
        CREATE TEMPORARY VIEW c AS SELECT * FROM STREAM(a) UNION ALL SELECT * FROM STREAM(b);
        CREATE STREAMING TABLE d AS SELECT * FROM STREAM(c)
        """
    )
    assert suite.shape("d") == "struct<value:int>"
    assert suite.rows("d") == sorted([(value,) for value in [1, 2, 3, 4]] * 2)


def test_multiple_inputs(suite):
    suite.run(
        """
        CREATE VIEW a AS SELECT * FROM VALUES (1), (2), (3) AS t(x);
        CREATE VIEW b AS SELECT * FROM VALUES (4), (5), (6) AS t(y);
        CREATE VIEW c AS SELECT x AS z FROM a UNION SELECT y AS z FROM b
        """
    )
    assert suite.shape("c") == "struct<z:int>"
    assert suite.rows("c") == [(value,) for value in range(1, 7)]


@pytest.mark.parametrize("setter", ["a", "b", "c", "d"])
def test_connect_retains_and_fuses_confs(suite, setter):
    # a -> b \
    #          d
    #      c /
    # The Scala test sets one key per dataset and reads the fused map; d is a streaming
    # table there only because resolution does not validate it. A real run needs a view.
    def zone(dataset):
        return ZONE if dataset == setter else ""

    suite.run(
        f"{zone('a')} CREATE VIEW a AS SELECT 1 AS x",
        f"{zone('b')} CREATE VIEW b AS SELECT * FROM a",
        f"{zone('c')} CREATE VIEW c AS SELECT 2 AS x",
        f"{zone('d')} CREATE MATERIALIZED VIEW d AS "
        f"SELECT x, {INSTANT} AS tz FROM (SELECT * FROM b UNION SELECT * FROM c)",
    )
    assert suite.rows("d") == [(1, IN_TOKYO), (2, IN_TOKYO)]


def test_confs_are_not_fused_past_materialization_points(suite, spark):
    in_session_zone = spark.sql(f"SELECT {INSTANT}").first()[0]
    assert in_session_zone != IN_TOKYO
    suite.run(
        f"{ZONE} CREATE VIEW a AS SELECT 1 AS x",
        f"CREATE MATERIALIZED VIEW b AS SELECT x, {INSTANT} AS tz FROM a",
        f"CREATE MATERIALIZED VIEW d AS SELECT x, {INSTANT} AS tz FROM b",
    )
    assert suite.rows("b") == [(1, IN_TOKYO)]
    assert suite.rows("d") == [(1, in_session_zone)]


def test_setting_the_same_conf_with_the_same_value_is_totally_cool(suite):
    suite.dry(
        "SET key = val; CREATE VIEW a AS SELECT * FROM VALUES (1), (2), (3) AS t(x)",
        "SET key = val; CREATE VIEW b AS SELECT * FROM VALUES (1), (2), (3) AS t(x)",
        "SET key = val; CREATE MATERIALIZED VIEW c AS SELECT * FROM a UNION SELECT * FROM b",
    )


def test_named_query_only(suite):
    suite.source()
    suite.run(
        """
        CREATE TEMPORARY VIEW a AS SELECT value AS x FROM STREAM(src);
        CREATE STREAMING TABLE b;
        CREATE FLOW `b-query` AS INSERT INTO b BY NAME SELECT * FROM STREAM(a)
        """
    )
    assert suite.shape("b") == "struct<x:int>"
    assert suite.rows("b") == [(1,), (2,), (3,), (4,)]


def test_default_query_and_named_query(suite):
    # A dry run: Spark's concurrent two-flow writes to one table can lose rows.
    suite.source()
    suite.dry(
        """
        CREATE TEMPORARY VIEW a AS SELECT * FROM STREAM(src);
        CREATE STREAMING TABLE b;
        CREATE FLOW b AS INSERT INTO b BY NAME SELECT value AS y FROM STREAM(src);
        CREATE FLOW b2 AS INSERT INTO b BY NAME SELECT * FROM STREAM(a)
        """
    )


def test_a_multi_query_table(suite):
    # Two complete queries fail validation, so a client can only reach this with streams.
    suite.source()
    suite.dry(
        """
        CREATE STREAMING TABLE a;
        CREATE FLOW a AS INSERT INTO a BY NAME SELECT * FROM STREAM(src);
        CREATE FLOW a2 AS INSERT INTO a BY NAME SELECT * FROM STREAM(src)
        """
    )


def test_correct_types_of_flows_after_connection(suite):
    # A batch flow reports PLANNING and a streaming flow does not. The append-once flow is
    # dropped, since Connect refuses `once`. Spark cannot publish the Scala test's hyphenated
    # table names, which resolution alone never tries.
    suite.source()
    messages = suite.run(
        """
        CREATE VIEW complete_view AS SELECT * FROM VALUES (1), (2) AS t(x);
        CREATE TEMPORARY VIEW incremental_view AS SELECT * FROM STREAM(src);
        CREATE MATERIALIZED VIEW complete_table AS SELECT * FROM complete_view;
        CREATE STREAMING TABLE incremental_table;
        CREATE FLOW incremental_table AS INSERT INTO incremental_table BY NAME
          SELECT * FROM STREAM(incremental_view)
        """
    )
    assert suite.statuses(messages, "complete_table") == ["QUEUED", "PLANNING", "STARTING", "RUNNING", "COMPLETED"]
    assert suite.statuses(messages, "incremental_table") == ["QUEUED", "STARTING", "RUNNING", "COMPLETED"]


def test_pipeline_level_default_confs_are_applied_with_correct_precedence(suite):
    suite.run(
        f"CREATE MATERIALIZED VIEW a AS SELECT 1 AS x, {INSTANT} AS tz",
        f"{ZONE} CREATE MATERIALIZED VIEW b AS SELECT x, {INSTANT} AS tz FROM a",
        conf={"spark.sql.session.timeZone": ANOTHER_ZONE},
    )
    assert suite.rows("a") == [(1, IN_LIMA)]
    assert suite.rows("b") == [(1, IN_TOKYO)]


def test_external_sink(suite):
    suite.source()
    suite.dry(
        python="""
@dp.temporary_view
def a():
  return spark.readStream.table("src").selectExpr("value AS x")

dp.create_sink("sink_a", format = "memory")

@dp.append_flow(target = "sink_a", name = "sink_flow")
def flow():
  return spark.readStream.table("a")
"""
    )


def test_auto_cdc_flow_resolves(suite):
    suite.source()
    suite.dry(
        python="""
@dp.table
def events():
  return spark.readStream.table("src").selectExpr("value AS id", "value AS seq")

dp.create_streaming_table("target")
dp.create_auto_cdc_flow(target="target", source="events", keys=["id"], sequence_by="seq", name="auto_cdc_flow")
"""
    )


def test_resolution_finishes_when_flows_exceed_parallelism(suite):
    suite.run(";".join(f"CREATE VIEW v{index} AS SELECT {index} AS x" for index in range(25)))
    for index in range(25):
        assert suite.rows(f"v{index}") == [(index,)]


def test_retryable_flows_resolve_when_consumers_exceed_parallelism(suite):
    consumers = ";".join(f"CREATE VIEW c{index} AS SELECT x FROM src" for index in range(20))
    suite.run(f"{consumers}; CREATE VIEW src AS SELECT * FROM VALUES (1), (2), (3) AS t(x)")
    for index in range(20):
        assert suite.rows(f"c{index}") == [(1,), (2,), (3,)]


@pytest.mark.skip(reason="observes SQLConf.get from a Scala Input during FlowAnalysis, and needs a custom Input")
def test_per_flow_confs_are_visible_to_the_analyzer_but_do_not_leak():
    pass


@pytest.mark.skip(reason="needs a Scala Input and a barrier inside concurrent flow analysis")
def test_per_flow_confs_stay_isolated_when_flows_are_resolved_in_parallel():
    pass


def test_per_flow_confs_reach_the_analyzer(suite, spark):
    key = "spark.sql.caseSensitive"
    assert spark.conf.get(key) == "false"
    source = "CREATE VIEW src AS SELECT id AS foo FROM RANGE(1)"
    suite.dry(source, "CREATE VIEW consumer AS SELECT Foo FROM src")

    with pytest.raises(AnalysisException) as error:
        suite.dry(source, f"SET {key} = true; CREATE VIEW consumer AS SELECT Foo FROM src")
    assert "Failed to resolve flows in the pipeline" in str(error.value)
    assert f"Flows with errors: {suite.name('consumer')}" in str(error.value)
    assert spark.conf.get(key) == "false"
