"""EndToEndAPISuite (APITest) and PythonPipelineSuite cases the older ports left out or checked weakly.

`Project` drives the CLI's own `run`: the same spec file, globs, module loading and event handling as
`spark-pipelines`. Only the session is the test's, because a session on Sail keeps its memory catalog
to itself unless the server shares it. test_end_to_end_cli.py covers the CLI as a separate process.

`Graph` keeps PythonPipelineSuite's `buildGraph`. The Scala suite inspects the resolved graph; a client
sees the same facts in the event stream: which flows exist and under which name, a PLANNING step on
batch flows only, and a dependency finishing before its reader starts. PipelineEvent carries only a
timestamp and a message, so the source code locations of those events never reach a client.
"""

import pyspark.pipelines as dp
import pytest
from pysail.testing.spark.utils.common import is_jvm_spark
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.pipelines import cli
from pyspark.pipelines.add_pipeline_analysis_context import add_pipeline_analysis_context
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

FIVE = [(i,) for i in range(5)]
RATE = "spark.readStream.format('rate').load()"
HEADER = "from pyspark import pipelines as dp\nfrom pyspark.sql import DataFrame\n"


def rows(frame):
    return sorted(tuple(row) for row in frame.collect())


def unique_schema(prefix, request):
    return prefix + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]


class Session:
    """Stands in for the CLI's `SparkSession.builder` and hands it the test's session."""

    def __init__(self, spark):
        self.spark = spark
        self.builder = self

    def config(self, *_):
        return self

    def getOrCreate(self):
        return self.spark


class Project:
    def __init__(self, spark, schema, root):
        self.spark = spark
        self.schema = schema
        self.root = root

    def write(self, sources, include):
        for name, contents in sources.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(contents)
        libraries = "\n".join(f'  - glob:\n      include: "{pattern}"' for pattern in include)
        spec = self.root / "pipeline.yaml"
        spec.write_text(
            f"""
name: test-pipeline
database: "{self.schema}"
storage: "{(self.root / "storage").as_uri()}"
libraries:
{libraries}
"""
        )
        return spec

    def run(self, sources, include=("transformations/**",), *, dry=False, full=(), full_all=False, refresh=()):
        spec = self.write(sources, include)
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(cli, "SparkSession", Session(self.spark))
            patch.setattr(self.spark, "stop", lambda: None)
            cli.run(spec, list(full), full_all, list(refresh), dry)

    def table(self, name):
        return rows(self.spark.sql(f"SELECT * FROM {name}"))


@pytest.fixture
def project(spark, tmp_path, request):
    schema = unique_schema("ap_", request)
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    spark.sql(f"USE {schema}")
    yield Project(spark, schema, tmp_path)
    spark.sql("USE default")
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


DEFINITION = "transformations/definition.sql"


def test_cli_sql_mv_st_and_flows(project):
    """APITest:97: SQL Pipeline with mv, st, and flows. The spec names one glob and one file."""
    project.run(
        {
            "transformations/st.sql": (
                "\nCREATE STREAMING TABLE st;\nCREATE FLOW f AS INSERT INTO st BY NAME SELECT * FROM STREAM mv WHERE id > 2;\n"
            ),
            "transformations/mvs/mv.sql": "\nCREATE MATERIALIZED VIEW mv\nAS SELECT * FROM RANGE(5);\n",
        },
        ["transformations/mvs/**", "transformations/st.sql"],
    )
    assert project.table("st") == [(3,), (4,)]


def test_cli_sql_cte(project):
    """APITest:120: SQL Pipeline with CTE."""
    project.run(
        {
            DEFINITION: """
CREATE MATERIALIZED VIEW a AS SELECT 1;
CREATE MATERIALIZED VIEW d AS
WITH c AS (
 WITH b AS (
   SELECT * FROM a
 )
 SELECT * FROM b
)
SELECT * FROM c;
"""
        }
    )
    assert project.table("d") == [(1,)]


def test_cli_sql_subquery(project):
    """APITest:145: SQL Pipeline with subquery."""
    project.run(
        {
            DEFINITION: """
CREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(5);
CREATE MATERIALIZED VIEW b AS SELECT * FROM RANGE(5)
WHERE id = (SELECT max(id) FROM a);
"""
        }
    )
    assert project.table("b") == [(4,)]


def test_cli_sql_join(project):
    """APITest:163: SQL Pipeline with join."""
    project.run(
        {
            DEFINITION: """
CREATE TEMPORARY VIEW a AS SELECT id FROM range(1,3);
CREATE TEMPORARY VIEW b AS SELECT id FROM range(1,3);
CREATE MATERIALIZED VIEW c AS SELECT a.id AS id1, b.id AS id2
FROM a JOIN b ON a.id=b.id
"""
        }
    )
    assert project.table("c") == [(1, 1), (2, 2)]


def test_cli_sql_aggregation(project):
    """APITest:182: SQL Pipeline with aggregation."""
    project.run(
        {
            DEFINITION: """
CREATE MATERIALIZED VIEW a AS SELECT id AS value, (id % 2) AS isOdd FROM range(1,10);
CREATE MATERIALIZED VIEW b AS SELECT isOdd, max(value) AS
maximum FROM a GROUP BY isOdd LIMIT 2;
"""
        }
    )
    assert project.table("b") == [(0, 8), (1, 9)]


def test_cli_sql_table_properties(project):
    """APITest:200: SQL Pipeline with table properties."""
    project.run(
        {
            DEFINITION: """
CREATE MATERIALIZED VIEW mv TBLPROPERTIES ('prop1'='foo1', 'prop2'='bar2')
AS SELECT 1;
CREATE STREAMING TABLE st TBLPROPERTIES ('prop3'='foo3', 'prop4'='bar4')
AS SELECT * FROM STREAM(mv);
"""
        }
    )
    for table, expected in [("mv", {"prop1": "foo1", "prop2": "bar2"}), ("st", {"prop3": "foo3", "prop4": "bar4"})]:
        properties = {row[0]: row[1] for row in project.spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
        assert expected.items() <= properties.items()


def test_cli_sql_schema(project):
    """APITest:228: SQL Pipeline with schema."""
    project.run(
        {
            DEFINITION: """
CREATE MATERIALIZED VIEW a (id LONG COMMENT 'comment') AS SELECT * FROM RANGE(5);
CREATE STREAMING TABLE b (id LONG COMMENT 'comment') AS SELECT * FROM STREAM a;
"""
        }
    )
    for table in "ab":
        assert [field.name for field in project.spark.table(table).schema.fields] == ["id"]


def test_cli_python_and_sql(project):
    """APITest:255: Pipeline with Python and SQL."""
    project.run(
        {
            DEFINITION: """
CREATE STREAMING TABLE c;
CREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(5);
""",
            "transformations/definition.py": HEADER
            + """
@dp.append_flow(target = "c", name = "append_to_c")
def flow():
  return spark.readStream.table("b").filter("id >= 3")

@dp.materialized_view
def b():
  return spark.read.table("a")
""",
        }
    )
    assert project.table("b") == FIVE
    assert project.table("c") == [(3,), (4,)]


STREAM_C = """
CREATE STREAMING TABLE c;
CREATE FLOW f AS INSERT INTO c BY NAME SELECT * FROM STREAM b WHERE id > 2;
"""
READ_SRC = (
    HEADER
    + """
@dp.materialized_view
def a():
  return spark.read.table("src")

@dp.table
def b():
  return spark.readStream.table("src")
"""
)


def test_cli_referencing_internal_datasets(project):
    """APITest:287: Pipeline referencing internal datasets."""
    project.run(
        {
            "transformations/mv.py": HEADER + "\n@dp.materialized_view\ndef src():\n  return spark.range(5)\n",
            "transformations/st.py": READ_SRC,
            DEFINITION: STREAM_C,
        },
        ["transformations/mv.py", "transformations/st.py", DEFINITION],
    )
    assert project.table("a") == FIVE
    assert project.table("b") == FIVE
    assert project.table("c") == [(3,), (4,)]


def test_cli_referencing_external_datasets(project):
    """APITest:331: Pipeline referencing external datasets."""
    project.spark.sql("CREATE TABLE src AS SELECT * FROM RANGE(5)")
    project.run(
        {"transformations/definition.py": READ_SRC, DEFINITION: STREAM_C},
        ["transformations/definition.py", DEFINITION],
    )
    assert project.table("a") == FIVE
    assert project.table("b") == FIVE
    assert project.table("c") == [(3,), (4,)]


@pytest.mark.parametrize(
    "line",
    [
        pytest.param("spark = SparkSession.active()", id="explicit-APITest-369"),
        pytest.param("spark: SparkSession", id="placeholder-APITest-392"),
    ],
)
def test_cli_session_in_a_python_file(project, line):
    """APITest:369 and 392: an assigned `spark` and a bare annotation both reach the query function."""
    source = f"""from pyspark import pipelines as dp
from pyspark.sql import SparkSession

{line}

@dp.materialized_view
def mv():
  return spark.range(5)
"""
    project.run({"transformations/definition.py": source})
    assert project.table("mv") == FIVE


def test_cli_materialized_view_streaming_table_and_append_flow(project):
    """APITest:415: Python Pipeline with materialized_view, create_streaming_table, and append_flow."""
    project.run(
        {
            "transformations/st.py": HEADER
            + """
dp.create_streaming_table(
  name = "a",
  schema = "id LONG",
  comment = "streaming table a",
)

@dp.append_flow(target = "a", name = "append_to_a")
def flow():
  return spark.readStream.table("src")
""",
            "transformations/mv.py": HEADER
            + """
@dp.materialized_view(
  name = "src",
  comment = "source table",
  schema = "id LONG"
)
def irrelevant():
  return spark.range(5)
""",
        }
    )
    assert project.table("a") == FIVE


def test_cli_temporary_view(project):
    """APITest:456: Python Pipeline with temporary_view."""
    project.spark.sql("CREATE TABLE src AS SELECT * FROM RANGE(5)")
    project.run(
        {
            "transformations/definition.py": HEADER
            + """
@dp.temporary_view(
 name = "view_1",
 comment = "temporary view 1"
)
def irrelevant():
  return spark.range(5)

@dp.materialized_view(
 name = "mv_1",
)
def irrelevant_1():
  return spark.read.table("view_1")
"""
        },
        ["transformations/definition.py"],
    )
    assert project.table("mv_1") == FIVE


@pytest.mark.parametrize("sink_format", ["parquet", "json"])
def test_cli_sink(project, tmp_path, sink_format):
    """APITest:491: Python Pipeline with parquet sink, and with json sink."""
    project.spark.sql("CREATE TABLE src AS SELECT * FROM RANGE(5)")
    target = tmp_path / "sink"
    project.run(
        {
            "transformations/definition.py": HEADER
            + f"""
dp.create_sink(
  "mySink",
  format = "{sink_format}",
  options = {{"path": "{target}"}}
)

@dp.append_flow(
  target = "mySink",
)
def mySinkFlow():
  return spark.readStream.table("src")
"""
        },
        ["transformations/definition.py"],
    )
    assert rows(project.spark.read.format(sink_format).load(str(target))) == FIVE


def assert_partitioned_by(spark, table, column):
    columns = spark.catalog.listColumns(table)
    assert [item.name for item in columns if item.isPartition] == [column]


def assert_clustered_by(spark, table, names):
    columns = spark.catalog.listColumns(table)
    assert [item.name for item in columns if item.isCluster] == names


def test_cli_partition_columns(project):
    """APITest:542: Python Pipeline with partition columns."""
    project.run(
        {
            "transformations/definition.py": HEADER
            + """
from pyspark.sql.functions import col

@dp.materialized_view(partition_cols = ["id_mod"])
def mv():
  return spark.range(5).withColumn("id_mod", col("id") % 2)

@dp.table(partition_cols = ["id_mod"])
def st():
  return spark.readStream.table("mv")
"""
        }
    )
    for table in ["mv", "st"]:
        assert project.table(table) == [(0, 0), (1, 1), (2, 0), (3, 1), (4, 0)]


def test_cli_cluster_columns(project):
    """APITest:573: Python Pipeline with cluster columns."""
    project.run(
        {
            "transformations/definition.py": HEADER
            + """
from pyspark.sql.functions import col

@dp.materialized_view(cluster_by = ["cluster_col1"])
def mv():
  df = spark.range(10)
  df = df.withColumn("cluster_col1", col("id") % 3)
  df = df.withColumn("cluster_col2", col("id") % 2)
  return df

@dp.table(cluster_by = ["cluster_col1"])
def st():
  return spark.readStream.table("mv")
"""
        }
    )
    for table in ["mv", "st"]:
        assert project.table(table) == [(i, i % 3, i % 2) for i in range(10)]
        assert_clustered_by(project.spark, table, ["cluster_col1"])


def test_cli_dry_run(project):
    """APITest:633: Pipeline with dry run."""
    project.run(
        {
            DEFINITION: "\nCREATE MATERIALIZED VIEW a AS SELECT * FROM RANGE(5);\nCREATE MATERIALIZED VIEW b AS SELECT * FROM a WHERE id > 2;\n"
        },
        [DEFINITION],
        dry=True,
    )
    assert not project.spark.catalog.tableExists("a")
    assert not project.spark.catalog.tableExists("b")


@pytest.mark.parametrize(
    ("options", "expected_a", "expected_b", "expected_mv"),
    [
        pytest.param({}, [1, 2, 3], [1, 2, 3], [1, 2, 3], id="refresh-all-by-default"),
        pytest.param({"full_all": True}, [2, 3], [2, 3], [2, 3], id="full-refresh-all"),
        pytest.param(
            {"refresh": ["b"], "full": ["mv", "a"]}, [2, 3], [1, 2, 3], [2, 3], id="selective-full-and-refresh"
        ),
        pytest.param({"full": ["a"]}, [2, 3], [1], [1], id="selective-full"),
    ],
)
def test_cli_selective_refresh(project, options, expected_a, expected_b, expected_mv):
    """APITest:693: the four SelectiveRefreshTestCase cases of APITest:652."""
    spark = project.spark
    spark.sql("DROP TABLE IF EXISTS source_data")
    spark.sql("CREATE TABLE source_data AS SELECT * FROM RANGE(1, 2)")
    sources = {
        "transformations/st.sql": """
CREATE STREAMING TABLE a AS SELECT * FROM STREAM source_data;
CREATE STREAMING TABLE b AS SELECT * FROM STREAM source_data;
""",
        "transformations/mv.sql": "\nCREATE MATERIALIZED VIEW mv AS SELECT * FROM a;\n",
    }
    include = ["transformations/st.sql", "transformations/mv.sql"]
    project.run(sources, include)
    spark.sql("INSERT OVERWRITE TABLE source_data SELECT * FROM VALUES (2), (3) AS t(id)")
    project.run(sources, include, **options)
    for table in ["a", "b", "mv"]:
        spark.catalog.refreshTable(table)
    assert project.table("a") == [(i,) for i in expected_a]
    assert project.table("b") == [(i,) for i in expected_b]
    assert project.table("mv") == [(i,) for i in expected_mv]


class Graph:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.catalog = spark.catalog.currentCatalog()
        self.storage = storage

    def build(self, body, catalog=None, schema=None):
        graph = create_dataflow_graph(self.spark, catalog or self.catalog, schema or self.schema, None)
        registry = SparkConnectGraphElementRegistry(self.spark, graph)
        scope = {"spark": self.spark, "dp": dp, "F": F}
        scope.update(StructType=StructType, StructField=StructField, LongType=LongType, StringType=StringType)
        with (
            add_pipeline_analysis_context(spark=self.spark, dataflow_graph_id=graph, flow_name=None),
            graph_element_registration_context(registry),
        ):
            exec(body, scope)  # noqa: S102 the port of Spark's buildGraph runs its snippet
        return graph

    def attempt(self, body, *, dry=False):
        """The event messages of a run, and the error that ended it if any."""
        events = []
        try:
            for item in start_run(self.spark, self.build(body), None, False, None, dry, self.storage):
                if "pipeline_event_result" in item:
                    events.append(item["pipeline_event_result"].event.message)
        except AnalysisException as error:
            return events, error
        return events, None

    def run(self, body):
        events, error = self.attempt(body)
        assert error is None, events
        return events

    def name(self, table, schema=None):
        return f"{self.catalog}.{schema or self.schema}.{table}"

    def table(self, name):
        return rows(self.spark.table(f"{self.schema}.{name}"))


@pytest.fixture
def graph(spark, tmp_path, request):
    schema = unique_schema("gr_", request)
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Graph(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def flow_events(events, name):
    return [event for event in events if event.startswith((f"Flow {name} ", f"Flow '{name}' "))]


def statuses(events, name):
    """A flow's lifecycle words, as FlowProgressEventLogger reports them."""
    return [event.split("\n")[0].rstrip(".").split(" ")[-1] for event in flow_events(events, name)]


def flow_events_with_index(events, name):
    return [(i, statuses([event], name)[0]) for i, event in enumerate(events) if flow_events([event], name)]


def first_index(events, name, status):
    return next(index for index, word in flow_events_with_index(events, name) if word == status)


def test_flows_run_into_their_target(graph, spark):
    """PythonPipelineSuite:302: flows. A table source stands in for rate, which a triggered run would not end."""
    spark.sql(f"CREATE TABLE {graph.schema}.ext AS SELECT * FROM RANGE(2)")
    source = f"spark.readStream.table('{graph.schema}.ext')"
    events = graph.run(
        f"""
@dp.table()
def a():
  return {source}

@dp.append_flow(target = "a")
def supplement():
  return {source}
"""
    )
    assert "COMPLETED" in statuses(events, graph.name("a"))
    assert "COMPLETED" in statuses(events, graph.name("supplement"))
    # Spark's two file sink flows race on one table and a retry may drop a batch, so counts are not stable.
    assert set(graph.table("a")) == {(0,), (1,)}


def test_failed_flow_progress_event(graph):
    """PythonPipelineSuite:152: a failed flow's message. The location is not in PipelineEvent."""
    events, error = graph.attempt(
        """
@dp.table()
def table1():
    df = spark.createDataFrame([(25,), (30,), (45,)], ["age"])
    return df.select("name")
"""
    )
    assert error is not None
    assert any(
        "A column, variable, or function parameter with name `name` cannot be resolved." in event
        and graph.name("table1") in event
        for event in events
    ), events


def test_flow_progress_events(graph):
    """PythonPipelineSuite:182: each flow's lifecycle, batch with PLANNING and streaming without."""
    events = graph.run(
        """
@dp.table(
 comment = 'my table'
)
def table1():
    return spark.readStream.table('mv')

@dp.materialized_view
def mv2():
   return spark.range(26, 29)

@dp.materialized_view
def mv():
   df = spark.createDataFrame([(25,), (30,), (45,)], ["age"])
   return df.select("age")

@dp.append_flow(
 target = 'table1'
)
def standalone_flow1():
   return spark.readStream.table('mv2')
"""
    )
    # Presence, as assertFlowProgressEvent checks: the two flows into table1 may race and retry on Spark.
    for name in ["mv2", "mv"]:
        assert {"QUEUED", "PLANNING", "STARTING", "RUNNING", "COMPLETED"} <= set(statuses(events, graph.name(name)))
    for name in ["table1", "standalone_flow1"]:
        words = set(statuses(events, graph.name(name)))
        assert {"QUEUED", "STARTING", "RUNNING", "COMPLETED"} <= words
        assert "PLANNING" not in words


def test_external_sink_passes_a_dry_run(graph):
    """PythonPipelineSuite:321: external sink. Kafka is never contacted; test_cli_sink shows options arriving."""
    events, error = graph.attempt(
        f"""
dp.create_sink(
  "myKafkaSink",
  format = "kafka",
  options = {{"kafka.bootstrap.servers": "host1:port1,host2:port2"}}
)

@dp.append_flow(
  target = "myKafkaSink"
)
def mySinkFlow():
  return {RATE}
""",
        dry=True,
    )
    assert error is None, events


def test_internal_references_order_flows_and_keep_their_read_mode(graph):
    """PythonPipelineSuite:351: dependencies and streaming flows, as the events show them."""
    events = graph.run(
        """
@dp.materialized_view
def src():
  return spark.range(5)

@dp.materialized_view
def a():
  return spark.read.table("src")

@dp.table
def b():
  return spark.readStream.table("src")

@dp.materialized_view
def c():
  return spark.sql("SELECT * FROM src")

@dp.table
def d():
  return spark.sql("SELECT * FROM STREAM src")
"""
    )
    for name in ["src", "a", "c"]:
        assert "PLANNING" in statuses(events, graph.name(name))
    for name in ["b", "d"]:
        assert "PLANNING" not in statuses(events, graph.name(name))
    finished = first_index(events, graph.name("src"), "COMPLETED")
    for name in ["a", "b", "c"]:
        assert finished < first_index(events, graph.name(name), "STARTING")
    for name in "abcd":
        assert graph.table(name) == FIVE


def test_external_references_keep_their_read_mode(graph, spark):
    """PythonPipelineSuite:397: batch and streaming flows over an external table."""
    spark.sql(f"CREATE TABLE {graph.schema}.ext AS SELECT * FROM RANGE(5)")
    name = f"{graph.catalog}.{graph.schema}.ext"
    events = graph.run(
        f"""
@dp.materialized_view
def a():
  return spark.read.table("{name}")

@dp.materialized_view
def b():
  return spark.table("{name}")

@dp.table
def c():
  return spark.readStream.table("{name}")

@dp.materialized_view
def d():
  return spark.sql("SELECT * FROM {name}")

@dp.table
def e():
  return spark.sql("SELECT * FROM STREAM {name}")
"""
    )
    for flow in "abd":
        assert "PLANNING" in statuses(events, graph.name(flow))
    for flow in "ce":
        assert "PLANNING" not in statuses(events, graph.name(flow))
    for flow in "abcde":
        assert graph.table(flow) == FIVE


@pytest.mark.parametrize("scope", ["internal", "external"])
def test_every_flow_reports_its_missing_source(graph, scope):
    """PythonPipelineSuite:440 and 470: five flows fail to resolve, each naming the table."""
    source = "src" if scope == "internal" else f"{graph.catalog}.{graph.schema}.src"
    expected = "`src`" if scope == "internal" else f"`{graph.catalog}`.`{graph.schema}`.`src`"
    kinds = ("table", "table", "table", "materialized_view", "table")
    if scope == "external":
        kinds = ("table", "materialized_view", "materialized_view", "materialized_view", "table")
    queries = (
        f'spark.read.table("{source}")',
        f'spark.table("{source}")',
        f'spark.readStream.table("{source}")',
        f'spark.sql("SELECT * FROM {source}")',
        f'spark.sql("SELECT * FROM STREAM {source}")',
    )
    body = "".join(
        f"@dp.{kind}\ndef {name}():\n  return {query}\n\n" for kind, name, query in zip(kinds, "abcde", queries)
    )
    events, error = graph.attempt(body, dry=True)
    assert error is not None
    assert "Failed to resolve flows in the pipeline" in str(error)
    for name in "abcde":
        failures = [event for event in events if "Failed to resolve flow" in event and graph.name(name) in event]
        assert failures, events
        assert "TABLE_OR_VIEW_NOT_FOUND" in failures[0]
        assert expected in failures[0]


def test_external_reads_outside_a_query_function_are_batch(graph, spark):
    """PythonPipelineSuite:499: frames made at definition time."""
    spark.sql(f"CREATE TABLE {graph.schema}.ext AS SELECT * FROM RANGE(5)")
    name = f"{graph.catalog}.{graph.schema}.ext"
    events = graph.run(
        f"""
spark_sql_df = spark.sql("SELECT * FROM {name}")
read_table_df = spark.read.table("{name}")

@dp.materialized_view
def mv_from_spark_sql_df():
  return spark_sql_df

@dp.materialized_view
def mv_from_read_table_df():
  return read_table_df
"""
    )
    for flow in ["mv_from_spark_sql_df", "mv_from_read_table_df"]:
        assert "PLANNING" in statuses(events, graph.name(flow))
        assert graph.table(flow) == FIVE


def test_internal_read_outside_a_query_function_depends_on_its_source(graph):
    """PythonPipelineSuite:522: a frame made at definition time still tracks its input."""
    events = graph.run(
        """
@dp.materialized_view
def src():
  return spark.range(5)

read_table_df = spark.read.table("src")

@dp.materialized_view
def mv_from_read_table_df():
  return read_table_df
"""
    )
    assert "PLANNING" in statuses(events, graph.name("mv_from_read_table_df"))
    finished = first_index(events, graph.name("src"), "COMPLETED")
    assert finished < first_index(events, graph.name("mv_from_read_table_df"), "STARTING")


@pytest.fixture
def other_schemas(graph, spark):
    names = [f"{graph.schema}_a", f"{graph.schema}_b"]
    for name in names:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {name}")
    yield names
    for name in names:
        spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")


BATCH_IN_TABLE = "INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE"


def test_batch_tables_report_their_qualified_names(graph, other_schemas):
    """PythonPipelineSuite:586: the Scala case only resolves, and validation is what names these batch tables."""
    for name, qualified in [
        ("mv_1", f"`{graph.catalog}`.`{graph.schema}`.`mv_1`"),
        (f"{other_schemas[0]}.mv_2", f"`{graph.catalog}`.`{other_schemas[0]}`.`mv_2`"),
    ]:
        _, error = graph.attempt(f'@dp.table(name = "{name}")\ndef irrelevant():\n  return spark.range(5)\n', dry=True)
        assert error is not None
        assert error.getCondition() == BATCH_IN_TABLE
        assert qualified in str(error)


def test_streaming_tables_with_qualified_names_pass_a_dry_run(graph, other_schemas):
    """PythonPipelineSuite:586: st_1 and schema_b.st_2."""
    events, error = graph.attempt(
        f"""
@dp.table(name = "st_1")
def irrelevant_2():
  return {RATE}

@dp.table(name = "{other_schemas[1]}.st_2")
def irrelevant_3():
  return {RATE}
""",
        dry=True,
    )
    assert error is None, events


def test_three_part_names_are_kept(graph):
    """PythonPipelineSuite:644: an unknown catalog passes registration and resolution."""
    _, error = graph.attempt(
        '@dp.table(name = "some_catalog.some_schema.mv")\ndef irrelevant_1():\n  return spark.range(5)\n', dry=True
    )
    assert error is not None
    assert error.getCondition() == BATCH_IN_TABLE
    assert "`some_catalog`.`some_schema`.`mv`" in str(error)
    events, error = graph.attempt(
        f'@dp.table(name = "some_catalog.some_schema.st")\ndef irrelevant_2():\n  return {RATE}\n', dry=True
    )
    assert error is None, events


def test_temporary_views_resolve_through_a_run(graph, spark):
    """PythonPipelineSuite:667: mv_1 reads view_3 so a run exercises them; a view reports no flow of its own."""
    events = graph.run(
        """
@dp.materialized_view
def mv_1():
  return spark.read.table("view_3")

@dp.temporary_view
def view_1():
  return spark.range(5)

@dp.temporary_view(name= "view_2")
def irrelevant_1():
  return spark.read.table("view_1")

@dp.temporary_view(name= "view_3")
def irrelevant_2():
  return spark.read.table("view_1")
"""
    )
    assert not any("view_" in event for event in events), events
    assert graph.table("mv_1") == FIVE
    assert not spark.catalog.tableExists(f"{graph.schema}.view_1")


@pytest.mark.parametrize(
    ("name", "flow"),
    [
        pytest.param("", "supplement", id="PythonPipelineSuite-711"),
        pytest.param("something", "something", id="PythonPipelineSuite-730"),
    ],
)
def test_a_flow_with_a_multipart_target_reaches_it(graph, spark, name, flow):
    """PythonPipelineSuite:711 and 730: the flow keeps its own name and writes into the target."""
    spark.sql(f"CREATE TABLE {graph.schema}.ext AS SELECT * FROM RANGE(2)")
    source = f"spark.readStream.table('{graph.schema}.ext')"
    arguments = f'target = "{graph.schema}.a"' + (f', name = "{name}"' if name else "")
    events = graph.run(
        f"""
@dp.table()
def a():
  return {source}

@dp.append_flow({arguments})
def supplement():
  return {source}
"""
    )
    assert "COMPLETED" in statuses(events, graph.name("a"))
    assert "COMPLETED" in statuses(events, graph.name(flow))
    assert set(graph.table("a")) == {(0,), (1,)}


def test_partition_columns_reach_the_catalog(graph, spark):
    """PythonPipelineSuite:810: MV/ST with partition columns works."""
    graph.run(
        """
from pyspark.sql.functions import col

@dp.materialized_view(partition_cols = ["id_mod"])
def mv():
  return spark.range(5).withColumn("id_mod", col("id") % 2)

@dp.table(partition_cols = ["id_mod"])
def st():
  return spark.readStream.table("mv")
"""
    )
    for table in ["mv", "st"]:
        assert graph.table(table) == [(i, i % 2) for i in range(5)]
        assert_partitioned_by(spark, f"{graph.schema}.{table}", "id_mod")


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("spark.range(1)\n", id="PythonPipelineSuite-844"),
        pytest.param("@dp.temporary_view\ndef view_1():\n  return spark.range(5)\n", id="PythonPipelineSuite-855"),
        pytest.param(
            '@dp.append_flow(target = "a")\ndef flow():\n  return spark.range(5)\n', id="PythonPipelineSuite-868"
        ),
    ],
)
def test_an_empty_pipeline_has_no_parameters(graph, body):
    """PythonPipelineSuite:844, 855 and 868: checkError with an empty parameter map."""
    _, error = graph.attempt(body, dry=True)
    assert error is not None
    assert error.getCondition() == "RUN_EMPTY_PIPELINE"
    assert error.getMessageParameters() == {}


def test_an_empty_cluster_by_leaves_no_transform(graph, spark):
    """PythonPipelineSuite:1373: empty cluster_by list should work and create table with no clustering."""
    graph.run(
        """
from pyspark.sql.functions import col

@dp.materialized_view(cluster_by = [])
def mv():
  return spark.range(5).withColumn("id_mod", col("id") % 2)

@dp.table(cluster_by = [])
def st():
  return spark.readStream.table("mv")
"""
    )
    for table in ["mv", "st"]:
        name = f"{graph.schema}.{table}"
        assert_clustered_by(spark, name, [])
        assert [item.name for item in spark.catalog.listColumns(name) if item.isPartition] == []


GUARD = "ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION"
START_STREAM = 'spark.readStream.format("rate").load().writeStream.format("memory").queryName("{name}").start()'


def in_function(statement):
    return f"""
@dp.materialized_view()
def mv():
  df = spark.range(5)
  {statement}
  return df
"""


def at_definition(statement):
    return f"""
df = spark.range(5)
{statement}

@dp.materialized_view()
def mv():
  df = spark.range(5)
  return df
"""


@pytest.mark.parametrize("wrap", [at_definition, in_function])
def test_show_create_table_is_a_supported_command(graph, spark, wrap):
    """PythonPipelineSuite:1465 and 1477: the SHOW CREATE TABLE entry of the supported list."""
    spark.sql(f"CREATE TABLE {graph.schema}.src AS SELECT * FROM RANGE(5)")
    graph.build(wrap(f'spark.sql("SHOW CREATE TABLE {graph.catalog}.{graph.schema}.src")'))


def test_starting_a_stream_inside_a_query_function_is_blocked(graph):
    """PythonPipelineSuite:1496: the writeStream entry of the eager execution list."""
    with pytest.raises(AnalysisException) as error:
        graph.build(in_function(START_STREAM.format(name="pyapi_inside")))
    assert error.value.getCondition() == GUARD


@pytest.mark.xfail(
    not is_jvm_spark(), strict=True, reason="Sail has no memory format for writeStream outside a pipeline sink"
)
def test_starting_a_stream_outside_a_query_function_is_allowed(graph, spark):
    """PythonPipelineSuite:1510: the writeStream entry of the eager execution list."""
    try:
        graph.build(at_definition(START_STREAM.format(name="pyapi_outside")))
        assert [query.name for query in spark.streams.active] == ["pyapi_outside"]
    finally:
        for query in spark.streams.active:
            query.stop()
