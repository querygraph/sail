"""SqlPipelineSuite, SqlQueryOriginSuite, ViewSuite and the SQL parser suites, what other files leave out.

The parser suites compare plans. Here the same statement reaches the parser through a query or through
SQL registration, and what it parsed to is read from a published table's catalog metadata and rows.
"""

from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException, ParseException, UnsupportedOperationException
from pyspark.pipelines.spark_connect_graph_element_registry import SparkConnectGraphElementRegistry
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph, start_run

KINDS = ["MATERIALIZED VIEW", "STREAMING TABLE"]


class Sql:
    def __init__(self, spark, schema, storage):
        self.spark = spark
        self.schema = schema
        self.storage = storage
        self.catalog = spark.catalog.currentCatalog()

    def registry(self):
        graph = create_dataflow_graph(self.spark, self.catalog, self.schema, None)
        return graph, SparkConnectGraphElementRegistry(self.spark, graph)

    def run(self, *files, dry=False, into=None):
        graph, registry = self.registry()
        for index, sql in enumerate(files):
            registry.register_sql(sql, Path(f"file{index}.sql"))
        into = [] if into is None else into
        for item in start_run(self.spark, graph, None, False, None, dry, self.storage):
            if "pipeline_event_result" in item:
                into.append(item["pipeline_event_result"].event.message)
        return into

    def failure(self, *files, dry=False):
        messages = []
        with pytest.raises(AnalysisException) as error:
            self.run(*files, dry=dry, into=messages)
        return error.value, messages

    def table(self, name):
        return f"{self.schema}.{name}"

    def full(self, name):
        return f"{self.catalog}.{self.schema}.{name}"

    def quoted(self, name):
        return f"`{self.catalog}`.`{self.schema}`.`{name}`"

    def rows(self, name):
        return sorted(tuple(row) for row in self.spark.table(self.table(name)).collect())

    def details(self, name):
        rows = self.spark.sql(f"DESCRIBE TABLE EXTENDED {self.table(name)}").collect()
        return {row.col_name: row.data_type for row in rows}

    def partitions(self, name):
        return [c.name for c in self.spark.catalog.listColumns(self.table(name)) if c.isPartition]

    def properties(self, name):
        rows = self.spark.sql(f"SHOW TBLPROPERTIES {self.table(name)}").collect()
        return {row.key: row.value for row in rows}

    def source(self, name="input"):
        self.spark.sql(f"CREATE TABLE {self.table(name)} (a BIGINT) USING parquet")
        self.spark.sql(f"INSERT INTO {self.table(name)} VALUES (7)")

    def input_of(self, kind, name="input"):
        return f"STREAM {name}" if kind == "STREAMING TABLE" else name


@pytest.fixture
def space(spark, tmp_path, request):
    schema = "aq_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA {schema}")
    yield Sql(spark, schema, (tmp_path / "storage").as_uri())
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


# SqlPipelineSuite


def test_partition_columns_are_registered(space):
    """SqlPipelineSuite:252: both identity columns are partition columns, in declared order.

    The Scala graph only resolves. A published table needs a column outside its partition columns, and
    on Spark's session catalog the partition columns must come last.
    """
    clause = "CREATE MATERIALIZED VIEW a\nPARTITIONED BY (id1, id2)\nAS SELECT"
    tail = "id as id1, id as id2 FROM range(1,2)"
    assert space.run(f"{clause} {tail}", dry=True)[-1].startswith("Run is COMPLETED")
    space.run(f"{clause} id as id3, {tail}")
    assert space.partitions("a") == ["id1", "id2"]
    assert sorted(tuple(row) for row in space.spark.table(space.table("a")).select("id1", "id2", "id3").collect()) == [
        (1, 1, 1)
    ]


def test_mv_and_st_with_partition_columns(space):
    """SqlPipelineSuite:270: both tables are partitioned by id_mod and hold the same rows."""
    space.run(
        """
CREATE MATERIALIZED VIEW mv
PARTITIONED BY (id_mod)
AS
SELECT
  id,
  id % 2 AS id_mod
FROM range(3);

CREATE STREAMING TABLE st
PARTITIONED BY (id_mod)
AS
SELECT * FROM STREAM(mv);
"""
    )
    for name in ["mv", "st"]:
        assert space.partitions(name) == ["id_mod"]
        assert space.rows(name) == [(0, 0), (1, 1), (2, 0)]


def test_every_column_as_a_partition_column_is_rejected_by_a_run(space):
    """SqlPipelineSuite:252's text, which the Scala test never runs."""
    error, _ = space.failure(
        "CREATE MATERIALIZED VIEW a\nPARTITIONED BY (id1, id2)\nAS SELECT id as id1, id as id2 FROM range(1,2)"
    )
    assert error.getCondition() == "ALL_PARTITION_COLUMNS_NOT_ALLOWED"


def test_a_non_identity_partition_transform_is_rejected(space):
    """SqlPipelineSuite:311: the registration error names the transform."""
    _, registry = space.registry()
    sql = "\nCREATE MATERIALIZED VIEW a\nPARTITIONED BY (year(id1))\nAS SELECT id as id1, id as id2 FROM range(1,2)"
    with pytest.raises(AnalysisException) as error:
        registry.register_sql(sql, Path("a.sql"))
    assert error.value.getCondition() == "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR"
    assert "Invalid partitioning transform" in error.value.getMessageParameters()["message"]
    assert not space.spark.catalog.tableExists(space.table("a"))


def test_table_properties_are_registered(space):
    """SqlPipelineSuite:329: the Scala graph only resolves, so the stream is what lets a run publish them."""
    space.source()
    space.run("CREATE STREAMING TABLE st TBLPROPERTIES ('prop1'='foo', 'prop2'='bar') AS SELECT * FROM STREAM input")
    properties = space.properties("st")
    assert (properties["prop1"], properties["prop2"]) == ("foo", "bar")


def test_spark_confs_are_captured_at_each_statement(space):
    """SqlPipelineSuite:345: a flow sees the SET statements before it, and two keys accumulate.

    The Scala test reads Flow.sqlConf. A client sees a conf through what the flow computes, so the keys
    here are ones a query can read back. conf.test is the time zone and conf.test2 is case sensitivity.
    Both act during analysis, which Spark runs under the flow's confs. Spark's batch write runs after
    those confs are restored, so an execution-time reader such as current_timezone() races between
    flows on Spark and is not used. ID resolves only while the session is case insensitive, and the
    struct's field a is unambiguous only once it is sensitive.
    """
    space.source()
    hour = "hour(TIMESTAMP '2024-01-01 12:00:00+00:00')"
    space.run(
        f"""
CREATE MATERIALIZED VIEW a AS SELECT {hour} AS h, ID FROM range(1);
SET spark.sql.session.timeZone = Asia/Tokyo;
CREATE MATERIALIZED VIEW b AS SELECT {hour} AS h, ID FROM range(1);
SET spark.sql.session.timeZone = Europe/Paris;
SET spark.sql.caseSensitive = true;
CREATE STREAMING TABLE c AS SELECT {hour} AS h, named_struct('A', a, 'a', 2).a AS v FROM STREAM input;
"""
    )
    assert space.rows("a") == [(12, 0)]
    assert space.rows("b") == [(21, 0)]
    assert space.rows("c") == [(13, 2)]


def test_a_double_quoted_location_is_rejected(space):
    """SqlPipelineSuite:386, with the Scala text's double-quoted path."""
    _, registry = space.registry()
    with pytest.raises(ParseException) as error:
        registry.register_sql(
            'CREATE STREAMING TABLE a\nLOCATION "/path/to/table"\nAS SELECT * FROM range(1,2)', Path("a.sql")
        )
    assert "Specifying location is not supported" in str(error.value)


def test_an_empty_file_path_reports_the_whole_message(space):
    """SqlPipelineSuite:475: the flow's resolution failure carries the message."""
    _, messages = space.failure("\nCREATE MATERIALIZED VIEW a AS SELECT * FROM csv.``;\n")
    failures = [message for message in messages if "Failed to resolve flow" in message]
    assert any("The location name cannot be empty string, but `` was given." in message for message in failures), (
        messages
    )


def test_table_schema_is_parsed(space):
    """SqlPipelineSuite:540: LONG with a comment validates against RANGE and is published as declared."""
    sql = "\nCREATE MATERIALIZED VIEW a (id LONG COMMENT 'comment') AS SELECT * FROM RANGE(5);\n"
    assert space.run(sql, dry=True)[-1].startswith("Run is COMPLETED")
    space.run(sql)
    assert space.spark.table(space.table("a")).dtypes == [("id", "bigint")]
    assert [(c.name, c.description) for c in space.spark.catalog.listColumns(space.table("a"))] == [("id", "comment")]
    assert space.rows("a") == [(i,) for i in range(5)]


def test_qualified_names_between_two_other_schemas(space, spark):
    """SqlPipelineSuite:759, the last pair: neither side is in the pipeline's schema.

    The Scala pair crosses catalogs. InMemoryTableCatalog is a Spark test class, so it crosses schemas.
    """
    first, second = f"{space.schema}_x", f"{space.schema}_y"
    spark.sql(f"CREATE SCHEMA {first}")
    spark.sql(f"CREATE SCHEMA {second}")
    try:
        space.run(
            f"""
CREATE MATERIALIZED VIEW {first}.upstream_mv6 (id BIGINT) AS SELECT * FROM RANGE(10);
CREATE MATERIALIZED VIEW {second}.downstream_mv6 AS SELECT id FROM {first}.upstream_mv6
WHERE (id%2)=0;
"""
        )
        rows = spark.sql(f"SELECT * FROM {second}.downstream_mv6").collect()
        assert sorted(row.id for row in rows) == [0, 2, 4, 6, 8]
        spark.sql(f"DROP TABLE {first}.upstream_mv6")
        spark.sql(f"DROP TABLE {second}.downstream_mv6")
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {first} CASCADE")
        spark.sql(f"DROP SCHEMA IF EXISTS {second} CASCADE")


# SqlQueryOriginSuite


@pytest.mark.parametrize(
    ("text", "offending", "line"),
    [
        pytest.param(
            "-- comment 1\nCREATE MATERIALIZED VIEW a.b.c PARTITIONED BY (year(x)) AS SELECT 1;\n\n"
            "USE DATABASE d ; -- comment 2\n",
            "CREATE MATERIALIZED VIEW a.b.c PARTITIONED BY (year(x)) AS SELECT 1",
            2,
            id="basic-first",
        ),
        pytest.param(
            "-- comment 1\nCREATE TEMPORARY VIEW v AS SELECT 1;\n\nSET CATALOG test.cat ; -- comment 2\n",
            "SET CATALOG test.cat ",
            4,
            id="basic-second",
        ),
        pytest.param(
            'CREATE STREAMING TABLE `a.\\n` PARTITIONED BY (year(x)) AS SELECT "\\n";\n'
            "CREATE VIEW my_view AS SELECT * FROM `a.\\n`;\n",
            'CREATE STREAMING TABLE `a.\\n` PARTITIONED BY (year(x)) AS SELECT "\\n"',
            1,
            id="escaped-newline-first",
        ),
        pytest.param(
            'CREATE STREAMING TABLE `a.\\n` AS SELECT "\\n";\nCREATE FLOW f AS INSERT INTO `a.\\n` SELECT "\\n";\n',
            'CREATE FLOW f AS INSERT INTO `a.\\n` SELECT "\\n"',
            2,
            id="escaped-newline-second",
        ),
        pytest.param(
            "\n     \tCREATE FLOW f AS INSERT INTO t SELECT 1;\n",
            "CREATE FLOW f AS INSERT INTO t SELECT 1",
            2,
            id="white-space",
        ),
        pytest.param(
            "\nCREATE\nMATERIALIZED VIEW mv\nPARTITIONED BY (year(x))\nAS\nSELECT 1;\n",
            "CREATE\nMATERIALIZED VIEW mv\nPARTITIONED BY (year(x))\nAS\nSELECT 1",
            2,
            id="multiline",
        ),
        pytest.param(
            "\n-- comment\nCREATE MATERIALIZED VIEW mv -- another comment\nPARTITIONED BY (year(x))\nAS SELECT 1;\n",
            "CREATE MATERIALIZED VIEW mv -- another comment\nPARTITIONED BY (year(x))\nAS SELECT 1",
            3,
            id="preceding-comment",
        ),
        pytest.param(
            "CREATE MATERIALIZED VIEW v PARTITIONED BY (year(x)) AS SELECT 'my ; string';",
            "CREATE MATERIALIZED VIEW v PARTITIONED BY (year(x)) AS SELECT 'my ; string'",
            1,
            id="semicolon-in-a-string-literal",
        ),
    ],
)
def test_a_pipeline_statement_keeps_its_origin(space, text, offending, line):
    """SqlQueryOriginSuite: test_sql_query_origins.py swaps the Scala statements for SELECT; these keep them.

    A rejected clause or statement stands in the Scala statement, so its text and line reach the client.
    """
    _, registry = space.registry()
    with pytest.raises(AnalysisException) as error:
        registry.register_sql(text, Path("file.sql"))
    parameters = error.value.getMessageParameters()
    assert parameters["codeLocation"] == f"Query defined at file.sql:{line}"
    assert parameters["offendingQuery"] == f"\nOffending query:\n{offending}\n"


# ViewSuite


def test_create_persisted_views(space):
    """ViewSuite:41."""
    space.run("CREATE VIEW mypersistedview AS SELECT * FROM range(1, 4);")
    assert space.details("mypersistedview")["View Text"] == "SELECT * FROM range(1, 4)"
    assert space.details("mypersistedview")["Type"] == "VIEW"
    assert space.rows("mypersistedview") == [(1,), (2,), (3,)]


def test_a_persisted_view_reads_an_external_table(space):
    """ViewSuite:70."""
    space.spark.sql(f"CREATE TABLE {space.table('mytable')} AS SELECT * FROM range(1, 4)")
    source = space.quoted("mytable")
    space.run(f"CREATE VIEW mypersistedview AS SELECT * FROM {source};")
    assert space.details("mypersistedview")["View Text"] == f"SELECT * FROM {source}"
    assert space.rows("mypersistedview") == [(1,), (2,), (3,)]


def test_a_persisted_view_of_a_non_existent_dataset_is_a_direct_failure(space):
    """ViewSuite:133: UnresolvedPipelineException.directFailures is the summary's first list."""
    error, messages = space.failure("CREATE VIEW myview AS SELECT * FROM nonexistent_view;")
    assert f"Flows with errors: {space.full('myview')}\n" in str(error)
    assert "Flows that failed due to upstream errors: \n" in str(error)
    failures = [message for message in messages if message.startswith("Failed to resolve flow")]
    assert len(failures) == 1
    assert "nonexistent_view" in failures[0]
    assert not space.spark.catalog.tableExists(space.table("myview"))


def test_a_persisted_view_cannot_stream_an_external_table(space):
    """ViewSuite:171."""
    space.source("external_t1")
    error, _ = space.failure("CREATE VIEW mypersistedview AS SELECT * FROM STREAM(external_t1);")
    assert error.getCondition() == "INVALID_FLOW_QUERY_TYPE.STREAMING_RELATION_FOR_PERSISTED_VIEW"


def test_a_persisted_view_reads_another_persisted_view(space):
    """ViewSuite:190."""
    space.run(
        """CREATE VIEW pv3 AS SELECT * FROM pv2;
CREATE VIEW pv2 AS SELECT * FROM pv1;
CREATE VIEW pv1 AS SELECT * FROM range(1, 4);"""
    )
    for name, text in [("pv1", "range(1, 4)"), ("pv2", "pv1"), ("pv3", "pv2")]:
        assert space.details(name)["View Text"] == f"SELECT * FROM {text}"
    assert space.rows("pv3") == [(1,), (2,), (3,)]


def test_a_persisted_view_reads_a_materialized_view(space):
    """ViewSuite:250."""
    source = space.quoted("mymv")
    space.run(
        f"""CREATE MATERIALIZED VIEW mymv AS SELECT * FROM RANGE(1, 4);
CREATE VIEW mypersistedview AS SELECT * FROM {source};"""
    )
    assert space.details("mypersistedview")["View Text"] == f"SELECT * FROM {source}"
    assert space.rows("mypersistedview") == [(1,), (2,), (3,)]


def test_a_persisted_view_reads_a_streaming_table(space):
    """ViewSuite:284."""
    space.spark.sql(f"CREATE TABLE {space.table('external_t1')} AS SELECT * FROM range(3)")
    source = space.quoted("myst")
    space.run(
        f"""CREATE STREAMING TABLE myst AS SELECT * FROM STREAM(external_t1);
CREATE VIEW mypersistedview AS SELECT * FROM {source};"""
    )
    assert space.details("mypersistedview")["View Text"] == f"SELECT * FROM {source}"
    assert space.rows("mypersistedview") == [(0,), (1,), (2,)]


def test_a_materialized_view_reads_a_persisted_view(space):
    """ViewSuite:318."""
    space.run(
        """
CREATE VIEW pv AS SELECT * FROM range(1, 4);
CREATE MATERIALIZED VIEW myviewreader AS SELECT * FROM pv;"""
    )
    assert space.details("pv")["View Text"] == "SELECT * FROM range(1, 4)"
    assert space.rows("myviewreader") == [(1,), (2,), (3,)]
    assert space.rows("pv") == [(1,), (2,), (3,)]


# CreatePipelineDatasetAsSelectParserSuiteBase, once per dataset kind


COLUMNS = """
CREATE {kind} table1 (
  a INT NOT NULL,
  b STRUCT<field: STRING, field2: INT> COMMENT "column description comment"
)
AS SELECT {select} FROM {source}
"""


def struct_source(space):
    space.spark.sql(f"CREATE TABLE {space.table('input')} (a INT, b STRUCT<field: STRING, field2: INT>) USING parquet")
    space.spark.sql(f"INSERT INTO {space.table('input')} SELECT 1, named_struct('field', 'x', 'field2', 2)")


@pytest.mark.parametrize("kind", KINDS)
def test_column_definitions_are_published(space, kind):
    """ParserSuiteBase:32, first statement: types and comments. The literal satisfies NOT NULL."""
    struct_source(space)
    space.run(COLUMNS.format(kind=kind, select="1 AS a, b", source=space.input_of(kind)))
    columns = space.spark.catalog.listColumns(space.table("table1"))
    assert [(c.name, c.dataType, c.description) for c in columns] == [
        ("a", "int", None),
        ("b", "struct<field:string,field2:int>", "column description comment"),
    ]
    assert space.rows("table1")[0][0] == 1


@pytest.mark.parametrize("kind", KINDS)
def test_column_definitions_keep_not_null(space, kind):
    """ParserSuiteBase:32, the Scala text over a nullable column: nullability was parsed, so it conflicts."""
    struct_source(space)
    error, _ = space.failure(COLUMNS.format(kind=kind, select="*", source=space.input_of(kind)))
    assert "USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE" in str(error)


def test_a_schema_conflict_has_a_structured_condition(space):
    """ParserSuiteBase:32, as above: the condition a client reads."""
    struct_source(space)
    error, _ = space.failure(COLUMNS.format(kind="MATERIALIZED VIEW", select="*", source="input"))
    assert error.getCondition() == "USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE"


@pytest.mark.parametrize("kind", KINDS)
def test_columns_are_inferred_without_definitions(space, kind):
    """ParserSuiteBase:32, second statement."""
    space.source()
    space.run(f"CREATE {kind} table1 AS SELECT * FROM {space.input_of(kind)}")
    assert space.spark.table(space.table("table1")).dtypes == [("a", "bigint")]


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(("clause", "expected"), [("PARTITIONED BY (a)", ["a"]), ("", [])], ids=["partitioned", "none"])
def test_partitioning_is_published(space, kind, clause, expected):
    """ParserSuiteBase:77."""
    space.source()
    space.run(f"CREATE {kind} table1 {clause} AS SELECT 1 AS v, a FROM {space.input_of(kind)}")
    assert space.partitions("table1") == expected


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    ("clause", "expected"), [("COMMENT 'this is a comment'", "this is a comment"), ("", None)], ids=["comment", "none"]
)
def test_comment_is_published(space, kind, clause, expected):
    """ParserSuiteBase:119."""
    space.source()
    space.run(f"CREATE {kind} table1 {clause} AS SELECT * FROM {space.input_of(kind)}")
    assert space.details("table1").get("Comment") == expected


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("provider", ["parquet", "json"])
def test_using_clause_is_published(space, kind, provider):
    """ParserSuiteBase:163. parquet is the Scala text and the default, so json shows the clause is read."""
    space.source()
    space.run(f"CREATE {kind} table1 USING {provider} AS SELECT * FROM {space.input_of(kind)}")
    assert space.details("table1")["Provider"] == provider
    assert space.rows("table1") == [(7,)]


@pytest.mark.parametrize("kind", KINDS)
def test_no_using_clause_takes_the_default_provider(space, kind):
    """ParserSuiteBase:163, without the clause: the provider is the session's default."""
    space.source()
    space.run(f"CREATE {kind} table1 AS SELECT * FROM {space.input_of(kind)}")
    assert space.details("table1")["Provider"] == space.spark.conf.get("spark.sql.sources.default")


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("clause", ["IF NOT EXISTS", ""], ids=["if-not-exists", "plain"])
def test_if_not_exists_parses_and_publishes(space, kind, clause):
    """ParserSuiteBase:174."""
    space.source()
    space.run(f"CREATE {kind} {clause} table1 AS SELECT * FROM {space.input_of(kind)}")
    assert space.rows("table1") == [(7,)]


@pytest.mark.parametrize("kind", KINDS)
def test_table_properties_parse_each_value_kind(space, kind):
    """ParserSuiteBase:185."""
    space.source()
    space.run(
        f"CREATE {kind} table1 TBLPROPERTIES(a=FALSE, b='long comment', c=100) AS SELECT * FROM {space.input_of(kind)}"
    )
    properties = space.properties("table1")
    assert {key: properties.get(key) for key in "abc"} == {"a": "false", "b": "long comment", "c": "100"}


@pytest.mark.parametrize("kind", KINDS)
def test_no_table_properties_publishes_none(space, kind):
    """ParserSuiteBase:185, second statement."""
    space.source()
    space.run(f"CREATE {kind} tabel1 AS SELECT * FROM {space.input_of(kind)}")
    assert not set(space.properties("tabel1")) & {"a", "b", "c"}


def test_a_streaming_table_of_a_batch_query_parses(space):
    """ParserSuiteBase:227 for STREAMING TABLE's "SELECT 1", and SqlPipelineSuite:329's text.

    It registers, so it parsed; the run is what refuses a batch query.
    """
    error, _ = space.failure("\nCREATE STREAMING TABLE table1 AS\nSELECT 1")
    assert error.getCondition() == "INVALID_FLOW_QUERY_TYPE.BATCH_RELATION_FOR_STREAMING_TABLE"


# CreateStreamingTableParserSuite


def test_a_streaming_table_without_a_query_keeps_its_comment(space):
    """CreateStreamingTableParserSuite:31, given the flow a run needs."""
    space.source()
    space.run(
        "CREATE STREAMING TABLE st COMMENT 'populate with flow later';"
        "CREATE FLOW f AS INSERT INTO st BY NAME SELECT * FROM STREAM input"
    )
    assert space.details("st")["Comment"] == "populate with flow later"
    assert space.rows("st") == [(7,)]


# CreateFlowCommandSuite


def direct_flow_error(space, sql):
    with pytest.raises(UnsupportedOperationException) as error:
        space.spark.sql(sql).collect()
    return error.value


def tables(space):
    space.spark.sql(f"CREATE TABLE {space.table('a')} (col1 BIGINT, col2 BIGINT) USING parquet")
    space.spark.sql(f"CREATE TABLE {space.table('b')} (col1 BIGINT, col2 BIGINT) USING parquet")


@pytest.mark.parametrize("name", ["f", "F COMMENT 'comment'"], ids=["no-comment", "comment"])
def test_flow_comment_parses_to_the_direct_execution_error(space, name):
    """CreateFlowCommandSuite:35: only a statement that parsed reaches this error."""
    tables(space)
    a, b = space.table("a"), space.table("b")
    error = direct_flow_error(space, f"CREATE FLOW {name} AS INSERT INTO {a} SELECT * FROM {b}")
    assert error.getCondition() == "UNSUPPORTED_FEATURE.CREATE_FLOW_QUERY_EXECUTION"


def test_a_commented_flow_runs(space):
    """CreateFlowCommandSuite:35, through a pipeline."""
    space.source()
    space.run(
        "CREATE STREAMING TABLE st;CREATE FLOW f COMMENT 'comment' AS INSERT INTO st BY NAME SELECT * FROM STREAM input"
    )
    assert space.rows("st") == [(7,)]


@pytest.mark.parametrize("name", ["f", "c.f", "c.d.f"])
def test_multipart_flow_names_parse_to_the_direct_execution_error(space, name):
    """CreateFlowCommandSuite:46."""
    tables(space)
    a, b = space.table("a"), space.table("b")
    error = direct_flow_error(space, f"CREATE FLOW {name} AS INSERT INTO {a} SELECT * FROM {b}")
    assert error.getCondition() == "UNSUPPORTED_FEATURE.CREATE_FLOW_QUERY_EXECUTION"


def test_flow_insert_overwrite_parses_to_the_direct_execution_error(space):
    """CreateFlowCommandSuite:58."""
    a, b = space.table("a"), space.table("b")
    space.spark.sql(f"CREATE TABLE {a} (col2 BIGINT, col1 BIGINT) USING parquet PARTITIONED BY (col1)")
    space.spark.sql(f"CREATE TABLE {b} (col1 BIGINT, col2 BIGINT) USING parquet")
    error = direct_flow_error(
        space, f"CREATE FLOW f AS INSERT OVERWRITE {a} PARTITION (col1) BY NAME SELECT col1, col2 FROM {b}"
    )
    assert error.getCondition() == "UNSUPPORTED_FEATURE.CREATE_FLOW_QUERY_EXECUTION"


def test_escaped_hyphenated_names_resolve_in_a_dry_run(space):
    """SqlPipelineSuite:137. The Scala graph only resolves: Spark's session catalog refuses this name in a real run."""
    sql = "CREATE MATERIALIZED VIEW `hyphen-mv` AS SELECT * FROM range(1, 4);"
    sql += "CREATE MATERIALIZED VIEW `other-hyphen-mv` AS SELECT * FROM `hyphen-mv`"
    assert space.run(sql, dry=True)[-1].startswith("Run is COMPLETED")
