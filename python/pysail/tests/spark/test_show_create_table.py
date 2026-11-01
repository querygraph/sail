"""SHOW CREATE TABLE, measured on Spark 4.2.1, whose ShowCreateTableCommandBase sets the layout."""

import pytest
from pyspark.errors.exceptions.connect import AnalysisException

SCHEMA = "show_create"


@pytest.fixture
def schema(spark):
    spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    spark.sql(f"CREATE SCHEMA {SCHEMA}")
    yield f"{spark.catalog.currentCatalog()}.{SCHEMA}"
    spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")


def statement(spark, name):
    frame = spark.sql(f"SHOW CREATE TABLE {name}")
    assert frame.schema.simpleString() == "struct<createtab_stmt:string>"
    rows = frame.collect()
    assert len(rows) == 1
    return rows[0][0]


@pytest.mark.parametrize(
    ("ddl", "expected"),
    [
        pytest.param(
            "(a INT, b STRING) USING parquet",
            "(\n  a INT,\n  b STRING COLLATE UTF8_BINARY)\nUSING parquet\n",
            id="columns",
        ),
        pytest.param(
            "(a INT NOT NULL COMMENT 'ca', `b c` STRING, d DOUBLE, e DECIMAL(10,2), f ARRAY<INT>, "
            "g MAP<STRING,INT>, h STRUCT<x:INT,y:STRING>) USING parquet COMMENT 'tc' "
            "TBLPROPERTIES ('k'='v','a.b'='c')",
            "(\n  a INT COMMENT 'ca',\n  `b c` STRING COLLATE UTF8_BINARY,\n  d DOUBLE,\n  e DECIMAL(10,2),\n"
            "  f ARRAY<INT>,\n  g MAP<STRING COLLATE UTF8_BINARY, INT>,\n"
            "  h STRUCT<x: INT, y: STRING COLLATE UTF8_BINARY>)\nUSING parquet\nCOMMENT 'tc'\n"
            "TBLPROPERTIES (\n  'a.b' = 'c',\n  'k' = 'v')\n",
            id="types-comment-properties",
        ),
        pytest.param(
            "(a INT, p STRING, q INT) USING parquet PARTITIONED BY (p, q)",
            "(\n  a INT,\n  p STRING COLLATE UTF8_BINARY,\n  q INT)\nUSING parquet\nPARTITIONED BY (p, q)\n",
            id="partitions",
        ),
        pytest.param(
            "(a INT, b STRING) USING parquet CLUSTERED BY (a) SORTED BY (b) INTO 4 BUCKETS",
            "(\n  a INT,\n  b STRING COLLATE UTF8_BINARY)\nUSING parquet\nCLUSTERED BY (a)\nSORTED BY (b)\n"
            "INTO 4 BUCKETS\n",
            id="buckets",
        ),
        pytest.param(
            "USING parquet AS SELECT 1 AS a, 'x' AS b",
            "(\n  a INT,\n  b STRING COLLATE UTF8_BINARY)\nUSING parquet\n",
            id="ctas",
        ),
        pytest.param(
            "(a INT DEFAULT 5, ts TIMESTAMP, d DATE, by BINARY) USING parquet",
            "(\n  a INT DEFAULT 5,\n  ts TIMESTAMP,\n  d DATE,\n  by BINARY)\nUSING parquet\n",
            id="default-and-simple-types",
        ),
    ],
)
def test_table(spark, schema, ddl, expected):
    spark.sql(f"CREATE TABLE {SCHEMA}.t {ddl}")
    assert statement(spark, f"{SCHEMA}.t") == f"CREATE TABLE {schema}.t {expected}"


def test_options_and_location(spark, schema, tmp_path):
    spark.sql(f"CREATE TABLE {SCHEMA}.t (a INT) USING csv OPTIONS (header 'true', sep ',') LOCATION '{tmp_path}/t'")
    text = statement(spark, f"{SCHEMA}.t")
    prefix = (
        f"CREATE TABLE {schema}.t (\n  a INT)\nUSING csv\nOPTIONS (\n  'header' = 'true',\n  'sep' = ',')\nLOCATION '"
    )
    assert text.startswith(prefix)
    assert text.endswith(f"{tmp_path}/t'\n")


def test_view(spark, schema):
    spark.sql(f"CREATE VIEW {SCHEMA}.v (x COMMENT 'cx') COMMENT 'vc' AS SELECT 1 AS a")
    assert statement(spark, f"{SCHEMA}.v") == (
        f"CREATE VIEW {SCHEMA}.v (\n  x COMMENT 'cx')\nCOMMENT 'vc'\nWITH SCHEMA COMPENSATION\nAS SELECT 1 AS a\n"
    )


def test_temporary_view_is_refused(spark):
    spark.sql("CREATE OR REPLACE TEMPORARY VIEW show_create_temporary AS SELECT 1 AS a")
    try:
        with pytest.raises(AnalysisException) as error:
            spark.sql("SHOW CREATE TABLE show_create_temporary").collect()
    finally:
        spark.sql("DROP VIEW show_create_temporary")
    assert error.value.getCondition() == "EXPECT_PERMANENT_VIEW_NOT_TEMP"
    assert error.value.getMessageParameters() == {
        "viewName": "`show_create_temporary`",
        "operation": "SHOW CREATE TABLE",
    }


def test_as_serde_is_refused_on_a_data_source_table(spark, schema):
    spark.sql(f"CREATE TABLE {SCHEMA}.t (a INT) USING parquet")
    with pytest.raises(AnalysisException) as error:
        spark.sql(f"SHOW CREATE TABLE {SCHEMA}.t AS SERDE").collect()
    assert error.value.getCondition() == "UNSUPPORTED_SHOW_CREATE_TABLE.ON_DATA_SOURCE_TABLE_WITH_AS_SERDE"
    catalog, namespace, table = schema.split(".") + ["t"]
    assert error.value.getMessageParameters() == {"tableName": f"`{catalog}`.`{namespace}`.`{table}`"}


def test_a_missing_table_is_not_found(spark, schema):
    with pytest.raises(AnalysisException) as error:
        spark.sql(f"SHOW CREATE TABLE {SCHEMA}.nope").collect()
    assert "TABLE_OR_VIEW_NOT_FOUND" in str(error.value)
