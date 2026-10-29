from decimal import Decimal

import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DecimalType, LongType, MapType, StringType, StructField, StructType

try:
    from pyspark import pipelines as dp
    from pyspark.pipelines.graph_element_registry import graph_element_registration_context
except ImportError:
    pytest.skip("Declarative pipelines require Spark 4.1+", allow_module_level=True)


@pytest.mark.parametrize(
    "schema",
    [
        "id BIGINT",
        "id BIGINT, optional STRING",
        StructType([StructField("id", LongType(), metadata={"comment": "identifier"})]),
    ],
)
def test_declared_schema_validation(pipeline_spark, pipeline, schema):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view(schema=schema)
        def output():
            return pipeline_spark.range(3)

    run(dry=True)
    assert not pipeline_spark.catalog.tableExists("output")


def test_incompatible_declared_schema_prevents_all_writes(pipeline_spark, pipeline):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def valid():
            return pipeline_spark.range(1)

        @dp.materialized_view(schema="id STRING")
        def invalid():
            return pipeline_spark.range(1)

    with pytest.raises(AnalysisException, match="USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE"):
        run()
    assert not pipeline_spark.catalog.tableExists("valid")
    assert not pipeline_spark.catalog.tableExists("invalid")


def test_inferred_columns_must_be_declared(pipeline_spark, pipeline):
    """ConnectInvalidPipelineSuite: inferred schema is not a subset of the declaration."""
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view(schema="expected BIGINT")
        def output():
            return pipeline_spark.range(2).selectExpr("id AS `incorrect-col-name`")

    with pytest.raises(AnalysisException, match="USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE") as error:
        run(dry=True)
    assert "incorrect-col-name" in str(error.value)
    assert not pipeline_spark.catalog.tableExists("output")


def test_declared_nonnullable_field_cannot_receive_nullable_data(pipeline_spark, pipeline):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view(schema=StructType([StructField("id", LongType(), nullable=False)]))
        def output():
            return pipeline_spark.createDataFrame([(1,), (None,)], "id BIGINT")

    with pytest.raises(AnalysisException, match="USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE"):
        run(dry=True)
    assert not pipeline_spark.catalog.tableExists("output")


@pytest.mark.parametrize("metadata", [{}, {"comment": "declared", "tag": "declaration"}])
def test_declared_metadata_takes_precedence(pipeline_spark, pipeline, metadata):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view(schema=StructType([StructField("id", LongType(), metadata=metadata)]))
        def output():
            return pipeline_spark.range(2).select(F.col("id").alias("id", metadata={"comment": "inferred"}))

    run(dry=True)
    assert not pipeline_spark.catalog.tableExists("output")


@pytest.mark.parametrize(
    "schema, expression",
    [
        ("value DECIMAL(8, 2)", lambda: F.lit(1).cast("decimal(4, 2)")),
        ("value ARRAY<DECIMAL(8, 2)>", lambda: F.array(F.lit(1).cast("decimal(4, 2)"))),
        ("value MAP<STRING, DECIMAL(8, 2)>", lambda: F.create_map(F.lit("key"), F.lit(1).cast("decimal(4, 2)"))),
        (
            "value STRUCT<amount: DECIMAL(8, 2), optional: STRING>",
            lambda: F.struct(F.lit(1).cast("decimal(4, 2)").alias("amount", metadata={"comment": "inferred"})),
        ),
        ("value INTERVAL DAY TO SECOND", lambda: F.expr("INTERVAL '1' HOUR")),
    ],
)
def test_compatible_nested_schema_merges(pipeline_spark, pipeline, schema, expression):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view(schema=schema)
        def output():
            return pipeline_spark.range(1).select(expression().alias("value"))

    run(dry=True)
    assert not pipeline_spark.catalog.tableExists("output")


@pytest.mark.parametrize(
    "schema, expression",
    [
        ("value DECIMAL(4, 2)", lambda: F.lit(1).cast("decimal(8, 2)")),
        ("value DECIMAL(8, 3)", lambda: F.lit(1).cast("decimal(4, 2)")),
        ("value STRING", lambda: F.lit(None)),
        ("value ARRAY<STRING>", lambda: F.array(F.lit(None))),
        ("value INTERVAL HOUR", lambda: F.expr("INTERVAL '1' DAY")),
    ],
)
def test_incompatible_nested_schema_merges(pipeline_spark, pipeline, schema, expression):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view(schema=schema)
        def output():
            return pipeline_spark.range(1).select(expression().alias("value"))

    with pytest.raises(AnalysisException, match="USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE"):
        run(dry=True)
    assert not pipeline_spark.catalog.tableExists("output")


@pytest.mark.parametrize(
    "schema, expression, data_type, value",
    [
        ("value DECIMAL(8, 2)", lambda: F.lit(1).cast("decimal(4, 2)"), DecimalType(8, 2), Decimal("1.00")),
        (
            "value ARRAY<DECIMAL(8, 2)>",
            lambda: F.array(F.lit(1).cast("decimal(4, 2)")),
            ArrayType(DecimalType(8, 2)),
            [Decimal("1.00")],
        ),
        (
            "value MAP<STRING, DECIMAL(8, 2)>",
            lambda: F.create_map(F.lit("key"), F.lit(1).cast("decimal(4, 2)")),
            MapType(StringType(), DecimalType(8, 2)),
            {"key": Decimal("1.00")},
        ),
        (
            "value STRUCT<second: BIGINT, amount: DECIMAL(8, 2)>",
            lambda: F.struct(F.lit(1).cast("decimal(4, 2)").alias("amount"), F.lit(7).cast("long").alias("second")),
            StructType([StructField("second", LongType()), StructField("amount", DecimalType(8, 2))]),
            Row(second=7, amount=Decimal("1.00")),
        ),
    ],
)
def test_declared_schema_publication(pipeline_spark, pipeline, schema, expression, data_type, value):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view(schema=schema)
        def output():
            return pipeline_spark.range(1).select(expression().alias("value"))

    run()
    result = pipeline_spark.table("output")
    assert result.schema["value"].dataType == data_type
    assert result.collect() == [Row(value=value)]


def test_declared_schema_publication_preserves_order_and_comments(pipeline_spark, pipeline):
    _, registry, run = pipeline
    schema = StructType(
        [
            StructField("second", LongType(), metadata={"comment": "two"}),
            StructField("first", LongType(), metadata={"comment": "one"}),
        ]
    )
    with graph_element_registration_context(registry):

        @dp.materialized_view(schema=schema)
        def output():
            return pipeline_spark.range(2).selectExpr("id AS first", "id + 10 AS second")

    run()
    result = pipeline_spark.table("output")
    assert result.schema == schema
    assert result.orderBy("first").collect() == [Row(second=10, first=0), Row(second=11, first=1)]


def test_inferred_publication_schema_is_nullable_recursively(pipeline_spark, pipeline):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):

        @dp.materialized_view
        def output():
            return pipeline_spark.range(1).select(F.struct("id").alias("value"))

    run()
    assert pipeline_spark.table("output").schema == StructType(
        [StructField("value", StructType([StructField("id", LongType())]))]
    )


@pytest.mark.parametrize(
    "schema, query",
    [
        ("id BIGINT, optional STRING", lambda spark: spark.range(2)),
        (
            "value STRUCT<amount: DECIMAL(8, 2), optional: STRING>",
            lambda spark: spark.range(1).select(
                F.struct(F.lit(1).cast("decimal(4, 2)").alias("amount")).alias("value")
            ),
        ),
    ],
)
def test_missing_declared_columns_fail_after_table_materialization(pipeline_spark, pipeline, schema, query):
    _, registry, run = pipeline
    with graph_element_registration_context(registry):
        dp.materialized_view(name="output", schema=schema)(lambda: query(pipeline_spark))

    with pytest.raises(AnalysisException):
        run()
    assert pipeline_spark.catalog.tableExists("output")
    assert pipeline_spark.table("output").count() == 0


def test_schema_refresh_updates_comments_without_reordering_existing_columns(pipeline_spark, pipeline_factory):
    for value, fields in [
        (1, [StructField("first", LongType(), metadata={"comment": "old"}), StructField("second", LongType())]),
        (2, [StructField("second", LongType(), metadata={"comment": "new"}), StructField("first", LongType())]),
    ]:
        _, registry, run = pipeline_factory()
        with graph_element_registration_context(registry):
            dp.materialized_view(name="output", schema=StructType(fields))(
                lambda value=value: pipeline_spark.range(1).select(
                    F.lit(value).cast("long").alias("first"), F.lit(10).cast("long").alias("second")
                )
            )
        run()

    result = pipeline_spark.table("output")
    assert result.schema.fieldNames() == ["first", "second"]
    assert all(field.dataType == LongType() and field.nullable for field in result.schema)
    # Match CatalogV2 comment semantics. Spark's session catalog exposes a
    # removed comment as JSON null rather than removing its metadata key.
    assert result.schema["first"].metadata.get("comment") is None
    assert result.schema["second"].metadata["comment"] == "new"
    assert result.collect() == [Row(first=2, second=10)]


@pytest.mark.parametrize("full_refresh", [False, True])
def test_materialized_view_refresh_replaces_schema_in_place(pipeline_spark, pipeline_factory, full_refresh):
    """MaterializeTablesSuite: complete tables use the new schema on each refresh.

    The upstream suite uses a V2 test catalog that supports column alterations;
    Spark's default V1 session catalog does not support all of these operations.
    """
    location = None
    for query, expected in [
        (lambda: pipeline_spark.range(2).selectExpr("id AS first"), [Row(first=0), Row(first=1)]),
        (lambda: pipeline_spark.range(3).selectExpr("id AS second"), [Row(second=0), Row(second=1), Row(second=2)]),
        (lambda: pipeline_spark.range(1).select(F.lit("one").alias("second")), [Row(second="one")]),
    ]:
        _, registry, run = pipeline_factory()
        with graph_element_registration_context(registry):
            dp.materialized_view(name="output")(query)
        run(full_refresh_all=full_refresh)
        result = pipeline_spark.table("output")
        assert result.orderBy(result.columns[0]).collect() == expected
        current_location = (
            pipeline_spark.sql("DESCRIBE EXTENDED output").where("col_name = 'Location'").first().data_type
        )
        if location is None:
            location = current_location
        assert current_location == location
