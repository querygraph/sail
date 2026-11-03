"""GeoArrow types survive ordinary expressions and worker serialization."""
import pytest
from pyspark.sql import functions as F
from shapely import from_wkt


@pytest.mark.parametrize("expression,expected", [
    ("ST_GeomFromWKB(ST_AsBinary(ST_Point(CAST(id AS DOUBLE),2.0)))", "point"),
    ("ST_GeogFromWKB(ST_AsBinary(ST_Point(CAST(id AS DOUBLE),2.0)))", "point"),
    ("CASE WHEN id % 2 = 0 THEN ST_Point(CAST(id AS DOUBLE),2.0) END", "nullable"),
    ("CASE WHEN id % 2 = 0 THEN ST_Point(CAST(id AS DOUBLE),2.0) ELSE ST_Point(CAST(id AS DOUBLE),9.0) END", "alternate"),
    ("coalesce(CASE WHEN id % 2 = 0 THEN ST_Point(CAST(id AS DOUBLE),2.0) END, ST_Point(CAST(id AS DOUBLE),9.0))", "alternate"),
    ("coalesce(ST_Point(CAST(id AS DOUBLE),2.0), NULL)", "point"),
    ("coalesce(NULL, ST_Point(CAST(id AS DOUBLE),2.0))", "point"),
    ("element_at(array(ST_Point(CAST(id AS DOUBLE),2.0)),1)", "point"),
    ("element_at(array(NULL,ST_Point(CAST(id AS DOUBLE),2.0)),2)", "point"),
    ("try_element_at(array(ST_Point(CAST(id AS DOUBLE),2.0)),2)", "null"),
], ids=["builtin-wkb", "builtin-geography", "case-null", "case-arms", "coalesce", "coalesce-null-last", "coalesce-null-first", "array", "array-null", "array-missing"])
def test_geometry_expression_and_shuffle(spark, expression, expected):
    source = spark.range(0, 9, numPartitions=4)
    # Exercise nested physical expressions as well as materialized Arrow fields
    # transferred through repartitioning. id keeps every input non-constant.
    nested = source.select("id", F.expr(f"ST_AsText({expression})").alias("wkt"))
    geometry = source.select("id", F.expr(expression).alias("geom")).repartition(4, "id")
    shuffled = geometry.select("id", F.expr("ST_AsText(geom)").alias("wkt"))
    for frame in (nested, shuffled):
        rows = frame.orderBy("id").collect()
        assert len(rows) == 9
        for i, row in enumerate(rows):
            assert row.id == i
            if expected == "null" or (expected == "nullable" and i % 2):
                assert row.wkt is None
            else:
                y = 9 if expected == "alternate" and i % 2 else 2
                assert from_wkt(row.wkt).equals(from_wkt(f"POINT({i} {y})"))


@pytest.mark.parametrize("expression", [
    "CASE WHEN id=0 THEN ST_Point(CAST(id AS DOUBLE),2.0) ELSE ST_AsBinary(ST_Point(CAST(id AS DOUBLE),2.0)) END",
    "coalesce(ST_Point(CAST(id AS DOUBLE),2.0), ST_AsBinary(ST_Point(CAST(id AS DOUBLE),2.0)))",
    "element_at(array(ST_Point(CAST(id AS DOUBLE),2.0), ST_AsBinary(ST_Point(CAST(id AS DOUBLE),2.0))),1)",
    "CASE WHEN id=0 THEN ST_GeomFromWKB(ST_AsBinary(ST_Point(CAST(id AS DOUBLE),2.0))) ELSE ST_GeogFromWKB(ST_AsBinary(ST_Point(CAST(id AS DOUBLE),2.0))) END",
], ids=["case-binary", "coalesce-binary", "array-binary", "geometry-geography"])
def test_mixed_storage_or_geometry_types_are_not_silently_relabelled(spark, expression):
    with pytest.raises(Exception, match="st_astext.*binary|No kernel matching"):
        spark.range(3).select(F.expr(f"ST_AsText({expression})")).collect()


def test_array_column_indexing_preserves_geometry_across_projections(spark):
    arrays = spark.range(0, 7, numPartitions=4).select(
        "id", F.array(F.expr("ST_Point(CAST(id AS DOUBLE),2.0)")).alias("points"))
    selected = arrays.select("id", F.col("points")[0].alias("geom")).repartition(4, "id")
    rows = selected.select("id", F.expr("ST_AsText(geom)").alias("wkt")).orderBy("id").collect()
    assert len(rows) == 7
    assert all(from_wkt(row.wkt).equals(from_wkt(f"POINT({i} 2)"))
               for i, row in enumerate(rows))


@pytest.mark.parametrize("expression", [
    "element_at(array(NULL,ST_Point(1.0,2.0)),2)",
    "coalesce(ST_Point(1.0,2.0), NULL)",
    "coalesce(NULL, ST_Point(1.0,2.0))",
], ids=["array", "coalesce-null-last", "coalesce-null-first"])
def test_literal_geometry_survives_constant_folding_and_broadcast(spark, expression):
    # Literal inputs allow planning to fold the array to ScalarValue::List or
    # simplify coalesce before the selected geometry is repeated over many rows.
    source = spark.range(0, 9, numPartitions=4)
    nested = source.select(F.expr(f"ST_AsText({expression})").alias("wkt"))
    shuffled = source.select(F.expr(expression).alias("geom")).repartition(4).select(
        F.expr("ST_AsText(geom)").alias("wkt"))
    for frame in (nested, shuffled):
        rows = frame.collect()
        assert len(rows) == 9
        assert all(from_wkt(row.wkt).equals(from_wkt("POINT(1 2)")) for row in rows)


def test_ordinary_struct_property_metadata_uses_builtin_worker_registry(spark):
    from pyspark.sql.types import StringType, StructField, StructType
    schema = StructType([StructField("value", StringType(), True,
                                    {"description": "ordinary property metadata"})])
    nodes = spark.createDataFrame([("a",), ("b",), (None,)], schema).select(F.struct("value").alias("node"))
    values = nodes.repartition(4).select(F.col("node.value").alias("value")).collect()
    assert sorted(row.value for row in values if row.value is not None) == ["a", "b"]
    assert sum(row.value is None for row in values) == 1
