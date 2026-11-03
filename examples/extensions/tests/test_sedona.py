import math

from pyspark.sql import functions as F
from shapely import from_wkt


def test_geometry_metadata_survives_worker_expressions_and_shuffle(spark):
    source = spark.range(0, 17, numPartitions=4)
    geometry = source.select("id", F.call_function("st_point", F.col("id").cast("double"), F.lit(2.0)).alias("geom"))
    shuffled = geometry.repartition(4, "id")
    rows = shuffled.select("id", F.call_function("st_astext", "geom").alias("wkt"),
                           F.call_function("st_distance", "geom", F.call_function("st_point", F.lit(0.0), F.lit(2.0))).alias("distance")).orderBy("id").collect()
    assert len(rows) == 17
    for i, row in enumerate(rows):
        assert row.id == i
        assert from_wkt(row.wkt).equals(from_wkt(f"POINT({i} 2)"))
        assert row.distance == float(i)


def test_scalar_sql_uses_native_sedona(spark):
    row = spark.sql("""SELECT ST_AsText(ST_Point(1.0, 2.0)) AS text,
      ST_Distance(ST_Point(0.0, 0.0), ST_Point(3.0, 4.0)) AS distance,
      ST_Area(ST_GeomFromWKT('POLYGON ((0 0, 2 0, 2 3, 0 3, 0 0))')) AS area,
      ST_Intersects(ST_Point(1.0,1.0), ST_GeomFromWKT('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')) AS inside
    """).collect()[0]
    assert from_wkt(row.text).equals(from_wkt("POINT(1 2)"))
    assert row.distance == 5.0
    assert row.area == 6.0
    assert row.inside is True


def test_dataframe_function_names_and_nulls(spark):
    frame = spark.createDataFrame([(1, "POINT(0 0)"), (2, None)], "id long, wkt string")
    result = frame.select("id", F.call_function("st_astext", F.call_function("st_geomfromwkt", "wkt")).alias("text")).orderBy("id").collect()
    assert from_wkt(result[0].text).equals(from_wkt("POINT(0 0)"))
    assert result[1].text is None


def test_spatial_join_baseline_retains_duplicates_residuals(spark):
    spark.createDataFrame([(1, "POINT(0 0)"), (2, "POINT(2 2)"), (3, None)], "id long, wkt string").createOrReplaceTempView("points")
    spark.createDataFrame([(10, "POLYGON((-1 -1,1 -1,1 1,-1 1,-1 -1))"), (11, "POLYGON((-1 -1,1 -1,1 1,-1 1,-1 -1))"), (12, "POLYGON((1 1,3 1,3 3,1 3,1 1))")], "zone long, wkt string").createOrReplaceTempView("zones")
    query = """SELECT p.id, z.zone FROM points p JOIN zones z
      ON ST_Intersects(ST_GeomFromWKT(p.wkt), ST_GeomFromWKT(z.wkt))
      AND z.zone <> 11 ORDER BY p.id,z.zone"""
    assert [tuple(row) for row in spark.sql(query).collect()] == [(1, 10), (2, 12)]
    cross = """SELECT p.id,z.zone FROM points p CROSS JOIN zones z
      WHERE ST_Intersects(ST_GeomFromWKT(p.wkt), ST_GeomFromWKT(z.wkt)) ORDER BY p.id,z.zone"""
    assert [tuple(row) for row in spark.sql(cross).collect()] == [(1, 10), (1, 11), (2, 12)]


def test_unmodified_apache_sedona_connect_helpers(spark):
    from sedona.spark.sql.st_constructors import ST_Point
    from sedona.spark.sql.st_functions import ST_AsText
    from sedona.spark.sql.st_predicates import ST_Intersects

    rows = spark.range(1).select(ST_AsText(ST_Point(1.0, 2.0)).alias("wkt"),
                                ST_Intersects(ST_Point(0.0, 0.0), ST_Point(0.0, 0.0)).alias("hit")).collect()
    assert from_wkt(rows[0].wkt).equals(from_wkt("POINT(1 2)"))
    assert rows[0].hit
