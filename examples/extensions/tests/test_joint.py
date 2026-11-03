"""One Spark session crosses both separately built native library boundaries."""
from pyspark.sql import functions as F
from sail_nutmeg import Nutmeg


def test_spatial_neighbors_feed_graph_analytics(spark):
    places = spark.createDataFrame([("a", 0.0, 0.0), ("b", 1.0, 0.0), ("c", 2.0, 0.0), ("isolate", 10.0, 0.0)], "id string,x double,y double")
    places.createOrReplaceTempView("places")
    # Actual Apache SedonaDB distance kernels run inside Sail. Only scalar
    # results and graph columns are collected, never a geometry client UDT.
    edges = spark.sql("""SELECT a.id AS source, b.id AS target FROM places a CROSS JOIN places b
      WHERE a.id <> b.id AND ST_Distance(ST_Point(a.x,a.y),ST_Point(b.x,b.y)) <= 1.01""")
    nodes = places.select(F.col("id").alias("node_id"))
    nm = Nutmeg(spark)
    receipt = nm.stage("spatial", nodes, edges)
    assert receipt.nodeCount == 4 and receipt.edgeCount == 4
    components = nm.run("spatial", "wcc").select("nodeId", "componentId").orderBy("nodeId").collect()
    groups = {row.nodeId: row.componentId for row in components}
    assert groups["a"] == groups["b"] == groups["c"]
    assert groups["isolate"] != groups["a"]
    assert nm.run("spatial", "degree").join(places, F.col("nodeId") == places.id).count() == 4
