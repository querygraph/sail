"""Datasets defined in Python: file and Delta streaming sources, ORC output and append flows.

The demo driver replaces the `{{...}}` placeholders with paths in its work directory.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(format="orc", comment="Customers, written as ORC")
def customers_orc():
    return spark.read.table("demo.customers_raw")  # noqa: F821


@dp.table(comment="Click events from JSON files landing in a directory")
def clicks():
    return spark.readStream.format("json").schema("user INT, page STRING").load("{{CLICKS}}")  # noqa: F821


@dp.table(format="delta", comment="Rows read incrementally from a Delta table")
def delta_events():
    return spark.readStream.format("delta").load("{{DELTA_SOURCE}}")  # noqa: F821


# One streaming table fed by two append flows.
dp.create_streaming_table("all_activity", comment="Orders and clicks, one row per event")


@dp.append_flow(target="all_activity")
def activity_from_orders():
    return spark.readStream.table("orders").select(  # noqa: F821
        F.col("customer_id").alias("user"), F.lit("order").alias("kind")
    )


@dp.append_flow(target="all_activity")
def activity_from_clicks():
    return spark.readStream.table("clicks").select("user", F.lit("click").alias("kind"))  # noqa: F821
