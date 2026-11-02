"""Streaming from an Apache Iggy topic: a raw streaming table and an AutoCDC target.

The demo driver replaces `{{IGGY_URL}}` with the URL of the broker it started.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


def topic():
    return (
        spark.readStream.format("iggy")  # noqa: F821
        .option("url", "{{IGGY_URL}}")
        .option("stream", "demo")
        .option("topic", "customers")
        .option("timeout_ms", "5000")
        .schema("id BIGINT, name STRING, op STRING, seq BIGINT")
        .load()
    )


@dp.table(comment="Every message read from the Iggy topic")
def iggy_raw():
    return topic()


@dp.temporary_view
def iggy_changes():
    return topic()


dp.create_streaming_table("iggy_customers", format="delta", comment="Customers maintained from Iggy")

dp.create_auto_cdc_flow(
    target="iggy_customers",
    source="iggy_changes",
    keys=["id"],
    sequence_by="seq",
    apply_as_deletes=F.expr("op = 'D'"),
    except_column_list=["op"],
)
