"""AutoCDC: apply a change feed to a Delta table as SCD type 1.

Changes arrive as JSON files. Each change has a key (`id`), a sequence (`seq`) and an operation
(`op`): a later sequence wins, an earlier one is ignored, and `op = 'D'` deletes the row.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view
def customer_changes():
    return (
        spark.readStream.format("json")  # noqa: F821
        .schema("id INT, name STRING, country STRING, op STRING, seq INT")
        .load("{{CHANGES}}")
    )


dp.create_streaming_table("customers", format="delta", comment="Current customers, maintained by AutoCDC")

dp.create_auto_cdc_flow(
    target="customers",
    source="customer_changes",
    keys=["id"],
    sequence_by="seq",
    apply_as_deletes=F.expr("op = 'D'"),
    except_column_list=["op"],
)
