# Python datasets

A Python source file defines datasets with the decorators and functions of `pyspark.pipelines`, imported as `dp`. The file runs once, at registration, with a `spark` session in scope. Each decorated function is a **query function**: it returns a DataFrame, and the DataFrame's plan is what the dataset is filled from. The function body runs at registration; the query runs later, during the run.

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(comment="Customers, written as ORC", format="orc")
def customers_orc():
    return spark.read.table("demo.customers_raw")


@dp.table(comment="Click events from JSON files landing in a directory")
def clicks():
    return spark.readStream.format("json").schema("user INT, page STRING").load("/data/clicks")


@dp.temporary_view
def large_orders():
    return spark.read.table("orders").where("amount >= 100")
```

## Decorators

| Decorator | Defines |
|---|---|
| `@dp.materialized_view` | A materialized view, recomputed by every run. Its query reads batch sources. |
| `@dp.table` | A streaming table when the query reads a stream (`spark.readStream`), otherwise a materialized view. |
| `@dp.temporary_view` | A private view other datasets read; never published. |
| `@dp.append_flow(target=...)` | A flow that appends its query's rows to a streaming table or a sink. |

Each decorator can be used bare (`@dp.table`) or with options (`@dp.table(name="st", ...)`). Without `name=`, the function's name is the dataset's name.

`@dp.materialized_view`, `@dp.table` and `dp.create_streaming_table` accept:

| Option | Meaning |
|---|---|
| `name` | The dataset's name, one to three parts. |
| `comment` | Published as the table's comment. |
| `spark_conf` | Spark SQL settings for this dataset's flow, as a dict of strings. |
| `table_properties` | Properties stored on the table. |
| `partition_cols` | Columns to partition the table by. |
| `cluster_by` | Clustering columns, recorded on the table. Not together with `partition_cols`. |
| `schema` | A declared schema, as a DDL string (`"id INT, name STRING"`) or a `StructType`. The query must match it. |
| `format` | The table's format: `parquet` (the default), `delta`, `orc`, and the other formats Sail writes. |

`@dp.temporary_view` accepts `name`, `comment` and `spark_conf`. `@dp.append_flow` accepts `target`, `name` and `spark_conf`.

## Streaming tables and append flows

A streaming table with one query is a `@dp.table` whose function reads a stream:

```python
@dp.table(format="delta")
def delta_events():
    return spark.readStream.format("delta").load("/data/events")
```

A streaming table filled by several flows is created without a query and then targeted:

```python
dp.create_streaming_table("all_activity", comment="Orders and clicks, one row per event")


@dp.append_flow(target="all_activity")
def activity_from_orders():
    return spark.readStream.table("orders").select(
        F.col("customer_id").alias("user"), F.lit("order").alias("kind")
    )


@dp.append_flow(target="all_activity")
def activity_from_clicks():
    return spark.readStream.table("clicks").select("user", F.lit("click").alias("kind"))
```

Each flow matches the target's columns by name. A flow is named after its function, or by `name=`; two flows may not share a name.

The `once` option of `append_flow` is not supported, in Sail as in Spark 4.2.1 (`DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED`).

## Reading other datasets

Inside a query function, other datasets of the pipeline are read by name, like tables:

- `spark.read.table("orders")` reads a dataset as a batch source.
- `spark.readStream.table("orders")` reads it incrementally, which makes the reading dataset a streaming table.
- `spark.readStream.format("json").schema(...).load(path)` reads files arriving in a directory; `format("delta").load(path)` reads a Delta table incrementally; `format("iggy")` reads an Apache Iggy topic. The chapters on sources and on Iggy have the options.

Names resolve against the pipeline first, then the catalog. A name the pipeline does not define is a catalog table.

## Sinks

A sink is an external destination for streaming flows:

```python
dp.create_sink("events_out", format="parquet", options={"path": "/data/out/events"})


@dp.append_flow(target="events_out")
def to_events_out():
    return spark.readStream.table("all_activity")
```

`format` and `options` are the sink's format and its options; a file format needs a `path`. Two sinks need no path: `memory`, whose rows the session can read back through a temporary view named after the flow, and `console`, whose rows are read and discarded.

## What a query function may do

A query function describes a query; it must not run one. Inside it, `spark.sql()` is allowed, and any DataFrame method that builds a plan. A method that needs the server to analyze or execute the plan, such as `.schema`, `.columns`, `.collect()`, `.count()` or `.show()`, is refused:

```text
[ATTEMPT_ANALYSIS_IN_PIPELINE_QUERY_FUNCTION] ...
```

Likewise `schema=` must be a DDL string or a `StructType` built in Python, not a schema obtained by analyzing another DataFrame.

## Per-flow settings

`spark_conf` applies to that dataset's flow. A flow that reads a temporary view inherits the view's `spark_conf` too; if the two give one key different values, the run fails with `DUPLICATE_FLOW_SQL_CONF`. Tables contribute nothing to what reads them. The setting most often used this way is the retry count:

```python
@dp.table(spark_conf={"spark.sql.pipelines.maxFlowRetryAttempts": "0"})
def st():
    return spark.readStream.table("src")
```

## Mixing with SQL

A dataset defined in a SQL file is read from Python by its name, and the other way round. Registration happens for every file before the run, so order between files does not matter.
