# Differences from Spark

Sail Declarative Pipelines follows Spark 4.2.1. Every behavior in this manual was measured on Spark first, and where Sail differs it is on purpose. This chapter lists the differences a user can see.

## Runs that are stopped

| Spark | Sail |
|---|---|
| Interrupting a run cancels the operation; the flows that already started run to their end. | The flows in progress are stopped. |
| Releasing a session during a run lets its flows finish. | The flows are stopped. |

Sail stops the flows because otherwise nothing could stop a run, and a server cannot shut down with flows still writing. A stopped flow leaves its checkpoint at its last completed batch; the next run continues from there.

## Registration

| Spark | Sail |
|---|---|
| A SQL file whose statement fails keeps the definitions registered before it. | A failed SQL file registers nothing. |
| A `CREATE MATERIALIZED VIEW` or `CREATE STREAMING TABLE` run directly as a query writes the `INSERT` before rejecting the statement. | The statement is rejected before anything is written. |
| Table names with dots or hyphens, `` `a.b` `` or `` `my-table` ``, are refused by the session catalog. | They are accepted, as everywhere in Sail. |

In each case Spark's client aborts the run too; the difference is what is left behind on the server.

## Datasets

| Spark | Sail |
|---|---|
| A column added to a streaming table is placed where a Scala `Map` puts it, which varies past four columns. | Added columns follow the target table's order. |
| A `writeStream` to the `memory` sink outside a pipeline is supported. | It is not. Inside a pipeline, a `memory` sink works. |
| Flow failure events are emitted in the order of a Scala `Map`, which is unspecified. | They are emitted sorted by name, the downstream group before the direct one. |

## Errors

| Spark | Sail |
|---|---|
| `DATA_SOURCE_NOT_FOUND` for a missing format reaches the client wrapped in `TableMaterializationException` inside an `UnknownException`. | The same message, as the error itself. |
| The merge error for incompatible column types ends with its `SQLSTATE`. | The same text; the `SQLSTATE` suffix is rendered as Spark renders it. |

## ORC

Timestamps are written as ORC `TIMESTAMP_INSTANT`. Spark's stored type depends on the time zone of the JVM that writes, which a Sail worker has no equivalent of. One visible consequence: an instant within the one-second window before 1970-01-01 that ORC cannot represent fails the write on Sail, where Spark writes it and reads back a wrong value.

## AutoCDC targets

Spark 4.2.1 refuses a Delta table as an AutoCDC target (`AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE`), because its Delta library exposes no row-level operations. Sail accepts Delta targets; this is a deliberate extension, and the usual way to run AutoCDC on Sail.

Spark with Iceberg 1.13 refuses three AutoCDC cases that change nested types. Sail completes them.

## Sources

Apache Iggy is a source on Sail only; Spark has no Iggy source.

## What is the same

Everything else this manual describes: the registration protocol, the validation order and conditions, flow configuration fusion, refresh selection, retries, the events, the checkpoint rules of streaming tables, the Delta source's options and conditions, the SCD1 semantics of AutoCDC, and the watermark rules of windowed aggregates. The frozen test suite ported from Spark, 300 selected public tests and 67 upstream tests, runs on every change, and the pipelines test files are rerun on Spark itself: at milestone 1, only the differences above fail there.
