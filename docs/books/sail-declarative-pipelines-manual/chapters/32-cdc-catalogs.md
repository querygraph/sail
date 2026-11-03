# AutoCDC targets in Unity Catalog and Iceberg

AutoCDC writes Delta tables by path and in Sail's own catalogs under a fence, as the AutoCDC chapter describes. Two other targets take a plain path, in which the catalog or the table format orders the commits and Sail keeps its progress in the checkpoint, as Spark does.

## Delta tables managed by Unity Catalog

With a Unity Catalog configured as a Sail catalog, a streaming table named in that catalog is a managed Delta table whose commits Unity ratifies.

```python
dp.create_streaming_table("unity.default.customers", format="delta")

dp.create_auto_cdc_flow(
    target="unity.default.customers",
    source="changes",
    keys=["id"],
    sequence_by="seq",
    apply_as_deletes=sf.expr("is_delete"),
    except_column_list=["is_delete"],
)
```

What differs from a fenced Delta target:

- Unity orders the commits, so there is no lease and no control store for the target. The checkpoint is the progress.
- A full refresh drops the table and creates it again, because Unity does not replace a table in place; the new table has a new id in Unity.
- `DROP TABLE ... PURGE` is not accepted by Unity; Sail drops without it.
- Two pipelines running one flow into one table at once each keep their own progress. Nothing stops the second; the data converges because rows win by sequence, but remembered deletes may interleave.

The auxiliary table lives in the same schema as the target, as `__spark_autocdc_aux_state_<table>`.

The catalogs chapter describes how a Unity Catalog is attached to a Sail server. Sail's own test suite runs against Unity Catalog built from source; `PYSAIL_INTEGRATION_SUITE=unity` selects the suite and `PYSAIL_UNITY_CATALOG_URL` points it at a running server.

## Iceberg

A streaming table with `format="iceberg"` is an Iceberg table.

```python
dp.create_streaming_table("events_current", format="iceberg")
dp.create_auto_cdc_flow(target="events_current", source="changes", keys=["id"], sequence_by="seq")
```

- Iceberg orders the commits; the checkpoint is the progress, and a batch replayed after a crash is safe because rows win by sequence.
- A new source column arrives by `ALTER TABLE ... ADD COLUMNS`, which Sail issues before the batch. Nested fields are added inside their struct or array element.
- A full refresh drops and recreates the table, so a changed key or type takes effect then.
- The table carries no fence. An ordinary write from another session is not refused.

Sail completes three nested-type changes that Spark's Iceberg connector refuses: a full refresh that changes keys and types, a selective full refresh, and a new field inside an array of structs. On Spark those runs fail with `Cannot update '<column>', not a primitive type`.
