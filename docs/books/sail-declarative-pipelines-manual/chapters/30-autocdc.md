# AutoCDC

AutoCDC applies a feed of changes to a table. Each change row carries the key of the row it changes, a sequencing value that orders changes to the same key, and, if the feed has deletes, a marker that says the row is gone. Sail keeps the latest row per key, which Spark calls SCD type 1.

This chapter covers what you write, what you see in the target, how runs and refreshes behave, which queries may stand between the source and the target, and what an operator needs to know about the state Sail keeps.

## Defining a flow

An AutoCDC flow is defined in Python with `create_auto_cdc_flow`. Spark 4.2.1 has no SQL form for it, and neither has Sail: `CREATE FLOW ... AS INSERT INTO` is an append flow, not AutoCDC.

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as sf

@dp.temporary_view
def changes():
    return (
        spark.readStream.format("json")
        .schema("id INT, name STRING, seq BIGINT, is_delete BOOLEAN")
        .load("/data/landing")
    )

dp.create_streaming_table("customers", format="delta")

dp.create_auto_cdc_flow(
    target="customers",
    source="changes",
    keys=["id"],
    sequence_by="seq",
    apply_as_deletes=sf.expr("is_delete"),
    except_column_list=["is_delete"],
)
```

The arguments:

| Argument | Meaning |
| :-- | :-- |
| `target` | A streaming table declared with `create_streaming_table`. Give it a format: `delta` or `iceberg`. |
| `source` | A streaming view, or a table read as a stream, that yields the change rows. |
| `keys` | The columns that identify a row. Several keys are allowed. |
| `sequence_by` | The column that orders changes to one key. A later value wins. |
| `apply_as_deletes` | An expression; a change row for which it is true deletes the key. Optional. |
| `column_list` | Keep only these columns in the target. Optional. |
| `except_column_list` | Keep every column but these; usually the delete marker. Optional. |
| `stored_as_scd_type` | Only `1`. SCD type 2 is refused with `AUTOCDC_SCD2_NOT_SUPPORTED`. |
| `name` | The flow's name; the target's name when omitted. |

The key and sequence columns must exist in the source, and a column cannot be both excluded and a key. These are checked when the pipeline is defined, with the same conditions Spark raises.

## What the target holds

After a run the target holds one row per key: the row with the highest sequence value seen, with the columns the flow keeps. One more column, `__spark_autocdc_metadata`, is a struct of `deleteSequence` and `upsertSequence`; Spark adds it too.

```text
+---+-----+---+--------------------------+
|id |name |seq|__spark_autocdc_metadata  |
+---+-----+---+--------------------------+
|1  |bob  |3  |{NULL, 3}                 |
+---+-----+---+--------------------------+
```

Ordering follows the sequence value, not arrival:

- A change whose sequence is lower than the one already applied to its key changes nothing, in this run or a later one.
- Two changes to one key with the same sequence value are a tie. Spark leaves the result undefined; Sail keeps the last one it read.
- A delete removes the row. A later upsert with a higher sequence brings the key back; an upsert with a lower sequence than the delete is ignored, because the delete is remembered.

Deletes are remembered in an auxiliary table beside the target, named `__spark_autocdc_aux_state_<target>`. You can read it; do not write it. Dropping it forgets the deletes it held, so an older upsert for a deleted key would then land; Spark behaves the same way after the table is dropped.

## Runs

A run reads what the source holds when the run starts and applies it in batches of at most 10,000 rows per source partition. The run ends when it has applied everything up to that point; new data that arrived during the run waits for the next run. This is the behaviour of Spark's `AvailableNow` trigger.

Progress lives under the pipeline's storage root:

```text
<storage>/_checkpoints/<target>/<flow>/<n>/
```

`n` counts full refreshes. A later run continues from the recorded progress, so a file read in one run is never read again, and a source file deleted after it was read is not missed unless you ask for a full refresh.

### Full refresh

`spark-pipelines run --full-refresh customers` (or `--full-refresh-all`) rebuilds the target from the source's beginning:

1. The target's rows are removed.
2. The auxiliary table is dropped and created empty, so remembered deletes are forgotten, as Spark does.
3. Any operator state (see below) starts empty.
4. Progress moves to a new checkpoint directory, `<n+1>`.

A source that no longer holds its beginning refuses a full refresh before anything is removed, for instance a Delta source whose first versions were vacuumed, or an Iggy topic whose retention moved on.

A target with the Spark table property `pipelines.reset.allowed` set to `false` is not rebuilt; the run becomes an ordinary one, as in Spark.

### Selective refresh

`--refresh customers` runs only the flows into that target; other tables keep their contents.

## Queries between the source and the target

The flow's source can be more than a read: a view that filters, projects, joins, aggregates or deduplicates streams. Spark runs these as a stream with a state store; Sail keeps the equivalent state between batches and runs, under the checkpoint directory. These forms are supported:

| Query | Behaviour |
| :-- | :-- |
| Filters, projections, added or renamed columns | Stateless. |
| `withWatermark(...).groupBy(window(...), ...)` | A window is emitted once, when the watermark passes its end. Tumbling and sliding windows, with or without a start time. One stream or several. |
| `dropDuplicates()` and `dropDuplicates([cols])` | A key seen in any earlier batch or run is dropped. |
| Stream joined with a table | Stateless; the table is read at each batch. |
| Two streams joined (inner) | Both sides are kept. A pair is emitted once, when its later row arrives. |

Spark's watermark rule is followed exactly: the watermark is the latest event time seen, in whole milliseconds, less the delay, and it never moves back. A row is late only when its window has already ended by the watermark; a row behind the watermark whose window is still open still counts. With several watermarked streams the query's watermark is the smallest, and a stream that has produced nothing yet counts as zero. After a restart each stream's watermark starts again, as in Spark, so a window closes only when every stream has moved in the new run.

These are refused, with Spark's error where Spark has one:

- An aggregate over a stream without a watermark: `STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION`.
- `dropDuplicatesWithinWatermark`.
- A left join whose right side is a stream, a right join whose left side is a stream, and a full join over any stream. Window functions, sorts and limits over a stream.
- A union of a stream with a table.

A changed query over existing state is refused: if the columns or types that a deduplication or window keeps are not the ones in the state, the run stops with a message that names the flow and asks for a full refresh.

## Targets

| Target | How Sail writes it |
| :-- | :-- |
| Delta, by path or in the memory, Hive or second catalog | Fenced: every write goes through the control store described below. The table refuses `ALTER TABLE` and any write that does not come from the pipeline. |
| Delta managed by Unity Catalog | Plain: Unity orders the commits. See the catalogs chapter. |
| Iceberg | Plain: Iceberg orders the commits. New columns arrive by `ALTER TABLE ... ADD COLUMNS`, which Sail issues; a full refresh drops and recreates the table. |

A target created before the first run is adopted on that run. A target you drop and create again while enrolled is refused by the next ordinary run, with a message that says to run a full refresh; the full refresh adopts the new table.

### Schema evolution

A new source column is added to the target before the next batch. Nested fields are added inside their struct, and for arrays of structs inside the element. A column whose type changed is refused with Spark's `DATATYPE_MISMATCH`; a full refresh, which recreates the target, takes the new type.

## What an operator sees

### The control store

For a fenced Delta target, Sail records every batch before its first effect and every write's receipt in a small database:

```text
<state root>/_control/authority.db
```

The state root is the warehouse directory. For a remote warehouse, such as `s3://bucket/warehouse`, it is a local directory instead, named by `SAIL_AUTOCDC_STATE_DIR` or, when that is unset, `~/.sail/autocdc/<sha256 of the warehouse URL>`. One machine owns a remote warehouse's flows. The warehouse carries a marker file, `_autocdc_state_owner`, naming that state directory by an id the directory keeps. A machine that finds the marker but not the directory, or another directory, stops with a message that says what to restore, so a flow never starts over against tables that have history. Removing the marker and running a full refresh is the way to adopt the warehouse on a new machine.

### Leases and recovery

A run holds a lease on each target for as long as it runs, renewed every five seconds, with a fifteen-second term. If the process dies, the next run waits for the lease to lapse, at most about seventeen seconds, then resumes the unfinished batch from the recorded state and repeats only the writes that have no receipt. Nothing is written twice. A run that fails for an ordinary reason releases the lease at once, so the next run, often the full refresh the error asked for, does not wait.

Two sessions cannot run the same flow at once: the second waits on the lease, then recovers whatever the first left unfinished, and the first can no longer write.

Process death is tested, including kills at every stage of a batch. Loss of power, and restoring the state directory from a backup, are not qualified.

### Enrolled tables

An enrolled Delta table carries the property `sail.autocdc.authority`. Any Sail session, on any server, refuses to write such a table unless the write comes from the pipeline that enrolled it:

```text
table file:///.../customers/ is enrolled with AutoCDC authority ... and refuses a write without its fence
```

`ALTER TABLE` is refused the same way, whether the table is named through a catalog or by its path (`ALTER TABLE delta.`/path/to/customers` ...`). To change the table yourself, drop it and run a full refresh, which recreates it; a table created again at the same location is not the enrolled one, and nothing refuses writes to it until the refresh adopts it.

## Limits in this release

- SCD type 2 is refused.
- A join of two streams keeps every row of both sides for good; Spark evicts join state only with watermarks on both sides, which Sail does not do yet.
- `dropDuplicates` on some columns keeps an arbitrary row among the duplicates within one batch, as Spark does within one microbatch.
- Iceberg and Unity targets are not fenced. Two pipelines writing one such target at once each keep their own progress; Spark has the same exposure.
- On an Iceberg target, Spark's own connector refuses three nested-type changes during a full refresh or evolution that Sail completes.
