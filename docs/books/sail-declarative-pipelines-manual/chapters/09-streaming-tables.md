# Streaming tables

A streaming table is filled incrementally. Each run reads what its sources received since the last run, in one bounded batch, and appends the result. There is no long-running stream: a run is Spark's `availableNow` trigger, executed to completion, and the next run picks up where it left off.

## Sources

A flow of a streaming table reads one or more streaming sources:

| Source | How it is read | Progress kept |
|---|---|---|
| A table in the catalog, `spark.readStream.table("t")` or `STREAM t` | The table's files. A run reads the files that were not read before. | The set of files read. |
| A directory of files, `spark.readStream.format("json").schema(...).load(path)` | Files in the directory, in `json`, `csv`, `parquet` or `orc`. | The set of files read. |
| A Delta table, `spark.readStream.format("delta").load(path)` | The table's snapshot on the first run, then each new commit. | The Delta version reached. |
| An Apache Iggy topic, `spark.readStream.format("iggy")` | The topic's messages. | The offset per partition. |
| Another streaming table or a temporary view over a stream | As the dataset's own source. | Per flow. |

A file source needs a schema; a table source has one. A directory with no files on the first run is refused, since nothing says what its rows look like. Files whose names start with `.` or `_` are ignored, as Spark ignores them.

Reading a materialized view with `STREAM` is allowed and reads the files the view's last refresh wrote.

## What a run reads

A streaming table's flow is given a batch of what is new: the files not yet read, the Delta commits after the last version, the Iggy messages after the last offset. The batch is written to the table, and only then is the progress recorded. If a run is interrupted after the batch was recorded as pending but before it was written, the next run replays exactly that batch.

This gives **at-least-once** delivery: a batch is never lost, and a batch whose write finished but whose record did not can be written twice. For a plain file table that means duplicate rows after a crash at that moment; for a Delta target the second write is a second commit of the same rows.

Overwriting a source table does not re-read its old rows: the files are new, so they are read, and the files that disappeared are forgotten.

## Checkpoints

Progress lives under the pipeline's storage root, one directory per flow:

```text
<storage>/_checkpoints/<catalog>/<database>/<table>/<flow>/<version>/state.json
```

`version` is `0` for the first run and goes up by one at each full refresh. An ordinary run continues the latest version. `state.json` records the batches completed, what was read, the batch in progress if any, and the watermark of a windowed aggregate. Deleting the directory makes the next run start over, reading every source from its origin into a table that still holds the old rows; use a full refresh instead.

## What the query may do

The query of a streaming table transforms its stream row by row: projections, filters, column expressions, and joins with batch tables. Spark's rules for append-mode streaming apply, and some shapes are refused up front rather than run wrongly:

| Shape | Allowed |
|---|---|
| Join of a stream with a batch table (inner, left with the stream on the left, right with the stream on the right) | yes; the batch side is read at each run |
| Join of two streams | no |
| Full outer join involving a stream | no |
| `GROUP BY` over a stream without a watermark | no: `streaming aggregations without watermark` |
| `GROUP BY window(...)` with a watermark on the read | yes, see below |
| `DISTINCT` over a stream | no |
| Window functions, `ORDER BY`, `LIMIT` over a stream | no |
| `UNION` of a stream with a batch query | no, as in Spark |

The refusals are reported when the run starts, before anything is written.

## Windowed aggregates

A streaming table may aggregate over a tumbling window with a watermark:

```python
@dp.table
def counts():
    return (
        spark.readStream.table("events")
        .withWatermark("ts", "1 second")
        .groupBy(F.window("ts", "2 seconds"))
        .count()
    )
```

Rows are held until the watermark passes the end of their window, then aggregated once and appended. Between runs the open windows' rows are kept under the checkpoint. The rules are Spark's:

- The watermark is the latest event time seen minus the delay, in whole milliseconds. It starts at 0 and never moves back.
- A row is late, and dropped, only when its window has already ended by the watermark the batch started with. A row behind the watermark whose window is still open is counted.
- A window is written once, when it closes. A later run never updates it.

At milestone 1 the window must be tumbling (size and slide equal), with no start offset, and the watermark must be placed directly on the streaming read, with one watermark per query. Sliding windows, several watermarks, deduplication and stream-stream joins are supported for AutoCDC flows (see that chapter), not yet for append flows.

## Several flows into one table

A streaming table may be the target of several append flows. Each keeps its own checkpoint, and each run reads each flow's sources independently:

```sql
CREATE STREAMING TABLE all_activity;
CREATE FLOW from_orders AS INSERT INTO all_activity BY NAME SELECT ... FROM STREAM orders;
CREATE FLOW from_clicks AS INSERT INTO all_activity BY NAME SELECT ... FROM STREAM clicks;
```

## Schema changes

A streaming table's schema is the union of what its flows produce, merged with what the table already has. A new column in a flow's query is added to the table; later rows have it, earlier rows read it as null. A column that a flow stops producing stays in the table and is written as null. A column whose type changes incompatibly fails the run (`CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE`). A declared schema is exact: the query must produce it.
