# Sources and formats

A pipeline reads from tables, files, Delta tables and Apache Iggy topics, and writes tables in the formats Sail writes. This chapter covers the batch and streaming sources other than Iggy, which has a chapter of its own.

## Tables

Any table the catalog knows is a source. Read it as a batch source with `spark.read.table("t")` or by name in SQL, and as a streaming source with `spark.readStream.table("t")` or `STREAM t`. A streaming read of a table reads its files: the first run reads every file, later runs the files added since.

The datasets of the pipeline are tables too, once the run has filled them. Reading a materialized view with `STREAM` works and reads the files its last refresh wrote.

## Files

A directory of files is read directly:

```sql
CREATE MATERIALIZED VIEW events AS SELECT * FROM json.`/data/events`;
CREATE MATERIALIZED VIEW counts AS SELECT * FROM parquet.`/data/counts`;
```

```python
@dp.table
def clicks():
    return spark.readStream.format("json").schema("user INT, page STRING").load("/data/clicks")
```

The formats are `parquet`, `orc`, `json` and `csv`. A streaming file read needs a schema. Files whose names begin with `.` or `_` are skipped. An empty location, as in ``csv.`` `` with nothing between the backticks, is refused: "The location name cannot be empty string".

## Delta tables

A Delta table is read incrementally with the `delta` format, by path or, when the table is in the catalog, by name:

```python
@dp.table
def events():
    return spark.readStream.format("delta").load("/data/events")


@dp.table
def events_by_name():
    return spark.readStream.table("delta_events")
```

The first run reads the table's current snapshot. Each later run reads the commits made since, in order. The checkpoint records the Delta version reached. The behavior follows Spark's Delta source, and every rule below was measured on Spark 4.2.1 with Delta 4.4.1:

- A commit that only adds files adds its rows.
- A commit that deletes or updates rows stops the stream with an error, because an append-only consumer cannot apply it: `DELTA_SOURCE_IGNORE_DELETE` for a delete, `DELTA_SOURCE_TABLE_IGNORE_CHANGES` for an update. The options below say what to do instead.
- A commit with no data change (a metadata change, an optimize) adds nothing.
- Deletion vectors are applied: a row deleted by a deletion vector is not read from the snapshot.
- A table that was dropped and recreated at the same path is a different table, and the stream stops rather than mixing the two. A full refresh starts from the new table.

### Options

| Option | Effect |
|---|---|
| `ignoreDeletes` | A commit that only deletes rows is skipped. An update still stops the stream. |
| `ignoreChanges` | A commit that updates rows is read as the files it rewrote, so updated rows appear again (and so do the unchanged rows of those files). |
| `skipChangeCommits` | A commit that updates or deletes rows is skipped entirely. |
| `startingVersion` | On the first run, start after this version instead of from the snapshot; `latest` starts from the end, recording nothing until a later run has something to read. Ignored once the stream has a checkpoint. |
| `startingTimestamp` | Like `startingVersion`, by the first commit at or after the timestamp. A timestamp after the last commit is `DELTA_TIMESTAMP_GREATER_THAN_COMMIT`. |
| `readChangeFeed` | Read the change data feed instead of the table: each row carries `_change_type`, `_commit_version` and `_commit_timestamp`. The table must have the feed enabled for the versions read. |
| `excludeRegex` | Files whose paths match the regular expression are not read. |
| `maxFilesPerTrigger`, `maxBytesPerTrigger`, `withEventTimeOrder` | Accepted and validated as Spark does. A run reads to the end of the log in one batch, so they do not split it. |

An invalid option fails the stream with Spark's condition: a version that does not exist is `DELTA_VERSION_NOT_FOUND`; a negative or non-numeric version, or a zero or non-numeric `maxFilesPerTrigger`, is `DELTA_ILLEGAL_OPTION`; both `startingVersion` and `startingTimestamp` is `DELTA_STARTING_VERSION_AND_TIMESTAMP_BOTH_SET`; a timestamp that does not parse is `DELTA_TIMESTAMP_INVALID`. Time travel (`versionAsOf`, `timestampAsOf`) is refused in a streaming read.

### Schema

A Delta source's partition columns are columns of the stream. A table with column mapping is read by physical column names, including nested ones. A column added to the source widens the streaming table that reads it. A column renamed or dropped under column mapping stops the stream: the schema it was reading no longer exists.

### Delta as a target

A dataset is written as a Delta table with `format="delta"` in Python or `USING delta` in a table definition. A streaming table with a Delta target gets one Delta commit per batch, which is what makes a replayed batch after a crash a second commit of the same rows rather than duplicate files.

## ORC

ORC is read like any file format, and written by giving a dataset `format="orc"`:

```python
@dp.materialized_view(format="orc", table_properties={"compression": "zstd"})
def customers_orc():
    return spark.read.table("demo.customers_raw")
```

The compression codec is taken from `compression`, then `orc.compress`, then the session's default, and is `zstd` when none is given. Sail writes one ORC file per output partition with a single footer.

Timestamps are written as ORC `TIMESTAMP_INSTANT`, which does not depend on the writer's time zone (Spark's choice depends on its JVM's zone). One consequence is deliberate: an instant inside ORC's one-second window before 1970-01-01 fails the write with an error, where Spark writes it and reads back a wrong value.

ORC reads refuse the Hive and forced-positional column-evolution modes (`orc.force.positional.evolution`), and need an object store that reports an ETag or version for each file, which local files and S3 do.

## Parquet, CSV and JSON as targets

Without `format`, a dataset is a Parquet table. `format="csv"` and `format="json"` write those formats. For a file table, partition columns are stored last, in the Spark directory layout.
