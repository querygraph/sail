# Running

`spark-pipelines` has three commands.

| Command | What it does |
|---|---|
| `spark-pipelines dry-run` | Registers the project and analyzes the whole graph. Writes nothing. |
| `spark-pipelines run` | Registers, analyzes, then fills every dataset. |
| `spark-pipelines init --name NAME` | Creates a sample project. |

`run` and `dry-run` take `--spec PATH`; without it they look for `spark-pipeline.yaml` or `spark-pipeline.yml` in the current directory.

## What a run does

1. **Registration.** Every source file is read and its datasets and flows are registered. A SQL file that fails registers nothing.
2. **Analysis.** Every flow's query is resolved against the graph and the catalog, in dependency order, and the datasets' schemas are determined. A dry run stops here.
3. **Materialization.** Tables that do not exist are created; tables whose schema changed are altered; tables being fully refreshed are truncated. Persisted views are published.
4. **Execution.** Flows run in dependency order. A materialized view is recomputed; a streaming table's flows read what is new and append it; an AutoCDC flow applies its changes.

Nothing is written before analysis succeeds. If a query cannot be resolved, the run fails with the first problem, and the datasets that depend on it are reported too:

```text
Failed to resolve flows in the pipeline.
...
Flow 'broken' has FAILED. Error: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column ...
```

## Events

The tool prints the server's events as the run proceeds. For each flow:

```text
Flow orders is QUEUED.
Flow orders is STARTING.
Flow orders is RUNNING.
Flow orders has COMPLETED.
```

A materialized view's flow also reports `PLANNING` before `STARTING`. A flow left out of a selective refresh reports `Flow 'x' is EXCLUDED.` A flow whose upstream failed reports `Flow 'x' SKIPPED due to upstream failure(s).` The run ends with `Run is COMPLETED.`, or with the error that stopped it. A dry run reports only its end.

## Refresh options

By default every selected dataset is refreshed: a materialized view is recomputed, a streaming table reads what is new.

| Option | Effect |
|---|---|
| `--refresh a,b` | Refresh only the named datasets. The others are excluded. |
| `--full-refresh a,b` | Reset the named datasets and recompute them from scratch: a streaming table is truncated, opens a new checkpoint, and reads its sources from the beginning. |
| `--full-refresh-all` | Reset and recompute every dataset. |

`--refresh` and `--full-refresh` may be combined for different datasets. Naming a dataset in both is an error, and so is naming any dataset together with `--full-refresh-all`:

```text
Cannot specify a subset to refresh when full refresh all is set to true
```

A name may be bare or qualified; `orders` and `demo.orders` are the same dataset.

A selective refresh runs only the selected flows. A selected dataset still reads the current contents of what it depends on; an unselected upstream table is not recomputed first. A persisted view is published whenever its definition is part of the project, selected or not.

### Full refresh of a streaming table

A full refresh of a streaming table starts a new checkpoint version. Its flows read every source from the origin, which for a file source means every file in the directory, for a Delta source the table's current snapshot, for an Iggy topic its retained messages. If a source no longer holds its origin, the refresh fails rather than rebuilding a table from a partial history.

A table can opt out of full refreshes with the property `pipelines.reset.allowed` set to `false`. A full refresh of such a table is downgraded to an ordinary refresh, and the table keeps its data.

A full refresh of an AutoCDC target drops its auxiliary state and replays the change feed; the AutoCDC chapter describes it.

### Changed streaming sources

A streaming table remembers which streaming sources its flow read. If the flow's sources change between runs (a source added or removed), the run refuses to continue the old checkpoint:

```text
Flow 'st' had streaming sources added or removed. It will not be retried.
Please perform a full refresh to rebuild it against the current sources.
```

A full refresh of that table accepts the new source set.

## Retries

A flow that fails is retried, up to `spark.sql.pipelines.maxFlowRetryAttempts` times (default 2, so three attempts). A materialized view is truncated before a retry so a failed attempt leaves no partial result behind. After the last attempt the flow is reported as failed:

```text
Flow 'st' has FAILED more than 2 times and will not be restarted.
Error: ...
```

A flow whose source set changed is not retried.

The setting can be given per dataset with `spark_conf` in Python, with `SET` in a SQL file, or for the whole project in the spec's `configuration`.

## Failures and the rest of the graph

When a flow fails for good, the datasets that read from it are skipped, and the rest of the graph runs. The run's result is the first failure. A table whose flow failed may hold a partial result, as in Spark; the next run recomputes a materialized view, and a streaming table replays the batch that did not finish.

## Interrupting a run

Stopping the tool (Ctrl-C) or closing its session interrupts the run on the server: the flows in progress are stopped, and their checkpoints are left at the last completed batch. The next run continues from there. This differs from Spark, which lets the flows that already started finish; the chapter on differences explains why.

## Running the same graph twice

A run is tied to the graph the tool registered. Each `spark-pipelines run` registers the project afresh, so repeated runs are independent. Starting a second run on a graph whose run is still in progress is refused (`EXECUTION_STATE_EXECUTE_HOLDER_ALREADY_EXISTS_GRAPH`).
