# Operations

This chapter covers running the server that pipelines run on: its settings, execution modes, sessions, and shutdown.

## Starting a server

```sh
sail spark server --port 15002
```

Settings are environment variables of the form `SAIL_<SECTION>__<KEY>`, or the matching keys in Sail's configuration file. The ones a pipeline deployment meets:

| Setting | Meaning |
|---|---|
| `SAIL_CATALOG__LIST`, `SAIL_CATALOG__DEFAULT_CATALOG`, `SAIL_CATALOG__DEFAULT_DATABASE`, `SAIL_CATALOG__SHARED_MEMORY` | The catalogs; see the catalogs chapter. |
| `SAIL_MODE` | `local` (the default): queries run in the server process. `local-cluster`: the server starts worker processes on the same machine and runs queries across them. `kubernetes-cluster`: workers are Kubernetes pods. |
| `SAIL_EXECUTION__DEFAULT_PARALLELISM` | How many partitions a query runs with. |
| `SAIL_RUNTIME__STACK_SIZE` | The stack size of the runtime's threads, in bytes. Deep plans, such as a wide AutoCDC merge, need `16777216`. |
| `SAIL_IGGY_SERVER_BINARY` | Only for Sail's own test suite: where to find an Iggy broker to start. |

The warehouse, where the memory catalog keeps table data, is `spark-warehouse` under the server's working directory, or the `spark.sql.warehouse.dir` of the session.

## Cluster mode

In `local-cluster` and `kubernetes-cluster` modes a pipeline runs the same way: the server is the driver, analysis and the catalog stay on it, and the queries of each flow run on the workers. Every behavior in this manual was run in `local-cluster` mode as well as `local`.

Two things differ for the operator:

- A streaming table's checkpoint is written by the driver; the workers never touch it.
- When a session that used checkpoints is released, the driver asks the workers to stop. If it cannot confirm that every worker writing on the session's behalf has stopped, it does not delete the session's checkpoint files, and the release reports that failure. The files are left for the next run, and the server exits 1 at shutdown. Never deleting what a live writer may still be using is the rule; the message says which prefix was kept.

A worker that is told to stop finishes within 5 seconds, and a driver waits at most 30 seconds for its workers. These bounds are backstops; a stop that hits them is logged.

## Sessions

Each `spark-pipelines` invocation, and each `SparkSession.builder.remote(...)` in Python, is a Spark Connect session. Sessions are independent: a graph registered in one is not visible from another, and a run belongs to the session that started it.

Closing a session (`spark.stop()`, or the tool exiting) stops its run, as the chapter on running describes, and cleans up what the session owned: its checkpoint writers, its memory sinks' files, its streams. A later request on a released session is refused:

```text
[INVALID_HANDLE.SESSION_CLOSED] The handle <id> is invalid. Session was closed. SQLSTATE: HY000
```

Releasing a session twice, or releasing one the server never saw, succeeds.

## Shutdown

`SIGINT` (Ctrl-C) stops the server. It closes every session, waits for their cleanups, and stops the drivers and workers. The exit status is 0 when every cleanup succeeded. A cleanup that failed, for example because a checkpoint prefix could not be confirmed deletable, makes the exit status 1, with the failure in the log; nothing is deleted on a guess.

A job failure that was already reported to its query is not a shutdown failure: it is logged, not repeated. A commit whose outcome is unknown is reported, since only the operator can decide what to do with it.

A server started from a shell that blocks `SIGINT` inherits the block and cannot be stopped with it; send `SIGTERM`, or start it from a shell that does not block the signal.

## Logs

The server logs to standard error. A pipeline run logs its flows' events at `INFO`, and the events are also sent to the tool, which prints them. Session creation and removal are logged by the session manager; a canceled job in cluster mode is logged by the driver with its job id.

## Storage

The pipeline's storage root (`storage` in the spec) and the warehouse may be local directories or object stores (`s3://` and the others Sail's object store support covers). Checkpoint writes are single atomic puts, which is what makes a crash mid-run recoverable on both. Remote warehouses are tested against an S3-compatible store; AutoCDC's own state directory is described in its chapter.
