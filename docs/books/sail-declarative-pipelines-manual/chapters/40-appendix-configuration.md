# Appendix: configuration

## Pipeline settings

These are Spark SQL settings. They can be given for the whole project in the spec's `configuration`, for one dataset with `spark_conf` in Python, or for the flows after a `SET` in a SQL file.

| Key | Meaning | Default |
|---|---|---|
| `spark.remote` | Where the server is, for the `spark-pipelines` tool. | `sc://localhost:15002` |
| `spark.sql.pipelines.maxFlowRetryAttempts` | How many times a failed flow is retried. | `2` |

## Table properties

| Property | Meaning |
|---|---|
| `pipelines.reset.allowed` | `false` exempts a table from full refreshes: a full refresh of it is downgraded to an ordinary refresh. |
| `pipelines.autocdc.keyColumnNames`, `pipelines.autocdc.scdType` | Recorded on an AutoCDC target by its flow; see the AutoCDC chapter. |

## Spec fields

| Field | Meaning |
|---|---|
| `name` | The pipeline's name. Required. |
| `storage` | The storage root, an absolute URI with a scheme. Required. |
| `catalog`, `database` (`schema`) | Where unqualified names belong. |
| `configuration` | Spark SQL settings, as strings. |
| `libraries[].glob.include` | Source file patterns, relative to the spec. |

## Tool options

| Command and option | Meaning |
|---|---|
| `run --spec PATH` | The spec file; otherwise `spark-pipeline.yaml` or `.yml` in the current directory. |
| `run --refresh a,b` | Refresh only these datasets. |
| `run --full-refresh a,b` | Reset and recompute these datasets. |
| `run --full-refresh-all` | Reset and recompute everything. |
| `dry-run [--spec PATH]` | Analyze only. |
| `init --name NAME` | Create a sample project. |

## Server settings

Environment variables of the form `SAIL_<SECTION>__<KEY>`.

| Variable | Meaning | Default |
|---|---|---|
| `SAIL_CATALOG__LIST` | The catalogs. | one memory catalog named `sail` |
| `SAIL_CATALOG__DEFAULT_CATALOG` | The catalog sessions start in. | the first in the list |
| `SAIL_CATALOG__DEFAULT_DATABASE` | The database sessions start in. | `["default"]` |
| `SAIL_CATALOG__SHARED_MEMORY` | Share memory catalogs across sessions. | `false` |
| `SAIL_MODE` | `local`, `local-cluster` or `kubernetes-cluster`. | `local` |
| `SAIL_EXECUTION__DEFAULT_PARALLELISM` | Partitions per query. | the number of cores |
| `SAIL_RUNTIME__STACK_SIZE` | Runtime thread stack size in bytes. | the runtime's default |
| `SAIL_EXECUTION__FILE_LISTING_CACHE__TYPE` | An optional file-listing cache. | `none` |
| `SAIL_EXECUTION__FILE_LISTING_CACHE__TTL` | Its entries' lifetime in seconds. | `1800` |

## Source options

Delta source options (`ignoreDeletes`, `ignoreChanges`, `skipChangeCommits`, `startingVersion`, `startingTimestamp`, `readChangeFeed`, `excludeRegex`, `maxFilesPerTrigger`, `maxBytesPerTrigger`, `withEventTimeOrder`) are in the chapter on sources. Iggy source options (`url`, `stream`, `topic`, `timeout_ms`) are in the Iggy chapter. ORC write options (`compression`, `orc.compress`) are in the chapter on sources.
