# Projects and specs

A pipeline project is a directory with a spec file and the source files it names. The `spark-pipelines` tool reads the spec, registers every dataset and flow it finds in the sources, and starts a run on the server.

## The spec file

The spec is a YAML file. Named `spark-pipeline.yaml` (or `.yml`) in the current directory, the tool finds it on its own; under any other name, pass it with `--spec`:

```yaml
name: retail
catalog: sail
database: demo
storage: "file:///data/pipelines/retail"
configuration:
  "spark.remote": "sc://localhost:15002"
libraries:
  - glob:
      include: "transformations/**"
```

| Field | Required | Meaning |
|---|---|---|
| `name` | yes | The pipeline's name, used in messages. |
| `storage` | yes | Where the pipeline keeps its state: checkpoints and the files of memory sinks. An absolute URI with a scheme (`file://`, `s3://`, …). |
| `catalog` | no | The catalog that unqualified dataset names belong to. Defaults to the session's current catalog. |
| `database` (or `schema`) | no | The database that unqualified dataset names belong to. Defaults to the session's current database. |
| `configuration` | no | Spark SQL settings applied to the session, as strings. `spark.remote` here tells the tool where the server is. |
| `libraries` | no | Where the sources are. Each entry is a `glob` with an `include` pattern, relative to the spec file. |

A field outside this list is an error (`PIPELINE_SPEC_UNEXPECTED_FIELD`). `name` and `storage` are required.

The storage root must be absolute and carry a scheme. A bare path such as `/data/pipelines` is refused at the start of the run:

```text
[PIPELINE_STORAGE_ROOT_INVALID] Pipeline storage root must be an absolute path
with a URI scheme (e.g., file://, s3a://, hdfs://). Got: `/data/pipelines`.
```

## Source files

The `include` patterns select the source files. Two kinds are read:

- **Python files** (`*.py`) are executed with `pyspark.pipelines` imported as `dp` and a `spark` session available. The decorators and functions they call register datasets and flows. The chapter on Python datasets describes them.
- **SQL files** (`*.sql`) are parsed and registered statement by statement. The chapter on SQL pipeline files describes what they may contain.

A project may mix both. A dataset defined in SQL can be read from Python and the other way round, because registration happens before any query runs and names are resolved against the whole graph.

## Where the server is

The tool connects through Spark Connect. It finds the server in one of two ways:

- `spark.remote` in the spec's `configuration`, as above;
- the `SPARK_REMOTE` environment variable, for example `SPARK_REMOTE=sc://localhost:15002`.

The spec wins when both are set.

## Layout of the storage root

Under the storage root the pipeline writes:

```text
<storage>/_checkpoints/<catalog>/<database>/<table>/<flow>/<n>/state.json
<storage>/_checkpoints/<catalog>/<database>/<table>/<flow>/<n>/state/<batch>/...
<storage>/_memory_sinks/<flow>/...
```

- `_checkpoints` holds one directory per flow that reads streaming sources. Each full refresh opens a new numbered version `<n>`; ordinary runs continue the latest one. `state.json` records the files, Delta versions and offsets already read, the batch in progress if any, and for a windowed aggregate the watermark. The `state/<batch>` directories hold the rows of windows still open.
- `_memory_sinks` holds the rows written to a `memory` sink, which the session reads back through a temporary view named after the flow.

Nothing else is written there. The datasets themselves live in the catalog's warehouse.

## Names

A dataset's name may have one, two or three parts: `orders`, `demo.orders`, `sail.demo.orders`. One- and two-part names are completed with the spec's `catalog` and `database`. Names are case-insensitive: `Orders` and `orders` are the same dataset, and the catalog stores the lowercase form.

A flow has a one-part name. A dataset defined with a query gets a flow of the same name; an append flow is named after its function, or by `name=`.

Two datasets with the same name in one project is an error (`PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT`), as are two flows with the same name (`PIPELINE_DUPLICATE_IDENTIFIERS.FLOW`).
