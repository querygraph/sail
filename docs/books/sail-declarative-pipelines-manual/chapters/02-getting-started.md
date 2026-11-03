# Getting started

This chapter builds Sail, installs the client, and runs a first pipeline on one machine.

## Build Sail

Sail is built from its repository with a Rust toolchain:

```sh
git clone https://github.com/querygraph/sail
cd sail
git checkout sdp-batch-m1
cargo build --release -p sail-cli
```

The binary is `target/release/sail`.

## Install the client

The client is the unchanged Spark 4.2.1 Python package. No JVM is needed; it talks to Sail over Spark Connect.

```sh
python3 -m venv .venv
.venv/bin/pip install 'pyspark[connect]==4.2.1' pyyaml
```

This installs the `spark-pipelines` command into `.venv/bin`. `python -m pyspark.pipelines.cli` is the same program.

## Start a server

```sh
SAIL_CATALOG__SHARED_MEMORY=true target/release/sail spark server --port 15002
```

Port 15002 is where a Spark Connect client looks when it is told nothing else.

`SAIL_CATALOG__SHARED_MEMORY=true` makes every session on this server share one in-memory catalog. Without it each session has a catalog of its own, so the tables a `spark-pipelines run` creates are visible only to that run's session. The setting is off by default; the chapter on catalogs explains the choice, and how to use a Hive metastore instead.

## Create a project

```sh
.venv/bin/spark-pipelines init --name first
```

This creates:

```text
first/
  spark-pipeline.yml
  pipeline-storage/
  transformations/
    example_python_materialized_view.py
    example_sql_materialized_view.sql
```

The spec names the project and points `storage` at `pipeline-storage` as a `file://` URI. The two examples each define a materialized view.

## Run it

From the project directory, check the graph without writing anything:

```sh
cd first
../.venv/bin/spark-pipelines dry-run
```

Then run it:

```sh
../.venv/bin/spark-pipelines run
```

The tool prints the pipeline's events as they happen:

```text
Flow example_python_materialized_view is QUEUED.
Flow example_sql_materialized_view is QUEUED.
Flow example_python_materialized_view is PLANNING.
Flow example_python_materialized_view is STARTING.
Flow example_python_materialized_view is RUNNING.
Flow example_python_materialized_view has COMPLETED.
...
Run is COMPLETED.
```

A second `run` recomputes both views; the chapter on running covers the refresh options.

## Look at the result

From any session on the same server:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark.table("example_python_materialized_view").show()
```

The datasets are ordinary tables in the catalog's warehouse, by default the `spark-warehouse` directory under the server's working directory.

## Where the server is

The tool finds the server through `spark.remote` in the spec's `configuration`, or through the `SPARK_REMOTE` environment variable:

```sh
SPARK_REMOTE=sc://localhost:15002 ../.venv/bin/spark-pipelines run
```

## Next

- The demo in `examples/declarative-pipelines/` of the Sail repository is a complete project with streaming tables, file and Delta sources, ORC output, AutoCDC and Apache Iggy, with a driver that checks every result. The chapter on the demo walks through it.
- The chapter on projects and specs describes the spec file in full.
