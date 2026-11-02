# Sail Declarative Pipelines demo

This demo proves, on one machine, what Sail Declarative Pipelines supports. It runs real
pipeline projects through the unchanged Spark 4.2.1 `spark-pipelines` CLI against a Sail
server built from this branch. After each step it checks the results through a Spark Connect
session and prints PASS or FAIL.

## Requirements

- A Rust toolchain that builds Sail (see the repository's contributor guide).
- Python 3.10 or later with `pyspark[connect]==4.2.1` and `pyyaml`. No JVM is needed: the CLI talks to Sail over Spark Connect.
- Optional: an Apache Iggy broker, `iggy-server`, to include the Iggy streaming checks.

## Build and run

From the repository root:

```sh
cargo build --release -p sail-cli
python3 -m venv .venv
.venv/bin/pip install 'pyspark[connect]==4.2.1' pyyaml
PYTHON=.venv/bin/python examples/declarative-pipelines/run.sh
```

`run.sh` uses `target/release/sail`, or `target/debug/sail` when no release build exists. Set
`SAIL_BINARY` to use another binary. The run takes a few minutes. It works in a new temporary
directory, which it deletes when every check passes; pass `--keep` or `--workdir DIR` to keep it.

### Including Apache Iggy

Sail reads Iggy through the Iggy SDK at revision `153fdf466aea94b7bcb90681ee7274f9f76ec00d`.
Build the broker at the same revision:

```sh
git clone https://github.com/apache/iggy && cd iggy
git checkout 153fdf466aea94b7bcb90681ee7274f9f76ec00d
cargo build --release --bin iggy-server
```

Then name it when running the demo:

```sh
IGGY_SERVER_BINARY=/path/to/iggy/target/release/iggy-server \
  PYTHON=.venv/bin/python examples/declarative-pipelines/run.sh
```

The demo starts and stops the broker itself. Without a broker, the Iggy checks are reported as
SKIP, never as PASS.

## What it runs

The server is started with `SAIL_CATALOG__SHARED_MEMORY=true`, so the CLI's session and the
demo's checking session see one catalog, as Spark sessions do.

| Project | Files | What it shows |
|---|---|---|
| `projects/retail` | `orders.sql` | A streaming table over a table, a materialized view, a temporary view, a persisted view, comments. |
| | `sources.py` | An ORC materialized view, JSON-file and Delta streaming sources, two append flows into one streaming table. |
| | `cdc.py` | AutoCDC (SCD type 1) from a JSON change feed into a Delta table, with deletes and sequencing. |
| `projects/broken` | `broken.sql` | A graph with an unresolved column: the run fails before anything is written. |
| `projects/iggy` | `iggy.py` | A streaming table and an AutoCDC target that read an Iggy topic. |

The steps, in order:

1. **Dry run.** The graph is analyzed and nothing is created.
2. **First run.** Every dataset is created and checked.
3. **Second run.** New rows, files, a Delta commit and changes arrive. Streaming tables read only what is new. AutoCDC applies a delete and an insert, and ignores an out-of-order update.
4. **Third run.** With no new data, nothing changes.
5. **Selective refresh.** `run --refresh daily_revenue` runs only that dataset.
6. **Full refresh.** `run --full-refresh-all` rebuilds streaming tables from their sources and replays the change feed to the same AutoCDC state.
7. **Broken graph.** The run fails with `UNRESOLVED_COLUMN`, and the valid dataset in the same graph is not created either.
8. **Iggy.** Three runs over one topic: first read, only new messages, nothing twice.
9. **Shutdown.** The server stops cleanly on SIGINT.

The demo prints the final datasets, then a summary. The exit status is 0 only when every check
passed. Server and CLI logs stay in the work directory when a check fails.

## Reading the projects

The project files are ordinary SDP sources and can be run on their own with
`spark-pipelines run --spec pipeline.yaml`, given a spec like the one `demo.py` writes. The
`{{...}}` placeholders in the Python files are paths that the driver fills in.
