# The demo

The Sail repository carries a demo at `examples/declarative-pipelines/`. It runs real pipeline projects through the unchanged `spark-pipelines` tool against a Sail server, then checks every result through a Spark Connect session of its own and prints PASS or FAIL for each check. Its exit status is 0 only when every check passed. It is the proof that the features in this manual work together, on one machine, after a checkout and a build.

## Running it

From the repository root, after `cargo build --release -p sail-cli` and a Python environment with `pyspark[connect]==4.2.1` and `pyyaml`:

```sh
PYTHON=.venv/bin/python examples/declarative-pipelines/run.sh
```

With an Apache Iggy broker, the Iggy checks run too:

```sh
IGGY_SERVER_BINARY=/path/to/iggy-server PYTHON=.venv/bin/python examples/declarative-pipelines/run.sh
```

Without a broker those checks are reported as SKIP, never as PASS. The run takes a few minutes. It works in a new temporary directory, deleted when every check passes; `--keep` or `--workdir DIR` keeps it, with the server and tool logs.

`run.sh` starts the server with `SAIL_CATALOG__SHARED_MEMORY=true`, so the tool's session and the checking session see one catalog. It also unblocks `SIGINT` before starting, so the server can be stopped at the end; a server started from a shell that blocks the signal cannot be.

## The projects

| Project | File | What it shows |
|---|---|---|
| `retail` | `orders.sql` | A streaming table over a table, a materialized view, a temporary view, a persisted view, comments. |
| | `sources.py` | An ORC materialized view, JSON-file and Delta streaming sources, two append flows into one streaming table. |
| | `cdc.py` | AutoCDC from a JSON change feed into a Delta table, with deletes and sequencing. |
| `broken` | `broken.sql` | A graph with an unresolved column. |
| `iggy` | `iggy.py` | A streaming table and an AutoCDC target reading one Iggy topic. |

The driver, `demo.py`, writes each project's spec with the server's address and fills in the `{{...}}` placeholders in the Python files with paths in its work directory. The project files are otherwise ordinary SDP sources.

## The steps

1. **Setup.** Two source tables (`demo.orders_raw`, `demo.customers_raw`), a directory of JSON click files, a directory of JSON change files, and a Delta table.
2. **Dry run.** The graph is analyzed and no dataset is created.
3. **First run.** Every dataset is created and checked: the streaming table read its source; the materialized view aggregates it; the temporary view feeds a count but is not published; the persisted view is queryable; comments are published; the ORC view is stored as `.orc` files; the JSON and Delta sources were read; the two append flows filled one table; AutoCDC applied the change feed, with the later sequence winning.
4. **Second run.** A new order, a new click file, a new Delta commit and new changes arrive. Each streaming table appended only what was new; the materialized view was recomputed; AutoCDC applied a delete and an insert and ignored an out-of-order update.
5. **Third run.** With no new data, every streaming table and the AutoCDC target are unchanged.
6. **Selective refresh.** `run --refresh daily_revenue` recomputes that view from the current `orders` and leaves `orders` alone; a normal run then picks up the waiting order.
7. **Full refresh.** After the source table is overwritten, `run --full-refresh-all` rebuilds every streaming table from its sources and replays the change feed to the same AutoCDC state.
8. **Broken graph.** The run fails with `UNRESOLVED_COLUMN`, and the valid dataset in the same graph is not created either.
9. **Iggy.** Three runs over one topic: the first reads it; the second reads only the new messages, applying a delete and an insert and ignoring a stale update; the third reads nothing.
10. **Shutdown.** The server stops cleanly on `SIGINT`.

At the milestone the demo reports 48 checks passed, 0 failed, 0 skipped with a broker.

## What it found

The demo's first run failed every AutoCDC step: Sail's default warehouse is the relative path `spark-warehouse`, and the AutoCDC commit path could not turn a relative location into a URL. No test had seen it, because every test fixture sets an absolute warehouse. The fix resolves the location the way Delta's own table source does. It is a good example of what the demo is for: it runs the configuration a user would, not the one the tests do.
