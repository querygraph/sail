# Nutmeg

Graph analytics inside Sail: Grust's graph kernels as a Spark data source
and as SQL table functions, so a Spark Connect client projects a graph from
any DataFrame Sail can read, runs algorithms in the engine's process, and
gets DataFrames back — no separate graph database, no copy into a billed
instance. Grust's `GRUST-SAIL.md` has the design and every Sail-side change
it needs; the Neo4j comparison it answers is worked through end to end in
[`examples/citibike`](examples/citibike/README.md).

## Which of the two shapes you want

Grust reaches Sail two ways. They share no code, they are deployed
differently, and choosing between them is the first decision, not a detail.
Grust's `GRUST-SAIL.md` calls them Path 2 and Path 1.

**Nutmeg — the embedded shape, this repository.** `nutmeg-server` *is* the
Sail Spark Connect server, with Nutmeg's data source and table functions
registered in every session. A staged graph is Arrow memory in that process;
a kernel reads it in place. Nothing crosses a process boundary: no edge list
goes over a wire, no result comes back over one.

**`grust-sail` — the client shape, in Grust.** A Spark Connect client that
links no Sail crate at all. It speaks the same gRPC protocol PySpark speaks,
to a **stock Sail server of any topology**, and needs no Sail change, no
codec and no extension point. It keeps a graph in two ordinary Delta tables
(`grust_nodes`, `grust_edges`), writes through staged Arrow IPC views and
`MERGE INTO`, and reads results back as Arrow IPC.

Choose like this:

| | embed Nutmeg | use the `grust-sail` client |
|---|---|---|
| deployment | one machine, Sail in `local` mode | any Sail server, including one you do not control |
| where a kernel runs | in the Sail process, on the staged Arrow | in **your** process, on a graph read out of Sail |
| what crosses the wire | the result rows only | the graph (or the rows a pushed-down SQL query returns), then the result |
| Sail change needed | one session-factory hook (upstream, #2630) | none |
| cluster modes | **does not work** (see below) | not blocked by anything in Nutmeg's way |
| graph survives a restart | no — staged graphs are process memory | yes — they are Delta tables |

**One machine and speed → embed Nutmeg.** The edge list never moves and the
kernel reads the staged Arrow directly, but you are bounded by that one
machine's memory, and every staged graph is gone when the server restarts.

**An existing Sail cluster → the client.** You pay a round trip: to run a
kernel you pull the edge list (or a filtered projection of it) out of the
server into your own process, and the result goes back the same way, in
Spark Connect responses — `grust-sail` sets its gRPC client's decoding limit
to 16 MiB of Arrow payload plus 1 MiB of envelope
(`MAX_ARROW_IPC_PAYLOAD_BYTES`), so a graph arrives as many responses rather
than one. In exchange the server never has to know what a graph
kernel is, so nothing about its topology matters.

Two things about the client shape stated exactly, because it is easy to
promise more than it does:

- **`grust-sail` runs no graph kernel of its own, and pushes none into
  Sail.** What it pushes down as SQL is degree aggregates, triplet joins,
  traversals lowered from Grust's traversal IR, and the pushable subset of
  read-only Cypher; variable-length paths are explicitly *not* pushed
  (`SparkDialect::recursive_cte_supported` is `false` for Sail 0.7.1) and
  fall back to reading the graph out. To run PageRank, Leiden or anything
  else in `grust-algorithms` over Sail-held data, you read the graph into
  your process (`read_graph`, `load_graph_arrow_ipc`, `query_arrow_ipc`) and
  run the kernel there yourself. There is no algorithm API on `grust-sail`
  today.
- **"Works against a cluster" is an argument, not a measurement.**
  `grust-sail` holds a client to one endpoint URL and sends ordinary Spark
  SQL and `LocalRelation` temp views; it contains no Sail-internal plan node,
  so none of the reasons Nutmeg fails in a cluster mode apply to it. But
  every configured `grust-sail` test and benchmark in Grust runs against a
  single-node `sail spark server`; there is no `local-cluster` or
  `kubernetes-cluster` run of it recorded. Treat cluster support as
  unobstructed rather than as verified.

The rest of this page is about the embedded shape.

## Built on Grust's abstractions

Nutmeg adds a way in and a way out, not a second graph library.

- **The algorithm catalog is Grust's procedure registry.** Names,
  arguments, option names, defaults and declared outputs come from
  `grust-algorithm-procedures`, the registry behind
  `CALL grust.algorithms.pagerank(...)`; calls are validated by Grust's
  validator. An algorithm added to Grust appears in Nutmeg by name, and a
  test fails until it is served.
- **The graph layout is grust-arrow's**, and projections are built by
  Grust's `GraphProjection::from_arrow_batches` with Grust's projection
  options (`orientation`, `nodeLabels`, `relationshipTypes`,
  `weightProperty`, `defaultWeight`). Other tables are renamed into that
  layout when staged.
- **The stored graph is grust-sail's.** A Grust graph that `SailGraphStore`
  keeps in Sail's catalog (`grust_nodes`, `grust_edges`) is staged into
  Nutmeg with one call, in the server, and never leaves it:
  `Nutmeg.graph.project_grust(name)` selects those two tables into the
  projection's shape and stages the rows. `grust-sail` itself offers no
  algorithm API — it reads a graph out (`read_graph`, `load_graph_arrow_ipc`,
  `query_arrow_ipc`) for a caller to run kernels on in its own process. That
  is the other shape, above.

## Layout

- `crates/nutmeg-graph` — Sail-agnostic core over DataFusion 55: a named
  registry of staged graphs and every algorithm Grust registers as a
  DataFusion table function (`nutmeg_pagerank('g', '{"damping": 0.85}')`,
  …). Result schemas are observed from the kernels, not transcribed.
- `crates/nutmeg-sail` — the `nutmeg` Spark data source
  (`spark.read.format("nutmeg").option("graph", "g").option("algorithm", "pagerank")`
  to run an algorithm; `df.write.format("nutmeg").option("graph", "g")
  .option("part", "edges")` to stage a projection's rows) and a session
  mutator that registers the source and the table functions in a Sail
  session.
- `crates/nutmeg-server` — the Sail Spark Connect server with Nutmeg
  registered. Depends on one small hook in Sail (see below).
- `python/nutmeg` — a PySpark client shaped like the `graphdatascience`
  verbs: `project`, `page_rank.stream`, `wcc.stream`, …

## Column names

Results carry the column names Grust's registry declares, the ones
`CALL grust.algorithms.<name>(...) YIELD ...` uses. A few differ from Neo4j
GDS's for the same quantity (Yen's `pathIndex`, GDS `index`; `iterations`
and `converged`, GDS `ranIterations` and `didConverge`; ...). Opting in per
read with `columnNames` = `gds` (the `nutmeg` read option, a key in a table
function's JSON configuration, or `Nutmeg(spark, column_names="gds")` for a
whole client) renames those columns, from the cited table
`nutmeg_graph::GDS_COLUMN_ALIASES`. Only names change: where GDS's row shape
differs from Grust's, it still does.

## Score precision

`pagerank` and `articleRank` take Grust 0.23's `precision` option, `f64` (the
default) or `f32`, and the `score` column's Arrow type follows it: Spark
`double` at `f64`, `float` at `f32`.

```python
nm.pagerank.stream(g, precision="f32")            # score: float
nm.article_rank.stream(g, precision="f64")        # score: double (the default)
```

```sql
SELECT score FROM nutmeg_pagerank('g', '{"precision": "f32"}')
```

`precision` is Grust's own option, not one of Nutmeg's: it goes through the
same registry validation as `damping`, and a value that is not `f64` or
`f32` is refused at planning with a message naming the option, exactly as a
bad `orientation` or a misspelled `dampng` is. What Nutmeg owes it is the
schema. A read's schema is found by running the kernel once on a three-node
probe graph, and that observation is now cached per value of `precision`
rather than per algorithm, so the type a read *reports* is the type its
batches carry. `f32` halves the score vector and its scratch; the scores
agree with the `f64` ones to about f32's own resolution, not bit for bit.

## Row order

Grust's kernels are deterministic for a given input, but row order is part
of the input. Projection rows follow the order nodes are staged in, or, when
only edges are staged, the order ids first appear among the edges. Adjacency
follows edge order. Leiden and Louvain visit nodes in that order, and label
propagation breaks ties by it. The order also decides which of several
equal-cost paths is reported, how components and colours are labelled, and
the order of floating-point sums. A DataFrame's row order is not fixed, and
Sail's scan order changes from run to run. In the Citi Bike example, the
same query gave Leiden modularity 0.4398 in one run and 0.4389 in another.

So staging keeps each graph in a **canonical order** by default: the write
option `order` = `canonical`, or `project(..., order="canonical")` in the
Python client. After every write, the whole part is sorted, including rows
kept from earlier `append` writes, so neither the order of the rows nor the
order of the appends changes a result:

- nodes by `node_id`. Grust refuses duplicate ids, so this is a total order
  on every graph that can run. Nodes derived from edges alone are put in id
  order as well.
- edges by `source`, `target`, `edge_id` (nulls last), `label`, then every
  other staged column by name. Two edges that tie on all of these are equal
  in every column a kernel reads, so their relative order cannot change any
  result. Parallel edges without an `edge_id` are still ordered, for
  example by their weights.

Ids are Utf8 once staged, so the order is text order: `"10"` sorts before
`"9"`. Determinism only needs a total order, and no kernel reads the row
order as a numeric one. It does mean the canonical order is not the order an
`ORDER BY` on integer ids gives.

`order` = `asStaged` keeps rows in arrival order with no sort. It is for
callers who already guarantee an order or who want to skip the cost. An
`asStaged` append leaves the part unsorted until the next canonical write,
which sorts the whole part again. If appends of differently shaped
DataFrames disagree on a column's type (for example `w` as an integer in one
and a double in the next), there is no single sorted form, and a canonical
write is refused with a message that names the column. Cast the column, or
stage with `asStaged`.

**Cost.** Each canonical write does an in-memory sort of every staged row of
that part on the server, not only the new rows, while holding the graph's
write lock. Rows are compared in Arrow's row format. Time is O(n log n) in
the part's rows, so staging in k appends costs k such sorts. Peak memory is
about twice the part, plus the sort keys (the key columns encoded) and 16
bytes of permutation per row. For edges the keys are the ids, the type and
the numeric properties. For nodes they are only the ids. That peak is
admitted from the memory budget before the sort starts (see below), so a
write whose rows fit but whose sort does not is refused under `canonical`
and accepted under `asStaged`. Stage a large graph in one write rather than
many appends, or use `asStaged` if the order is already fixed.

## Memory

One budget bounds everything Nutmeg holds in the Sail process: the staged
rows of every graph, each write's transient copies and its sort, every
cached projection, and the kernels running on them. It is set in bytes by
the `NUTMEG_MEMORY_BYTES` environment variable when the server starts, and
defaults to 8 GiB (`8589934592`). For example:

```sh
NUTMEG_MEMORY_BYTES=34359738368 nutmeg-server   # 32 GiB
```

The budget is fixed once the first graph is staged. Before this change the
variable limited each projection separately and staging was not counted at
all. It now limits the process total.

The budget is Grust's own admission: one `ExecutionContext` whose
reservations are taken before memory is allocated and returned when the
memory is freed. A write is admitted a batch at a time as Sail streams it:

- Each incoming batch is refused before it is copied if its rows cannot fit
  in what is free. What the renamed batch keeps alive is then admitted.
- A canonical write admits the sort's permutation, the larger of its keys and
  its sorted copy, before sorting.
- The new part is built aside and swapped in. A refused write leaves the
  graph exactly as it was: the same rows, revision, cached projections and
  bytes. A refused write to a new graph leaves no graph behind.
- The error is `ResourcesExhausted`. It names the graph, the part, what the
  write needed, what is in use and the limit.

Bytes are measured as the Arrow allocations the rows keep alive, each counted
once at its capacity. A batch sliced from a larger one is charged for the
whole buffer when it is staged `asStaged`. A canonical write copies it
compactly. Its sorted copy is admitted at an upper bound before it is made
and shrunk to its real size once it is, so a sorted part is charged exactly
what it keeps alive; the peak keeps the bound. Replacing a part, restaging
(which evicts the graph's cached projections) and `Registry::drop` return
their bytes. A read still running
on an evicted projection holds its bytes until it finishes.

To see what is using memory, `nutmeg_graphs()` and
`spark.read.format("nutmeg").load()` report `stagedBytes` per graph.
`SELECT * FROM nutmeg_memory()` reports:

- `limitBytes`
- `usedBytes`: staged rows, writes in progress, projections and kernels
- `stagedBytes`: the total over graphs
- `peakBytes`

### Reads

Every read (a `spark.read.format("nutmeg")` scan, a `nutmeg_<algorithm>`
table function, or `nutmeg_graph::Query` from Rust) runs on its own Grust
child of the budget's execution:

- **Memory** is shared. Every byte a read takes counts against the child
  and against `NUTMEG_MEMORY_BYTES` in one admission, so any number of
  concurrent reads cannot together exceed the budget. A read that does not
  fit is refused with `ResourcesExhausted`; the others go on.
- **What outlives a read stays on the budget.** Staged rows, their sort, and
  cached projections are built on the budget's execution and owned by it.
  So is the transpose an in-arc kernel (PageRank, SCC, label propagation,
  Yen's, spectral) builds the first time it needs one. It is cached with the
  projection. What only the read uses is charged to the read and returned
  when it finishes: nodes derived from edges, node properties, kernel
  scratch, result batches.
- **Stopping is per read.** Each read has its own cancellation, work counter,
  work budget and deadline. The kernel runs through a view of the cached
  projection (`GraphProjection::with_execution`) on the read's child. When a
  read is cancelled or hits a limit, it fails and nothing else is affected:
  not the budget, not the cached projection, not any other read on it.
  Cancelling the budget's execution would stop every read.

A read has no limits of its own by default. It runs until it finishes,
fails, or is cancelled, bounded only by the shared budget. A read opts in
with read options (or keys in a table function's JSON configuration). Nutmeg
takes them out before Grust validates the rest, as it does `columnNames`:

| option | meaning |
|---|---|
| `timeoutMs` | deadline, in milliseconds from when the read starts running |
| `workLimit` | Grust work units the read may charge |
| `memoryLimitBytes` | a memory ceiling of the read's own, within the budget |
| `concurrency` | threads the read's kernel may use |

`concurrency` is the read's, not the graph's: the kernel runs on the read's
own child execution, so one read asking for eight threads gives none to
another read sharing the same cached projection. Without it the kernel runs
the code that predates threads, single-threaded, which is what a server with
its own scheduler should get unless it asks otherwise.

There is no default deadline or work limit. Grust's work units are not
comparable across kernels, and wall time depends on load. A default would
fail large, legitimate reads on a busy machine and not on an idle one, and
Spark sets no query timeout by default either.

**Cancellation.** A read's kernel runs when its query executes, not while
it is planned. The scan plans a `NutmegAlgorithmExec`, one partition, and
nothing runs until its stream is first polled. Then the read's child
execution is created (so `timeoutMs` counts from there) and the kernel starts
on a thread of its own, not on the async runtime's. It sends each batch
through a channel of two batches, and waits when the channel is full, so a
slow consumer holds a read to a few batches: all-pairs shortest paths, whose
cursor computes each batch when it is pulled, then holds its workspace and
those batches rather than its whole result. Dropping the stream before its
end cancels the read, and the kernel stops at its next check with
`cancelled`, or at its next send, which finds the channel closed. Sail's
interrupt (`spark.interruptAll()`, `interruptTag`, `interruptOperation`)
drops the stream, so it stops the kernel. A `LIMIT` that has its rows ends
the stream and stops the read the same way. `Query::cancel` stops a read
from Rust. A projection being built when a read is cancelled is finished and
cached, as it belongs to the budget.

`EXPLAIN` plans without executing, so it runs no kernel. The schema a read
reports is still found once per algorithm, by running it on a three-node
probe graph at planning.

`SELECT * FROM nutmeg_reads()` (or `Nutmeg.reads()` from Python) lists the
reads running now and the last 256 that ended: `readId`, `algorithm`,
`graph`, `state` (`running`, `finished`, `cancelled`, `failed`), `message`,
`batches`, `rows`, and the read's own `liveBytes`, `peakBytes` and
`workUnits`. A read leaves `running` only when its thread has returned from
the kernel.

**Materialised reads.** `NUTMEG_READS=materialized` runs every read the old
way: inside the scan, while the query is planned, collected into an
in-memory table before its first row leaves. No interrupt reaches it.
`nutmeg-server` selects it in Sail's cluster modes (`local-cluster`,
`kubernetes-cluster`) unless `NUTMEG_READS` says otherwise, because there
Sail's driver encodes each stage's plan for a worker and its codec refuses a
node it does not know, `NutmegAlgorithmExec` included, while it can encode
an in-memory table. Staging does not work in a cluster mode either: the
codec refuses the stage writer's `DataSinkExec` too. Even if it did not, a
Kubernetes worker is a process of its own and would not hold the driver's
graphs (local-cluster workers are actors in the server's process). Nutmeg is
a local-mode extension today.

## Grust and Sail

Both are pinned in the workspace `Cargo.toml`, so **a clean clone builds with
no sibling checkouts**:

| dependency | pin | why this kind of pin |
|---|---|---|
| `grust-algorithms`, `grust-algorithm-procedures`, `grust-procedures`, `grust-core` | crates.io `0.23.0` ("Langoustine") | Grust publishes |
| `sail-common`, `sail-common-datafusion`, `sail-telemetry`, `sail-session`, `sail-spark-connect` | git `lakehq/sail` rev `f1cf1729b1d083f2b97f1ce6e68a0d92c5ccee8f` | Sail does not publish to crates.io — "There is no plan to publish it as Rust crates for use in other Rust projects" (lakehq/sail#1991) |

`SAIL_COMMIT` is the revision the manifest pins. `GRUST_COMMIT` is the
commit `querygraph/grust`'s `v0.23.0` tag names (`6504c0c`), which is what
the published crates were cut from; the build resolves Grust by version, not
by that commit. Both files are a record for a reader, not an input —
nothing in the build reads either.

Nutmeg's own workspace is `publish = false` and stays that way, because it
compiles Sail's crates into its server. Nutmeg is released as a git tag, not
to crates.io.

Registering an external data source and table functions in a Sail session
needs one hook: a way for an embedder to choose the session factory,
described in `docs/sail-prs.md`. Upstream Sail has it as of the commit
above, so `nutmeg-server` builds against Sail's own main with no local
branch. Nothing graph-specific goes into Sail. No second change is needed:
Sail's SQL resolver already finds table functions registered in the session,
so `SELECT * FROM nutmeg_pagerank('g')` works in Spark SQL as well as
`spark.read.format("nutmeg")`.

Nutmeg is a **local-mode** extension: it does not work in Sail's cluster
modes (`local-cluster`, `kubernetes-cluster`). See the materialised-reads
note above and `CHANGELOG.md`.

## Building and testing

The gate, run before a commit is pushed, is one `&&` chain:

```sh
cargo fmt --all -- --check \
  && cargo clippy -p nutmeg-graph --all-targets -- -D warnings \
  && cargo clippy -p nutmeg-sail -p nutmeg-server --all-targets -- -D warnings \
  && cargo test -p nutmeg-graph \
  && cargo test -p nutmeg-sail --release \
  && git diff --check
```

Clippy runs in two invocations, and `nutmeg-graph`'s tests run alone, on
purpose. Five of `nutmeg-graph`'s tests go through `ctx.sql`, which needs
DataFusion's `sql` feature. That feature turns on `datafusion-common/sql`,
which adds a `DataFusionError::SQL` variant that Sail's exhaustive match over
`DataFusionError` (`sail-common-datafusion/src/error.rs`) does not cover, so
a build that has it cannot compile `nutmeg-sail` or `nutmeg-server`. The
feature is therefore on `nutmeg-graph`'s `datafusion` dev-dependency and
nowhere else. Under Cargo's resolver 2 a dev-dependency's features are
unified into a build only when that package's own test, example or bench
targets are being built: `cargo test -p nutmeg-graph` and `cargo clippy -p
nutmeg-graph --all-targets` have `sql`; `cargo clippy -p nutmeg-sail -p
nutmeg-server --all-targets` and every build of `nutmeg-server` do not. An
invocation that builds `nutmeg-graph`'s tests together with `nutmeg-sail`
(`cargo test --workspace`, `cargo clippy --workspace --all-targets`) fails to
compile `sail-common-datafusion`; that is the expected outcome, not a
regression, and the gate above is how the workspace is checked.

## The catalog

Nutmeg serves whatever Grust's procedure registry holds, through Grust's own
runner: there is no per-kernel match arm here, and a test fails when a
registered kernel is not served. At Grust 0.23.0 that is these 41 —

`allPairsShortestPaths`, `articleRank`, `articulationPoints`, `astar`,
`bellmanFord`, `betweenness`, `bfs`, `biconnectedComponents`, `bridges`,
`closeness`, `degree`, `dfs`, `dijkstra`, `eigenvector`, `estimateCsr`,
`fastRP`, `harmonic`, `hits`, `k1Coloring`, `kCore`, `katz`,
`labelPropagation`, `leiden`, `linkPrediction`,
`localClusteringCoefficient`, `longestPath`, `louvain`, `maxFlow`, `minCut`,
`modularity`, `multiSourceBfs`, `nodeSimilarity`, `pagerank`,
`projectionStats`, `scc`, `shortestPaths`, `spanningTree`,
`topologicalSort`, `triangleCount`, `wcc`, `yens`

— a test reads that list back out of this file and fails if it is not
exactly what Nutmeg serves, so it cannot go stale the way "the twelve" it
replaces did. Each comes with Grust's oracle-checked kernel, per-unit work
charging, exact
memory admission and cancellation, and each reachable both as
`spark.read.format("nutmeg")` and as `nutmeg_<name>(...)` in Spark SQL. A
kernel Grust adds appears here by name with no change to this repository.
