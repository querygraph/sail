# Two native extensions on Sail

For a design review, start at the shared
[Sail Extensions review request](../../docs/development/extensions/SAIL-EXTENSIONS-REVIEW-REQUEST.md).
It distinguishes the published prototype from the separately adoptable static
compatibility preflight and gives routes for scalars and stateful relations.

To write your own extension, start with
[Writing a Sail extension](WRITING-AN-EXTENSION.md): the protocol, a minimal
client, a minimal handler, and how to build and run.

For a fresh install, local and distributed deployment, and executable review
examples, use the [source-distribution tutorial](TUTORIAL.md).

The published `work/extensions-static-preflight` candidate additionally requires a
recorded static compatibility file in every extension wheel. Existing wheels
without that file must be rebuilt and reinstalled. Read
[Static compatibility checks before extension import](../../docs/development/extensions/static-compatibility-preflight.md)
for the schema, migration and validation boundary. When reviewing this candidate,
keep this checkout and skip the prototype clone commands in the linked tutorials.

The [Pecan graph algorithm plan](../../docs/development/extensions/portable-graph-plan.md)
adds client-controlled PageRank and WCC through ordinary distributed Sail queries.
The Python distribution is `pyspark-pecan`, imported as `pyspark_pecan`.
Use its [testing tutorial](graph-algorithms/TESTING.md) to build this branch and
check the algorithms locally, with worker processes, or across two hosts.
The [all-method tutorial](benchmarks/TUTORIAL.md) runs Pecan, Banda and Grenada
with reference, advanced and fused WCC methods. Their
[benchmark report](../../docs/development/extensions/pecan-nutmeg-benchmark.md)
records elapsed time, process memory, resource limits and every outcome.
The [server-side graphframes-rs integration](../../docs/development/extensions/graphframes-rs-plan.md)
remains a later option.

This branch implements a local and distributed proof of concept for the fifth-revision
[Sail extension proposal](https://github.com/querygraph/grust/blob/7fc0514/docs/proposals/sail-extension-api.md).
The [implementation plan](../../docs/development/extensions/implementation-plan.md)
and [graph-table follow-up plan](../../docs/development/extensions/datafusion-graph-plan.md)
describe the implementation, evidence and remaining indexed spatial-join work.
The [review and resolution record](../../docs/development/extensions/implementation-review-resolution.md)
separates the original findings, implemented corrections and remaining acceptance gaps.

- **Apache SedonaDB:** 128 actual native/GEOS scalar UDFs, imported from a separate
  Python wheel through DataFusion FFI. Spark SQL, DataFrame expressions and
  Apache Sedona's unmodified Connect helpers call them by name. Ordinary Sail
  spatial joins evaluate those predicates.
- **Nutmeg graph tables:** normal Sail relations with degree, triplet and bounded
  walk helpers compiled into DataFusion joins/aggregations. This path runs on
  workers with extensions disabled and constructs no CSR.
- **Nutmeg native kernels:** session-scoped graph staging, native Grust algorithms, and graph
  drop through Spark Connect relation extensions. Staging consumes two DataFrame
  inputs atomically, including every partition. Reads pin a graph revision and
  stream results through DataFusion FFI.

Both packages have independent Cargo workspaces and no Sail engine dependency.
Nutmeg shares a small dependency-free memory-lease ABI definition with Sail.
The host and plugins use DataFusion 55.1.0 and Arrow 59.3.0. This is an experimental
Python bootstrap API with API/DataFusion/Arrow version checks, not a stable binary
compatibility promise. Sail source SHA and Rust compiler version are not manifest
acceptance keys. Installed native packages are trusted code.

The [maintainer design review](../../docs/development/extensions/design-review.md)
consolidates the delivered architecture, necessary host changes, evidence,
limitations and possible review units. The
[implemented follow-up](../../docs/development/extensions/review-follow-up.md)
records explicit resource-domain ownership, unchanged-wheel qualification and
the downloadable review evidence bundle. The [expanded ABI review](../../docs/development/extensions/abi-review.md)
separates compiler changes, upstream movement, dependency diagnostics and
refusal tests; the original `sail-extensions-1` tag remains a fixed snapshot.

## Build and run

Prerequisites: Rust 1.97.1, Python 3.12 with a shared library, uv, Git, protoc,
and GEOS development libraries (`brew install geos` on macOS). Use a normal
Python installation or a uv-managed interpreter. Cargo and Python dependencies
are locked. Sedona's preparation script fetches a pinned Apache checkout and
applies the checked-in DataFusion port.

```bash
examples/extensions/scripts/build.sh
```

Set `SAIL_EXTENSION_PYTHON` to choose the interpreter, `SAIL_EXTENSION_VENV` for
the environment, and `SAIL_EXTENSION_TARGET` for build artifacts. The build script
prints the resulting executable, Python interpreter and wheel directory. On
macOS, native Rust tests need the interpreter's library directory in
`DYLD_LIBRARY_PATH`; the verification script sets it.

Start a server with its Python library and environment visible:

```bash
export PYTHONHOME=$(.venv/bin/python -c 'import sys; print(sys.base_prefix)')
export PYTHONPATH=$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
export DYLD_LIBRARY_PATH=$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')
SAIL_EXPERIMENTAL_EXTENSIONS=1 SAIL_MODE=local \
  target/extensions-poc/host/debug/sail spark server --port 50051
```

The environment flag is an explicit opt-in. With it set, all installed
`pysail.extensions` entry points are loaded in name order. Mismatched builds,
function/type-URL collisions are refused before execution.
Without it, ordinary Sail operation remains available.

For distributed execution, set `SAIL_MODE=local-cluster`. Workers normally run
as actors in the server process. Also set `SAIL_EXPERIMENTAL_PROCESS_WORKERS=1`
to launch separate Sail worker executables on this host; they inherit the Python
environment and independently load their installed native wheels. Each worker
must have identical package content and manifest configuration. Missing or
different content is rejected during task decoding, including changes under an
unchanged package version. `SAIL_EXPERIMENTAL_WORKER_PYTHONPATH` can select a
different installed worker environment for compatibility testing.

Sedona scalar expressions run on workers. Selected compositions preserve geometry
field metadata through expression serialization, constant folding and shuffles.
Nutmeg retains graph
state on the driver: distributed node/edge inputs are gathered into a driver-only
native stage, and its output can feed worker stages. Regions containing a Nutmeg
operation have one attempt, even when ordinary tasks allow retries. An error
after a mutation might have committed is reported as indeterminate.

Native graph sessions prepay their configured quota from Sail's process memory
pool. With extensions enabled, each server manager explicitly shares a resource domain
with its sessions and actor workers. Independent managers remain isolated even
with equal pool configurations; separate processes have separate pools. A finite Greedy/Fair configuration enforces contention;
an unbounded pool remains unbounded. Native quotas cannot spill and stay charged
until the last session, producer, plan or exported buffer owner releases them.
The native budget subdivides that prepaid host quota; it accounts for graph
buffers and admitted build/kernel workspace. This is not an allocator-level
limit on every allocation or total process RSS. Runtime, transport, metadata
and other nonparticipating allocations still need headroom.

```python
from pyspark.sql.connect.session import SparkSession
from pyspark.sql import functions as F
from sail_nutmeg import Nutmeg

spark = SparkSession.builder.remote("sc://127.0.0.1:50051").create()
spark.sql("SELECT ST_AsText(ST_Point(1.0, 2.0)) AS wkt").show()
nodes = spark.range(3, numPartitions=4).selectExpr("CAST(id AS STRING) AS node_id")
edges = spark.range(3, numPartitions=4).selectExpr(
    "CAST(id AS STRING) AS source", "CAST((id + 1) % 3 AS STRING) AS target"
)
nm = Nutmeg(spark)
graph = nm.tables(nodes, edges)
graph.degrees().show()  # ordinary Sail worker plans; no native snapshot or CSR
graph.walks(2).show()
print(nm.stage("cycle", nodes, edges))
nm.nodes("cycle").show()  # normalized Arrow snapshot scan, no CSR
nm.run("cycle", "pagerank").select("nodeId", "score").show()
print(nm.status())
print(nm.drop("cycle"))
spark.stop()
```

`tests/test_joint.py` runs the combined example: Sedona distance predicates
construct a spatial-neighbor graph inside Sail, and Nutmeg computes connected
components, including an isolated vertex.

## Verification and boundaries

```bash
.venv/bin/python -m pytest examples/extensions/tests \
  --sail-binary target/extensions-poc/host/debug/sail -q
# Repeat with --execution-mode local-cluster and --execution-mode process-cluster.
```

For a commit-specific PoC receipt, create a detached worktree at the candidate SHA,
build there using its own target directory, then run `scripts/verify.sh` from
this directory. Install its pinned nightly formatter with
`rustup toolchain install nightly-2026-05-28 --profile minimal --component rustfmt`;
compilation still uses the selected stable Rust toolchain. The formatter honors
the repository's import-grouping rules, matching the CI check. Verification
refuses a moving branch/dirty tracked tree and prints the
exact verified SHA. Package native tests exercise real FFI calls as well as the
wire/server tests. Evidence records build errors as well as final outcomes.

Not implemented here: distributed residency of native staged graphs/CSR,
distributed iterative native graph algorithms, automatic retries of native
driver operations, Sedona's optimized `SpatialJoinExec`, Sedona aggregate/window
UDF registration, geometry-column collection through the Sedona client UDT,
generic catalogs/formats or mutable per-query Sedona options. `ST_AsText`, counts
and other server-side scalar results are supported. Five colliding Sail spatial
function names are deliberately omitted from the Sedona package; see its README.

The process-worker fixture runs multiple processes on one host. It does not
qualify multi-host networking or a Kubernetes deployment. Kubernetes workers
receive the opt-in flag but require the wheels in their image.

The separate two-host harness checks actual network execution with a worker on
each machine. Copy `scripts/two-host.example.json` outside the source tree and
replace the example addresses, paths and SSH host. Install identical executable
and native wheel bytes on both machines; the harness refuses identity mismatches.
The advertised addresses and configured ports must be reachable between hosts,
and the controller must already have working noninteractive SSH to the worker.

```bash
.venv/bin/python examples/extensions/scripts/two_host.py \
  --config /path/to/two-host.json --output /path/to/new-evidence-directory
```

The receipt retains host/package identities, results, both worker endpoints,
successful task records from both workers, and process cleanup checks. Failure
receipts retain completed checks and errors. The trusted startup worker launcher
uses literal JSON argv and a bounded heartbeat lease; it is a PoC harness, not a
general cluster deployment service. See the
[Linux environment recipe](../../docs/development/extensions/linux-environment.md)
for the independent Colima gate.

Nutmeg staging is overwrite-only. A newly planned request is a new
operation; no exactly-once guarantee spans retries/reconnects. A single completed
physical mutation returns its cached receipt on repeat execution; an in-flight or
abandoned attempt is refused as indeterminate. The per-session default graph
budget is 256 MiB, nested inside its prepaid host reservation. Graph projection
construction and staging's final canonical sort are synchronous regions that
query interruption cannot preempt; streaming kernel cancellation does not imply
preemption of those regions.
