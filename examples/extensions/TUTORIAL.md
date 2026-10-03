# Install and run Sail with Sedona and Nutmeg

Start with the [shared review request](../../docs/development/extensions/SAIL-EXTENSIONS-REVIEW-REQUEST.md)
to choose the prototype or the separate static-preflight candidate. For Sedona,
prepare the machine, build in step 4, start Sail in step 5, and run spatial SQL
in step 6. Step 8 adds workers; step 6 alone does not build or start Sail.

This walkthrough treats `sail-extensions` in `querygraph/sail` as a source
distribution. Build Sail and its two independent extension wheels from that
checkout; a released `pysail` wheel does not contain this branch's host changes.
All commands below run from the checkout root unless stated otherwise.

The prototype runtime is `bd8ce9ae8839477e2c08a0475ab7900b115c5366`; review-document
commits may advance the branch. If you selected the preflight candidate, stay in
its checkout and **skip this tutorial's clone commands**, including the Linux
example. Its recorded qualification is macOS ARM64 only; the prototype's Linux
and two-host receipts do not qualify the candidate.

The same installation supports either review track. Build both extensions for
this walkthrough and choose the Sedona or Nutmeg examples, or run both.
The two-host qualification harness requires both packages.

## 1. Choose an execution mode

| Mode | Deployment | Where extension work runs |
| --- | --- | --- |
| Local | One Sail server | All work in the server process |
| Actor cluster | Driver and worker actors in one process | Distributed task scheduling within that process |
| Process cluster | Driver and two worker processes on one host | Sedona scalars and relational graph plans on workers |
| Two hosts | Driver plus one worker on each host | Networked worker tasks; historical prototype qualification uses the supplied harness |

In every mode, Nutmeg native staging and algorithms remain on the driver.
Distributed input partitions are gathered there; results can feed distributed
SQL downstream. Nutmeg graph-table joins and aggregations run directly in Sail's
DataFusion engine on workers, without building CSR. This release does not
implement distributed native PageRank or distributed CSR residency.

## 2. Get the source

Install Git, then clone the review branch, unless already in the chosen checkout:

```bash
git clone --branch sail-extensions https://github.com/querygraph/sail.git
cd sail
git rev-parse HEAD
```

The checkout supplies source, Cargo lockfiles, Python dependency locks, the Sedona
port, and vendored Nutmeg source. Record the printed commit with review results.
The older `sail-extensions-1` tag remains a historical snapshot; this review route
uses the `sail-extensions` branch and its updated documentation.
Internet access is needed for the initial dependency downloads and pinned
SedonaDB checkout. No Spark JVM, Sedona JAR, or separate graph service is needed.

## 3. Prepare a build machine

Use Python 3.12 with a shared library, Rust 1.97.1, a C/C++ toolchain, protoc
including its standard protobuf headers, GEOS 3.12 or newer, and uv. The native
host and extensions use DataFusion 55.1.0 and Arrow 59.3.0; the client's PyArrow
version is independently pinned in `requirements.lock`.

Allow substantial free disk for three Rust builds. Earlier gate directories
occupied tens of GiB each; this is not a measured minimum. Four build jobs is
the script default; reduce it if compilation exhausts memory.

### Native macOS

With Xcode command-line tools, Homebrew and rustup installed:

```bash
brew install geos protobuf pkg-config cmake uv
rustup toolchain install 1.97.1 --profile minimal --component rustfmt,clippy
rustup override set 1.97.1
uv python install 3.12
export SAIL_EXTENSION_PYTHON="$(uv python find 3.12)"
geos-config --version
protoc --version
df -h .
```

On Apple Silicon, use a native arm64 terminal, Rust toolchain and Python
interpreter together. Check `rustc -vV` and
`"$SAIL_EXTENSION_PYTHON" -c 'import platform; print(platform.machine())'`.
If uv selects an existing Intel interpreter, set `SAIL_EXTENSION_PYTHON` to an
explicit arm64 Python 3.12 path. The wheels must match the interpreter and host.

### Linux, including a Linux VM on a Mac

The checked-in Dockerfile supplies the Linux compiler and system libraries.
With a running Linux Docker engine, run the following from the host checkout:

```bash
docker build -t sail-extensions-build -f examples/extensions/scripts/Dockerfile.linux .
docker volume create sail-extensions-work
docker run --name sail-extensions-review -it \
  -p 127.0.0.1:50051:50051 \
  -v sail-extensions-work:/work \
  sail-extensions-build bash
```

Inside the container, clone into its Linux filesystem. Use the branch selected
for your review; the commands below select the prototype. Skip them if that
checkout is already present:

```bash
git clone --branch sail-extensions https://github.com/querygraph/sail.git /work/sail
cd /work/sail
export SAIL_EXTENSION_PYTHON="$(uv python find 3.12)"
df -h .
```

Keep source, venv and build output in the volume. A macOS bind mount can make
Rust source traversal very slow. Docker Desktop or Colima can supply the Linux
engine; size the VM's actual memory and disk before compiling. The Dockerfile
also works as the prerequisite inventory for a native Linux installation.
The remainder runs inside this container, including the client in a second
`docker exec -it sail-extensions-review bash` terminal. Use `/work/sail` there.
For host clients to reach the published port, bind Sail to `0.0.0.0` in step 5;
otherwise keep its loopback default. This recipe is a one-host deployment.

## 4. Build and install

Use a dedicated checkout: the script synchronizes `.venv` to the dependency
lock, so this environment should not contain unrelated packages.

```bash
export CARGO_BUILD_JOBS=4
bash examples/extensions/scripts/build.sh
.venv/bin/python examples/extensions/sedona/scripts/smoke.py
.venv/bin/python - <<'PYCODE'
from importlib.metadata import entry_points
for entry in sorted(entry_points(group="pysail.extensions"), key=lambda e: e.name):
    loaded = entry.load()
    extension = loaded() if callable(loaded) else loaded
    print(entry.name, entry.value, extension.manifest())
PYCODE
```

The Python inventory snippet explicitly calls `entry.load()`; it is not a test
of the host's static preflight. The candidate's rejection-before-import tests
are linked from the shared review request.

The build fetches the pinned SedonaDB source, applies its DataFusion port,
builds and repairs both native wheels, installs them, then builds Sail.
It checks Sedona's bundled native-library dependencies. Expected artifacts:

- Server: `target/extensions-poc/host/debug/sail`
- Interpreter and client packages: `.venv/bin/python`
- Platform-specific distributable wheels: `target/extensions-poc/wheels/`
- GEOS dependency report: `target/extensions-poc/sedona-native-dependencies.json`

Discovery should list `sedona` and `nutmeg`. These are installed wheels, not
editable Python packages. The embedded interpreter does not process editable
`.pth` files. Re-running the build is supported. Advanced overrides are
`SAIL_EXTENSION_VENV` and `SAIL_EXTENSION_TARGET`; adjust later paths if used.

## 5. Start a local Sail server

In terminal A, from the checkout root, configure the embedded Python runtime:

```bash
export PYTHONHOME="$(.venv/bin/python -c 'import sys; print(sys.base_prefix)')"
export PYTHONPATH="$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
export DYLD_LIBRARY_PATH="$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')"
export LD_LIBRARY_PATH="$DYLD_LIBRARY_PATH"
export SAIL_EXPERIMENTAL_EXTENSIONS=1
export SAIL_EXECUTION__DEFAULT_PARALLELISM=4
export SAIL_CLUSTER__WORKER_INITIAL_COUNT=2
export SAIL_CLUSTER__WORKER_MAX_COUNT=2
SAIL_MODE=local SAIL_EXPERIMENTAL_PROCESS_WORKERS=0 \
  target/extensions-poc/host/debug/sail spark server --ip 127.0.0.1 --port 50051
```

Leave the server in the foreground. Stop it with Ctrl-C before switching modes.
The opt-in loads all installed `pysail.extensions` entry points. There is no
per-query plugin installation. Native extensions are trusted code.

## 6. Sedona review: spatial SQL and a shuffle

In terminal B, from the same checkout, run:

```bash
SPARK_CONNECT_MODE_ENABLED=1 .venv/bin/python - <<'PYCODE'
from pyspark.sql.connect.session import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.remote("sc://127.0.0.1:50051").create()
try:
    row = spark.sql("""SELECT
        ST_AsText(ST_Point(1.0, 2.0)) AS wkt,
        ST_Distance(ST_Point(0.0, 0.0), ST_Point(3.0, 4.0)) AS distance
    """).first()
    print(row)
    assert row.distance == 5.0
    points = spark.range(0, 17, numPartitions=4).selectExpr(
        "id", "ST_Point(CAST(id AS DOUBLE), 2.0) AS geom")
    rows = points.repartition(4, "id").selectExpr(
        "id", "ST_AsText(geom) AS wkt",
        "ST_Distance(geom, ST_Point(0.0, 2.0)) AS distance"
    ).orderBy("id").collect()
    assert [r.distance for r in rows] == [float(i) for i in range(17)]
    print("Sedona: 17 geometry rows survived the shuffle")
finally:
    spark.stop()
PYCODE
```

The first row contains the point `(1, 2)` in WKT and distance `5.0`.
The second query constructs geometry before repartition and consumes it afterward.
In worker modes it exercises geometry metadata and native scalar transport.
The pinned Apache Sedona Python package also supplies unmodified Connect helpers;
`tests/test_sedona.py` exercises those and spatial joins with residual predicates.

This wheel exports 128 SedonaDB scalars plus aliases. General Sail joins execute
spatial predicates; indexed Sedona spatial joins are outside this prototype. Collect
WKT, binary, booleans or numbers, rather than a Spark geometry UDT. Five colliding
names retain Sail's definitions, including its existing SRID placeholders;
see [Sedona scope](sedona/README.md#scope-and-next-increment).

## 7. Nutmeg review: graph tables, then native PageRank

In terminal B:

```bash
SPARK_CONNECT_MODE_ENABLED=1 .venv/bin/python - <<'PYCODE'
import math
from pyspark.sql.connect.session import SparkSession
from sail_nutmeg import Nutmeg
spark = SparkSession.builder.remote("sc://127.0.0.1:50051").create()
try:
    nodes = spark.range(0, 3, numPartitions=4).selectExpr(
        "CAST(id AS STRING) AS node_id")
    edges = spark.range(0, 3, numPartitions=4).selectExpr(
        "CAST(id AS STRING) AS source", "CAST((id + 1) % 3 AS STRING) AS target")
    nm = Nutmeg(spark)
    graph = nm.tables(nodes, edges).validate()
    graph.degrees().orderBy("nodeId").show()
    assert graph.walks(2).count() == 3
    receipt = nm.stage("cycle", nodes, edges)
    print(receipt)
    assert receipt.nodeCount == 3 and receipt.edgeCount == 3
    assert nm.nodes("cycle").count() == 3
    scores = nm.run("cycle", "pagerank").orderBy("nodeId").collect()
    assert [r.nodeId for r in scores] == ["0", "1", "2"]
    assert all(math.isclose(r.score, 1 / 3, abs_tol=1e-10) for r in scores)
    print(scores)
    print(nm.status())
    assert nm.drop("cycle").dropped
finally:
    spark.stop()
PYCODE
```

Every cycle vertex has one incoming and one outgoing edge. There are three
length-two walks and PageRank returns one third for each vertex.
`tables()` uses ordinary relations and works even with the extension opt-in off.
`stage()` eagerly creates a session-owned native snapshot; `run()` returns a lazy
DataFrame; `nodes()` scans Arrow rows without CSR. Native algorithms build a
shared projection on demand. `drop()` removes the named graph, while existing
readers retain their snapshot. Staged graphs do not survive a server restart.

By default each native session prepays a 256 MiB quota. Set
`SAIL_NUTMEG_MEMORY_BYTES` before server startup to change it. To enable finite
host admission, also set `SAIL_RUNTIME__MEMORY_POOL__TYPE=greedy` and
`SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE` (bytes). Allow room for every active
session's quota and query work. A server manager shares an explicit resource
domain with its sessions and actor workers; independent managers and separate
processes have separate domains. Equal settings alone do not share a pool.
These budgets cover participating allocations, not total process RSS.

## 8. Run with workers on one host

Stop the local server. In terminal A retain step 5's exports, then choose one:

```bash
# Worker actors inside the server process:
SAIL_MODE=local-cluster SAIL_EXPERIMENTAL_PROCESS_WORKERS=0 \
  target/extensions-poc/host/debug/sail spark server --ip 127.0.0.1 --port 50051
```

```bash
# Separate worker executables, with two workers configured in step 5:
SAIL_MODE=local-cluster SAIL_EXPERIMENTAL_PROCESS_WORKERS=1 \
  target/extensions-poc/host/debug/sail spark server --ip 127.0.0.1 --port 50051
```

Repeat either review example unchanged. Workers inherit the Python environment
and load their own extension bindings. Driver and workers must see identical
package content and manifest configuration, not just equal version numbers.
Nutmeg native regions remain driver-only and do not automatically retry.

## 9. Run the automated review checks

These tests start and stop their own servers on temporary ports. Run from a
fresh terminal without inherited `PYTHONHOME`, `PYTHONPATH`, or library overrides:

```bash
.venv/bin/python -m pytest examples/extensions/tests \
  --sail-binary "$PWD/target/extensions-poc/host/debug/sail" --execution-mode local -q
.venv/bin/python -m pytest examples/extensions/tests \
  --sail-binary "$PWD/target/extensions-poc/host/debug/sail" --execution-mode local-cluster -q
.venv/bin/python -m pytest examples/extensions/tests \
  --sail-binary "$PWD/target/extensions-poc/host/debug/sail" --execution-mode process-cluster -q
```

The recorded suite has 64 passes and one worker-only skip in local mode, and
65 passes in each worker mode. For focused review, replace the test directory
with `examples/extensions/tests/test_sedona.py`, or with
`examples/extensions/tests/test_nutmeg.py examples/extensions/tests/test_graph_tables.py`.
The whole suite additionally covers joint spatial/graph queries, resource
admission, protocol rejection, cancellation, and lifecycle behavior.
These checks verify functionality, not performance.

## 10. Run across two hosts

Use two machines with the same OS/architecture and compatible Python 3.12 and
system libraries. Build once and distribute that executable and those repaired
wheels: the harness intentionally requires byte-identical executable and package
identities. Independently built executables can differ even at the same SHA.
Mixed native architectures cannot satisfy this harness's identity requirement.

1. Clone the selected review branch on both hosts. Install the runtime prerequisites and uv on
   each. Use a normal filesystem and a stable absolute checkout path.
2. On the driver, complete step 4. Copy
   `target/extensions-poc/host/debug/sail` and `target/extensions-poc/wheels/`
   to the corresponding paths on the worker (create directories first).
   Preserve the executable bit. Do not copy a venv between machines.
3. On the worker, create its own environment and install the copied wheels:

```bash
uv venv --python 3.12 .venv
uv pip sync --python .venv/bin/python examples/extensions/requirements.lock
uv pip install --python .venv/bin/python target/extensions-poc/wheels/*.whl
.venv/bin/python examples/extensions/sedona/scripts/smoke.py
```

4. Configure noninteractive SSH from the driver to the worker. Verify
   `ssh -o BatchMode=yes worker-host hostname`, replacing `worker-host` with
   your SSH alias. The driver is also the harness controller.
5. Copy the configuration template outside the checkout:

```bash
cp examples/extensions/scripts/two-host.example.json ../sail-two-host.json
```

6. Edit every placeholder path/address in that JSON. `driver` and the first
   worker describe the driver machine; the second worker describes the remote
   machine and has an `ssh` alias. Each `repo`, `python`, and `sail` is an absolute
   path on its own machine. Use the new `.venv/bin/python` paths and the copied
   binary paths. `advertise` must be a reachable host address, not `127.0.0.1` or
   the template's documentation-only `192.0.2.*` addresses.
7. Permit TCP between hosts for the driver gateway (template port 50152) and
   both worker ports (50161 and 50162). The Connect port (50151) must be reachable
   by the client/controller. Run on a trusted private network; this recipe does
   not configure authentication or TLS. For a VM, these must be guest-reachable
   addresses and forwarded ports, not merely the macOS host's addresses.
8. From the driver checkout, with no manually started server on those ports:

```bash
.venv/bin/python examples/extensions/scripts/two_host.py \
  --config ../sail-two-host.json --output ../sail-two-host-evidence-1
```

The output directory must be new. This is a supervised qualification deployment:
the harness starts the driver and both workers, checks spatial expressions,
relational graph work and native graph composition, then shuts them down.
It does not leave a persistent cluster for interactive use. Use step 8 for a
persistent interactive process cluster. Long-running multi-host service
management and Kubernetes images are outside this tutorial.

Inspect `receipt.json`: require `outcome: "passed"`, matching inventories,
completed tasks from both workers, and no live supervised processes in cleanup.
`server-and-workers.log` contains the execution log. A failure also writes a
receipt with the error and completed evidence; retain it when reporting issues.
Registration alone does not establish that both workers executed tasks.

## 11. Troubleshooting and review evidence

| Symptom | Check |
| --- | --- |
| Missing `libpython` or Python initialization failure | Use the built venv's base prefix and library directory from step 5; remove stale overrides before rebuilding |
| Missing `google/protobuf/any.proto` | Install protobuf development headers, not just the protoc executable; Linux Dockerfile includes `libprotobuf-dev` |
| GEOS wheel repair fails | Check `geos-config`, GEOS version, platform dependencies and the native-dependency report |
| Unknown spatial function or relation URL | Both wheels must be installed in the server's Python environment and the extension opt-in must be set |
| Worker identity mismatch | Reinstall the same wheel bytes everywhere; local package edits change identity even at the same version |
| Native memory admission refused | Account for prepaid quotas of all sessions, finite host pool capacity and retained readers |
| Two-host startup hangs/fails | Check SSH, absolute remote paths, advertised addresses and bidirectional worker/gateway connectivity |
| Compiler killed or disk full | Check actual VM/container memory and free disk; lower build jobs and retain the failed log |

When sharing a result, include source SHA, platform/architecture, toolchain,
mode, commands, outcome and logs. Existing evidence and its exact revision
boundaries are in the [design review](../../docs/development/extensions/design-review.md)
and [compatibility matrix](../../docs/development/extensions/compatibility-matrix.json).
These reviewer instructions do not broaden historical gate verdicts
to untested platforms or convert this experimental bootstrap into a stable ABI.
