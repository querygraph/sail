# Run Pecan, Nutmeg Banda and Nutmeg Grenada

This tutorial builds the Sail extension branch, runs every PageRank/WCC method,
and produces correctness receipts with elapsed time and available memory data.
It covers a local server, separate worker processes, and a two-host functional
check. The last section runs isolated Linux benchmark trials.

Commands use Bash from the checkout root unless a step says otherwise. No Spark
JVM, Sedona JAR or separate graph database is required. Build both native wheels;
the common server deployment and two-host checks discover Sedona and Nutmeg.

| Path | Graph representation and execution |
| --- | --- |
| **Pecan** | Python controls iterations; Sail/DataFusion executes graph-table joins, aggregates and Parquet writes. |
| **Nutmeg Banda** | Nutmeg stages a native graph on the driver and runs local native kernels over a projection/CSR. |
| **Nutmeg Grenada** | Nutmeg graph tables feed Pecan's controller. It shares Pecan's implementation and does not construct CSR. |

| Algorithm | Flavor | Pecan / Grenada method | Banda kernel |
| --- | --- | --- | --- |
| PageRank | Reference | `power` | `pagerank` |
| PageRank | Advanced | `delta` | `pagerankDelta` |
| WCC | Reference | `min_label` | `wcc` (union-find) |
| WCC | Advanced | `randomized` | `wccRandomized` |
| WCC | Advanced, fused plan | `randomized_fused` | `wccRandomizedFused` |

There are fifteen executable combinations: each path has two PageRank methods
and three WCC plans. Reference and advanced flavors remain available; advanced
WCC offers both the original and fused plans.
The CLI retains `--variant optimized` for the original advanced flavor and
`--variant fused` for fused WCC. This name makes
no promise of lower elapsed time or memory. Original methods remain available;
Pecan defaults to `power` and `min_label`.

## 1. Freeze the branch

```bash
git clone --branch work/extensions-datafusion-graphs https://github.com/querygraph/sail.git sail-graph-review
cd sail-graph-review
git switch --detach
export REVIEW_SOURCE_SHA="$(git rev-parse HEAD)"
printf 'Source: %s\n' "$REVIEW_SOURCE_SHA"
```

Keep this checkout unchanged during the build and checks. The older
`sail-extensions-1` tag and released Sail wheels do not contain this surface.
Record the SHA with results; version `0.1.0` alone does not identify wheel bytes.

## 2. Prepare and build

The prerequisites are Python 3.12 with its shared library, Rust 1.97.1, C/C++
build tools, protobuf compiler **and standard headers**, GEOS 3.12+, and uv.
Check free disk before compiling; the separate Rust targets can consume tens of
GiB each. Use matching architectures for Rust, Python, Sail and the wheels.

### Native macOS

With Xcode command-line tools, Homebrew and rustup installed:

```bash
brew install geos protobuf pkg-config cmake uv
rustup toolchain install 1.97.1 --profile minimal --component rustfmt,clippy
rustup override set 1.97.1
uv python install 3.12
export SAIL_EXTENSION_PYTHON="$(uv python find 3.12)"
rustc -vV
"$SAIL_EXTENSION_PYTHON" -c 'import platform; print(platform.machine())'
geos-config --version
protoc --version
df -h .
```

On Apple Silicon use an arm64 terminal and interpreter for a native build.
Do not combine an arm64 host with x86 wheels or copy another host's virtualenv.

### Linux, including Colima or Docker Desktop

The checked-in Dockerfile installs the prerequisites. Run this on the host with
a Linux Docker engine; allocate enough VM RAM/disk before building. The example
container needs eight available CPUs and a 32 GiB memory limit.

```bash
docker build -t sail-pecan-review-build -f examples/extensions/scripts/Dockerfile.linux .
docker volume create pecan-review-work
docker run --name pecan-review -it --init \
  --cpus 8 --cpuset-cpus 0-7 --memory 32g --memory-swap 32g \
  -p 127.0.0.1:50051:50051 \
  -e REVIEW_SOURCE_SHA="$REVIEW_SOURCE_SHA" \
  -v pecan-review-work:/targets sail-pecan-review-build bash
```

Inside that container:

```bash
git clone --branch work/extensions-datafusion-graphs https://github.com/querygraph/sail.git /targets/sail
cd /targets/sail
git switch --detach "$REVIEW_SOURCE_SHA"
export SAIL_EXTENSION_PYTHON="$(uv python find 3.12)"
df -h .
```

Run the remaining build and functional commands inside the container. A second
terminal uses `docker exec -it pecan-review bash`, then `cd /targets/sail`.
Keeping source, venv and outputs in this Linux volume avoids macOS shared-mount
build overhead. Native Linux installations can use the Dockerfile as their
dependency inventory instead.

### Build the host and clients

On the chosen build machine, from its frozen checkout:

```bash
unset PYTHONHOME PYTHONPATH DYLD_LIBRARY_PATH LD_LIBRARY_PATH
export CARGO_BUILD_JOBS=4
bash examples/extensions/scripts/build.sh
.venv/bin/python - <<'PY'
from importlib.metadata import version
from pyspark_pecan import GraphAlgorithms
from sail_nutmeg import Nutmeg
for package in ('pyspark', 'protobuf', 'pyspark-pecan',
                'sail-nutmeg', 'sail-sedona-extension'):
    print(package, version(package))
PY
```

The script synchronizes a dedicated `.venv` to the dependency lock, builds and
repairs both native wheels, installs them and Pecan, then builds Sail. Expected:

| Artifact | Path |
| --- | --- |
| Sail | `target/extensions-poc/host/debug/sail` |
| Python | `.venv/bin/python` |
| Repaired native wheels | `target/extensions-poc/wheels/` |

The client baseline is PySpark 4.0.1 / protobuf 7.36.2. Utilities and `gf_axpb`
are built into this Sail branch; there is no third native utils wheel.
Pecan's distribution is `pyspark-pecan`, import `pyspark_pecan`.
Embedded Python requires installed packages; editable `.pth` paths are not enough.

## 3. Run all fifteen combinations locally

The helper starts a fresh Sail server/session for **each** case, supplies its
embedded-Python and staging configuration, validates results, and stops it.
Do not start a manual server for this step.

```bash
unset PYTHONHOME PYTHONPATH DYLD_LIBRARY_PATH LD_LIBRARY_PATH
export REVIEW_SOURCE_SHA="$(git rev-parse HEAD)"
review_output=$(mktemp -d "$PWD/target/pecan-method-review.XXXXXX")
.venv/bin/python examples/extensions/benchmarks/tutorial_methods.py \
  --sail-binary "$PWD/target/extensions-poc/host/debug/sail" \
  --runtime-source-sha "$REVIEW_SOURCE_SHA" \
  --native-source-sha "$REVIEW_SOURCE_SHA" \
  --mode local --output "$review_output/local"
```

Require fifteen `passed` rows and exit code zero. The fixture has 128 vertices,
512 directed edges, 6 isolates, 16 dangling vertices, 16 self-loops and 10 weak
components. Every WCC partition is checked against independent union-find.
PageRank uses damping 0.85, tolerance `1e-8`, and cap 1,000; every output gets
an independent fixed-point residual and full-vector reference check.

The helper prints actual call seconds and sampled execution RSS/PSS in MiB.
On macOS, process memory is shown as `unavailable`; it is never substituted with
zero. Linux memory covers all visible processes and requires a private PID
namespace: do not run with `--pid=host`. This is a **functional tutorial**, not
an isolated benchmark. The outer container is reused, cgroup peaks accumulate,
and the build above is a development build. Use step 7 for comparisons.

Each case retains `receipt.json`, server log, memory samples and result Parquet.
The top-level `tutorial-summary.json` retains every outcome and command.
Failures do not disappear because later cases succeed. Use a new output path
when repeating; existing data is not overwritten.
Ordinary launch/receipt errors are recorded before the remaining cases continue;
an operator interruption preserves a partial summary and stops. In the Docker
recipe, these output directories live in the persistent Linux volume.

To focus on a reviewer’s path, add one of these selectors to the same command:

```text
--engine pecan
--engine nutmeg-native       # Banda: five native calls
--engine nutmeg-datafusion   # Grenada: five relational calls
```

`--algorithm pagerank|wcc` and `--variant reference|optimized|fused` select a
method. Fused is available only for WCC; PageRank/fused is rejected. The existing [graph_cell implementation](graph_cell.py) contains the API
calls used by the helper, so the tutorial does not maintain a second validator.

## 4. Repeat with separate worker processes

```bash
.venv/bin/python examples/extensions/benchmarks/tutorial_methods.py \
  --sail-binary "$PWD/target/extensions-poc/host/debug/sail" \
  --runtime-source-sha "$REVIEW_SOURCE_SHA" \
  --native-source-sha "$REVIEW_SOURCE_SHA" \
  --mode process-cluster --output "$review_output/process-cluster"
```

Again require fifteen passed rows. Pecan/Grenada relational work can execute on
the two workers. Banda gathers input to the driver and runs its kernels there;
worker input/output does not make native PageRank or union-find distributed.

The helper uses four execution partitions, four requested threads, 32 asynchronous
task slots per worker, a 16 GiB Sail pool per process and an 8 GiB native allowance.
Slots are scheduling capacity, not CPU cores. Pools are per process; the Linux
container's 32 GiB bound is aggregate. Every path loads the same extensions and
prepays the native allowance. Reservation bytes are not allocated RSS.

For broader correctness tests, run the package suite in each mode:

```bash
(
  set -e
  for mode in local process-cluster; do
    .venv/bin/python examples/extensions/scripts/test_graph_algorithms.py \
      --sail-binary "$PWD/target/extensions-poc/host/debug/sail" \
      --execution-mode "$mode" --output "$review_output/tests-$mode"
  done
)
```

Each invocation must print `portable-graphs: PASSED <mode>` with no failures.

## 5. Use the APIs against a persistent server

In terminal A, set the complete runtime environment and start Sail:

```bash
unset PYTHONHOME PYTHONPATH DYLD_LIBRARY_PATH LD_LIBRARY_PATH
mkdir -p "$PWD/target/pecan-interactive-staging"
export SAIL_GRAPH_UTILS_ROOT="file://$PWD/target/pecan-interactive-staging"
export PYTHONHOME="$(.venv/bin/python -c 'import sys; print(sys.base_prefix)')"
export PYTHONPATH="$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
export DYLD_LIBRARY_PATH="$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')"
export LD_LIBRARY_PATH="$DYLD_LIBRARY_PATH"
export SAIL_EXPERIMENTAL_EXTENSIONS=1
export SAIL_EXECUTION__DEFAULT_PARALLELISM=4
export SAIL_CLUSTER__WORKER_INITIAL_COUNT=2
export SAIL_CLUSTER__WORKER_MAX_COUNT=2
export SAIL_CLUSTER__WORKER_TASK_SLOTS=32
export SAIL_RUNTIME__MEMORY_POOL__TYPE=greedy
export SAIL_RUNTIME__MEMORY_POOL__GREEDY__MAX_SIZE=17179869184
export SAIL_NUTMEG_MEMORY_BYTES=8589934592
export TOKIO_WORKER_THREADS=4 RAYON_NUM_THREADS=4
SAIL_MODE=local SAIL_EXPERIMENTAL_PROCESS_WORKERS=0 \
  "$PWD/target/extensions-poc/host/debug/sail" spark server --ip 127.0.0.1 --port 50051
```

To use workers, stop the server and rerun its last command with
`SAIL_MODE=local-cluster SAIL_EXPERIMENTAL_PROCESS_WORKERS=1`.
Inside Docker use `--ip 0.0.0.0` only when a client must reach the published port;
clients inside the same container use loopback. The staging root must preexist
and remain exclusively managed by Sail.

In terminal B, this complete example makes all fifteen API calls on a tiny graph:

```bash
.venv/bin/python - <<'PY'
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect import functions as F
from pyspark_pecan import GraphAlgorithms, GraphUtils
from sail_nutmeg import Nutmeg

spark = SparkSession.builder.remote('sc://127.0.0.1:50051').create()
try:
    utils = GraphUtils(spark)
    assert {'fs', 'owned_runs_v1', 'axpb'} <= utils.capabilities
    vertices = spark.createDataFrame([(0,), (1,), (2,), (3,), (9,)], 'id long')
    edges = spark.createDataFrame([(0, 1), (1, 2), (2, 0), (2, 3)], 'src long, dst long')
    graph = GraphAlgorithms(spark)

    def relational(name, nodes, links):
        for method in ('power', 'delta'):
            with graph.pagerank(nodes, links, method=method, tolerance=1e-8,
                                max_iterations=1000, partitions=4) as result:
                print(name, 'PageRank', method, result.iterations)
                result.frame.orderBy('id').show()
        for method in ('min_label', 'randomized', 'randomized_fused'):
            with graph.wcc(nodes, links, method=method, seed=42,
                           max_iterations=100, partitions=4) as result:
                print(name, 'WCC', method, result.iterations)
                result.frame.orderBy('id').show()

    relational('Pecan', vertices, edges)
    nm = Nutmeg(spark)
    tables = nm.tables(vertices, edges, node_id='id', source='src', target='dst')
    relational('Grenada', tables.nodes.select(F.col('node_id').alias('id')),
               tables.edges.select(F.col('source').alias('src'), F.col('target').alias('dst')))

    nm.stage('tutorial', vertices.select(F.col('id').cast('string').alias('node_id')),
             edges.select(F.col('src').cast('string').alias('source'),
                          F.col('dst').cast('string').alias('target')))
    try:
        for kernel in ('pagerank', 'pagerankDelta'):
            print('Banda', kernel)
            nm.run('tutorial', kernel, damping=0.85, tolerance=1e-8,
                   maxIterations=1000, precision='f64', orientation='outgoing',
                   concurrency=4).orderBy('nodeId').show()
        for kernel in ('wcc', 'wccRandomized', 'wccRandomizedFused'):
            options = dict(maxIterations=100, seed=42) if kernel != 'wcc' else {}
            print('Banda', kernel)
            nm.run('tutorial', kernel, concurrency=4, **options).orderBy('nodeId').show()
    finally:
        nm.drop('tutorial')
finally:
    spark.stop()
PY
```

WCC groups `0,1,2,3` together and leaves `9` isolated. PageRank gives a
probability-normalized stationary approximation, including uniform dangling
redistribution. Tiny result tables are collected here for display; algorithm
execution itself keeps graph rows on the server. This example reuses a session
and native projection, so its calls are not benchmark trials.

Power's tolerance controls successive-step L1 change. Delta's tolerance controls
the full normalized fixed-point residual; its stationary L1 error bound is that
residual divided by reset probability. Delta retains inactive signed residual and
allows reactivation. Its active-edge count measures propagated messages, not
physical Parquet reads avoided. Reference WCC methods differ: minimum-label
propagation can take graph-diameter rounds, while Banda uses union-find. Advanced
WCC uses seeded GF64 contraction and requires `axpb`; missing capability fails.

Keep Pecan result DataFrames inside their context. To retain output, call
`result.write_parquet('s3://bucket/caller-owned-output')` before closing and choose
a path outside the owned staging run. Normal closure removes staging. Failed or
interrupted writes may defer cleanup to session shutdown; cleanup is best effort,
and `cleanup_deferred`/`run_path` identify that case. Native `drop()` releases the
named graph; retained readers can still hold their snapshot. Close the session
and stop terminal A's server when finished.

## 6. Verify two physical hosts

Use the full [two-host installation recipe](../graph-algorithms/TESTING.md#8-test-two-physical-hosts):
it gives artifact transfer, independent venv installation and the launcher JSON.
Both hosts need the same clean source SHA, byte-identical Sail/native packages,
compatible OS/architecture/Python, noninteractive SSH, and reachable gateway and
worker ports. Build once and copy repaired artifacts; do not copy virtualenvs.

On **each** host put private storage configuration outside Git, mode `0600`:

```json
{
  "SAIL_GRAPH_UTILS_ROOT": "s3://YOUR_BUCKET/graph-review",
  "AWS_DEFAULT_REGION": "YOUR_REGION",
  "AWS_ACCESS_KEY_ID": "YOUR_ACCESS_KEY",
  "AWS_SECRET_ACCESS_KEY": "YOUR_SECRET_KEY"
}
```

Use a dedicated shared prefix with read/write/list/delete permissions. Add
`AWS_SESSION_TOKEN` for temporary credentials, or `AWS_ENDPOINT` and
`AWS_ALLOW_HTTP="true"` for an HTTP-compatible object store. A private local
directory on the driver cannot serve the other host.

Copy `examples/extensions/scripts/two-host.example.json` outside the checkout.
Replace every repo/Python/Sail path and example IP with real absolute paths and
reachable addresses. Add `environment_file` to the driver **and both workers**,
using each host's private file path. The launcher supplies embedded Python and
starts/stops all processes; no manual server should occupy those ports.

```bash
.venv/bin/python examples/extensions/scripts/two_host.py \
  --config ../sail-graph-two-host.json --exercise portable-graphs \
  --output ../pecan-two-host-reference
.venv/bin/python examples/extensions/scripts/two_host.py \
  --config ../sail-graph-two-host.json --exercise portable-graphs \
  --pagerank-method delta --pagerank-iterations 1000 --tolerance 1e-8 \
  --wcc-method randomized --wcc-iterations 100 --seed 42 \
  --output ../pecan-two-host-advanced
.venv/bin/python examples/extensions/scripts/two_host.py \
  --config ../sail-graph-two-host.json --exercise portable-graphs \
  --pagerank-method delta --pagerank-iterations 1000 --tolerance 1e-8 \
  --wcc-method randomized_fused --wcc-iterations 100 --seed 42 \
  --worker-task-slots 4 --output ../pecan-two-host-fused
```

Require `outcome: passed`, supervisor return code zero, both workers' completed
tasks inside each algorithm's iteration windows, and all cleanup PIDs absent.
These commands check all five Pecan methods. The fused fixture needs four
asynchronous task slots per worker (eight total); `--worker-task-slots` defaults
to two. Slots are scheduling capacity, not CPU cores, and larger plans may need
more. `--exercise extensions` separately checks Nutmeg graph-table operations
and driver-native integration; it is not an
all-fifteen physical-host algorithm test. Grenada shares the relational controller;
Banda remains driver-local. The launcher does not leave an interactive cluster
running and does not configure TLS/authentication: use a trusted private network.

## 7. Measure time and memory in isolated Linux trials

The tutorial helper is for functionality. Use release artifacts and a fresh
container per trial for comparisons. In the Linux build container from step 2:

```bash
cd /targets/sail
unset PYTHONHOME PYTHONPATH DYLD_LIBRARY_PATH LD_LIBRARY_PATH
export CARGO_INCREMENTAL=0
export PYO3_PYTHON="$PWD/.venv/bin/python"
export LD_LIBRARY_PATH="$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')"
CARGO_TARGET_DIR="$PWD/target/extensions-release/host" \
  cargo build --release --locked -p sail-cli
mkdir -p target/extensions-release/wheels
CARGO_TARGET_DIR="$PWD/target/extensions-release/nutmeg" \
  .venv/bin/python -m maturin build --release --locked \
  --manifest-path examples/extensions/nutmeg/Cargo.toml \
  --interpreter "$PWD/.venv/bin/python" \
  --out "$PWD/target/extensions-release/wheels" --auditwheel repair
uv pip install --python .venv/bin/python --reinstall target/extensions-release/wheels/*.whl
git status --porcelain
```

Require empty source status. Stop the build container and other task workloads
before timing. The installed Sedona package remains identical for every path;
only its discovery is used. Record release binary and installed-package hashes,
which the benchmark receipts collect automatically.

On the **host**, in its checkout at the same SHA, with Python 3.10+ and Docker:

```bash
docker stop pecan-review
export REVIEW_SOURCE_SHA="$(git rev-parse HEAD)"
export REVIEW_DOCKER_CONTEXT="$(docker context show)"
python3 - <<'PY'
import json, os
from pathlib import Path
config = json.loads(Path('examples/extensions/benchmarks/matrix.example.json').read_text())
config.update(run_id='pecan-review', docker_context=os.environ['REVIEW_DOCKER_CONTEXT'],
              image='sail-pecan-review-build', target_volume='pecan-review-work',
              container_repo='/targets/sail', container_python='/targets/sail/.venv/bin/python',
              container_sail_binary='/targets/sail/target/extensions-release/host/release/sail',
              container_root='/targets/pecan-review-measurements',
              host_output=str(Path('../pecan-review-evidence').resolve()))
for key in ('runtime_source_sha', 'native_source_sha', 'harness_source_sha'):
    config[key] = os.environ['REVIEW_SOURCE_SHA']
Path('../pecan-matrix.json').write_text(json.dumps(config, indent=2) + '\n')
PY
python3 examples/extensions/benchmarks/run_matrix.py --config ../pecan-matrix.json --dry-run
python3 examples/extensions/benchmarks/run_matrix.py --config ../pecan-matrix.json --prepare-only
python3 examples/extensions/benchmarks/run_matrix.py --config ../pecan-matrix.json --skip-prepare
python3 examples/extensions/benchmarks/summarize.py \
  --evidence ../pecan-review-evidence --output ../pecan-review-summary
```

The base template covers 150 cells: the twelve reference/advanced-unfused
combinations on sparse 10k/100k/1m
graphs with process workers, 100k locally, three repetitions, and a separate
chain-512 WCC diagnostic. It uses 8 CPUs/32 GiB/no swap per fresh container.
Check capacity with a separate pilot configuration before starting large cases;
keep its `run_id`, container root and host output distinct. Task-slot or native
admission failures are outcomes, not permission to rewrite successful cells under
different limits. A changed resource configuration starts a separate matrix.

Read `tables.md` for median/range time and memory, `cells.csv` for every outcome,
and `summary.json` for counts and integrity checks. Retain nonconvergence,
timeouts, memory/admission failures and cells not run. The bounded chain is
expected to exceed reference minimum-label's 100-round cap. Resume an unchanged
interrupted matrix with `--skip-prepare --resume`; never replace its failures
with smaller inputs. Use new output paths for a separate run or summary.

End-to-end time includes input reads, conversion, native staging/CSR, iterations,
materializations and result Parquet; startup, reference generation, verification
and cleanup are outside it. RSS sums shared pages per process; PSS apportions
them. Samples can miss brief peaks. Cgroup peak includes cache and lifetime
activity; the pre-verification peak still includes startup. Native admission
statistics are not RSS/PSS. Checksum reads warm the filesystem cache, and VM-wide
steal/background load must accompany timings. See the [benchmark protocol](README.md)
for exact validation/error bounds and evidence interpretation.

The [fusion supplement](README.md#run-all-methods) adds 78 WCC cells with fused
and contemporaneous unfused controls on all three paths. It uses its own output
root and the same CPU, memory, input and validation settings. Build the current
Nutmeg wheel before running it; an earlier wheel lacks `wccRandomizedFused`.

Fusion removes a priority table and two joins per relational contraction round,
but recomputes priorities per edge row; it also defers initial edge deduplication.
Banda already handles both endpoints in
one loop; its fused option skips only the initial kernel sort, retaining native
staging/CSR. Round-one edge counts can differ because duplicates are retained
until the first contraction. Later contracted graphs and final labels must
agree for the same seed. Read the measured supplement before concluding that
fusion improves a particular workload.
