# Test Pecan graph algorithms on Sail

This tutorial builds the extension branch and tests Pecan's PageRank and weakly connected
components (WCC) locally, with worker processes, and across two hosts. The Python
client controls iterations; Sail/DataFusion executes the joins, aggregates and
Parquet writes. PageRank retains power iteration and adds delta/frontier execution;
WCC retains minimum-label propagation and adds seeded randomized contraction
with explicit unfused and fused plans.
The existing methods remain the defaults. The
[all-path tutorial](../benchmarks/TUTORIAL.md) also runs Nutmeg Banda and Grenada,
including all fifteen valid method combinations and the isolated benchmark.

Commands use Bash and run from the checkout root unless stated otherwise. Steps
1–5 are the automated review path. Steps 6–7 provide an interactive example;
step 8 tests two physical hosts. No Spark JVM or separate graph service is needed.

## 1. Get and freeze the source

```bash
git clone --branch work/extensions-datafusion-graphs https://github.com/querygraph/sail.git sail-graph-review
cd sail-graph-review
git switch --detach
git rev-parse HEAD
```

Record the printed SHA. Keep that checkout unchanged while building/testing.
The older `sail-extensions-1` tag and released Sail wheels do not contain this
graph client and host utilities service.

## 2. Prepare a build environment

Choose one option. The build needs Python 3.12 with its shared library, Rust
1.97.1, a C/C++ toolchain, protobuf compiler and headers, GEOS 3.12+, and uv.
Allow tens of GiB of free disk for the Rust build directories; check available
space before starting.

### macOS

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

Use matching architectures for Rust, Python and native wheels. On Apple Silicon,
use an arm64 terminal and interpreter for a native build.

### Linux or a Linux Docker VM

The checked-in image supplies the build prerequisites. From the host checkout,
with a running Linux Docker engine:

```bash
docker build -t sail-graph-review-build -f examples/extensions/scripts/Dockerfile.linux .
docker volume create sail-graph-review-work
docker run --name sail-graph-review -it \
  -p 127.0.0.1:50051:50051 \
  -v sail-graph-review-work:/work \
  sail-graph-review-build bash
```

Inside the container, clone into the Linux volume. Replace `SOURCE_SHA_FROM_STEP_1`
with the SHA recorded on the host:

```bash
git clone --branch work/extensions-datafusion-graphs https://github.com/querygraph/sail.git /work/sail
cd /work/sail
git switch --detach SOURCE_SHA_FROM_STEP_1
export SAIL_EXTENSION_PYTHON="$(uv python find 3.12)"
df -h .
```

Run subsequent commands inside this container. For a second terminal, use
`docker exec -it sail-graph-review bash`, then `cd /work/sail`. The image also
provides the prerequisite inventory for native Linux. Keep source and build
output in the Linux volume when using Docker on a Mac.

## 3. Build Sail and install the clients

Use a dedicated environment: the script synchronizes `.venv` to the dependency
lock, then installs the two native extension wheels and Pecan (`pyspark-pecan`).

```bash
unset PYTHONHOME PYTHONPATH DYLD_LIBRARY_PATH LD_LIBRARY_PATH
export CARGO_BUILD_JOBS=4
bash examples/extensions/scripts/build.sh
.venv/bin/python - <<'PY'
from importlib.metadata import version
from pyspark_pecan import GraphAlgorithms, GraphUtils
for name in ("pyspark", "protobuf", "pyspark-pecan",
             "sail-sedona-extension", "sail-nutmeg"):
    print(name, version(name))
PY
```

Expected artifacts:

| Artifact | Path |
| --- | --- |
| Sail executable | `target/extensions-poc/host/debug/sail` |
| Python interpreter | `.venv/bin/python` |
| Native wheels for distribution | `target/extensions-poc/wheels/` |

The locked client versions include PySpark 4.0.1 and protobuf 7.36.2. The graph
client and the two native extensions are version 0.1.0. The utils service is
compiled into this Sail executable; it is not another native wheel to install.

## 4. Run the graph correctness suite

Start in a fresh terminal at the checkout root. These commands start and stop
their own servers on temporary ports and create their own staging directories.
Do not start a manual server for them.

```bash
unset PYTHONHOME PYTHONPATH DYLD_LIBRARY_PATH LD_LIBRARY_PATH
review_output=$(mktemp -d /tmp/sail-graph-checks.XXXXXX)
printf 'Results: %s\n' "$review_output"
(
  set -e
  for mode in local local-cluster process-cluster; do
    .venv/bin/python examples/extensions/scripts/test_graph_algorithms.py \
      --sail-binary "$PWD/target/extensions-poc/host/debug/sail" \
      --execution-mode "$mode" --output "$review_output/$mode"
  done
)
```

Require a successful pytest summary and a `portable-graphs: PASSED <mode>` line
for **each** mode. A failure
stops this block with a nonzero exit status; it is not a pass for the remaining
modes. Each output directory must be new, so generate a new `review_output`
when repeating the run.

| Mode | What it exercises |
| --- | --- |
| `local` | One server process |
| `local-cluster` | Driver and worker actors in one process |
| `process-cluster` | Driver plus two separate worker executables on one host |

The suite checks both PageRank methods and all three WCC methods: exact WCC labels and independently
calculated PageRank scores,
including sinks, isolates, duplicates, self-loops, empty graphs, invalid inputs,
convergence limits, cancellation and result ownership. Delta tests also require
frontier shrinkage and later reactivation, and validate the final global residual
against an independent linear solve. Randomized WCC tests check multiple seeds and
chain contraction. Each mode's server log
is at `$review_output/<mode>/server/server.log`. Successful shutdown leaves no
Parquet files in that mode's `staging` directory; empty directories may remain.

## 5. Test the protocol and existing extensions

The protocol tests exercise real Connect requests, filesystem boundaries,
idempotent allocation/removal, debug-log token protection, cancellation during a
write, and cleanup after a client is killed. They also start their own servers:

```bash
.venv/bin/python -m pytest \
  examples/extensions/tests/test_graph_utils.py \
  examples/extensions/tests/test_graph_logging.py \
  --sail-binary "$PWD/target/extensions-poc/host/debug/sail" \
  --execution-mode process-cluster -q
```

To include Sedona, Nutmeg and the other extension regression tests, run the full
directory instead:

```bash
.venv/bin/python -m pytest examples/extensions/tests \
  --sail-binary "$PWD/target/extensions-poc/host/debug/sail" \
  --execution-mode process-cluster -q
```

Repeat with `local` or `local-cluster` to review those modes. At the recorded
revision the full suite has 70 passes in each worker mode; local has 69 passes
and one skip for a test that requires worker topology. Counts can change with
later commits: require no failures and inspect any skips.

## 6. See PageRank and WCC results interactively

In **terminal A**, from the checkout root, prepare a dedicated local staging
directory and the embedded Python environment:

```bash
mkdir -p /tmp/sail-graph-review-staging
export SAIL_GRAPH_UTILS_ROOT=file:///tmp/sail-graph-review-staging
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

Keep the server in the foreground. In Docker, use `--ip 0.0.0.0` if connecting
through the published host port; clients inside the container can use loopback.
The staging directory must already exist and be exclusively managed by Sail.

In **terminal B**, from the same checkout, probe the service:

```bash
.venv/bin/python - <<'PY'
from pyspark.sql.connect.session import SparkSession
from pyspark_pecan import GraphUtils
spark = SparkSession.builder.remote("sc://127.0.0.1:50051").create()
try:
    utils = GraphUtils(spark)
    print(utils.engine, sorted(utils.capabilities), utils.root)
    assert {"fs", "owned_runs_v1"} <= utils.capabilities
    print(spark.sql("SELECT gf_version() AS version").first().version)
finally:
    spark.stop()
PY
.venv/bin/python examples/extensions/graph-algorithms/examples/run.py \
  --remote sc://127.0.0.1:50051 --partitions 4 --iterations 10
```

The probe reports engine `sail`, capabilities including `fs`, `owned_runs_v1`
and `axpb`, and scalar version `sail-gf-utils/1`. On macOS the root may be
canonicalized from `/tmp` to `/private/tmp`.

The example uses vertices `0,1,2,3,9` and edges `0→1,1→2,2→0,2→3`.
It prints two tables. Expected answers, rounded for display:

| id | PageRank after 10 iterations | WCC component |
| --- | --- | --- |
| 0 | 0.197173 | 0 |
| 1 | 0.245113 | 0 |
| 2 | 0.283858 | 0 |
| 3 | 0.197173 | 0 |
| 9 | 0.076684 | 9 |

PageRank scores sum approximately to one. Vertex 9 remains an isolated
component. The PageRank run uses a fixed iteration count; it makes no convergence
claim. The example closes its result contexts and session, releasing its staging.

To exercise the optimized methods on the same graph and server:

```bash
.venv/bin/python examples/extensions/graph-algorithms/examples/run.py \
  --remote sc://127.0.0.1:50051 --partitions 4 \
  --pagerank-method delta --tolerance 1e-8 --iterations 1000 \
  --wcc-method randomized --seed 42 --wcc-iterations 100
```

Expect the same WCC labels. Delta prints its push count and final global residual,
which must be at most `1e-8`; its normalized scores approximate stationary
PageRank rather than the fixed-ten-step values above. It retains unsent residual
and allows reactivation; its activity threshold scales with the requested
tolerance, vertex count and current score mass. The final full residual check
remains the convergence criterion. `--tolerance` on `--pagerank-method power` instead controls
the L1 difference between successive iterates. Omitting tolerance on power keeps
the fixed-step behavior. Cap exhaustion raises an error, not a convergence claim.
Randomized WCC requires the `axpb` capability reported by the probe.

Delta's active-edge count measures propagated messages; a Sail join may still
scan the full Parquet edge input. Use the [benchmark guide](../benchmarks/README.md)
for time/memory comparisons of **all retained and optimized methods** through
Pecan, **Nutmeg Banda** (native staged kernels), and **Nutmeg Grenada** (Nutmeg graph
tables using Pecan's controller). Grenada shares Pecan's algorithms.

## 7. Run the same example with separate workers

Stop terminal A's server with Ctrl-C. In that terminal retain the exports from
step 6 and start:

```bash
SAIL_MODE=local-cluster SAIL_EXPERIMENTAL_PROCESS_WORKERS=1 \
  target/extensions-poc/host/debug/sail spark server --ip 127.0.0.1 --port 50051
```

Repeat terminal B's probe and either example unchanged. Expect the corresponding
answers and certificates from step 6. The
server log reports worker PIDs different from the driver PID. A local file root
works here because all processes use the same host filesystem. Stop the server
before proceeding to the supervised two-host test.

## 8. Test two physical hosts

Call the driver/controller **A** and the remote worker **B**. The harness runs
one worker on each host. Use compatible OS, architecture, Python 3.12 and system
libraries. Build once on A and distribute that executable and those repaired
native wheels; the harness requires byte-identical artifacts and matching clean
source checkouts. Do not copy a virtual environment between hosts.

### 8a. Install the same source and artifacts on B

On B, clone the branch and detach at A's recorded SHA:

```bash
git clone --branch work/extensions-datafusion-graphs https://github.com/querygraph/sail.git sail-graph-review
cd sail-graph-review
git switch --detach SOURCE_SHA_FROM_A
```

On A, create the binary and wheel directories on B, then copy the artifacts.
These example paths assume the B checkout is `/home/reviewer/sail-graph-review`
and its SSH alias is `worker-host`; replace both with your actual values:

```bash
ssh -o BatchMode=yes worker-host hostname
ssh worker-host 'mkdir -p /home/reviewer/sail-graph-review/target/extensions-poc/host/debug /home/reviewer/sail-graph-review/target/extensions-poc/wheels'
scp target/extensions-poc/host/debug/sail worker-host:/home/reviewer/sail-graph-review/target/extensions-poc/host/debug/sail
scp target/extensions-poc/wheels/*.whl worker-host:/home/reviewer/sail-graph-review/target/extensions-poc/wheels/
```

On B, from its checkout root, with Python 3.12 and uv installed:

```bash
chmod +x target/extensions-poc/host/debug/sail
uv venv --python 3.12 .venv
uv pip sync --python .venv/bin/python examples/extensions/requirements.lock
uv pip install --python .venv/bin/python target/extensions-poc/wheels/*.whl
uv pip install --python .venv/bin/python --no-deps examples/extensions/graph-algorithms
.venv/bin/python examples/extensions/sedona/scripts/smoke.py
git rev-parse HEAD
git status --porcelain
```

Both hosts must print the same SHA and empty status output. Both native wheels
are required by the qualification harness, including its portable-graph mode.

### 8b. Configure storage reachable from both hosts

Use an existing shared S3 bucket and a dedicated prefix. On **each** host create
a private JSON file outside the checkout, such as `/absolute/path/graph-store.json`:

```json
{
  "SAIL_GRAPH_UTILS_ROOT": "s3://YOUR_BUCKET/graph-review",
  "AWS_DEFAULT_REGION": "YOUR_REGION",
  "AWS_ACCESS_KEY_ID": "YOUR_ACCESS_KEY",
  "AWS_SECRET_ACCESS_KEY": "YOUR_SECRET_KEY"
}
```

Replace the placeholders. Add `AWS_SESSION_TOKEN` when using temporary AWS
credentials. For an S3-compatible service, also set `AWS_ENDPOINT` to its URL;
HTTP endpoints require `"AWS_ALLOW_HTTP": "true"`. Every value must be a JSON
string. Both hosts need read/write/list/delete access to the same prefix.
Use `chmod 600` on the file and keep it out of Git. A private `file:///tmp/...`
directory on A is insufficient for B's workers.

### 8c. Configure and run the launcher on A

```bash
cp examples/extensions/scripts/two-host.example.json ../sail-graph-two-host.json
```

Edit that JSON as follows:

- Set `repo`, `python` and `sail` to absolute paths on each target's host.
- The `driver` and first `workers` entry describe A; the second worker describes
  B and has its noninteractive SSH alias in `ssh`.
- Use reachable IPs/hostnames for `advertise`. Replace the template's `192.0.2.*`
  addresses; do not use loopback for communication between hosts.
- Add `"environment_file": "/absolute/path/graph-store.json"` to the driver
  **and both worker entries**, using the path on the corresponding host.
- Permit bidirectional traffic for the gateway and worker ports: template ports
  50152, 50161 and 50162. The controller also needs the Connect port, 50151.

Use a trusted private network; this launcher does not configure authentication
or TLS. For VMs, configure guest-reachable addresses and port forwarding.
With no manual server occupying those ports, run from A's checkout:

```bash
unset PYTHONHOME PYTHONPATH DYLD_LIBRARY_PATH LD_LIBRARY_PATH
.venv/bin/python examples/extensions/scripts/two_host.py \
  --config ../sail-graph-two-host.json --exercise portable-graphs \
  --output ../sail-graph-two-host-result-1
```

The default exercise runs power PageRank for three steps and minimum-label WCC.
Run the optimized methods separately, keeping the same source, artifacts and
storage configuration and choosing a new output directory:

```bash
.venv/bin/python examples/extensions/scripts/two_host.py \
  --config ../sail-graph-two-host.json --exercise portable-graphs \
  --pagerank-method delta --pagerank-iterations 1000 --tolerance 1e-8 \
  --wcc-method randomized --wcc-iterations 100 --seed 42 \
  --output ../sail-graph-two-host-result-optimized-1
```

The output directory must be new. The harness configures embedded Python,
starts the driver/workers, checks graph answers and task placement, and shuts
everything down. It does not leave an interactive cluster running.
The optimized exercise independently verifies delta's global residual and
stationary-score error and checks exact randomized WCC labels. Its iteration
windows retain frontier and contraction metrics. This is a functional distributed
test, not a performance measurement.

Inspect `../sail-graph-two-host-result-1/receipt.json`. Require:

- `outcome` is `passed`.
- Every inventory has the same source commit, binary hash and native packages.
- `checks.iteration_windows` records PageRank and WCC iteration stages, with
  matching `completed_worker_tasks` from both workers. The harness asserts this.
- `checks.methods` identifies the selected methods, limits, tolerance and seed.
  The optimized receipt also includes the independently checked PageRank residual
  and WCC contraction history.
- Every process in `process_cleanup` has `alive: false`.

Keep `server-and-workers.log` and the receipt, including on failure. For the
existing Sedona/Nutmeg two-host regression, repeat the command with
`--exercise extensions` and a different output directory.

## 9. Interpret cleanup and failures

Normal result closure, validation failures and cancellation between completed
writes remove staging immediately. An interrupted write reports
`cleanup_deferred=True` and `run_path`; the session retains ownership and attempts
cleanup when it closes or expires. Shutdown cleanup is best effort, with no
universal writer-drain barrier or crash-persistent orphan catalog. Empty
directories are harmless; remaining objects after writers have stopped can
require administrative cleanup.

| Symptom | Check |
| --- | --- |
| `graph utils Ping failed` | Correct branch binary, extension opt-in, valid precreated root, server log |
| `No module named pyspark_pecan` | Use `.venv/bin/python`; rerun the pure-client install command from step 8a if needed |
| `libpython` or interpreter startup error | Use step 6's runtime settings for manual startup; clear old overrides before automated tests |
| Missing `google/protobuf/any.proto` | Install protobuf headers as well as `protoc`; the Linux image includes both |
| Existing output directory | Pick a new test output directory; preserve the failed run's logs |
| Two-host identity mismatch | Same clean Git SHA, copied executable and identical installed native wheel contents |
| Worker cannot read staging | Same shared URI and credentials on all targets; `environment_file` must be set on every entry |
| `randomized WCC requires the axpb capability` | This branch's scalar implementation must be available on driver and workers; no hash fallback is used |
| `ConvergenceError` | Keep the selected method and tolerance visible; raise its iteration cap if appropriate, and retain failed benchmark outcomes |
| Compilation runs out of memory/disk | Check the actual host or VM limits and lower `CARGO_BUILD_JOBS` |

Include the Git SHA, OS/architecture, mode, command, outcome and logs in review
feedback. The [implementation review](../../../docs/development/extensions/portable-graph-validation.md)
contains the recorded qualification and downloadable evidence. The [API guide](README.md)
specifies algorithm options, result ownership and the versioned utils protocol.
