# Pecan and Nutmeg: PageRank/WCC benchmark

The [completed benchmark report](../../../docs/development/extensions/pecan-nutmeg-benchmark.md)
links every result, time/memory table, source identity and retained failure.
For installation and executable examples, use the [step-by-step tutorial](TUTORIAL.md).

This harness measures complete algorithm calls through Spark Connect against
the same Sail executable, input Parquet files, and resource envelope. It retains
successes, mismatches, errors, nonconvergence, timeouts, and memory-limit failures.

| CLI engine | Name | Execution |
| --- | --- | --- |
| `pecan` | Pecan | Python controller; Sail/DataFusion relational iterations |
| `nutmeg-native` | Nutmeg Banda | Native kernels over a driver-resident staged graph/CSR |
| `nutmeg-datafusion` | Nutmeg Grenada | Nutmeg `GraphTables`, adapted to Pecan's relational controller |

Grenada currently shares Pecan's PageRank/WCC implementation. It measures the
graph-table entry path and is not an independent third algorithm implementation.
It does not stage a native graph or build CSR. The names distinguish execution
paths; the installed native package remains `sail-nutmeg`.

## Algorithms and accuracy

Every existing method remains available. `--variant` chooses the implementation:

| Algorithm | Variant | Pecan / Grenada method | Banda kernel |
| --- | --- | --- | --- |
| PageRank | `reference` | `power` | `pagerank` (Grust 0.23.0) |
| PageRank | `optimized` | `delta` | `pagerankDelta` (local addition) |
| WCC | `reference` | `min_label` | `wcc` (Grust 0.23.0 union-find) |
| WCC | `optimized` | `randomized` | `wccRandomized` (local addition) |
| WCC | `fused` | `randomized_fused` | `wccRandomizedFused` (local addition) |

"Optimized" identifies the new implementation, not a claim that it is faster
on a particular workload. The two original WCC methods are different algorithms.
The new native kernels live in the vendored Nutmeg integration; no published
Grust crate was changed. See the [kernel contracts](../vendor/nutmeg-graph/OPTIMIZED_ALGORITHMS.md).

All PageRank methods use float64, damping 0.85, uniform initialization/restart,
uniform dangling redistribution, and tolerance `1e-8`. Duplicate edges contribute
separately. Reference methods stop on successive-iterate L1 change. Delta methods
retain signed residuals at inactive vertices, push an active frontier, and allow
reactivation. They normalize the output and certify its true fixed-point L1
residual before reporting convergence. The benchmark independently recomputes
that same final residual for **every** method and requires it to be at most the
tolerance plus `1e-12` numerical roundoff slack. Native `converged=false` and client convergence exceptions are retained
as nonconverged outcomes. Native parallelism is explicitly requested.

Delta PageRank follows the frontier motivation of
[GraphX](https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/PageRank.scala),
with an explicit global certificate for probability-normalized scores. It is not
GraphX's local-threshold stopping rule. Native delta traverses active adjacency;
the relational frontier join can still scan all Parquet edges. `active_edges`
counts propagated messages, not physical reads avoided. All materializations,
frontier statistics and certificates remain inside the measured algorithm call.

Minimum-label WCC needs up to the component diameter in propagation rounds,
followed by a no-change check. An ordered chain of V vertices takes V rounds
including that check. Randomized WCC instead contracts closed neighborhoods using
seeded GF64 affine priorities, retains representative maps, and expands final
labels. Its expected query-round bound is logarithmic; each run still has an
explicit cap. See [Bögeholz et al.](https://arxiv.org/abs/1802.09478).
Both new paths share the coefficient stream and seed, with original vertex IDs
kept as representatives. The benchmark records every contraction's graph size.

The advanced WCC algorithm has a second, explicit fused plan, adapted from
[graphframes-rs PR 56](https://github.com/SemyonSinchenko/graphframes-rs/pull/56/files).
Pecan/Grenada aggregate forward/reverse edge projections with `min_by` to retain
original representative IDs, removing a priority table and two joins per round.
They compute neighbor priorities per edge row instead of looking up one stored
priority per active vertex, trading repeated GF64 work for fewer joins/writes.
Both the relational and native fused plans defer initial edge deduplication until
after the first contraction. Banda already examines both endpoints in one edge
loop; its change removes only that initial sort. Native staging and projection
are unchanged. Duplicate-heavy input can increase first-round work. Original
randomized methods remain available; fusion is a measured alternative, not an
assumed speedup. The first fused `edges_before` includes duplicate non-loop rows;
the contracted graph and subsequent round traces match the unfused method.

Native reference WCC labels reflect lexicographic staging order of string IDs; Pecan labels
use numeric IDs. Outside the timed interval, normalize each output partition to
its minimum numeric vertex ID and compare every vertex with independent
union-find results. Casting the native label to BIGINT alone is insufficient.

Independent NumPy PageRank reference scores are also generated before measurement.
Validation checks every output ID, uniqueness, finite nonnegative scores,
normalization and full-vector L1/max-absolute errors. Against the precomputed
power reference, the conservative comparison bound is `2*d*t/(1-d)` for power
and `(1+d)*t/(1-d)` for delta, plus `1e-12` for numerical roundoff. The separate
true-residual certificate bounds stationary error by `residual/(1-d)` for every
method. Receipts retain actual errors and residuals, not only pass/fail.

## Timing and memory boundaries

Each trial starts a fresh Sail server and, in `process-cluster` mode, two worker
processes. Session creation and a `SELECT 1` readiness query precede timing.
The timer starts with input DataFrame handles and ends when the complete output
Parquet write returns. It includes input reads, API validation, representation
conversion, native staging/CSR construction when applicable, algorithm execution,
and result writing. Correctness validation and cleanup follow the timer.

Native `run()` is lazy: the output write executes the kernel exactly once.
Pecan returns an already materialized result, which is then exported to an
independent output path. Both paths therefore deliver a caller-owned Parquet
result. Pecan's own intermediate materialization remains part of its cost.
`output_action_seconds` includes native execution for Banda, so it is not a
kernel-only or directly comparable export-only number.

Dataset checksum reads precede timing and warm the OS page cache. No cache flush
is performed. Fresh Sail state does not mean cold filesystem cache. Binary and
dataset identities, source commits, package versions, settings, and outcomes are
recorded in every receipt.

Run one trial per fresh Linux container, with a private PID namespace, `--init`,
8 assigned CPUs, 32 GiB RAM and no swap. Run trials sequentially without builds
or other benchmark trials in parallel. Measurements include the Python
controller, driver, workers, sampler, and container init:

- Sampled RSS sums count shared mappings once per process.
- Sampled PSS apportions shared mappings. Missing PSS is unavailable, never zero.
- Kernel cgroup `memory.peak` includes the container's entire lifetime, imports,
  checksum reads, verification and filesystem page cache. It is not an
  algorithm-only allocation count. `cgroup_execution_after.memory.peak`, captured
  before verification, separately reports the lifetime peak through result delivery.
- Baseline and execution-phase samples are separate. Samples spanning phase
  changes are excluded from phase peaks. Sampled peaks can miss brief spikes;
  an unsampled phase has no reported process-memory peak.
- `/proc/stat` steal is measured across the whole Linux VM, not just the trial's
  assigned CPUs. Record physical-host background load and the VM configuration.

All three paths use the same installed Sedona and Nutmeg extensions. The Sail session
on the driver/local server prepays Nutmeg's 8 GiB allowance (`--native-quota 8589934592`) from its
16 GiB participating memory pool (`--sail-pool-bytes 17179869184`), even in
Pecan/Grenada trials. This reservation is not 8 GiB of allocated RSS. It leaves a
nominal 8 GiB for other participating operators on the driver/local server. The
receipt `remaining_participating_df_budget_bytes` names this remainder. Process
workers load scalars without binding driver-native Nutmeg sessions or prepaying
their quota. Separate processes have separate Sail pools; their nominal limits can sum above the
container's unchanged 32 GiB aggregate hard bound. Native diagnostics are
recorded separately from RSS/PSS.

`--worker-task-slots 32` provides concurrent asynchronous task capacity per
worker: two workers admit up to 64 task slots. These slots are not CPU cores or
OS worker threads. A relational stage may need more live tasks than CPU threads
while tasks exchange data or wait. `--threads 8` and the container's aggregate
eight-CPU quota remain separate limits. Worker task slots, Sail pool size and
native allowance are explicit configuration/receipt fields and export integrity
checks verify them. Invalid quotas that consume the whole participating pool
are rejected before launching a server.

Changing an admission setting requires a new configuration and output root;
preserve previous refusal/error outcomes under their original envelope. Extra
capacity is not evidence that every graph will fit: qualify representative large
cases before starting the full comparison matrix.

## Generate inputs

Use the locked extension Python environment. Generation and reference calculation
must run outside the measured container. Choose a fresh directory:

```bash
.venv/bin/python examples/extensions/benchmarks/graph_fixtures.py \
  --output /absolute/path/data/sparse-100000 --vertices 100000 \
  --family sparse --degree 8 --block-size 1024 --seed 20260927
```

The sparse family has component-separated directed graphs, a random recursive
tree in each component, skewed source selection, additional random edges,
isolates, dangling vertices, self-loops, and duplicate edges. IDs follow generation
order: early vertices have higher source-selection weights; IDs are not randomly
permuted. Components are bounded by `--block-size`, which matters for WCC diameter
and contraction work. Preserve these parameters in comparisons. This synthetic
family does not characterize arbitrary ID assignments or giant connected graphs.

For a separate diameter stress test:

```bash
.venv/bin/python examples/extensions/benchmarks/graph_fixtures.py \
  --output /absolute/path/data/chain-512 --vertices 512 --family chain
```

Run large sparse cases and bounded chains as distinct result groups. A chain
iteration limit or timeout is retained, not replaced by a shorter successful
chain or silently removed from summaries.

## Run a cell

Build an optimized Sail executable and Nutmeg wheel from a recorded immutable
source revision. Install Pecan and both native wheels in the locked environment.
Inside a fresh container, mount immutable source, those artifacts, inputs, and
a writable results directory. For example:

```bash
.venv/bin/python examples/extensions/benchmarks/graph_cell.py \
  --sail-binary /absolute/path/to/release/sail \
  --runtime-source-sha SOURCE_SHA_USED_TO_BUILD_SAIL \
  --native-source-sha SOURCE_SHA_USED_TO_BUILD_NUTMEG \
  --dataset /absolute/path/data/sparse-100000 \
  --output /absolute/path/results/pecan-pr-repeat-0 \
  --engine pecan --algorithm pagerank --variant optimized --mode process-cluster \
  --threads 8 --partitions 8 --worker-task-slots 32 \
  --sail-pool-bytes 17179869184 --native-quota 8589934592 --repeat 0
```

Use `nutmeg-native` or `nutmeg-datafusion` for the other entry paths, and `wcc`
for connected components. Keep identical settings and inputs. Use at least
three repetitions, vary order reproducibly, and retain every cell. Report median
and range, with outcomes separate from performance statistics. Local and
process-worker modes are distinct execution classes.

## Run all methods

Copy [`matrix.example.json`](matrix.example.json) outside the frozen checkout and
fill in the immutable image, source, binary, wheel-source and host paths. The
runner validates the configuration and creates a reproducible shuffled order:

```bash
python examples/extensions/benchmarks/run_matrix.py --config /private/matrix.json --dry-run
python examples/extensions/benchmarks/run_matrix.py --config /private/matrix.json --prepare-only
python examples/extensions/benchmarks/run_matrix.py --config /private/matrix.json --skip-prepare
# After interruption, retain finished cells, including failures:
python examples/extensions/benchmarks/run_matrix.py --config /private/matrix.json --skip-prepare --resume
```

The example covers all three entry paths and both variants of both algorithms:
10k, 100k and 1m vertices in process-worker mode, 100k in local mode, three repeats,
plus a separate 512-vertex WCC chain at a 100-round cap (150 cells total). The
reference min-label chain cells are expected to be nonconverged; they remain
failures to finish within that envelope. Native reference WCC has no round cap.
Randomization order, graph seed and algorithm seed are independent recorded
parameters. Run pilots in a separate output root; do not replace measured failures.

The [fusion matrix](fusion-matrix.example.json) is a separate 78-cell WCC
comparison: all three paths, `optimized` and `fused`, the same sizes/modes and
three repetitions, plus six chain diagnostics. It shuffles fused and unfused
controls together. Use a new native wheel containing `wccRandomizedFused` and
its recorded source revision. Keep the original 150-cell matrix intact.
To configure the supplement after filling the main configuration:

```bash
python3 - <<'PY'
import json
from pathlib import Path
config = json.loads(Path('../pecan-matrix.json').read_text())
fusion = json.loads(Path('examples/extensions/benchmarks/fusion-matrix.example.json').read_text())
config.update(run_id='pecan-fusion-review', variants=fusion['variants'], suites=fusion['suites'],
              container_root='/targets/pecan-fusion-review-measurements',
              host_output=str(Path('../pecan-fusion-review-evidence').resolve()))
Path('../pecan-fusion-matrix.json').write_text(json.dumps(config, indent=2) + '\n')
PY
python3 examples/extensions/benchmarks/run_matrix.py --config ../pecan-fusion-matrix.json --dry-run
python3 examples/extensions/benchmarks/run_matrix.py --config ../pecan-fusion-matrix.json --prepare-only
python3 examples/extensions/benchmarks/run_matrix.py --config ../pecan-fusion-matrix.json --skip-prepare
python3 examples/extensions/benchmarks/summarize.py \
  --evidence ../pecan-fusion-review-evidence --output ../pecan-fusion-review-summary
```

These commands assume both configurations use the same freshly built branch
revision and wheel. If artifacts differ between runs, record each actual source
and artifact identity. Comparisons of fusion use the supplement's own unfused
controls, with identical installed wheels, limits, inputs and timing boundaries.

Export every cell plus tables of median/range statistics for passed cells:

```bash
python examples/extensions/benchmarks/summarize.py \
  --evidence /absolute/path/to/pecan-review-evidence \
  --output /absolute/path/to/new-summary-directory
```

`cells.csv` retains failures and missing cells, `summary.json` records all outcome
counts and metric sample counts, and `tables.md` presents the comparisons. Missing
memory values remain unavailable. Incorrect or unfinished results never enter
successful time or memory statistics. An apparent pass whose receipt disagrees
with its planned cell, source, or artifact identities becomes `integrity_error`;
CSV and JSON retain its original outcome and explicit reasons. Conflicting Sail
binary or installed native-file hashes invalidate every affected passed trial;
dataset hashes must agree across trials of the same dataset. A receipt without
its matrix summary is `incomplete_record`, not a successful or never-run cell.
The exporter writes the complete report and exits nonzero when it finds an
integrity error.

Each cell emits `receipt.json`, `memory-samples.jsonl`, `server.log`, and complete
result Parquet files on success. An outer container timeout/memory failure must
also be recorded even if the process cannot write its receipt. `--allow-dirty`
and `--allow-unisolated` exist only for functional harness development; those runs
must not be published as benchmark measurements.
