# Pecan, Banda and Grenada: graph algorithms on Sail

This report covers PageRank and weakly connected components (WCC), retaining
reference implementations and adding delta/frontier PageRank, randomized
contraction WCC, and an explicit fused contraction plan. The
[build-and-run tutorial](../../../examples/extensions/benchmarks/TUTORIAL.md)
runs all fifteen path/method combinations locally and with Sail workers.

All methods are implemented and runnable. The two final matrices contain
**228 trials: 226 passes and two expected convergence caps**. On the tested
bounded-component sparse graphs, the reference methods take less elapsed time
than delta PageRank or unfused contraction. The fused relational WCC plan takes
10–26% less median time than its matched unfused controls with process workers,
but its 1m-vertex sampled peak PSS is 28–33% higher. These are workload-specific
observations, not new default-selection rules. Every result and retained failure
is available below.

## What is implemented

| Name | Entry point | Where graph work runs |
| --- | --- | --- |
| Pecan | `pyspark_pecan.GraphAlgorithms` | Python controls iterations; Sail/DataFusion executes relational queries and writes Parquet state. |
| Nutmeg Banda | `sail_nutmeg.Nutmeg.stage/run/drop` | Staging gathers the graph on Sail's driver; native kernels operate on its admitted projection/CSR. |
| Nutmeg Grenada | Nutmeg `GraphTables` adapted to `GraphAlgorithms` | The same Pecan controller and Sail/DataFusion operations, without native staging or CSR. |

Grenada is a separate graph-table entry path, not an independent third algorithm
implementation. Banda is inside Sail, in the driver's process. It is not a
separate graph service. Its additional graph representation has explicit memory
admission, but neither that accounting nor DataFusion's pool covers every byte
of process RSS.

| Algorithm | Flavor / benchmark selector | Pecan and Grenada method | Banda kernel |
| --- | --- | --- | --- |
| PageRank | Reference / `reference` | `power` | Grust 0.23.0 `pagerank` |
| PageRank | Advanced / `optimized` | `delta` | Local `pagerankDelta` |
| WCC | Reference / `reference` | `min_label` | Grust 0.23.0 `wcc` union-find |
| WCC | Advanced / `optimized` | `randomized` | Local `wccRandomized` |
| WCC | Advanced fused / `fused` | `randomized_fused` | Local `wccRandomizedFused` |

“Advanced” describes the added implementation, not a performance guarantee.
The CLI retains the selector `optimized`. There is no fused PageRank alias.
Pecan's defaults remain `power` and `min_label`; every earlier method is retained.
The native additions live in the vendored Nutmeg integration. No published Grust
crate was changed. The client distribution is `pyspark-pecan`; old
`pyspark_graph_algorithms` imports remain compatibility aliases.

### PageRank contracts

All measured methods use float64, damping 0.85, uniform initialization/restart,
uniform dangling redistribution, tolerance `1e-8` and a 1,000-iteration cap.
Duplicate edges contribute separately; isolates and self-loops are retained.
Reference methods stop on successive-iterate L1 change. The benchmark separately
checks the normalized output's true fixed-point L1 residual for every method.

Delta PageRank keeps signed residuals at inactive vertices and permits
reactivation. If `R` is residual L1 mass, `m` is score mass, and `N` is vertex
count, the frontier cutoff is `min(R/(2N), tolerance*m/(4N))`. Scores are
normalized and the full fixed-point residual is recomputed before convergence
is reported. The maintained-residual bound is only a trigger for that check.
In exact arithmetic, stationary L1 error is at most `residual/(1-damping)`.

This follows the active-message motivation of
[GraphX PageRank](https://github.com/apache/spark/blob/master/graphx/src/main/scala/org/apache/spark/graphx/lib/PageRank.scala),
with an explicit global accuracy contract rather than GraphX's local threshold
semantics. A frontier need not decrease every round. Native delta traverses
active adjacency; relational joins may still read all edge Parquet. Recorded
`active_edges` measures messages, not physical reads avoided. Native delta
builds an additional admitted outgoing index because Grust's public projection
API does not expose CSR access. Its fixed 4,096-source reduction blocks preserve
summation grouping across thread counts.

### WCC contracts and Sem's fusion

WCC treats edges as undirected. Minimum-label propagation can need component
diameter plus one rounds, including its no-change check. An ordered chain of
`V` vertices therefore takes `V` rounds. Native reference union-find is a
different algorithm and has no propagation-round cap.

Randomized WCC selects closed-neighborhood representatives with seeded GF64
affine priorities, contracts edges, retains mapping history and expands labels
in reverse. Both implementations use the same coefficient stream and preserve
original vertex IDs. The relational-contraction background is described by
[Bögeholz, Brand and Todor](https://arxiv.org/abs/1802.09478).
The logarithmic analysis assumes random coefficients; this implementation uses
seeded SplitMix64 for reproducibility. That analysis does not remove the explicit
cap or predict elapsed time on a particular graph.

The fused plan adapts
[graphframes-rs PR 56](https://github.com/SemyonSinchenko/graphframes-rs/pull/56/files),
pinned to
[`10715e28`](https://github.com/SemyonSinchenko/graphframes-rs/blob/10715e28d9f7c450e74881bcd4acce8dc99a250f/src/algorithm/connectivity/connected_components.rs).
Pecan/Grenada combine forward and reverse edge projections with `min_by` to
select neighbor IDs alongside minimum priorities. This removes the priority
table and two joins per round, plus the initial canonical-edge materialization:
exactly one initial write and one write per contraction round disappear.
Priorities are recomputed per edge row instead of once per active vertex, which
can increase GF64 work. GF64 priorities are injective for nonzero coefficients,
so duplicate rows cannot produce an ambiguous representative tie.

Banda already visits both endpoints in one edge loop. Its fused variant defers
the initial kernel sort/deduplication until after the first contraction. Native
input staging and CSR construction are unchanged. Both fused plans retain raw
non-loop edges initially, so duplicates can increase first-round work and the
first `edges_before` counter differs. Tests require identical representative
maps and later contracted graphs/traces. The earlier randomized plans remain
explicit controls; the upstream PR's speedup is not used as Sail evidence.

## Sail integration and memory ownership

These algorithm additions require **no further Sail host or scheduler changes**
beyond the previously delivered graph utilities. Source objects outside
`examples` and `docs` match runtime revision `70b0d1ca`. The host already supplies
opt-in `gf.utils.v1`, administrator-configured storage ownership, and
worker-registered `gf_axpb`. Algorithms use ordinary Connect joins, aggregates
and Parquet writes. The changes in this phase are client methods, local Nutmeg
kernels, tests and benchmark/tutorial code.

The [portable graph design](portable-graph-plan.md) and
[initial host validation](portable-graph-validation.md) describe those earlier
host changes. They reuse the relation/function registry and Sail's object-store
resolution layer. No graph scheduler, DataFusion fork, aggregate extension ABI
or raw Rust context crossing the native boundary is introduced. A compiled-in
server-side graphframes-rs controller remains a separate future option.

Pecan implements Sem's client-loop architecture, with explicit differences from
the original draft: a trusted `SAIL_GRAPH_UTILS_ROOT`, session/run tokens, typed
Arrow receipts, and bounded capability/storage operations. Utilities remain
compiled into Sail. A separate utilities wheel and a Spark JVM implementation
are not delivered. The `gf.utils.v1` wire contract is unchanged; the locked
client uses PySpark Connect 4.0.1 and protobuf 7.36.2, generated with protoc
36.1 / Python 7.36.1. This is a new graph API, not GraphFrames compatibility.

Pecan/Grenada inputs require unique, non-null BIGINT IDs and existing endpoints.
Properties are ignored. Vertices and edges are two snapshots rather than one
atomic read across mutable sources. A `GraphResult` owns its staged result;
exporting Parquet creates caller-owned output. Banda stages string IDs and uses
`stage/drop` ownership; its randomized WCC methods additionally require IDs that
parse uniquely as BIGINT. WCC history is retained until reverse expansion.
Interrupted writes can defer deletion while writers stop. Session cleanup is
best effort; there is no independent run TTL or persistent crash-recovery catalog.
Multiple physical hosts require shared storage.

DataFusion execution avoids mandatory native graph conversion, but joins,
shuffles, Parquet generations and page cache still use memory. Banda prepays a
quota from Sail's pool and accounts for admitted graph/kernel work. Reservation
is not RSS, and a shared process does not imply one universal allocator or one
physical copy. The measurements below distinguish these quantities rather than
assuming either architecture uses less memory.

## Measurement protocol

Morrobay was allocated to this work: an Intel iMac Pro, Xeon W-2191B, 36 logical
CPUs and 128 GiB RAM. Linux ran under Colima/QEMU with 24 guest CPUs and 64 GiB.
This is a Linux VM/container result, not bare-metal Linux. No other task build
or benchmark was run concurrently; ordinary macOS background activity and an
idle second Colima VM remained. Physical-host inventory and VM-wide steal are
retained in the evidence.

Each cell uses a fresh container with private PID namespace and `--init`,
8 guest CPU quota units on cpuset `0-7`, 32 GiB RAM, no swap and a 1,024-PID limit. Local mode has
one Sail execution process; process-worker mode has a driver and two workers
on the same physical host. Banda's kernel still runs only on the driver.
Every path loads the same Sedona/Nutmeg deployment; Sedona algorithms are not
measured here.

The fixed envelope is 32 asynchronous task slots per worker, a 16 GiB Sail
pool per process and an 8 GiB native allowance. The Connect session prepays
that allowance on the driver/local server; process workers load scalars without
binding driver-native Nutmeg sessions. The receipt
`remaining_participating_df_budget_bytes` is the driver/local pool remainder.
Slots are scheduling capacity, not CPU cores. Per-process pools do not override the 32 GiB aggregate cgroup
limit. Native concurrency and relational partition count are both 8. The local advanced
kernels cap pool width by the number of 4,096-source blocks; reference kernels
retain Grust's own parallel thresholds. Several phases, including randomized
WCC selection/sorting/expansion, remain serial.

The timer starts with lazy input DataFrame handles and ends after the complete
caller-owned output Parquet write. It includes input reads, validation,
conversion, staging/CSR when used, iterations, internal materializations,
per-iteration instrumentation, certificates and output writing. Startup, independent correctness
verification and cleanup are outside the timer. Native execution is lazy and
occurs inside the output write, so its output-action time is not export-only.
Dataset checksum reads precede measurement and warm filesystem cache; these
are not cold-cache results.

RSS and PSS are sampled totals over the controller, driver, workers, sampler
and init. RSS double-counts shared mappings; PSS apportions them. The sampler
waits 50 ms between scans, with scan time additional. Transition samples are
excluded and missing phase samples remain unavailable; short peaks can be
missed. Cgroup peak through result delivery is captured before verification,
but remains a lifetime peak including startup, checksum reads and page cache.
The later lifetime peak can include verification. Cached-page charges can remain
with the cgroup that instantiated them rather than follow each subsequent reader;
[Linux memory ownership](https://www.kernel.org/doc/html/v6.8/admin-guide/cgroup-v2.html#memory-ownership)
therefore further limits interpreting warm-cache cgroup peaks as per-call memory.
Native accounting peaks are separate from physical-memory measurements.

Sparse datasets use 10,000, 100,000 and 1,000,000 vertices, mean out-degree eight,
a recursive-tree component backbone, skewed/random extra edges,
5% isolates, duplicates, loops and dangling vertices. Components are bounded
by blocks of 1,024 vertices. IDs follow generation order without random
permutation. This workload does not establish behavior on giant components,
arbitrary ID layouts, dense graphs or real-world graph distributions.

| Vertices | Directed edges | Components | Isolates | Dangling vertices |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 80,000 | 510 | 500 | 1,165 |
| 100,000 | 800,000 | 5,093 | 5,000 | 11,830 |
| 1,000,000 | 8,000,000 | 50,928 | 50,000 | 118,296 |

The primary matrix has 150 cells: all twelve original combinations at three
sizes with process workers and at 100k locally, three repetitions each, plus
six chain-512 WCC diagnostics with a 100-round cap. The chain is a convergence
boundary test; failed propagation runs do not enter successful timing rankings.
The fusion supplement has 78 cells: both randomized plans on all three paths
at the same sizes/modes, with separate same-wheel unfused controls. Each phase
shuffles cells within each suite/repetition, using dataset/order seed 20260927
and WCC seed 42. Suites run in distributed, local, then chain order. The
supplement's own controls support its within-phase fusion comparison.

## Results

### Primary matrix: reference and advanced methods

The 150-cell primary matrix finished with **148 passes and two expected
nonconvergences**; there were no unexpected outcomes. All 144 sparse-graph
trials passed. The two nonconvergences are Pecan/Grenada minimum-label WCC on
chain-512 with a 100-round cap. VM-wide steal was recorded as 0.0% in all 150
cells; this does not measure host scheduling delay outside the Linux VM.

At **100k vertices / 800k edges with two process workers**, complete-call times
and sampled process memory were:

| Algorithm | Path | Method | Passes | Seconds | PSS MiB | RSS MiB | Cgroup MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| PageRank | Grenada | advanced | 3/3 | 16.66 [16.51, 17.11] | 2694.43 [2597.17, 2722.52] | 2832.91 [2736.61, 2861.58] | 2840.66 [2742.95, 2862.22] |
| PageRank | Grenada | reference | 3/3 | 12.17 [12.13, 12.30] | 2072.25 [2054.80, 2077.93] | 2213.63 [2194.78, 2214.75] | 2120.82 [2109.22, 2128.43] |
| PageRank | Banda | advanced | 3/3 | 1.38 [1.37, 1.38] | 767.87 [747.54, 818.25] | 892.68 [872.78, 944.16] | 702.40 [691.39, 704.04] |
| PageRank | Banda | reference | 3/3 | 0.92 [0.91, 0.94] | 758.49 [729.25, 785.29] | 883.05 [856.24, 914.19] | 691.79 [677.67, 695.51] |
| PageRank | Pecan | advanced | 3/3 | 16.74 [16.53, 16.82] | 2608.13 [2574.24, 2715.87] | 2748.19 [2715.64, 2855.22] | 2741.79 [2711.09, 2854.72] |
| PageRank | Pecan | reference | 3/3 | 12.19 [12.04, 12.23] | 2198.44 [2147.79, 2210.91] | 2339.10 [2285.23, 2343.89] | 2276.65 [2230.98, 2276.93] |
| WCC | Grenada | advanced | 3/3 | 3.71 [3.65, 3.95] | 1426.56 [1401.68, 1443.75] | 1562.39 [1538.16, 1579.07] | 1324.25 [1318.35, 1357.00] |
| WCC | Grenada | reference | 3/3 | 1.43 [1.41, 1.44] | 1366.71 [1322.76, 1374.52] | 1501.94 [1457.66, 1507.88] | 1290.73 [1228.71, 1300.49] |
| WCC | Banda | advanced | 3/3 | 0.87 [0.86, 0.91] | 728.02 [721.16, 771.09] | 855.65 [848.54, 895.22] | 635.94 [629.41, 669.51] |
| WCC | Banda | reference | 3/3 | 0.67 [0.66, 0.67] | 724.73 [695.79, 732.06] | 848.91 [822.21, 855.94] | 633.75 [626.22, 642.19] |
| WCC | Pecan | advanced | 3/3 | 3.71 [3.61, 3.72] | 1365.50 [1364.35, 1423.72] | 1505.28 [1500.41, 1560.17] | 1313.38 [1295.05, 1327.96] |
| WCC | Pecan | reference | 3/3 | 1.39 [1.39, 1.48] | 1314.69 [1308.64, 1339.59] | 1449.90 [1442.42, 1472.93] | 1239.71 [1222.26, 1252.66] |

Values are median [minimum, maximum], three repetitions. Cgroup MiB is the
lifetime peak through result delivery, not algorithm-only RSS. The
[full primary tables](pecan-nutmeg-benchmark/primary/tables.md) include every
size and local-mode result; [per-cell CSV](pecan-nutmeg-benchmark/primary/cells.csv)
retains all outcomes and metrics.

![Primary PageRank time and PSS](pecan-nutmeg-benchmark/figures/primary-pagerank-scaling.png)

![Primary WCC time and PSS](pecan-nutmeg-benchmark/figures/primary-wcc-scaling.png)

The added methods cost more elapsed time on these bounded-component fixtures.
At 100k, all PageRank paths use 57 reference iterations and 59 delta pushes;
relational minimum-label WCC needs four rounds and contraction needs nine.
At 1m, those counts are 59/62 for PageRank and four/eleven for WCC.
The observed delta runs propagate 45,820,538 edge messages at 100k and
477,240,350 at 1m, excluding their full certificates. The corresponding
reference step-count × edge-count totals are 45,600,000 and 472,000,000.
Later frontier shrinkage therefore does not reduce total messages here.

Memory also depends on method and size. At 1m, Pecan randomized WCC has median
sampled peak PSS 1,673.33 MiB versus 2,258.26 MiB for minimum-label, while times
are 9.47s versus 8.51s. Banda's corresponding PSS values are 1,918.85 and
1,762.25 MiB, with 8.44s and 6.81s elapsed. Relational PageRank PSS is
nonmonotonic between 100k and 1m; raw-sample reconstruction confirms the recorded
values, but no causal allocation explanation is claimed. The measurements do
not establish a general memory ordering between native and relational graphs.


### Matched fusion supplement

All 78 trials pass. These comparisons use newly built, same-wheel unfused
controls in the same phase; they do not compare a new fused binary only with
an earlier phase's control. With two process workers:

| Vertices | Path | Unfused seconds | Fused seconds | Fused / unfused median time | Unfused PSS MiB | Fused PSS MiB |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 10,000 | Pecan | 2.198 | 1.644 | 0.748 | 924.604 | 904.397 |
| 10,000 | Banda | 0.131 | 0.125 | 0.952 | 549.337 | 557.956 |
| 10,000 | Grenada | 2.188 | 1.698 | 0.776 | 929.128 | 869.569 |
| 100,000 | Pecan | 3.433 | 2.576 | 0.750 | 1385.647 | 1263.318 |
| 100,000 | Banda | 0.829 | 0.807 | 0.974 | 740.268 | 725.975 |
| 100,000 | Grenada | 3.497 | 2.596 | 0.742 | 1382.502 | 1277.760 |
| 1,000,000 | Pecan | 8.831 | 7.434 | 0.842 | 1721.359 | 2290.213 |
| 1,000,000 | Banda | 8.023 | 7.620 | 0.950 | 1918.264 | 1909.947 |
| 1,000,000 | Grenada | 8.706 | 7.807 | 0.897 | 1732.362 | 2214.545 |

The ratio is the **ratio of three-run medians**, not a paired-trial speedup.
The [full fusion tables](pecan-nutmeg-benchmark/fusion/tables.md) and
[per-cell CSV](pecan-nutmeg-benchmark/fusion/cells.csv) retain ranges, local-mode
results, RSS and cgroup peaks as well as PSS.

![Fused versus unfused WCC time and PSS](pecan-nutmeg-benchmark/figures/fusion-wcc-scaling.png)

For Pecan/Grenada, median time falls by 10.3–25.8% with process workers and
25.3–27.1% at 100k locally. At 1m, however, median sampled peak PSS rises from
1,721.36 to 2,290.21 MiB for Pecan and from 1,732.36 to 2,214.54 MiB for Grenada:
33.0% and 27.8% increases. Removing writes does not establish a process-memory
reduction. The allocation cause of this increase has not been isolated.

Banda's median time falls by 2.6–5.0% in the process-worker mode; its 100k local
difference is 1.0%. Its 10k/100k process-worker and 100k local timing ranges
overlap. Its PSS medians vary in both directions, including a 1.6% increase at 10k. The native plan change is smaller
and the complete call includes unchanged staging/CSR work. These observations
do not establish a general speed or memory advantage for fusion.

Reference defaults remain useful for these fixtures. Randomized contraction
addresses long propagation distances, as the separate chain check demonstrates.
Delta/frontier execution needs workloads where reduced total message work can
pay for its bookkeeping; that condition was not met in the measured sparse
PageRank cases. Giant components, varied ID permutations, real graph datasets
and multiple contraction seeds remain outside this measurement scope.

### Chain convergence diagnostic

Chain-512 has 511 edges. This single-repetition diagnostic uses a 100-round WCC
cap. Every randomized primary run finishes in nine contractions; minimum-label
reaches its cap. Times for nonconverged rows end at the cap error and do not
represent completed answers. Native reference WCC is union-find, with no
propagation-round cap.

| Path | Method | Outcome | Time boundary | Seconds | PSS MiB |
| --- | --- | --- | --- | ---: | ---: |
| Grenada | advanced | passed | completed output | 2.969 | 760.183 |
| Grenada | reference | nonconverged | until failure/cap | 9.725 | 789.921 |
| Banda | advanced | passed | completed output | 0.045 | 437.633 |
| Banda | reference | passed | completed output | 0.052 | 463.989 |
| Pecan | advanced | passed | completed output | 2.970 | 762.287 |
| Pecan | reference | nonconverged | until failure/cap | 9.093 | 777.437 |

Each primary native chain call has only one execution memory sample. Its sampled
PSS is retained but cannot establish the transient maximum. The fusion phase's
[chain diagnostic](pecan-nutmeg-benchmark/figures/fusion-chain-diagnostic.md)
records both plans separately.


Compact tables report medians; full tables and figures include observed ranges.
There are three repetitions except the chain diagnostic. Three repetitions describe these runs,
not statistical confidence or a universal ranking. Missing/failed trials are
kept distinct. Full tables and per-cell CSV retain all sizes, local mode,
chain outcomes, RSS, PSS, cgroup peaks, native accounting and VM-wide steal.

## Correctness, gates and retained failures

Independent NumPy power and union-find references are generated before timing.
Validation checks every vertex, uniqueness, nonnegative finite normalized ranks,
full-vector error, true residual and exact component membership. Native
reference WCC follows lexical staging labels, so component IDs are normalized
to the minimum numeric member outside measurement before comparison.

Every PageRank result must have true fixed-point L1 residual at most `1e-8`
plus `1e-12` roundoff slack. Against the independent power reference, the
conservative full-vector bound is `2*d*t/(1-d)` for reference methods and
`(1+d)*t/(1-d)` for delta, plus `1e-12`. Reported nonconvergence cannot become a
pass merely because a vector is numerically close. Native capped PageRank
returns `converged=false`; Pecan raises on its convergence cap.

The final Linux qualification covers 101 benchmark/launcher tests, 84 Pecan
tests in each execution mode, all fifteen tutorial methods in each mode, and
the exact fifteen-call persistent-server API example. Six further fused cases
cover every path at 100k and 1m vertices. Native release gates pass 73 vendor
tests, one allocator test and five bridge tests. Eighteen advanced/fused tests
also pass under a 0.25-CPU cgroup quota, including exercised PageRank widths
1, 2 and 8. The frozen macOS source passes the corresponding relational and
native unit gates; macOS relational runs use the previously installed native
wheel, so they do not qualify the new Banda kernels end to end.

The [independent primary audit](pecan-nutmeg-benchmark/audit/main-audit.md)
rehashes 1,010 distinct local evidence files and reconstructs all 20,480 raw
memory samples. It independently checks sixteen retained full output vectors
(eight PageRank and eight WCC); the other 134 cells are checked through their
validation receipts and inventories. All selected WCC answers match exactly;
selected PageRank true residuals range from `4.6662e-9` to `7.9206e-9`.
This is distinct from the harness's full-vector validation of every successful
trial. All 150 containers exited and were removed; every post-stop staging
scan is empty. There were no OOM, timeout, transport or cleanup failures.

The [independent fusion audit](pecan-nutmeg-benchmark/audit/fusion-audit.md)
rehashes 525 distinct files, reconstructs all 5,147 memory samples and checks six
full WCC output vectors with zero membership mismatches; 72 other cells receive
receipt/inventory checks. All 78 contraction traces agree across paths and plans
apart from the documented first raw-edge input count. Counts are six, nine and
eleven rounds at 10k, 100k and 1m, and nine on chain-512. VM-wide steal is 0.0%
in all 78 cells; all containers exit and are removed, with empty post-stop
staging scans. Banda's fused chain call has **no execution memory sample**:
its sampled PSS/RSS is unavailable, not zero. Neither audit finds an evidence
integrity defect. The task VM is stopped after collection; original raw evidence
and build artifacts remain on Morrobay.

The physical-host test places workers on Capitola and Morrobay with shared
object storage. Both workers complete tasks inside all 72 delta PageRank
iteration windows and both fused WCC contraction windows. The PageRank true
residual is `4.1599e-9`; WCC labels match exactly. All Sail processes and the
owned object-store server stop, and two independent scans find an empty staging
prefix. This is a seven-vertex functional proof of worker participation, not a
scaling measurement. Its unchanged older native wheel is loaded for deployment
parity; the tested algorithms are relational.

The first physical fused attempt refused an eight-task plan because its launcher
provided only four slots. The launcher now exposes `--worker-task-slots`; the
successful repeat supplies four per worker. The original refusal remains in the
evidence. An original storage scanner used a static algorithm-SHA label;
its bytes are preserved, and a separate attribution verdict identifies the
actual launcher/source revision, scanner hash and independent cleanup check.


The first matrix attempt is retained separately: 16 passed, four admission
errors and 130 not run. Its 4 GiB native allowance refused a 5,544,103,276-byte
staging workspace request; two relational contraction cells required 24 task
slots against 16 available. These were admission failures, not answer
mismatches or observed OOM kills. The completed outcomes were not rewritten.
The later 8 GiB allowance/64 total task slots were qualified at planned large
sizes before the complete primary run.

Earlier development failures, a two-host supervisor shutdown failure and the
native bridge's existing strict-Clippy `large_enum_variant` diagnostic remain
in the evidence. Vendor Clippy is strict; the bridge pass is explicitly
qualified by that single existing exemption. Gate verdicts name immutable,
clean source revisions and cannot be transferred to unrelated commits.

## Reproduce and inspect

Start with the [step-by-step tutorial](../../../examples/extensions/benchmarks/TUTORIAL.md).
It includes source checkout, Linux/macOS prerequisites, debug and release
builds, all fifteen methods, local/process-worker execution and the two-host
shared-storage test. Use the branch or the exact measured commits below;
`sail-extensions-1` is an older fixed tag without these graph additions.

| Evidence | Controller/harness source | Native wheel source | Sail runtime source |
| --- | --- | --- | --- |
| Primary 150 | [`3c1fd28c`](https://github.com/querygraph/sail/commit/3c1fd28c8745864ca7426f13f42ef251269cb103) | [`4e6100f0`](https://github.com/querygraph/sail/commit/4e6100f0320fb952e9926b867193ec1768edfcda) | [`70b0d1ca`](https://github.com/querygraph/sail/commit/70b0d1cab2cab945d4dbaf6842ee0e38c8aa1822) |
| Fusion 78 and final Linux tutorials | [`3fd69787`](https://github.com/querygraph/sail/commit/3fd6978757713482942656cb0b23205fe57647d0) | [`9d7155aa`](https://github.com/querygraph/sail/commit/9d7155aa2302d9dda4e93e48ee3fd81aac1ee637) | `70b0d1ca` |
| Constrained first attempt | [`a6567b51`](https://github.com/querygraph/sail/commit/a6567b511a2ef1d99688f644134effb8461594f8) | `4e6100f0` | `70b0d1ca` |
| Physical fused proof | `3fd69787` | `de8e6709` (unchanged, relational scope) | `70b0d1ca` |

The three roles are recorded separately because the preserved release Sail
binary predates these client/native additions. Git-object comparisons establish
unchanged host source. Fusion native source `9d7155aa` and final harness
`3fd69787` differ only in the launcher, its test and tutorial. Wheel, binary,
lockfile, image and installed-package hashes are retained with every relevant
qualification. Rebuilding the current branch as in the tutorial is a new run;
it must record its own source and artifact identities.

[Primary](pecan-nutmeg-benchmark/primary/summary.json),
[fusion](pecan-nutmeg-benchmark/fusion/summary.json) and
[constrained](pecan-nutmeg-benchmark/constrained/summary.json) summaries accompany
per-cell CSV and complete Markdown tables. The
[evidence guide](pecan-nutmeg-benchmark/README.md) describes the proof archive,
hash verification, audit scope and commands for rebuilding tables and figures.
It retains gate logs, raw memory samples, diagnostics, container outcomes,
source inventories and failure records. Dataset/output Parquet and binary
wheels are omitted from Git; their recorded hashes remain. Re-running the
published generator and harness provides new independently validated outputs.


The benchmark [harness guide](../../../examples/extensions/benchmarks/README.md)
defines outcomes, resource settings, timing and memory fields. The
[client API](../../../examples/extensions/graph-algorithms/README.md) and
[native kernel contract](../../../examples/extensions/vendor/nutmeg-graph/OPTIMIZED_ALGORITHMS.md)
state method options and ownership rules. Functional two-host evidence is
separate from single-host Linux performance measurements. It does not make
Banda a distributed native graph kernel or establish multi-host scaling.
