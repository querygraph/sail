# Independent fusion evidence review

Recorded 2026-09-27T21:15:51.307810+00:00.

All 78 trials pass, with no auditor issues or warnings. The exact recorded summarizer and independent receipt reconstruction agree on every median and range. No original outcome is replaced.

## Contemporaneous comparisons

Values are median [minimum–maximum]. PSS is MiB. Ratios are fused/unfused group medians. Sparse groups have three repeats; chain diagnostics have one.

| Suite | Dataset | Path | Unfused seconds | Fused seconds | Time ratio | Unfused PSS | Fused PSS | PSS ratio |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| chain-diagnostic | chain-512 | Grenada | 3.051 [3.051–3.051] | 2.238 [2.238–2.238] | 0.7336 | 756.365 [756.365–756.365] | 780.362 [780.362–780.362] | 1.0317 |
| chain-diagnostic | chain-512 | Banda | 0.046 [0.046–0.046] | 0.042 [0.042–0.042] | 0.9206 | 431.044 [431.044–431.044] | unavailable | unavailable |
| chain-diagnostic | chain-512 | Pecan | 3.007 [3.007–3.007] | 2.180 [2.180–2.180] | 0.7249 | 799.437 [799.437–799.437] | 747.541 [747.541–747.541] | 0.9351 |
| distributed | sparse-10000 | Grenada | 2.188 [2.109–2.228] | 1.698 [1.639–1.701] | 0.7759 | 929.128 [919.717–934.905] | 869.569 [867.413–903.845] | 0.9359 |
| distributed | sparse-10000 | Banda | 0.131 [0.129–0.137] | 0.125 [0.122–0.132] | 0.9522 | 549.337 [548.878–584.174] | 557.956 [549.651–561.932] | 1.0157 |
| distributed | sparse-10000 | Pecan | 2.198 [2.164–2.256] | 1.644 [1.612–1.667] | 0.7479 | 924.604 [909.503–936.853] | 904.397 [878.558–915.433] | 0.9781 |
| distributed | sparse-100000 | Grenada | 3.497 [3.476–3.515] | 2.596 [2.588–2.669] | 0.7424 | 1382.502 [1378.271–1415.433] | 1277.760 [1202.600–1315.730] | 0.9242 |
| distributed | sparse-100000 | Banda | 0.829 [0.824–0.852] | 0.807 [0.795–0.830] | 0.9737 | 740.268 [709.799–752.804] | 725.975 [722.529–766.491] | 0.9807 |
| distributed | sparse-100000 | Pecan | 3.433 [3.420–3.521] | 2.576 [2.576–2.673] | 0.7503 | 1385.647 [1364.733–1424.144] | 1263.318 [1249.291–1272.769] | 0.9117 |
| distributed | sparse-1000000 | Grenada | 8.706 [8.559–8.719] | 7.807 [7.638–7.997] | 0.8967 | 1732.362 [1669.694–1753.116] | 2214.545 [2096.026–2342.890] | 1.2783 |
| distributed | sparse-1000000 | Banda | 8.023 [7.947–8.268] | 7.620 [7.612–7.661] | 0.9498 | 1918.264 [1894.472–1932.335] | 1909.947 [1902.322–1944.565] | 0.9957 |
| distributed | sparse-1000000 | Pecan | 8.831 [8.671–9.611] | 7.434 [7.201–7.460] | 0.8418 | 1721.359 [1646.987–1735.003] | 2290.213 [2270.558–2326.631] | 1.3305 |
| local | sparse-100000 | Grenada | 2.773 [2.772–2.846] | 2.022 [2.009–2.082] | 0.7291 | 713.805 [707.430–735.004] | 690.855 [666.031–724.730] | 0.9678 |
| local | sparse-100000 | Banda | 0.764 [0.736–0.787] | 0.757 [0.749–0.766] | 0.9901 | 489.465 [477.723–519.174] | 488.520 [483.664–491.188] | 0.9981 |
| local | sparse-100000 | Pecan | 2.746 [2.740–2.767] | 2.052 [2.034–2.053] | 0.7472 | 727.543 [719.270–738.691] | 696.344 [684.242–705.520] | 0.9571 |

All fifteen fused medians are lower in elapsed time. This does not establish a general benefit: several native time ranges overlap, and four observed PSS medians increase (one is the single chain trial). At 1m, both relational PSS ranges rise entirely above their unfused ranges. All sparse and local outcomes, including these higher-memory cases, remain in the tables.

## Integrity, correctness and trace coverage

Harness `3fd6978757713482942656cb0b23205fe57647d0`, native `9d7155aa2302d9dda4e93e48ee3fd81aac1ee637`, runtime `70b0d1cab2cab945d4dbaf6842ee0e38c8aa1822`.

All 525 distinct delivered files were hashed (579 file checks). Binary SHA-256 is `7353a7df02a624b3336f6c30d0968cbd42ae1f4bd3333ed6af9c638bbc77165a`; native installed-file identity is `c78f891d12cd38aca422eb453958cc0336e7f058cd895c1c808e8bbd01c8d806`. All 78 cell limits agree with the frozen configuration: eight CPU units, cpuset 0–7, 32 GiB aggregate, 16 GiB Sail pool per process, 8 GiB driver/local native allowance, 32 task slots per worker and requested native concurrency eight.

The six delivered 100k repeat-1 result sets cover all three entry paths and both variants. Every one has 100,000 unique vertex IDs, 5,093 independently recomputed connected components and zero membership mismatches. Verification uses union-find over the actual delivered edge Parquet; it imports no tested algorithm. The other 72 final vectors were not copied locally, so their correctness is recorded server validation, not an independent raw-vector audit.

All 78 contraction traces have the expected SplitMix64 coefficient stream. For each dataset, every later common contracted-graph count agrees across paths, variants, modes and repeats. Counts do not prove all intermediate representative maps; the implementation regression tests cover that stronger property.

| Dataset | Cells | Rounds | Unfused first edges | Fused first edges |
| --- | ---: | ---: | ---: | ---: |
| sparse-100000 | 36 | 9 | [776998] | [799212] |
| sparse-1000000 | 18 | 11 | [7771135] | [7992341] |
| sparse-10000 | 18 | 6 | [77565] | [79897] |
| chain-512 | 6 | 9 | [511] | [511] |

Actual native kernel widths are three for 10k, eight for 100k/1m, and one for chain-512, with requested concurrency eight in every native trial. Both native variants follow the same width policy.

## Sampling, timing and cleanup

All 5,147 samples were rebuilt from their process entries, including phase maxima and summaries. There are 77 cells with execution PSS samples and one without. Native fused execution sample counts range 0–107, native unfused 1–114; relational counts range 22–110. The longest execution scan is 0.087394 seconds, additional to the 50 ms wait. The fused native chain takes about 42 ms and has no execution memory sample; its memory is not imputed from startup, verification, cgroup or the unfused trial.

The timer includes lazy graph evaluation, input validation, graph conversion/staging where applicable, contractions/back-expansion, internal writes, per-iteration instrumentation and the final caller-owned output write. Independent correctness, status reads/file hashing and cleanup occur after the timer. Native execution occurs inside the output action; output-action time is not export-only. PSS is the sum over the cell's private-PID-namespace processes. Cgroup through-result peaks include earlier startup/cache charges and have a distinct boundary.

All 78 containers exited zero, were removed, and had no recorded OOM, outer timeout, transport or cleanup error. The exact-source post-stop snapshot contains all 78 cell directories and zero remaining staging regular files. All 78 VM-wide steal observations are 0.0; this says nothing about unmeasured host scheduling delay.

This supplement retains contemporaneous unfused controls from the same native wheel. Earlier primary/constrained outcomes remain separate; no failed historical trial was erased or recast as a fusion pass.

## Limits

- Six 100k process-worker repeat-1 outputs have independent full-Parquet verification; the other 72 are receipt-only.
- The native fused chain trial has no execution RSS/PSS sample; unavailable is not zero memory.
- Ratios divide contemporaneous group medians, not paired-trial ratios or causal effect estimates.
- There are three sparse repetitions per group and one chain diagnostic per method; overlap is descriptive, not a significance test.
- All whole-VM steal observations are zero; that does not measure scheduling delay outside the VM.
- PSS sampling can miss peaks, and cgroup page-cache ownership makes its warm-cache peak a different memory boundary.
- Representative-map parity is established by implementation tests; exported benchmark traces compare coefficients/counts, and the selected full outputs verify the final partitions.
