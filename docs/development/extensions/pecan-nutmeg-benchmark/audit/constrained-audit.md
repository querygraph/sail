# Independent audit of the preserved constrained attempt

Written 2026-09-27T19:50:20.791708+00:00.

The preserved a656 attempt contains 20 completed cells: 16 passed and 4 admission errors. The remaining 130 planned cells are explicitly not run. Replaying the original a656 classification and summary code reproduces the exported groups and aggregates with no integrity errors. This is an audit of the original configuration; subsequent capacity changes must remain a separate run.

## Evidence and identity

Local source: `target/extensions-pecan-benchmark/export/export-constrained-a6567b51`; exported summary: sibling `constrained-a6567b51-summary`. Reproducible checker: `/tmp/audit_pecan_constrained.py`; machine findings: `constrained-audit-details.json`.

- Harness: `a6567b511a2ef1d99688f644134effb8461594f8`.
- Sail host: `70b0d1cab2cab945d4dbaf6842ee0e38c8aa1822`.
- Native kernels: `4e6100f0320fb952e9926b867193ec1768edfcda`.
- Sail binary SHA-256: `7353a7df02a624b3336f6c30d0968cbd42ae1f4bd3333ed6af9c638bbc77165a`.

All 20 receipts have the same source identities, clean source status, Sail binary hash and native installed-file hash set. All 142 copied regular files match the export manifest hashes and sizes. The export deliberately omits 66 Parquet files (54 result files and 12 input/reference files); their inventories match the receipt hashes and sizes, but their bytes were not independently re-read in this local audit. Original complete artifacts remain on the benchmark host.

## Outcomes and admission boundaries

The original envelope was 8 CPUs and 32 GiB aggregate container memory without swap. Each of two workers had the default eight task slots (16 total). Each process had a 10 GiB Sail pool and a 4 GiB prepaid native allowance. Task slots are asynchronous scheduling capacity, not CPU cores. Prepayment is accounting, not allocated RSS.

| Cell | Outcome | Recorded cause |
| --- | --- | --- |
| `distributed-r1-sparse-1000000-nutmeg-native-pagerank-optimized` | error | Native edge staging exceeded the 4 GiB sort working-space allowance before PageRank execution. |
| `distributed-r1-sparse-100000-nutmeg-datafusion-wcc-optimized` | error | Region requires 24 worker task slots; the configured maximum was 16. |
| `distributed-r1-sparse-1000000-nutmeg-native-pagerank-reference` | error | Native edge staging exceeded the 4 GiB sort working-space allowance before PageRank execution. |
| `distributed-r1-sparse-1000000-pecan-wcc-optimized` | error | Region requires 24 worker task slots; the configured maximum was 16. |

The native staging error reports 5,544,103,276 bytes (about 5.163 GiB, or 5.544 GB) of sort working space. The error also reports the sorted-copy estimate separately; this audit does not infer a combined required quota from those terms. Both native error snapshots have no graphs or reads and zero used/staged bytes after refusal, with a recorded peak of 459,642,172 bytes. These are admission errors, not observed OOM kills, timeouts, nonconvergence, or incorrect answers.

## Correctness and every completed cell

All 16 successful receipts record complete unique vertex coverage, zero null IDs, and either zero WCC membership mismatches or converged PageRank with independently computed fixed-point residual at most `1e-8` and reference L1 error within the declared bound. No mismatch outcome occurred. The four admission errors returned no validated answer. Because the export omits result Parquet, this audit verifies the recorded validation and evidence integrity, not a second full-vector calculation from raw result bytes.

| Sequence | Cell | Outcome |
| --- | --- | --- |
| 1 | `distributed-r1-sparse-100000-nutmeg-datafusion-wcc-reference` | passed |
| 2 | `distributed-r1-sparse-1000000-nutmeg-native-pagerank-optimized` | error |
| 3 | `distributed-r1-sparse-10000-nutmeg-native-wcc-reference` | passed |
| 4 | `distributed-r1-sparse-10000-pecan-pagerank-optimized` | passed |
| 5 | `distributed-r1-sparse-1000000-nutmeg-datafusion-pagerank-reference` | passed |
| 6 | `distributed-r1-sparse-1000000-nutmeg-datafusion-pagerank-optimized` | passed |
| 7 | `distributed-r1-sparse-100000-nutmeg-native-pagerank-reference` | passed |
| 8 | `distributed-r1-sparse-100000-nutmeg-datafusion-wcc-optimized` | error |
| 9 | `distributed-r1-sparse-100000-nutmeg-native-pagerank-optimized` | passed |
| 10 | `distributed-r1-sparse-1000000-pecan-pagerank-optimized` | passed |
| 11 | `distributed-r1-sparse-100000-nutmeg-native-wcc-optimized` | passed |
| 12 | `distributed-r1-sparse-10000-nutmeg-datafusion-wcc-reference` | passed |
| 13 | `distributed-r1-sparse-1000000-nutmeg-native-pagerank-reference` | error |
| 14 | `distributed-r1-sparse-100000-nutmeg-native-wcc-reference` | passed |
| 15 | `distributed-r1-sparse-100000-pecan-pagerank-optimized` | passed |
| 16 | `distributed-r1-sparse-1000000-pecan-wcc-optimized` | error |
| 17 | `distributed-r1-sparse-1000000-nutmeg-datafusion-wcc-reference` | passed |
| 18 | `distributed-r1-sparse-10000-pecan-pagerank-reference` | passed |
| 19 | `distributed-r1-sparse-10000-nutmeg-datafusion-pagerank-reference` | passed |
| 20 | `distributed-r1-sparse-1000000-pecan-pagerank-reference` | passed |

The exported CSV retains all 150 planned cells. Timing aggregation includes only passed cells; this partial, shuffled first repetition is not a complete comparative matrix.

## Memory and cleanup

All 3,659 raw memory samples were re-read. Their phase counts and RSS/PSS/cgroup-current maxima exactly reproduce all 20 receipt summaries. RSS sums process mappings and can double-count shared pages; PSS apportions shared mappings. Sampling can miss short peaks. Cgroup lifetime peaks include startup and cache and must not be presented as algorithm-only PSS. Native admission accounting is a separate measurement.

Every completed container exited, was no longer running, had `OOMKilled=false`, and was removed successfully. Required artifact copies succeeded and no transport errors were recorded. Every receipt has an empty cleanup-error list. The 16 successful receipts also record an empty post-shutdown staging-file list; error receipts naturally omit that field.

A separate post-stop cleanup snapshot at `2026-09-27T19:34:50.892467+00:00` covers all 20 cell directories and records zero remaining staging regular files. The export inventory also contains no staging regular-file paths. This establishes the recorded post-stop state for this attempt, without claiming a stronger general writer-drain guarantee.

## Remaining limits

- Raw output/reference Parquet was omitted from this local export, preventing independent recomputation of the 16 answers here.
- The run stopped after 20 cells; 130 cells, additional repetitions and most paired comparisons remain unmeasured in this configuration.
- The revised resource configuration requires its own identity, receipts and outcomes; it cannot replace these four failures.
