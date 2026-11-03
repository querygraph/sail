# Independent main evidence audit

Recorded 2026-09-27T20:40:38.556443+00:00. Audit verdict: **passed**.

150 planned cells; outcomes: 148 passed, 2 nonconverged.

Original trial outcomes are retained unchanged. An audit failure is a separate evidence-integrity finding, not a rewritten benchmark outcome.

## Identity and coverage

- harness_source_sha: `3c1fd28c8745864ca7426f13f42ef251269cb103`.
- runtime_source_sha: `70b0d1cab2cab945d4dbaf6842ee0e38c8aa1822`.
- native_source_sha: `4e6100f0320fb952e9926b867193ec1768edfcda`.

Performed 1150 inventory checks across 1010 unique files; reconstructed 20480 raw memory samples.
Raw correctness coverage: 16 raw_full_vector, 134 receipt_only.

Raw-vector checks read actual Parquet and independently recompute PageRank fixed-point residuals/full-vector errors and WCC components from input edges. Receipt-only checks validate recorded results and hashes; they are not a new calculation from omitted graph rows.

## Findings

No audit integrity defects found.

## Every planned outcome

| Sequence | Cell | Outcome | Correctness audit | Execution samples |
| --- | --- | --- | --- | ---: |
| 1 | `distributed-r1-sparse-100000-nutmeg-datafusion-wcc-reference` | passed | raw_full_vector | 19 |
| 2 | `distributed-r1-sparse-1000000-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 161 |
| 3 | `distributed-r1-sparse-10000-nutmeg-native-wcc-reference` | passed | receipt_only | 2 |
| 4 | `distributed-r1-sparse-10000-pecan-pagerank-optimized` | passed | receipt_only | 147 |
| 5 | `distributed-r1-sparse-1000000-nutmeg-datafusion-pagerank-reference` | passed | receipt_only | 539 |
| 6 | `distributed-r1-sparse-1000000-nutmeg-datafusion-pagerank-optimized` | passed | receipt_only | 678 |
| 7 | `distributed-r1-sparse-100000-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 14 |
| 8 | `distributed-r1-sparse-100000-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 47 |
| 9 | `distributed-r1-sparse-100000-nutmeg-native-pagerank-optimized` | passed | raw_full_vector | 20 |
| 10 | `distributed-r1-sparse-1000000-pecan-pagerank-optimized` | passed | receipt_only | 693 |
| 11 | `distributed-r1-sparse-100000-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 12 |
| 12 | `distributed-r1-sparse-10000-nutmeg-datafusion-wcc-reference` | passed | receipt_only | 12 |
| 13 | `distributed-r1-sparse-1000000-nutmeg-native-pagerank-reference` | passed | raw_full_vector | 98 |
| 14 | `distributed-r1-sparse-100000-nutmeg-native-wcc-reference` | passed | raw_full_vector | 9 |
| 15 | `distributed-r1-sparse-100000-pecan-pagerank-optimized` | passed | raw_full_vector | 199 |
| 16 | `distributed-r1-sparse-1000000-pecan-wcc-optimized` | passed | receipt_only | 105 |
| 17 | `distributed-r1-sparse-1000000-nutmeg-datafusion-wcc-reference` | passed | receipt_only | 88 |
| 18 | `distributed-r1-sparse-10000-pecan-pagerank-reference` | passed | receipt_only | 106 |
| 19 | `distributed-r1-sparse-10000-nutmeg-datafusion-pagerank-reference` | passed | receipt_only | 107 |
| 20 | `distributed-r1-sparse-1000000-pecan-pagerank-reference` | passed | receipt_only | 537 |
| 21 | `distributed-r1-sparse-100000-pecan-wcc-reference` | passed | raw_full_vector | 18 |
| 22 | `distributed-r1-sparse-1000000-pecan-wcc-reference` | passed | receipt_only | 86 |
| 23 | `distributed-r1-sparse-10000-nutmeg-native-pagerank-optimized` | passed | receipt_only | 4 |
| 24 | `distributed-r1-sparse-1000000-nutmeg-native-wcc-reference` | passed | raw_full_vector | 92 |
| 25 | `distributed-r1-sparse-10000-nutmeg-datafusion-pagerank-optimized` | passed | receipt_only | 148 |
| 26 | `distributed-r1-sparse-10000-pecan-wcc-optimized` | passed | receipt_only | 31 |
| 27 | `distributed-r1-sparse-100000-nutmeg-datafusion-pagerank-optimized` | passed | raw_full_vector | 200 |
| 28 | `distributed-r1-sparse-10000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 30 |
| 29 | `distributed-r1-sparse-100000-pecan-pagerank-reference` | passed | raw_full_vector | 150 |
| 30 | `distributed-r1-sparse-1000000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 105 |
| 31 | `distributed-r1-sparse-10000-pecan-wcc-reference` | passed | receipt_only | 11 |
| 32 | `distributed-r1-sparse-10000-nutmeg-native-pagerank-reference` | passed | receipt_only | 3 |
| 33 | `distributed-r1-sparse-10000-nutmeg-native-wcc-optimized` | passed | receipt_only | 2 |
| 34 | `distributed-r1-sparse-100000-nutmeg-datafusion-pagerank-reference` | passed | raw_full_vector | 149 |
| 35 | `distributed-r1-sparse-1000000-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 112 |
| 36 | `distributed-r1-sparse-100000-pecan-wcc-optimized` | passed | raw_full_vector | 48 |
| 37 | `distributed-r2-sparse-100000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 51 |
| 38 | `distributed-r2-sparse-10000-nutmeg-native-wcc-reference` | passed | receipt_only | 2 |
| 39 | `distributed-r2-sparse-10000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 31 |
| 40 | `distributed-r2-sparse-100000-nutmeg-datafusion-pagerank-optimized` | passed | receipt_only | 202 |
| 41 | `distributed-r2-sparse-1000000-nutmeg-datafusion-pagerank-optimized` | passed | receipt_only | 665 |
| 42 | `distributed-r2-sparse-100000-pecan-wcc-reference` | passed | receipt_only | 17 |
| 43 | `distributed-r2-sparse-100000-nutmeg-native-pagerank-reference` | passed | receipt_only | 13 |
| 44 | `distributed-r2-sparse-1000000-nutmeg-native-pagerank-reference` | passed | receipt_only | 99 |
| 45 | `distributed-r2-sparse-1000000-nutmeg-datafusion-wcc-reference` | passed | receipt_only | 87 |
| 46 | `distributed-r2-sparse-1000000-pecan-pagerank-optimized` | passed | receipt_only | 675 |
| 47 | `distributed-r2-sparse-10000-nutmeg-datafusion-pagerank-optimized` | passed | receipt_only | 148 |
| 48 | `distributed-r2-sparse-1000000-nutmeg-native-wcc-optimized` | passed | receipt_only | 112 |
| 49 | `distributed-r2-sparse-100000-pecan-pagerank-optimized` | passed | receipt_only | 201 |
| 50 | `distributed-r2-sparse-1000000-nutmeg-native-pagerank-optimized` | passed | receipt_only | 160 |
| 51 | `distributed-r2-sparse-100000-nutmeg-datafusion-pagerank-reference` | passed | receipt_only | 147 |
| 52 | `distributed-r2-sparse-1000000-nutmeg-datafusion-pagerank-reference` | passed | receipt_only | 548 |
| 53 | `distributed-r2-sparse-100000-nutmeg-native-pagerank-optimized` | passed | receipt_only | 20 |
| 54 | `distributed-r2-sparse-1000000-pecan-pagerank-reference` | passed | receipt_only | 543 |
| 55 | `distributed-r2-sparse-100000-nutmeg-native-wcc-reference` | passed | receipt_only | 9 |
| 56 | `distributed-r2-sparse-1000000-pecan-wcc-reference` | passed | receipt_only | 85 |
| 57 | `distributed-r2-sparse-100000-pecan-wcc-optimized` | passed | receipt_only | 47 |
| 58 | `distributed-r2-sparse-10000-nutmeg-native-pagerank-optimized` | passed | receipt_only | 4 |
| 59 | `distributed-r2-sparse-10000-pecan-pagerank-reference` | passed | receipt_only | 105 |
| 60 | `distributed-r2-sparse-10000-nutmeg-datafusion-wcc-reference` | passed | receipt_only | 12 |
| 61 | `distributed-r2-sparse-100000-nutmeg-native-wcc-optimized` | passed | receipt_only | 13 |
| 62 | `distributed-r2-sparse-10000-nutmeg-datafusion-pagerank-reference` | passed | receipt_only | 103 |
| 63 | `distributed-r2-sparse-10000-nutmeg-native-pagerank-reference` | passed | receipt_only | 3 |
| 64 | `distributed-r2-sparse-10000-pecan-pagerank-optimized` | passed | receipt_only | 150 |
| 65 | `distributed-r2-sparse-100000-pecan-pagerank-reference` | passed | receipt_only | 148 |
| 66 | `distributed-r2-sparse-1000000-nutmeg-native-wcc-reference` | passed | receipt_only | 91 |
| 67 | `distributed-r2-sparse-100000-nutmeg-datafusion-wcc-reference` | passed | receipt_only | 19 |
| 68 | `distributed-r2-sparse-10000-pecan-wcc-reference` | passed | receipt_only | 12 |
| 69 | `distributed-r2-sparse-10000-pecan-wcc-optimized` | passed | receipt_only | 32 |
| 70 | `distributed-r2-sparse-1000000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 106 |
| 71 | `distributed-r2-sparse-1000000-pecan-wcc-optimized` | passed | receipt_only | 106 |
| 72 | `distributed-r2-sparse-10000-nutmeg-native-wcc-optimized` | passed | receipt_only | 2 |
| 73 | `distributed-r3-sparse-100000-nutmeg-native-pagerank-optimized` | passed | receipt_only | 20 |
| 74 | `distributed-r3-sparse-100000-nutmeg-datafusion-wcc-reference` | passed | receipt_only | 18 |
| 75 | `distributed-r3-sparse-1000000-nutmeg-datafusion-pagerank-reference` | passed | receipt_only | 542 |
| 76 | `distributed-r3-sparse-100000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 47 |
| 77 | `distributed-r3-sparse-1000000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 104 |
| 78 | `distributed-r3-sparse-1000000-nutmeg-native-wcc-reference` | passed | receipt_only | 92 |
| 79 | `distributed-r3-sparse-1000000-nutmeg-datafusion-pagerank-optimized` | passed | receipt_only | 671 |
| 80 | `distributed-r3-sparse-10000-nutmeg-datafusion-wcc-reference` | passed | receipt_only | 12 |
| 81 | `distributed-r3-sparse-10000-pecan-pagerank-optimized` | passed | receipt_only | 144 |
| 82 | `distributed-r3-sparse-100000-pecan-pagerank-optimized` | passed | receipt_only | 197 |
| 83 | `distributed-r3-sparse-10000-nutmeg-datafusion-pagerank-optimized` | passed | receipt_only | 147 |
| 84 | `distributed-r3-sparse-100000-pecan-wcc-reference` | passed | receipt_only | 17 |
| 85 | `distributed-r3-sparse-100000-nutmeg-datafusion-pagerank-optimized` | passed | receipt_only | 197 |
| 86 | `distributed-r3-sparse-1000000-pecan-wcc-reference` | passed | receipt_only | 86 |
| 87 | `distributed-r3-sparse-1000000-nutmeg-native-pagerank-reference` | passed | receipt_only | 98 |
| 88 | `distributed-r3-sparse-10000-pecan-wcc-optimized` | passed | receipt_only | 30 |
| 89 | `distributed-r3-sparse-10000-pecan-wcc-reference` | passed | receipt_only | 11 |
| 90 | `distributed-r3-sparse-1000000-nutmeg-datafusion-wcc-reference` | passed | receipt_only | 85 |
| 91 | `distributed-r3-sparse-100000-nutmeg-native-wcc-reference` | passed | receipt_only | 10 |
| 92 | `distributed-r3-sparse-10000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 31 |
| 93 | `distributed-r3-sparse-10000-nutmeg-native-pagerank-optimized` | passed | receipt_only | 3 |
| 94 | `distributed-r3-sparse-1000000-pecan-wcc-optimized` | passed | receipt_only | 106 |
| 95 | `distributed-r3-sparse-10000-nutmeg-datafusion-pagerank-reference` | passed | receipt_only | 104 |
| 96 | `distributed-r3-sparse-1000000-pecan-pagerank-optimized` | passed | receipt_only | 663 |
| 97 | `distributed-r3-sparse-100000-nutmeg-datafusion-pagerank-reference` | passed | receipt_only | 150 |
| 98 | `distributed-r3-sparse-100000-pecan-wcc-optimized` | passed | receipt_only | 48 |
| 99 | `distributed-r3-sparse-1000000-nutmeg-native-wcc-optimized` | passed | receipt_only | 110 |
| 100 | `distributed-r3-sparse-10000-nutmeg-native-wcc-reference` | passed | receipt_only | 2 |
| 101 | `distributed-r3-sparse-1000000-pecan-pagerank-reference` | passed | receipt_only | 509 |
| 102 | `distributed-r3-sparse-100000-nutmeg-native-pagerank-reference` | passed | receipt_only | 14 |
| 103 | `distributed-r3-sparse-10000-pecan-pagerank-reference` | passed | receipt_only | 104 |
| 104 | `distributed-r3-sparse-10000-nutmeg-native-wcc-optimized` | passed | receipt_only | 2 |
| 105 | `distributed-r3-sparse-1000000-nutmeg-native-pagerank-optimized` | passed | receipt_only | 156 |
| 106 | `distributed-r3-sparse-100000-pecan-pagerank-reference` | passed | receipt_only | 147 |
| 107 | `distributed-r3-sparse-10000-nutmeg-native-pagerank-reference` | passed | receipt_only | 3 |
| 108 | `distributed-r3-sparse-100000-nutmeg-native-wcc-optimized` | passed | receipt_only | 12 |
| 109 | `local-r1-sparse-100000-nutmeg-datafusion-pagerank-reference` | passed | receipt_only | 120 |
| 110 | `local-r1-sparse-100000-pecan-wcc-reference` | passed | receipt_only | 14 |
| 111 | `local-r1-sparse-100000-nutmeg-datafusion-wcc-reference` | passed | receipt_only | 16 |
| 112 | `local-r1-sparse-100000-pecan-pagerank-reference` | passed | receipt_only | 119 |
| 113 | `local-r1-sparse-100000-pecan-pagerank-optimized` | passed | receipt_only | 162 |
| 114 | `local-r1-sparse-100000-nutmeg-native-pagerank-reference` | passed | receipt_only | 14 |
| 115 | `local-r1-sparse-100000-nutmeg-native-wcc-optimized` | passed | receipt_only | 14 |
| 116 | `local-r1-sparse-100000-pecan-wcc-optimized` | passed | receipt_only | 42 |
| 117 | `local-r1-sparse-100000-nutmeg-datafusion-pagerank-optimized` | passed | receipt_only | 161 |
| 118 | `local-r1-sparse-100000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 42 |
| 119 | `local-r1-sparse-100000-nutmeg-native-pagerank-optimized` | passed | receipt_only | 19 |
| 120 | `local-r1-sparse-100000-nutmeg-native-wcc-reference` | passed | receipt_only | 9 |
| 121 | `local-r2-sparse-100000-nutmeg-native-pagerank-reference` | passed | receipt_only | 13 |
| 122 | `local-r2-sparse-100000-nutmeg-datafusion-pagerank-optimized` | passed | receipt_only | 155 |
| 123 | `local-r2-sparse-100000-nutmeg-native-wcc-reference` | passed | receipt_only | 9 |
| 124 | `local-r2-sparse-100000-nutmeg-datafusion-pagerank-reference` | passed | receipt_only | 112 |
| 125 | `local-r2-sparse-100000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 41 |
| 126 | `local-r2-sparse-100000-pecan-wcc-optimized` | passed | receipt_only | 40 |
| 127 | `local-r2-sparse-100000-nutmeg-datafusion-wcc-reference` | passed | receipt_only | 13 |
| 128 | `local-r2-sparse-100000-pecan-pagerank-reference` | passed | receipt_only | 112 |
| 129 | `local-r2-sparse-100000-nutmeg-native-pagerank-optimized` | passed | receipt_only | 20 |
| 130 | `local-r2-sparse-100000-pecan-pagerank-optimized` | passed | receipt_only | 156 |
| 131 | `local-r2-sparse-100000-pecan-wcc-reference` | passed | receipt_only | 12 |
| 132 | `local-r2-sparse-100000-nutmeg-native-wcc-optimized` | passed | receipt_only | 13 |
| 133 | `local-r3-sparse-100000-nutmeg-native-wcc-reference` | passed | receipt_only | 9 |
| 134 | `local-r3-sparse-100000-nutmeg-native-pagerank-reference` | passed | receipt_only | 14 |
| 135 | `local-r3-sparse-100000-pecan-pagerank-reference` | passed | receipt_only | 126 |
| 136 | `local-r3-sparse-100000-nutmeg-datafusion-pagerank-optimized` | passed | receipt_only | 167 |
| 137 | `local-r3-sparse-100000-pecan-pagerank-optimized` | passed | receipt_only | 165 |
| 138 | `local-r3-sparse-100000-pecan-wcc-optimized` | passed | receipt_only | 43 |
| 139 | `local-r3-sparse-100000-pecan-wcc-reference` | passed | receipt_only | 13 |
| 140 | `local-r3-sparse-100000-nutmeg-native-pagerank-optimized` | passed | receipt_only | 20 |
| 141 | `local-r3-sparse-100000-nutmeg-datafusion-pagerank-reference` | passed | receipt_only | 122 |
| 142 | `local-r3-sparse-100000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 40 |
| 143 | `local-r3-sparse-100000-nutmeg-native-wcc-optimized` | passed | receipt_only | 12 |
| 144 | `local-r3-sparse-100000-nutmeg-datafusion-wcc-reference` | passed | receipt_only | 14 |
| 145 | `chain-diagnostic-r1-chain-512-nutmeg-datafusion-wcc-reference` | nonconverged | receipt_only | 127 |
| 146 | `chain-diagnostic-r1-chain-512-pecan-wcc-optimized` | passed | receipt_only | 40 |
| 147 | `chain-diagnostic-r1-chain-512-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 39 |
| 148 | `chain-diagnostic-r1-chain-512-nutmeg-native-wcc-reference` | passed | receipt_only | 1 |
| 149 | `chain-diagnostic-r1-chain-512-nutmeg-native-wcc-optimized` | passed | receipt_only | 1 |
| 150 | `chain-diagnostic-r1-chain-512-pecan-wcc-reference` | nonconverged | receipt_only | 121 |

## Sampling and contraction detail

| Path | Algorithm | Variant | Execution sample count: min / median / max |
| --- | --- | --- | --- |
| nutmeg-datafusion | pagerank | optimized | 147 / 182.0 / 678 |
| nutmeg-datafusion | pagerank | reference | 103 / 134.5 / 548 |
| nutmeg-datafusion | wcc | optimized | 30 / 42 / 106 |
| nutmeg-datafusion | wcc | reference | 12 / 18 / 127 |
| nutmeg-native | pagerank | optimized | 3 / 20.0 / 161 |
| nutmeg-native | pagerank | reference | 3 / 14.0 / 99 |
| nutmeg-native | wcc | optimized | 1 / 12 / 112 |
| nutmeg-native | wcc | reference | 1 / 9 / 92 |
| pecan | pagerank | optimized | 144 / 181.0 / 693 |
| pecan | pagerank | reference | 104 / 136.5 / 543 |
| pecan | wcc | optimized | 30 / 43 / 106 |
| pecan | wcc | reference | 11 / 17 / 121 |

Recorded whole-VM steal fractions: `{"observations": 150, "minimum": 0.0, "median": 0.0, "maximum": 0.0}`.

- `sparse-100000`: 9 identical contraction rounds across 18 successful cells, excluding the first raw-edge input count.
- `sparse-1000000`: 11 identical contraction rounds across 9 successful cells, excluding the first raw-edge input count.
- `sparse-10000`: 6 identical contraction rounds across 9 successful cells, excluding the first raw-edge input count.
- `chain-512`: 9 identical contraction rounds across 3 successful cells, excluding the first raw-edge input count.

Native advanced widths are recorded per dataset in the JSON. Native reference actual widths are not exported by these kernels and must not be inferred from requested concurrency.

Post-stop cleanup snapshot: `{"recorded_utc": "2026-09-27T20:35:26.574219+00:00", "run": "/targets/pecan-benchmark/runs/pecan-matrix-3c1fd28c", "harness_source_sha": "3c1fd28c8745864ca7426f13f42ef251269cb103", "completed_cell_directories": 150, "remaining_staging_regular_files": [], "outcome": "passed"}`.


RSS/PSS are sampled execution-phase totals; short peaks may be missed. RSS can double-count shared mappings; PSS apportions them. Cgroup peaks include cache and lifetime activity and are not interchangeable with PSS or native admission accounting. No elapsed-time or memory ranking is inferred by this audit.

Cross-path WCC trace comparisons check coefficients, active vertices and contracted edges; only the first raw-edge input count may differ for fused plans. Floating PageRank frontier traces are recorded without requiring equality across different reduction orders.

## Independent review of interpretation

Reviewed 2026-09-27T20:42:32.721158+00:00.

Eight selected PageRank vectors and eight selected WCC vectors were recomputed from actual Parquet. PageRank true residuals ranged from `4.66620445698e-09` to `7.9205451695e-09`; all eight WCC partitions matched the independently computed raw-edge components. The other 134 cells remain explicitly receipt-only coverage.

The native 8 GiB prepaid admission lease applies to the driver/local Connect session. Process workers have 16 GiB Sail pools without that driver-only Nutmeg prepayment. Source and receipt field names must not be read as one shared aggregate pool.

| Dataset | Compared delta cells | Pushes | Propagated edge messages | Reactivations |
| --- | ---: | ---: | ---: | ---: |
| sparse-10000 | 9 | 59 | 4656532 | 4485 |
| sparse-100000 | 18 | 59 | 45820538 | 67360 |
| sparse-1000000 | 9 | 62 | 477240350 | 975826 |

The integer frontier/message/reactivation traces agree across all three paths, modes and repetitions for each dataset in this run. This is an observation, not a general bitwise guarantee across floating reduction orders. Message counts do not measure physical Parquet reads avoided.

The following sampled execution PSS ranges are independently reconstructed from raw samples and include all three successful repetitions. The 1m values are lower than 100k for all four relational PR path/method groups. The audit found no aggregation or missing-sample explanation; the underlying cause has not been established. No monotonic memory-scaling inference is justified.

| Vertices | Path | Variant | PSS GiB: min / median / max |
| --- | --- | --- | ---: |
| sparse-100000 | nutmeg-datafusion | optimized | 2.536303 / 2.631278 / 2.658713 |
| sparse-100000 | nutmeg-datafusion | reference | 2.006639 / 2.023685 / 2.029227 |
| sparse-100000 | pecan | optimized | 2.513906 / 2.547007 / 2.652221 |
| sparse-100000 | pecan | reference | 2.097453 / 2.146918 / 2.159094 |
| sparse-1000000 | nutmeg-datafusion | optimized | 2.035265 / 2.105895 / 2.151004 |
| sparse-1000000 | nutmeg-datafusion | reference | 1.767990 / 1.771399 / 1.804674 |
| sparse-1000000 | pecan | optimized | 2.007875 / 2.038634 / 2.133479 |
| sparse-1000000 | pecan | reference | 1.873599 / 1.877886 / 1.940475 |

All 150 guest-steal observations are 0.0; they cover the whole VM and cell lifecycle, not only the timed algorithm or assigned cpuset. That does not establish absence of physical macOS host contention. Execution-memory samples range from one for native chain WCC to 693 for relational PageRank. The longest single process-memory scan was about 0.146 s, in addition to the configured 50 ms wait between scans. Sampled peaks cannot be treated as continuous maxima.

Every container exited and was removed, with no OOM kill, outer timeout, artifact-copy error or receipt cleanup error. The separate post-stop snapshot covers all 150 cells and reports zero remaining staging regular files, including both nonconverged chain cases. Original constrained-attempt admission failures remain in their separate audit and were not replaced.
