# Independent fusion evidence audit

Recorded 2026-09-27T21:09:48.836754+00:00. Audit verdict: **passed**.

78 planned cells; outcomes: 78 passed.

Original trial outcomes are retained unchanged. An audit failure is a separate evidence-integrity finding, not a rewritten benchmark outcome.

## Identity and coverage

- harness_source_sha: `3fd6978757713482942656cb0b23205fe57647d0`.
- runtime_source_sha: `70b0d1cab2cab945d4dbaf6842ee0e38c8aa1822`.
- native_source_sha: `9d7155aa2302d9dda4e93e48ee3fd81aac1ee637`.

Performed 579 inventory checks across 525 unique files; reconstructed 5147 raw memory samples.
Raw correctness coverage: 6 raw_full_vector, 72 receipt_only.

Raw-vector checks read actual Parquet and independently recompute PageRank fixed-point residuals/full-vector errors and WCC components from input edges. Receipt-only checks validate recorded results and hashes; they are not a new calculation from omitted graph rows.

## Findings

No audit integrity defects found.

## Every planned outcome

| Sequence | Cell | Outcome | Correctness audit | Execution samples |
| --- | --- | --- | --- | ---: |
| 1 | `distributed-r1-sparse-100000-nutmeg-datafusion-wcc-fused` | passed | raw_full_vector | 36 |
| 2 | `distributed-r1-sparse-1000000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 101 |
| 3 | `distributed-r1-sparse-100000-nutmeg-native-wcc-fused` | passed | raw_full_vector | 13 |
| 4 | `distributed-r1-sparse-10000-nutmeg-native-wcc-optimized` | passed | receipt_only | 2 |
| 5 | `distributed-r1-sparse-10000-nutmeg-native-wcc-fused` | passed | receipt_only | 2 |
| 6 | `distributed-r1-sparse-100000-nutmeg-native-wcc-optimized` | passed | raw_full_vector | 13 |
| 7 | `distributed-r1-sparse-10000-pecan-wcc-optimized` | passed | receipt_only | 30 |
| 8 | `distributed-r1-sparse-10000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 30 |
| 9 | `distributed-r1-sparse-1000000-pecan-wcc-optimized` | passed | receipt_only | 105 |
| 10 | `distributed-r1-sparse-10000-nutmeg-datafusion-wcc-fused` | passed | receipt_only | 23 |
| 11 | `distributed-r1-sparse-100000-pecan-wcc-optimized` | passed | raw_full_vector | 47 |
| 12 | `distributed-r1-sparse-1000000-nutmeg-datafusion-wcc-fused` | passed | receipt_only | 92 |
| 13 | `distributed-r1-sparse-1000000-pecan-wcc-fused` | passed | receipt_only | 87 |
| 14 | `distributed-r1-sparse-1000000-nutmeg-native-wcc-optimized` | passed | receipt_only | 114 |
| 15 | `distributed-r1-sparse-10000-pecan-wcc-fused` | passed | receipt_only | 22 |
| 16 | `distributed-r1-sparse-100000-nutmeg-datafusion-wcc-optimized` | passed | raw_full_vector | 48 |
| 17 | `distributed-r1-sparse-1000000-nutmeg-native-wcc-fused` | passed | receipt_only | 107 |
| 18 | `distributed-r1-sparse-100000-pecan-wcc-fused` | passed | raw_full_vector | 35 |
| 19 | `distributed-r2-sparse-100000-nutmeg-native-wcc-fused` | passed | receipt_only | 12 |
| 20 | `distributed-r2-sparse-1000000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 102 |
| 21 | `distributed-r2-sparse-1000000-nutmeg-native-wcc-fused` | passed | receipt_only | 106 |
| 22 | `distributed-r2-sparse-10000-nutmeg-datafusion-wcc-fused` | passed | receipt_only | 23 |
| 23 | `distributed-r2-sparse-1000000-pecan-wcc-fused` | passed | receipt_only | 85 |
| 24 | `distributed-r2-sparse-100000-pecan-wcc-fused` | passed | receipt_only | 35 |
| 25 | `distributed-r2-sparse-1000000-nutmeg-native-wcc-optimized` | passed | receipt_only | 112 |
| 26 | `distributed-r2-sparse-10000-nutmeg-native-wcc-fused` | passed | receipt_only | 2 |
| 27 | `distributed-r2-sparse-1000000-nutmeg-datafusion-wcc-fused` | passed | receipt_only | 88 |
| 28 | `distributed-r2-sparse-10000-pecan-wcc-fused` | passed | receipt_only | 22 |
| 29 | `distributed-r2-sparse-1000000-pecan-wcc-optimized` | passed | receipt_only | 104 |
| 30 | `distributed-r2-sparse-10000-nutmeg-native-wcc-optimized` | passed | receipt_only | 2 |
| 31 | `distributed-r2-sparse-100000-nutmeg-native-wcc-optimized` | passed | receipt_only | 13 |
| 32 | `distributed-r2-sparse-10000-pecan-wcc-optimized` | passed | receipt_only | 30 |
| 33 | `distributed-r2-sparse-100000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 47 |
| 34 | `distributed-r2-sparse-100000-pecan-wcc-optimized` | passed | receipt_only | 47 |
| 35 | `distributed-r2-sparse-100000-nutmeg-datafusion-wcc-fused` | passed | receipt_only | 35 |
| 36 | `distributed-r2-sparse-10000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 30 |
| 37 | `distributed-r3-sparse-100000-pecan-wcc-optimized` | passed | receipt_only | 47 |
| 38 | `distributed-r3-sparse-100000-nutmeg-native-wcc-optimized` | passed | receipt_only | 13 |
| 39 | `distributed-r3-sparse-10000-nutmeg-native-wcc-fused` | passed | receipt_only | 2 |
| 40 | `distributed-r3-sparse-100000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 47 |
| 41 | `distributed-r3-sparse-100000-nutmeg-datafusion-wcc-fused` | passed | receipt_only | 35 |
| 42 | `distributed-r3-sparse-1000000-nutmeg-native-wcc-fused` | passed | receipt_only | 107 |
| 43 | `distributed-r3-sparse-1000000-pecan-wcc-fused` | passed | receipt_only | 83 |
| 44 | `distributed-r3-sparse-1000000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 102 |
| 45 | `distributed-r3-sparse-100000-pecan-wcc-fused` | passed | receipt_only | 36 |
| 46 | `distributed-r3-sparse-1000000-nutmeg-native-wcc-optimized` | passed | receipt_only | 114 |
| 47 | `distributed-r3-sparse-10000-pecan-wcc-optimized` | passed | receipt_only | 31 |
| 48 | `distributed-r3-sparse-10000-nutmeg-datafusion-wcc-fused` | passed | receipt_only | 23 |
| 49 | `distributed-r3-sparse-100000-nutmeg-native-wcc-fused` | passed | receipt_only | 12 |
| 50 | `distributed-r3-sparse-10000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 29 |
| 51 | `distributed-r3-sparse-10000-nutmeg-native-wcc-optimized` | passed | receipt_only | 2 |
| 52 | `distributed-r3-sparse-1000000-nutmeg-datafusion-wcc-fused` | passed | receipt_only | 89 |
| 53 | `distributed-r3-sparse-1000000-pecan-wcc-optimized` | passed | receipt_only | 110 |
| 54 | `distributed-r3-sparse-10000-pecan-wcc-fused` | passed | receipt_only | 23 |
| 55 | `local-r1-sparse-100000-nutmeg-native-wcc-optimized` | passed | receipt_only | 13 |
| 56 | `local-r1-sparse-100000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 42 |
| 57 | `local-r1-sparse-100000-nutmeg-native-wcc-fused` | passed | receipt_only | 12 |
| 58 | `local-r1-sparse-100000-nutmeg-datafusion-wcc-fused` | passed | receipt_only | 31 |
| 59 | `local-r1-sparse-100000-pecan-wcc-fused` | passed | receipt_only | 30 |
| 60 | `local-r1-sparse-100000-pecan-wcc-optimized` | passed | receipt_only | 42 |
| 61 | `local-r2-sparse-100000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 43 |
| 62 | `local-r2-sparse-100000-pecan-wcc-fused` | passed | receipt_only | 31 |
| 63 | `local-r2-sparse-100000-nutmeg-native-wcc-optimized` | passed | receipt_only | 12 |
| 64 | `local-r2-sparse-100000-nutmeg-datafusion-wcc-fused` | passed | receipt_only | 30 |
| 65 | `local-r2-sparse-100000-nutmeg-native-wcc-fused` | passed | receipt_only | 13 |
| 66 | `local-r2-sparse-100000-pecan-wcc-optimized` | passed | receipt_only | 42 |
| 67 | `local-r3-sparse-100000-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 43 |
| 68 | `local-r3-sparse-100000-nutmeg-datafusion-wcc-fused` | passed | receipt_only | 32 |
| 69 | `local-r3-sparse-100000-nutmeg-native-wcc-optimized` | passed | receipt_only | 13 |
| 70 | `local-r3-sparse-100000-nutmeg-native-wcc-fused` | passed | receipt_only | 13 |
| 71 | `local-r3-sparse-100000-pecan-wcc-fused` | passed | receipt_only | 31 |
| 72 | `local-r3-sparse-100000-pecan-wcc-optimized` | passed | receipt_only | 42 |
| 73 | `chain-diagnostic-r1-chain-512-nutmeg-native-wcc-optimized` | passed | receipt_only | 1 |
| 74 | `chain-diagnostic-r1-chain-512-nutmeg-native-wcc-fused` | passed | receipt_only | 0 |
| 75 | `chain-diagnostic-r1-chain-512-nutmeg-datafusion-wcc-fused` | passed | receipt_only | 31 |
| 76 | `chain-diagnostic-r1-chain-512-pecan-wcc-fused` | passed | receipt_only | 30 |
| 77 | `chain-diagnostic-r1-chain-512-nutmeg-datafusion-wcc-optimized` | passed | receipt_only | 41 |
| 78 | `chain-diagnostic-r1-chain-512-pecan-wcc-optimized` | passed | receipt_only | 41 |

## Sampling and contraction detail

| Path | Algorithm | Variant | Execution sample count: min / median / max |
| --- | --- | --- | --- |
| nutmeg-datafusion | wcc | fused | 23 / 32 / 92 |
| nutmeg-datafusion | wcc | optimized | 29 / 43 / 102 |
| nutmeg-native | wcc | fused | 0 / 12 / 107 |
| nutmeg-native | wcc | optimized | 1 / 13 / 114 |
| pecan | wcc | fused | 22 / 31 / 87 |
| pecan | wcc | optimized | 30 / 42 / 110 |

Recorded whole-VM steal fractions: `{"observations": 78, "minimum": 0.0, "median": 0.0, "maximum": 0.0}`.

- `sparse-100000`: 9 identical contraction rounds across 36 successful cells, excluding the first raw-edge input count.
- `sparse-1000000`: 11 identical contraction rounds across 18 successful cells, excluding the first raw-edge input count.
- `sparse-10000`: 6 identical contraction rounds across 18 successful cells, excluding the first raw-edge input count.
- `chain-512`: 9 identical contraction rounds across 6 successful cells, excluding the first raw-edge input count.

Native advanced widths are recorded per dataset in the JSON. Native reference actual widths are not exported by these kernels and must not be inferred from requested concurrency.

Post-stop cleanup snapshot: `{"recorded_utc": "2026-09-27T21:07:54.158085+00:00", "run": "/targets/pecan-benchmark/runs/pecan-fusion-matrix-3fd69787", "harness_source_sha": "3fd6978757713482942656cb0b23205fe57647d0", "completed_cell_directories": 78, "remaining_staging_regular_files": [], "outcome": "passed"}`.


RSS/PSS are sampled execution-phase totals; short peaks may be missed. RSS can double-count shared mappings; PSS apportions them. Cgroup peaks include cache and lifetime activity and are not interchangeable with PSS or native admission accounting. No elapsed-time or memory ranking is inferred by this audit.

Cross-path WCC trace comparisons check coefficients, active vertices and contracted edges; only the first raw-edge input count may differ for fused plans. Floating PageRank frontier traces are recorded without requiring equality across different reduction orders.
