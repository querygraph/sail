# Benchmark numbers

Hot-path microbenchmarks, recorded per commit. See the [benchmark recipe](recipes/benchmark.md) for how each is run.

Every table names the machine and the commit. Medians are the number to compare; the minimum and maximum of the samples show how steady the run was. Cells that got worse are recorded like the rest.

## Machine

| | |
| :-- | :-- |
| Host | Apple M1 Max, 10 cores, 64 GiB, macOS 26.2 |
| Toolchain | rustc 1.98.1, `bench` profile |
| Caveat | A developer laptop, not a dedicated host: numbers compare runs on this machine and are not absolute. |

## Run of 2026-11-03

One pass of every bench binary, each tip's binaries one after the other, on the machine above with no build or test running (load average 8.2 at the start, 9.3 at the end; two unrelated system processes held one core each throughout). A pass taken fourteen minutes earlier, straight after a gate had finished (load average 12.1), agrees with this one within 2% on every median, so the medians are steady at this resolution.

| Tip | Commit | What it measures |
| :-- | :-- | :-- |
| `root` | `5b9111fc7` (the SDP root with B1; the benched crates are identical from `117ff726f` on) | the code as merged |
| `branch` | `ce5dada0f` (`agent/sail-performance-hot-paths`) | the hot-path rewrites on the pre-#2786 tree |
| `b3` | `c2bb8fdd6` (B3 on the root; SQL frontend only) | the single-statement fast path re-applied inside `SqlSource` |

Each cell is min / median / max in nanoseconds over the binary's own samples; `—` means the tip has no such case (the root's delete-partition cases are the per-file lookup it has; the branch's are its indexed lookup and the legacy one it kept).

What the numbers say, read across the tips:

- **Delete index.** Building the index is 25–40× faster on the branch (`mixed_32`: 9.9 µs root, 0.34 µs branch), and the branch's indexed partition lookup is 3–4× faster than its own legacy path and the root's per-file lookup (`empty_1000`: 440 µs root per file, 114 µs branch indexed). This is B5's gap.
- **Manifest parsing.** The branch is 6–7% *slower* at 100 and 1000 entries (`manifest_parse/1000`: 3.86 ms root, 4.10 ms branch) and equal at 1. Reusing the Avro reader did not pay on the rewritten `sail-iceberg`; B4 measures each of its commits separately before re-applying any.
- **Metadata discovery.** The branch is 14–18% faster from 256 snapshots up (`latest_4096`: 620 µs root, 507 µs branch); at 32 the difference is under 1 µs.
- **Metadata updates.** Parsing is 1.7–2.3× faster on the branch (`parse_1000`: 2.21 ms root, 0.97 ms branch), serialization 15–18%, and the current-snapshot lookup goes from linear in the snapshot count (415 ns at 1000) to constant (14 ns). B4's gap.
- **SQL frontend.** Object names, data types and expressions are the same on every tip. A simple statement parses in 68.6 µs on the root and 36.0 µs on the branch; B3 brings the root to 58.1 µs (−15%) and the complex statement from 536 µs to 526 µs. The remaining 22 µs on the simple statement is in the post-#2786 statement grammar itself, since the lexer and the sub-parsers are equal across tips; it is unexplained here and is not pursued further in B3.

### Iceberg delete index (`crates/sail-iceberg/benches/delete_index.rs`)

| Case | root min / median / max (ns) | branch min / median / max (ns) |
| :--- | ---: | ---: |
| `delete_index/empty` | 78 / 78 / 78 | 4 / 4 / 4 |
| `delete_index/path_8` | 2,984 / 2,992 / 2,999 | 79 / 79 / 84 |
| `delete_index/global_equality_8` | 2,349 / 2,356 / 2,367 | 51 / 52 / 59 |
| `delete_index/partition_8` | 2,276 / 2,281 / 2,288 | 190 / 192 / 209 |
| `delete_index/mixed_32` | 9,856 / 9,874 / 9,918 | 340 / 342 / 375 |
| `delete_partition/legacy_empty_1000` | — | 360,823 / 364,146 / 375,827 |
| `delete_partition/current_empty_1000` | — | 113,348 / 114,133 / 121,612 |
| `delete_partition/legacy_path_miss_1000` | — | 383,581 / 385,098 / 392,254 |
| `delete_partition/current_path_miss_1000` | — | 159,656 / 160,065 / 172,377 |
| `delete_partition/legacy_mixed_100` | — | 64,804 / 65,312 / 70,367 |
| `delete_partition/current_mixed_100` | — | 44,067 / 44,512 / 50,496 |
| `delete_partition/per_file_empty_1000` | 438,315 / 439,769 / 461,390 | — |
| `delete_partition/per_file_path_miss_1000` | 457,479 / 458,712 / 471,281 | — |
| `delete_partition/per_file_mixed_100` | 810,738 / 813,000 / 844,825 | — |

### Iceberg manifest parsing (`manifest_parsing.rs`)

| Case | root min / median / max (ns) | branch min / median / max (ns) |
| :--- | ---: | ---: |
| `manifest_parse/1` | 120,308 / 121,616 / 124,633 | 122,074 / 122,533 / 124,344 |
| `manifest_parse/100` | 487,670 / 490,980 / 501,512 | 518,045 / 520,776 / 530,553 |
| `manifest_parse/1000` | 3,832,117 / 3,862,200 / 4,004,892 | 4,097,150 / 4,104,542 / 4,284,700 |

### Iceberg metadata discovery (`metadata_discovery.rs`)

| Case | root min / median / max (ns) | branch min / median / max (ns) |
| :--- | ---: | ---: |
| `metadata_discovery_latest_32` | 5,633 / 5,719 / 7,719 | 4,871 / 4,959 / 5,166 |
| `metadata_discovery_numeric_hint_32` | 5,730 / 5,740 / 5,759 | 5,110 / 5,124 / 5,182 |
| `metadata_discovery_latest_256` | 39,824 / 39,949 / 40,402 | 33,453 / 33,674 / 33,950 |
| `metadata_discovery_numeric_hint_256` | 39,876 / 39,959 / 40,040 | 33,874 / 34,044 / 41,056 |
| `metadata_discovery_latest_4096` | 614,871 / 619,565 / 628,050 | 505,671 / 507,429 / 511,669 |
| `metadata_discovery_numeric_hint_4096` | 616,738 / 620,835 / 631,423 | 507,800 / 508,973 / 512,915 |

### Iceberg metadata updates (`metadata_updates.rs`)

| Case | root min / median / max (ns) | branch min / median / max (ns) |
| :--- | ---: | ---: |
| `updates_deserialize_append` | 1,870 / 1,876 / 1,964 | 1,760 / 1,768 / 1,806 |
| `metadata_current_snapshot_0` | 1 / 1 / 1 | 2 / 2 / 2 |
| `metadata_parse_0` | 19,810 / 19,883 / 20,228 | 11,387 / 11,431 / 11,648 |
| `metadata_serialize_0` | 5,555 / 5,578 / 5,659 | 5,345 / 5,371 / 5,396 |
| `metadata_roundtrip_0` | 25,508 / 25,642 / 26,619 | 18,081 / 18,125 / 18,181 |
| `metadata_current_snapshot_100` | 60 / 61 / 61 | 14 / 14 / 14 |
| `metadata_parse_100` | 231,644 / 232,134 / 232,948 | 103,704 / 104,155 / 104,981 |
| `metadata_serialize_100` | 93,483 / 94,015 / 94,388 | 75,271 / 75,566 / 75,895 |
| `metadata_roundtrip_100` | 323,908 / 326,560 / 331,821 | 180,532 / 181,136 / 193,132 |
| `metadata_current_snapshot_1000` | 414 / 415 / 416 | 14 / 14 / 14 |
| `metadata_parse_1000` | 2,204,898 / 2,212,888 / 2,220,871 | 961,219 / 966,234 / 972,470 |
| `metadata_serialize_1000` | 909,456 / 912,142 / 918,511 | 746,018 / 748,162 / 753,817 |
| `metadata_roundtrip_1000` | 3,131,418 / 3,140,992 / 3,221,490 | 1,716,804 / 1,722,334 / 1,730,537 |

### SQL frontend (`crates/sail-sql-analyzer/benches/sql_frontend.rs`)

| Case | root min / median / max (ns) | b3 min / median / max (ns) | branch min / median / max (ns) |
| :--- | ---: | ---: | ---: |
| `parse_object_name` | 4,610 / 4,629 / 4,668 | 4,498 / 4,514 / 4,802 | 4,494 / 4,525 / 4,652 |
| `parse_data_type_complex` | 66,441 / 66,570 / 66,802 | 65,971 / 66,126 / 66,419 | 66,285 / 66,495 / 66,902 |
| `parse_expression_complex` | 291,862 / 292,836 / 294,721 | 291,422 / 292,550 / 299,054 | 292,889 / 294,360 / 299,746 |
| `parse_statement_simple` | 68,250 / 68,563 / 69,688 | 57,612 / 58,085 / 62,357 | 35,816 / 36,041 / 36,690 |
| `parse_statement_complex` | 532,245 / 535,628 / 540,475 | 524,049 / 525,890 / 549,487 | 505,487 / 508,346 / 512,093 |
| `parse_statements_batch_64` | 1,476,040 / 1,484,360 / 1,502,428 | 1,484,543 / 1,494,992 / 1,521,122 | 1,462,500 / 1,473,431 / 1,494,662 |
| `analyze_cloned_ast_simple` | 668 / 677 / 720 | 652 / 678 / 688 | 630 / 674 / 769 |
| `analyze_cloned_ast_complex` | 6,810 / 6,899 / 7,025 | 6,776 / 6,826 / 7,072 | 6,478 / 6,543 / 8,617 |
| `frontend_simple` | 68,198 / 68,887 / 69,874 | 58,144 / 58,448 / 59,205 | 36,282 / 36,764 / 37,111 |
| `frontend_complex` | 537,077 / 542,458 / 560,799 | 526,959 / 533,622 / 590,135 | 515,221 / 518,654 / 523,298 |

### Object-store read cache (`object_store_cache.rs`, branch only until B6)

| Case | branch min / median / max (ns) |
| :--- | ---: |
| `raw_head` | 164 / 164 / 209 |
| `cached_head` | 300 / 300 / 310 |
| `raw_range_4k` | 201 / 203 / 230 |
| `cached_range_4k` | 544 / 548 / 556 |
| `cached_range_unaligned_2m` | 42,345 / 46,356 / 48,713 |
| `cached_get_ranges_32x4k` | 7,968 / 7,979 / 8,013 |
| `cached_concurrent_16x4k` | 9,179 / 9,192 / 9,202 |
| `cached_full_stream_16m` | 344,956 / 347,984 / 362,969 |

## Run of 2026-11-03, the Iceberg units

The same machine and binaries' build profile, one pass of the three Iceberg bench binaries built at the tip of the B4 and B5 offer (`ee5afb18f` on root `9090d7fb0`), taken in the first idle minute after a gate (load average 17.7 at the start, 17.1 at the end; the two system processes of the first run still held a core each). The root and branch columns are the first run's, for the same cases; the benched crates are unchanged between `5b9111fc7` and `9090d7fb0`. A pass of the same binaries' predecessors fourteen minutes earlier, under load 14, agrees within 1% on every case the intervening commit did not touch, and a repeat of this pass ten minutes later agrees within 2% on every case.

What each unit did, read against its own cases:

- **Newest-first lookups** (`2210872a1`): `metadata_current_snapshot_1000` 415 ns → 9 ns; the lookup is constant in the snapshot count.
- **Direct v2/v3 deserialization** (`a63542c79`): `metadata_parse_1000` 2.21 ms → 0.96 ms, `metadata_parse_0` 19.9 µs → 11.4 µs, equal to the branch. Serialization is not touched by this offer and stays at the root's number (0.91 ms at 1000; the branch's 0.75 ms came from a commit not re-applied).
- **Streamed discovery, path kept in place** (`129d94161`): `metadata_discovery_latest_4096` 620 µs → 460 µs, and 5.7 µs → 4.2 µs at 32. The fold alone, with the path copied per object, measured 613 µs; keeping the listing's path in place is where the time was, and this is below the branch's 507 µs.
- **Lookup without redundant work** (`7717969aa`) and **shared descriptors** (`ee5afb18f`): the lookup over eight path-scoped deletes 2.99 µs → 0.75 µs, over eight global equality deletes 2.36 µs → 0.26 µs, the mixed index of 32 9.87 µs → 1.38 µs, and the per-file routing of 100 files over a mixed index 813 µs → 111 µs. The first of the two measured alone took the per-file routing to 365 µs on an empty index; the rest is the descriptor sharing. The branch's 79 ns on the path-scoped case is not reached because the root's index checks each path-scoped position delete against the data file's partition and each equality delete against the data file's bounds, rules the branch did not have.
- **One clone per planned file** (`1ba43e432`) has no bench case; it removes one `DataFile` clone per planned file from the scan's routing and is recorded here for the list.

#### Iceberg metadata updates

| Case | root (5b9111fc7) median (ns) | this offer median (ns) | branch median (ns) |
| :--- | ---: | ---: | ---: |
| `updates_deserialize_append` | 1,876 | 1,860 | 1,768 |
| `metadata_current_snapshot_0` | 1 | 1 | 2 |
| `metadata_parse_0` | 19,883 | 11,403 | 11,431 |
| `metadata_serialize_0` | 5,578 | 5,560 | 5,371 |
| `metadata_roundtrip_0` | 25,642 | 17,085 | 18,125 |
| `metadata_current_snapshot_100` | 61 | 9 | 14 |
| `metadata_parse_100` | 232,134 | 103,387 | 104,155 |
| `metadata_serialize_100` | 94,015 | 93,448 | 75,566 |
| `metadata_roundtrip_100` | 326,560 | 196,901 | 181,136 |
| `metadata_current_snapshot_1000` | 415 | 9 | 14 |
| `metadata_parse_1000` | 2,212,888 | 959,581 | 966,234 |
| `metadata_serialize_1000` | 912,142 | 911,068 | 748,162 |
| `metadata_roundtrip_1000` | 3,140,992 | 1,878,325 | 1,722,334 |

#### Iceberg metadata discovery

| Case | root (5b9111fc7) median (ns) | this offer median (ns) | branch median (ns) |
| :--- | ---: | ---: | ---: |
| `metadata_discovery_latest_32` | 5,719 | 4,248 | 4,959 |
| `metadata_discovery_numeric_hint_32` | 5,740 | 4,269 | 5,124 |
| `metadata_discovery_latest_256` | 39,949 | 30,586 | 33,674 |
| `metadata_discovery_numeric_hint_256` | 39,959 | 30,426 | 34,044 |
| `metadata_discovery_latest_4096` | 619,565 | 459,794 | 507,429 |
| `metadata_discovery_numeric_hint_4096` | 620,835 | 462,477 | 508,973 |

#### Iceberg delete index

| Case | root (5b9111fc7) median (ns) | this offer median (ns) | branch median (ns) |
| :--- | ---: | ---: | ---: |
| `delete_index/empty` | 78 | 4 | 4 |
| `delete_index/path_8` | 2,992 | 750 | 79 |
| `delete_index/global_equality_8` | 2,356 | 258 | 52 |
| `delete_index/partition_8` | 2,281 | 320 | 192 |
| `delete_index/mixed_32` | 9,874 | 1,378 | 342 |
| `delete_partition/per_file_empty_1000` | 439,769 | 369,075 | — |
| `delete_partition/per_file_path_miss_1000` | 458,712 | 390,542 | — |
| `delete_partition/per_file_mixed_100` | 813,000 | 110,842 | — |

## Run of 2026-11-03, the object-store cache (B6)

The cache bench built at the B6 tip (`59738fe29` on root `4c8ccb97e`), one pass against the branch tip's own numbers from the first run. The pass was taken while another process held the machine (load average 130 at the start), so the medians are an upper bound. A repeat nine minutes later at load 25 agrees within 2% on every case but the 16 MiB full stream, which came down from 409 µs to 379 µs; that case is the one most exposed to the load, and the lower figure is the better estimate. The cache has no root counterpart: the root reads the inner store directly, which is what the `raw_*` cases measure on both tips.

| Case | this offer min / median / max (ns) | branch min / median / max (ns) |
| :--- | ---: | ---: |
| `raw_head` | 164 / 166 / 218 | 164 / 164 / 209 |
| `cached_head` | 299 / 303 / 309 | 300 / 300 / 310 |
| `raw_range_4k` | 202 / 205 / 248 | 201 / 203 / 230 |
| `cached_range_4k` | 544 / 551 / 565 | 544 / 548 / 556 |
| `cached_range_unaligned_2m` | 48,430 / 49,248 / 51,918 | 42,345 / 46,356 / 48,713 |
| `cached_get_ranges_32x4k` | 7,963 / 8,087 / 8,819 | 7,968 / 7,979 / 8,013 |
| `cached_concurrent_16x4k` | 9,249 / 9,343 / 9,460 | 9,179 / 9,192 / 9,202 |
| `cached_full_stream_16m` | 372,784 / 409,154 / 463,763 | 344,956 / 347,984 / 362,969 |

What the numbers say: a cached 4 KiB range costs 2.7× a raw one on an in-memory inner store (551 ns against 205 ns), which is the price of the page split, the key lookup and the copy when the inner store is as fast as memory; the cache earns its place only when the inner store is a network away, which the bench does not model. The unaligned 2 MiB read, the 32-range read and the 16-way concurrent read match the branch within 7%, so the layer behaves on the rewritten host as it did on the branch.

## Index of runs

| Date | Section | Tip measured | What it compares |
| :-- | :-- | :-- | :-- |
| 2026-11-03 | Run of 2026-11-03 | `5b9111fc7` (root), `ce5dada0f` (branch), `c2bb8fdd6` (B3) | the root against the hot-path branch, and the single-statement parse fast path |
| 2026-11-03 | the Iceberg units | `ee5afb18f` on `9090d7fb0` | each of the six Iceberg units against its own case on the root |
| 2026-11-03 | the object-store cache (B6) | `59738fe29` on `4c8ccb97e` | the re-homed cache against the branch's own numbers |

A new run is a new section above this index, named by its date and what it measured, with the machine, the load and the commits stated; the index gains one row.
