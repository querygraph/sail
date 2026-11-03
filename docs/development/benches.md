# Benchmark numbers

Hot-path microbenchmarks, recorded per commit. See the [benchmark recipe](recipes/benchmark.md) for how each is run.

Every table names the machine and the commit. Medians are the number to compare; the minimum and maximum of the samples show how steady the run was. Cells that got worse are recorded like the rest.

## Machine

| | |
| :-- | :-- |
| Host | Apple M1 Max, 10 cores, 64 GiB, macOS 26.2 |
| Toolchain | rustc 1.98.1, `bench` profile |
| Caveat | A developer laptop, not a dedicated host: numbers compare runs on this machine and are not absolute. |

## Tables

Filled in by B1 (the root) and B2 (the performance branch's own tip).
