# sem_benchmark: Nutmeg pagerank vs PySpark Pregel pagerank (gfrs-poc) on Sail

A POC harness that compares two PageRank implementations against the same Sail
server, the same two parquet input files per dataset and the same resource
envelope, and reports in the graphframes-rs benchmark style.

| `--engine` | Implementation | Notes |
| --- | --- | --- |
| `pregel` | [`gfrs_poc.pagerank`](../../../graph-algorithms/gfrs-poc) — pure PySpark port of the graphframes-rs Pregel engine | GraphX-style deltas, decreasing active frontier, parquet checkpoints (no purge) |
| `nutmeg` | `sail_nutmeg` native kernel `pagerankDelta` (Banda) | driver-resident CSR, tolerance-scaled residual pushes |

Both run until convergence with `tol=1e-5` (damping 0.85) and write exactly one
parquet result file (`id`, `pagerank`) under `--work-dir`.

## Harness shape

- **Inputs**: two parquet files per dataset on local disk
  (`<data-dir>/<dataset>/<dataset>-{v,e}.parquet`, downloaded from
  `datasets.ldbcouncil.org` on first use).
- **Cold runs**: every run (warmup included) starts a **fresh Sail server
  process** and stops it afterwards; every measured run is a "first run".
- **Server settings per run** (from `main.py` flags):
  - `SAIL_MODE=local`
  - `SAIL_RUNTIME__MEMORY_POOL__TYPE=fair`,
    `SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE=<--max-memory>`
  - `SAIL_RUNTIME__TEMPORARY_FILES__PATHS=["<--work-dir>/spill"]` (point this at
    fast local NVM)
  - `--use-smj` -> `SAIL_OPTIMIZER__PREFER_HASH_JOIN=false` (this is also the
    graphframes-rs default)
- **pregel checkpoints** land under `<--work-dir>/<engine>_<dataset>/run_<i>/gfrs/<uuid>/`
  (a fresh uuid per run, like graphframes-rs). **Nothing is purged**, so disk
  usage grows with the iteration count — keep `--work-dir` on a large volume.
- **Output**: `results/<engine>/<scale>/<dataset>/max_mem_<mem>_<hash|smj>_parts_<n>/`
  with `benchmark.json` (params, per-run wall time / peak RSS / peak disk /
  iterations / residual, stats, DTW-aligned series) plus `wall_time`, `rss` and
  `disk` gnuplot scripts and PNGs when gnuplot is installed.

The `datasets.py`, `monitor.py`, `stats.py` and `plotting.py` files are copied
as-is from `graphframes-rs/benches/python/`.

## What to do on a VPS

Prerequisites (Ubuntu/Debian; the same inventory as
`examples/extensions/scripts/Dockerfile.linux` minus GEOS — Sedona is not needed):

```bash
sudo apt-get update && sudo apt-get install -y \
  curl git build-essential clang libclang-dev libssl-dev pkg-config \
  libprotobuf-dev protobuf-compiler
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.97.1
source "$HOME/.cargo/env"
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Then, from the branch checkout (put the repo and the build targets on a fast
disk; the Rust builds need tens of GiB):

```bash
cd sail
make -C examples/extensions/benchmarks/sem_benchmark venv   # .venv: pyspark[connect], maturin, gfrs-poc
make -C examples/extensions/benchmarks/sem_benchmark build  # nutmeg wheel + sail host (release, target-cpu=native)
```

Smoke test (tiny 10-vertex graph, both engines, one run, no warmup):

```bash
make -C examples/extensions/benchmarks/sem_benchmark smoke
```

Measured runs (adjust `WORK_DIR` to your NVM mount; sizes from the LDBC catalog:
cit-Patents ~16M edges, graph500-24 ~260M edges, twitter_mpi ~1.4B edges):

```bash
S=examples/extensions/benchmarks/sem_benchmark
make -C $S bench      DS=cit-Patents ENGINE=both WORK_DIR=/mnt/nvm/sem RUNS=3 MAX_MEMORY=24G
make -C $S bench-smj  DS=cit-Patents ENGINE=both WORK_DIR=/mnt/nvm/sem RUNS=3 MAX_MEMORY=24G
make -C $S bench      DS=graph500-24  ENGINE=both WORK_DIR=/mnt/nvm/sem RUNS=3 MAX_MEMORY=24G
make -C $S bench      DS=twitter_mpi  ENGINE=both WORK_DIR=/mnt/nvm/sem RUNS=3 MAX_MEMORY=24G
```

Every Makefile variable (`WORK_DIR`, `RESULTS_DIR`, `DATA_DIR`, `MAX_MEMORY`,
`PARTITIONS`, `THREADS`, `DS`, `ENGINE`, `RUNS`, `TOL`) can be overridden on the
command line. `make -C $S clean` removes the workdir and results.

## Reading the numbers

- `wall_time_s` covers **input DataFrame handles -> completed result parquet
  write** (server startup, verification and cleanup are excluded), matching the
  boundary of the existing extension benchmarks.
- `peak_rss_kb` samples the **Sail server process** (`/proc/<pid>/status VmRSS`);
  `peak_disk_bytes` is the exact `du`-style size of the run's workdir tree.
- For `nutmeg`, per-run `details.native` carries the kernel-reported
  `iterations`, `converged` and `residual`; for `pregel`, `details.iterations`
  and `details.active_counts` come from the engine itself.

## Notes / POC limitations

- No checkpoint purge: the PySpark Connect API has no FS list/delete, so
  `gfrs-poc` keeps every `state-<i>` / `aggregated-messages-<i>` directory.
  Expect roughly `2 * iterations * (state size)` on disk per measured run.
- `--nutmeg-kernel pagerank` switches the nutmeg side to the Grust reference
  kernel (successive-iterate stopping); the default `pagerankDelta` is the
  apples-to-apples delta variant.
- `--nutmeg-memory` is prepaid from the same fair pool as `--max-memory`, so it
  must stay smaller (the harness rejects invalid pairs up front).
