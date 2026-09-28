#!/usr/bin/env python3
"""sem_benchmark: Nutmeg pagerank vs the gfrs-poc (PySpark Pregel) pagerank.

Both engines run against the same Sail server build (see ./Makefile), the same
two LDBC Graphalytics parquet files per dataset and the same resource envelope:

  * fresh Sail server per run (a "cold" Sail state; the OS page cache stays warm),
  * fair memory pool bounded by --max-memory, spill files under --work-dir,
  * optional SMJ join preference (--use-smj -> optimizer.prefer_hash_join=false),
  * pagerank until convergence with tol=1e-5 (override with --tol/--max-iter),
  * exactly one parquet result file per run under --work-dir.

Results are written in the graphframes-rs benchmark style:

    results/<engine>/<scale>/<dataset>/max_mem_<mem>_<join>_parts_<n>/
        benchmark.json + wall_time/rss/disk .dat/.gnuplot(+.png when gnuplot exists)

Usage (from the built venv, see ./Makefile and ./README.md):

    python3 main.py --engine both --dataset cit-Patents --work-dir /mnt/nvm/sem \
        --max-memory 24G --use-smj
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("SPARK_CONNECT_MODE_ENABLED", "1")

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[3]
sys.path.insert(0, str(HERE))
# POC shortcut: import gfrs_poc straight from its sources (no install step needed).
sys.path.insert(0, str(REPO_ROOT / "examples" / "extensions" / "graph-algorithms" / "gfrs-poc" / "src"))

import datasets
import monitor
import plotting
import stats
from runtime import SailServer

DEFAULT_SAIL_BINARY = REPO_ROOT / "target" / "release" / "sail"
logger = logging.getLogger("sem_benchmark")

ENGINES = ("pregel", "nutmeg")


def dataset_info(name: str) -> dict:
    """Catalog metadata for `name`, or a permissive stub for uncataloged graphs.

    LDBC Graphalytics parquet links are uniform
    (`<BASE_URL>/<name>-{v,e}.parquet`), so any dataset published there can be
    benchmarked; only the size class / counters then stay unknown.
    """
    if name in datasets.CATALOG:
        return datasets.info(name)
    logger.warning(
        "%s is not in the local catalog; assuming %s/%s-{v,e}.parquet "
        "(size class and vertex/edge counts unknown)",
        name, datasets.BASE_URL, name,
    )
    return {"name": name, "scale": "-", "nodes_str": "?", "edges_str": "?",
            "size": "?", "vertices": None, "edges": None}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Benchmark nutmeg pagerank vs gfrs-poc pregel pagerank on Sail.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--engine", default="both",
                   help="comma-separated subset of: {}".format(", ".join(ENGINES + ("both",))))
    p.add_argument("--dataset", default="cit-Patents",
                   help="LDBC dataset name (see graphframes-rs benches datasets catalog)")
    p.add_argument("--work-dir", required=True,
                   help="fast local (NVM) dir for spills, pregel checkpoints and results")
    p.add_argument("--max-memory", default="24G",
                   help="Sail fair memory pool size (runtime.memory_pool.fair.max_size)")
    p.add_argument("--use-smj", action="store_true",
                   help="prefer sort-merge join (optimizer.prefer_hash_join=false)")
    p.add_argument("--sail-binary", default=str(DEFAULT_SAIL_BINARY),
                   help="path to the sail server binary")
    p.add_argument("--tol", type=float, default=1e-5, help="pagerank convergence tolerance")
    p.add_argument("--damping", type=float, default=0.85, help="pagerank damping factor")
    p.add_argument("--max-iter", type=int, default=0,
                   help="iteration cap; 0 = run until convergence (pregel voting)")
    p.add_argument("--nutmeg-kernel", default="pagerankDelta",
                   choices=["pagerankDelta", "pagerank"],
                   help="nutmeg kernel: pagerankDelta is the delta/active-frontier variant")
    p.add_argument("--nutmeg-memory", default="8G",
                   help="SAIL_NUTMEG_MEMORY_BYTES (native per-session quota; must be "
                        "smaller than --max-memory, it is prepaid from the same pool)")
    p.add_argument("--partitions", type=int, default=8,
                   help="checkpoint repartitioning / default parallelism")
    p.add_argument("--threads", type=int, default=8, help="tokio/rayon threads, nutmeg concurrency")
    p.add_argument("--runs", type=int, default=3, help="number of measured runs per engine")
    p.add_argument("--warmup", type=int, default=1, help="number of discarded warmup runs")
    p.add_argument("--results-dir", default=str(HERE / "results"), help="root of the results tree")
    p.add_argument("--data-dir", default=str(HERE / "data"), help="dataset download location")
    p.add_argument("--port", type=int, default=0, help="fixed server port (0 = pick a free one)")
    p.add_argument("--target-samples", type=int, default=300,
                   help="target number of monitor samples per measured run")
    p.add_argument("--align", choices=["dtw", "duration"], default="dtw",
                   help="series alignment for the rss/disk bands")
    return p.parse_args()


# --------------------------------------------------------------------------
# sampling: the engine is the Sail server process, so we watch its RSS and the
# workdir disk consumption from a background thread (du-style, like monitor.py).
# --------------------------------------------------------------------------

class ServerSampler:
    """Samples (t, server RSS kb, workdir disk bytes) every `interval` seconds.

    Reuses the du-style tree walk and the /proc RSS readers from the copied
    ``monitor.py``; the watched process is the Sail server, since that is where
    both engines execute.
    """

    def __init__(self, pid: int, workdir: Path, interval: float):
        self.pid = pid
        self.workdir = str(workdir)
        self.interval = interval
        self.samples: list[tuple[float, float, float]] = []
        self._baseline = monitor._tree_size_kb(self.workdir) * 1024.0
        self._start = time.perf_counter()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self) -> None:
        while not self._stop.is_set():
            t = time.perf_counter() - self._start
            rss = monitor._vm_kb(monitor._read_proc_status(self.pid), "VmRSS")
            disk = max(0.0, monitor._tree_size_kb(self.workdir) * 1024.0 - self._baseline)
            self.samples.append((t, float(rss) if rss is not None else 0.0, disk))
            self._stop.wait(self.interval)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> list[tuple[float, float, float]]:
        self._stop.set()
        self._thread.join(timeout=self.interval * 2 + 1)
        return self.samples

    @property
    def peak_rss_kb(self) -> float | None:
        values = [rss for _t, rss, _d in self.samples if rss > 0]
        return max(values) if values else None

    @property
    def peak_disk_bytes(self) -> float:
        return max((disk for _t, _rss, disk in self.samples), default=0.0)


# --------------------------------------------------------------------------
# trial execution
# --------------------------------------------------------------------------

def create_session(endpoint: str):
    from pyspark.sql.connect.session import SparkSession

    spark = SparkSession.builder.remote(endpoint).create()
    try:
        from pyspark.sql.connect.client.retries import DefaultPolicy

        # Fail fast instead of retrying a dead server for minutes.
        spark.client.set_retry_policies(
            [DefaultPolicy(max_retries=1, initial_backoff=100, max_backoff=100, jitter=0)]
        )
    except Exception as exc:  # noqa: BLE001 - retry policy plumbing is best-effort
        logger.debug("could not set retry policies: %s", exc)
    return spark


def run_engine(spark, engine: str, args: argparse.Namespace, run_workdir: Path) -> dict:
    """Read the two parquet files, run pagerank, write one result parquet file.

    The timer starts at the input DataFrame handles and ends after the result
    write returns (same boundary as the existing extension benchmarks).
    """
    from pyspark.sql import functions as F

    dataset_dir = Path(args.data_dir) / args.dataset
    started = time.perf_counter()
    vertices = spark.read.parquet((dataset_dir / f"{args.dataset}-v.parquet").as_uri())
    edges = spark.read.parquet((dataset_dir / f"{args.dataset}-e.parquet").as_uri())

    details: dict = {}
    if engine == "pregel":
        # gfrs_poc follows the graphframes-rs naming: id / src / dst.
        edges = edges.select(F.col("source").alias("src"), F.col("target").alias("dst"))
        from gfrs_poc import pagerank

        pregel_info: dict = {}
        result = pagerank(
            edges,
            vertices,
            tol=args.tol,
            max_iter=args.max_iter,
            reset_prob=1.0 - args.damping,
            checkpoint_dir=str((run_workdir / "gfrs").resolve()),
            num_partitions=args.partitions,
            info=pregel_info,
        )
        details["iterations"] = pregel_info.get("iterations")
        details["active_counts"] = pregel_info.get("active_counts")
        out = result.select(F.col("id"), F.col("pagerank").alias("score"))
    else:
        from sail_nutmeg import Nutmeg

        nm = Nutmeg(spark)
        nodes = vertices.select(F.col("id").cast("string").alias("node_id"))
        links = edges.select(F.col("source"), F.col("target"))
        stage_started = time.perf_counter()
        staged = nm.stage("benchmark", nodes, links)
        details["stage_seconds"] = time.perf_counter() - stage_started
        details["stage_receipt"] = staged.asDict()
        cap = args.max_iter if args.max_iter > 0 else 1000
        options: dict = {"maxIterations": cap, "concurrency": args.threads}
        if args.nutmeg_kernel == "pagerankDelta":
            options.update(
                damping=args.damping,
                tolerance=args.tol,
                precision="f64",
                orientation="outgoing",
            )
        frame = nm.run("benchmark", args.nutmeg_kernel, **options)
        out = frame.select(F.col("nodeId").cast("long").alias("id"), F.col("score").alias("pagerank"))

    output_dir = run_workdir / "result"
    out.coalesce(1).write.mode("error").parquet(output_dir.as_uri())
    details["wall_time_s"] = time.perf_counter() - started
    details["result"] = str(output_dir)

    if engine == "nutmeg":
        # Diagnostics only; this never re-executes the kernel.
        try:
            status = nm.status()
            reads = [r for r in status.get("reads", []) if r.get("algorithm") == args.nutmeg_kernel]
            if reads:
                diag = reads[-1].get("diagnostics") or {}
                details["native"] = {
                    key: diag[key]
                    for key in ("iterations", "converged", "residual", "frontier_edges",
                                "certificate_passes")
                    if key in diag
                }
        except Exception as exc:  # noqa: BLE001 - diagnostics are best-effort
            details["native_status_error"] = repr(exc)
        try:
            nm.drop("benchmark")
        except Exception as exc:  # noqa: BLE001
            details["drop_error"] = repr(exc)
    return details


def run_trial(engine: str, args: argparse.Namespace, run_workdir: Path, interval: float) -> dict:
    """One fresh Sail server, one monitored pagerank run."""
    run_workdir.mkdir(parents=True, exist_ok=True)
    log_path = run_workdir / "client.log"
    handler = logging.FileHandler(log_path)
    handler.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(handler)
    server = sampler = spark = None
    try:
        server = SailServer(
            Path(args.sail_binary),
            run_workdir,
            max_memory=args.max_memory,
            use_smj=args.use_smj,
            partitions=args.partitions,
            threads=args.threads,
            log_path=run_workdir / "server.log",
            load_extensions=(engine == "nutmeg"),
            nutmeg_memory=args.nutmeg_memory,
            port=args.port,
        )
        endpoint = server.start()
        sampler = ServerSampler(server.process.pid, run_workdir, interval)
        sampler.start()
        spark = create_session(endpoint)
        row = spark.sql("SELECT 1 AS ready").first()
        if not row or row[0] != 1:
            raise RuntimeError("server readiness query failed")
        details = run_engine(spark, engine, args, run_workdir)
        samples = sampler.stop()
        return {
            "wall_time_s": details["wall_time_s"],
            "peak_rss_kb": sampler.peak_rss_kb,
            "peak_disk_bytes": sampler.peak_disk_bytes,
            "samples": samples,
            "details": details,
        }
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as exc:  # noqa: BLE001
                logger.debug("spark.stop failed: %s", exc)
        if sampler is not None:
            sampler.stop()
        if server is not None:
            server.stop()
        logging.getLogger().removeHandler(handler)
        handler.close()


# --------------------------------------------------------------------------
# reporting (graphframes-rs style)
# --------------------------------------------------------------------------

def environment_info() -> dict:
    import platform
    from importlib.metadata import version

    env = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "gnuplot": shutil.which("gnuplot") is not None,
    }
    for package in ("pyspark", "sail-nutmeg", "gfrs-poc"):
        try:
            env[f"package:{package}"] = version(package)
        except Exception:  # noqa: BLE001
            env[f"package:{package}"] = "not installed (source import)"
    try:
        r = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=REPO_ROOT, check=False
        )
        if r.returncode == 0:
            env["git_commit"] = r.stdout.strip()
    except OSError:
        pass
    return env


def benchmark_engine(engine: str, args: argparse.Namespace) -> None:
    ds_info = dataset_info(args.dataset)
    join = "smj" if args.use_smj else "hash"
    run_dir = (
        Path(args.results_dir) / engine / ds_info["scale"] / args.dataset
        / f"max_mem_{args.max_memory}_{join}_parts_{args.partitions}"
    )
    run_dir.mkdir(parents=True, exist_ok=True)
    workroot = Path(args.work_dir) / f"{engine}_{args.dataset}"
    workroot.mkdir(parents=True, exist_ok=True)

    def execute(run_i: str, interval: float) -> dict:
        return run_trial(engine, args, workroot / f"run_{run_i}", interval)

    logger.info("== %s / %s ==", engine, args.dataset)

    # --- warmup: discarded; warms the page cache and calibrates the interval ---
    if args.warmup > 0:
        warm = execute("warmup", 0.5)
        interval = max(0.02, min(1.0, warm["wall_time_s"] / args.target_samples))
        logger.info("warmup: %.2fs -> sampling interval %.3fs", warm["wall_time_s"], interval)
    else:
        warm = {"wall_time_s": None, "sampling_interval_s": None}
        interval = 0.5

    # --- measured runs ---
    results = []
    for i in range(args.runs):
        res = execute(i, interval)
        results.append(res)
        details = res["details"]
        logger.info(
            "run %d: %.3fs peak_rss=%s kB peak_disk=%.1f MB iterations=%s samples=%d",
            i, res["wall_time_s"], res["peak_rss_kb"], res["peak_disk_bytes"] / 1e6,
            details.get("iterations", details.get("native", {}).get("iterations")),
            len(res["samples"]),
        )

    # --- statistics ---
    times = [r["wall_time_s"] for r in results]
    time_stats = stats.describe(times)
    rss_peaks = [r["peak_rss_kb"] for r in results if r["peak_rss_kb"]]
    rss_stats = stats.describe(rss_peaks) if rss_peaks else None
    disk_peaks = [r["peak_disk_bytes"] for r in results]
    disk_stats = stats.describe(disk_peaks)
    medges = (ds_info["edges"] / time_stats["median"] / 1e6) if ds_info["edges"] else None

    # --- series alignment ---
    series = None
    samples_list = [r["samples"] for r in results]
    if all(len(s) >= 2 for s in samples_list):
        align = stats.align_series(samples_list, method=args.align)
        series = {
            "alignment": align["method"],
            "ref_run": align["ref_run"],
            "band_frac": align["band_frac"],
            "grid_size": len(align["grid"]),
            "rss_bands_gib": [[m / 1048576, lo / 1048576, hi / 1048576] for m, lo, hi in align["rss_bands"]],
            "disk_bands_gib": [[m / 1073741824, lo / 1073741824, hi / 1073741824] for m, lo, hi in align["disk_bands"]],
        }

    payload = {
        "algorithm": "pagerank",
        "engine": engine,
        "dataset": args.dataset,
        "size_class": ds_info["scale"],
        "params": {
            "max_memory": args.max_memory,
            "use_smj": args.use_smj,
            "join": join,
            "partitions": args.partitions,
            "threads": args.threads,
            "tol": args.tol,
            "damping": args.damping,
            "max_iter": args.max_iter,
            "nutmeg_kernel": args.nutmeg_kernel if engine == "nutmeg" else None,
            "nutmeg_memory": args.nutmeg_memory if engine == "nutmeg" else None,
            "sail_binary": str(Path(args.sail_binary).resolve()),
            "work_dir": str(workroot),
            "runs": args.runs,
            "warmup": args.warmup,
            "target_samples": args.target_samples,
            "align": args.align,
            "disk_mode": "du",
        },
        "graph": {
            "vertices": ds_info["vertices"],
            "edges": ds_info["edges"],
            "nodes_str": ds_info["nodes_str"],
            "edges_str": ds_info["edges_str"],
        },
        "warmup": {"wall_time_s": warm.get("wall_time_s"), "sampling_interval_s": warm.get("sampling_interval_s")},
        "runs": [
            {
                "index": i,
                "wall_time_s": r["wall_time_s"],
                "peak_rss_kb": r["peak_rss_kb"],
                "peak_disk_bytes": r["peak_disk_bytes"],
                "n_samples": len(r["samples"]),
                "details": r["details"],
            }
            for i, r in enumerate(results)
        ],
        "stats": {
            "wall_time_s": time_stats,
            "peak_rss_kb": rss_stats,
            "peak_disk_bytes": disk_stats,
            "medges_per_sec": medges,
        },
        "series": series,
        "raw_series": [
            {
                "t": [s[0] for s in r["samples"]],
                "rss_kb": [s[1] for s in r["samples"]],
                "disk_bytes": [s[2] for s in r["samples"]],
            }
            for r in results
        ],
        "environment": environment_info(),
    }
    with open(run_dir / "benchmark.json", "w") as f:
        json.dump(payload, f, indent=2)

    # --- plots ---
    title = (
        f"{engine} pagerank / {args.dataset} ({ds_info['scale']}) — "
        f"max_mem_{args.max_memory}_{join}_parts_{args.partitions}"
    )
    plotting.write_wall_time(run_dir, title, times, time_stats)
    if series is not None:
        grid = [i / (series["grid_size"] - 1) for i in range(series["grid_size"])]
        plotting.write_series(run_dir, "rss", title + " — RSS", "RSS (GiB)",
                              grid, [tuple(b) for b in series["rss_bands_gib"]])
        plotting.write_series(run_dir, "disk", title + " — disk usage", "disk consumed (GiB)",
                              grid, [tuple(b) for b in series["disk_bands_gib"]])
    rendered = sum(plotting.render(s) for s in run_dir.glob("*.gnuplot"))
    logger.info("-> %s (gnuplot: %s)", run_dir, "rendered" if rendered else "not available")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    args = parse_args()
    engines: list[str] = []
    for part in args.engine.split(","):
        part = part.strip()
        if part == "both":
            engines.extend(ENGINES)
        elif part in ENGINES:
            engines.append(part)
        else:
            raise SystemExit(f"unknown engine {part!r}; available: {', '.join(ENGINES + ('both',))}")
    if args.runs < 1:
        raise SystemExit("--runs must be >= 1")
    if not Path(args.sail_binary).exists():
        raise SystemExit(f"sail binary not found: {args.sail_binary} (run `make build` first)")
    if "nutmeg" in engines:
        # The native quota is prepaid from the fair pool; it must leave room for
        # the participating DataFusion operators (same rule as the extension
        # benchmarks' validate_admission_settings).
        from runtime import parse_memory

        if parse_memory(args.nutmeg_memory) >= parse_memory(args.max_memory):
            raise SystemExit(
                f"--nutmeg-memory ({args.nutmeg_memory}) must be smaller than "
                f"--max-memory ({args.max_memory})"
            )

    info = dataset_info(args.dataset)
    logger.info("dataset: %s (%s, %s nodes, %s edges, %s)", args.dataset, info["scale"],
                info["nodes_str"], info["edges_str"], info["size"])
    datasets.ensure_dataset(args.dataset, Path(args.data_dir))

    for engine in engines:
        benchmark_engine(engine, args)

    print(f"\nDone. Results under {args.results_dir}")


if __name__ == "__main__":
    main()
