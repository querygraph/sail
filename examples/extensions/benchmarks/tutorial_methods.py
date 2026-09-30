#!/usr/bin/env python3
"""Exercise every graph method with existing validation and timing machinery.

This is a functional tutorial, not a benchmark matrix: each case gets a fresh
Sail server, but the outer host/container is shared across cases. For publishable
comparisons use run_matrix.py, which creates a fresh container per trial.
"""
import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import subprocess
import sys

from graph_fixtures import prepare
from validation_outcome import effective_outcome, PARTIALLY_VERIFIED


ENGINES = ("pecan", "nutmeg-native", "nutmeg-datafusion")
ALGORITHMS = ("pagerank", "wcc")
VARIANTS = ("reference", "optimized", "fused")


def utc():
    return datetime.now(timezone.utc).isoformat()


def selection(value, choices):
    return choices if value == "all" else (value,)


def display(value, *, mib=False):
    if value is None:
        return "unavailable"
    return f"{value / 1024**2 if mib else value:.3f}"


def write_summary(path, summary):
    temporary = path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(summary, indent=2) + '\n')
    temporary.replace(path)


def run_case(command, log_path, receipt_path, private_container):
    """Ordinary child/receipt failures are retained; operator cancellation escapes."""
    row = dict(outcome='incomplete_record', returncode=None, original_receipt_outcome=None,
               error=None, end_to_end_seconds=None,
               sampled_execution_rss_bytes=None, sampled_execution_pss_bytes=None)
    errors = []
    try:
        with log_path.open('w') as log:
            completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
        row['returncode'] = completed.returncode
    except (OSError, subprocess.SubprocessError) as error:
        errors.append(f'child launch/execution failed: {error!r}')
    receipt = {}
    try:
        if receipt_path.exists():
            receipt = json.loads(receipt_path.read_text())
            if not isinstance(receipt, dict):
                raise ValueError('receipt must be a JSON object')
            row['original_receipt_outcome'] = receipt.get('outcome')
            if receipt.get('outcome') not in ('passed', PARTIALLY_VERIFIED, 'error', 'mismatch', 'nonconverged', 'timeout'):
                raise ValueError('receipt has no completed outcome')
            memory = receipt.get('memory', {})
            if not isinstance(memory, dict):
                raise ValueError('receipt memory must be a JSON object')
            phases = memory.get('phase_peaks', {})
            if not isinstance(phases, dict):
                raise ValueError('receipt memory phase_peaks must be a JSON object')
            peaks = phases.get('execute', {})
            if not isinstance(peaks, dict):
                raise ValueError('receipt execution memory must be a JSON object')
            metrics = dict(end_to_end_seconds=receipt.get('end_to_end_seconds'),
                           sampled_execution_rss_bytes=peaks.get('rss_bytes') if private_container else None,
                           sampled_execution_pss_bytes=peaks.get('pss_bytes') if private_container else None)
            for key, value in metrics.items():
                if value is not None and (not isinstance(value, (int, float)) or
                                          not math.isfinite(value) or value < 0):
                    raise ValueError(f'invalid numeric receipt field: {key}')
            row.update(metrics, outcome=effective_outcome(receipt))
    except (OSError, UnicodeError, ValueError, TypeError, AttributeError) as error:
        errors.append(f'receipt unavailable or invalid: {error!r}')
    if row['original_receipt_outcome'] == 'passed' and row['returncode'] != 0:
        errors.append('passed receipt disagrees with nonzero or unavailable child exit status')
    if errors:
        row.update(outcome='orchestration_error', error='; '.join(errors))
    elif receipt.get('error'):
        row['error'] = receipt['error']
    return row


def main(default_suite="ranking"):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sail-binary", type=Path, required=True)
    parser.add_argument("--runtime-source-sha", required=True)
    parser.add_argument("--native-source-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=("local", "process-cluster"), default="local")
    parser.add_argument("--engine", choices=("all", *ENGINES), default="all")
    parser.add_argument("--suite", choices=("ranking", "traversal"), default=default_suite)
    parser.add_argument("--algorithm", choices=("all", *ALGORITHMS, "bfs", "sssp"), default="all")
    parser.add_argument("--variant", choices=("all", *VARIANTS, "frontier", "push_pull", "delta_star"), default="all",
                        help="select an explicit method; advanced does not imply faster")
    parser.add_argument("--allow-dirty", action="store_true",
                        help="development smoke only; records the dirty source in each receipt")
    args = parser.parse_args()
    available = ({"pagerank": ("reference", "optimized"),
                  "wcc": ("reference", "optimized", "fused")} if args.suite == "ranking" else
                 {"bfs": ("reference", "frontier", "push_pull"),
                  "sssp": ("reference", "frontier", "delta_star")})
    if args.algorithm != "all" and args.algorithm not in available:
        parser.error("algorithm is not part of the selected suite")
    pairs = [(algorithm, variant) for algorithm in selection(args.algorithm, tuple(available))
             for variant in available[algorithm] if args.variant in ("all", variant)]
    if not pairs:
        parser.error("no algorithm in the selection supports this variant")
    args.sail_binary = args.sail_binary.resolve()
    if not args.sail_binary.is_file():
        parser.error("--sail-binary must name an existing executable")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    dataset = args.output / "dataset"
    if args.suite == "traversal":
        from traversal_fixture import prepare as prepare_traversal
        manifest = prepare_traversal(dataset, vertices=128, degree=4, source=0, directed=True)
    else:
        manifest = prepare(dataset, vertices=128, degree=4, block_size=32)
    private_container = Path("/.dockerenv").exists() and Path("/proc/stat").exists()
    summary = {
        "started_utc": utc(),
        "purpose": "functional tutorial; not isolated per-cell benchmark measurements",
        "mode": args.mode,
        "suite": args.suite,
        "dataset_counts": manifest["counts"],
        "memory_scope": (
            "all visible container processes; private PID namespace required; "
            "outer container reused across cases, so cgroup peaks are cumulative"
            if private_container else
            "process memory is unavailable in this tutorial table outside a private Linux container"
        ),
        "cells": [],
    }
    summary['planned_cells'] = len(selection(args.engine, ENGINES)) * len(pairs)
    summary_path = args.output / 'tutorial-summary.json'
    write_summary(summary_path, summary)
    print(json.dumps({"fixture": summary["dataset_counts"], "purpose": summary["purpose"]}), flush=True)
    print("engine | algorithm | flavor | outcome | call seconds | sampled execution RSS MiB | PSS MiB", flush=True)
    failed = False
    script = Path(__file__).with_name("graph_cell.py")
    for engine in selection(args.engine, ENGINES):
        for algorithm, variant in pairs:
            name = f"{engine}-{algorithm}-{variant}"
            output = args.output / name
            command = [
                sys.executable, str(script), "--sail-binary", str(args.sail_binary),
                "--runtime-source-sha", args.runtime_source_sha,
                "--native-source-sha", args.native_source_sha,
                "--dataset", str(dataset), "--output", str(output),
                "--engine", engine, "--algorithm", algorithm, "--variant", variant,
                "--mode", args.mode, "--partitions", "4", "--threads", "4",
                "--worker-task-slots", "32", "--sail-pool-bytes", str(16 * 1024**3),
                "--native-quota", str(8 * 1024**3),
                "--max-iterations", "1000", "--tolerance", "1e-8", "--seed", "42",
                "--allow-unisolated",
            ]
            if algorithm in ("bfs", "sssp"):
                command.extend(["--source", "0", "--directed", "--delta", "4.0"])
            if args.allow_dirty:
                command.append("--allow-dirty")
            receipt_path = output / "receipt.json"
            row = {
                "engine": engine, "algorithm": algorithm, "variant": variant,
                "outcome": 'incomplete_record', "returncode": None, "original_receipt_outcome": None,
                "receipt": str(receipt_path.relative_to(args.output)),
                "log": f"{name}.log", "command": command,
            }
            try:
                row.update(run_case(command, args.output / f'{name}.log', receipt_path, private_container))
            except (KeyboardInterrupt, SystemExit) as error:
                row.update(outcome='interrupted', error=repr(error))
                summary.update(finished_utc=utc(), outcome='interrupted')
                raise
            finally:
                summary['cells'].append(row)
                summary['updated_utc'] = utc()
                write_summary(summary_path, summary)
            outcome = row['outcome']
            failed |= outcome != 'passed'
            rss, pss = row['sampled_execution_rss_bytes'], row['sampled_execution_pss_bytes']
            flavor = {"reference": "reference", "optimized": "advanced", "fused": "advanced fused"}.get(variant, variant)
            print(f"{engine} | {algorithm} | {flavor} | {outcome} | "
                  f"{display(row['end_to_end_seconds'])} | {display(rss, mib=True)} | {display(pss, mib=True)}",
                  flush=True)
    summary.update(finished_utc=utc(), outcome="failed" if failed else "passed")
    write_summary(summary_path, summary)
    print(f"Receipts, result Parquet, server logs and summary: {args.output}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
