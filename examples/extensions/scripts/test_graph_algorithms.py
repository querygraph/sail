#!/usr/bin/env python3
"""Run Pecan against a real, isolated Sail server."""
import argparse
import importlib.util
import os
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sail-binary", type=Path, required=True)
    parser.add_argument("--execution-mode", choices=["local", "local-cluster", "process-cluster"], required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[3]
    package = repo / "examples/extensions/graph-algorithms"
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    root = output / "staging"
    root.mkdir()
    spec = importlib.util.spec_from_file_location("graph_test_fixture", repo / "examples/extensions/tests/conftest.py")
    fixture = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fixture)
    with fixture.start_server(str(args.sail_binary.resolve()), output / "server", mode=args.execution_mode,
                              extra_env={"SAIL_GRAPH_UTILS_ROOT": root.as_uri()}) as endpoint:
        env = dict(os.environ, SAIL_GRAPH_TEST_REMOTE=endpoint, PYTHONPATH=str(package / "src"))
        result = subprocess.run([sys.executable, "-m", "pytest", "-q", str(package / "tests")],
                                env=env, cwd=repo)
        if result.returncode:
            raise SystemExit(result.returncode)
    assert not list(root.rglob("*.parquet")), "server teardown left graph staging files"
    print(f"portable-graphs: PASSED {args.execution_mode}")


if __name__ == "__main__":
    main()
