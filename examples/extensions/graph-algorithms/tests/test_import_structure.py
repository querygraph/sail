"""Import structure and public aliases, without starting a Spark session."""
import ast
import importlib
import json
import os
from pathlib import Path
import pickle
import subprocess
import sys
from typing import Any

import pytest

import pyspark_pecan
from pyspark_pecan import _contracts, algorithms


PACKAGE = Path(__file__).resolve().parents[1] / "src" / "pyspark_pecan"


def function_import_lines(source: str) -> list[int]:
    tree = ast.parse(source)
    return sorted({child.lineno for node in ast.walk(tree)
                   if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                   for child in ast.walk(node)
                   if isinstance(child, (ast.Import, ast.ImportFrom))})


def test_package_has_no_function_local_imports() -> None:
    paths = sorted(PACKAGE.rglob("*.py"))
    assert paths
    violations = {str(path.relative_to(PACKAGE)): lines for path in paths
                  if (lines := function_import_lines(path.read_text()))}
    assert violations == {}


def test_ast_guard_detects_nested_async_and_method_imports() -> None:
    source = """import math
def outer():
    def inner():
        from os import path
    class Nested:
        async def method(self):
            import json
async def another():
    from sys import version
"""
    assert function_import_lines(source) == [4, 7, 9]


@pytest.mark.parametrize("entry", ["algorithms", "traversal_bfs", "wcc_randomized"])
def test_fresh_interpreter_imports_all_modules(entry: str) -> None:
    modules = ["pyspark_pecan." + path.stem for path in sorted(PACKAGE.glob("*.py"))
               if path.stem != "__init__"]
    code = ("import importlib, json, sys; "
            "importlib.import_module('pyspark_pecan.' + sys.argv[1]); "
            "[importlib.import_module(name) for name in json.loads(sys.argv[2])]")
    result = subprocess.run([sys.executable, "-c", code, entry, json.dumps(modules)],
                            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                            text=True, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def test_exception_aliases_qualified_name_and_pickle_are_compatible() -> None:
    exception = _contracts.ConvergenceError
    assert pyspark_pecan.ConvergenceError is exception
    assert algorithms.ConvergenceError is exception
    assert str(exception) == "<class 'pyspark_pecan.algorithms.ConvergenceError'>"
    for name in ("pagerank_delta", "pagerank_pregel_delta", "wcc_randomized", "traversal", "traversal_bfs",
                 "traversal_stepping"):
        assert importlib.import_module("pyspark_pecan." + name).ConvergenceError is exception
    error = exception("cap reached")
    error.cleanup_deferred = True
    restored = pickle.loads(pickle.dumps(error))
    assert type(restored) is exception
    assert restored.args == ("cap reached",)
    assert restored.cleanup_deferred is True
    # A pre-refactor pickle uses this same global lookup path.
    assert pickle.loads(b"cpyspark_pecan.algorithms\nConvergenceError\n.") is exception


@pytest.mark.parametrize("method,module,options", [
    ("pagerank", "pagerank_delta", {"method": "delta", "tolerance": 0.1}),
    ("pagerank", "pagerank_pregel_delta", {"method": "pregel_delta", "tolerance": 0.01}),
    ("wcc", "wcc_randomized", {"method": "randomized"}),
    ("wcc", "wcc_randomized", {"method": "randomized_fused"}),
    ("bfs", "traversal", {"source": 0}),
    ("sssp", "traversal", {"source": 0}),
])
def test_public_dispatch_resolves_module_function_at_call_time(monkeypatch: Any, method: str, module: str, options: dict[str, Any]) -> None:
    graph = algorithms.GraphAlgorithms.__new__(algorithms.GraphAlgorithms)
    vertices, edges, expected = object(), object(), object()
    calls = []

    def replacement(*args: Any, **kwargs: Any) -> Any:
        calls.append((args, kwargs))
        return expected

    monkeypatch.setattr(importlib.import_module("pyspark_pecan." + module), "execute", replacement)
    assert getattr(graph, method)(vertices, edges, **options) is expected
    assert len(calls) == 1
    assert calls[0][0] == (graph, vertices, edges)
