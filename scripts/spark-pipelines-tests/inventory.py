"""Inventory exact Spark SDP test call sites without pretending to expand Scala parameters.

The JSON records declarations, not an executable Scala test count. Parameterized
tests and shared suite helpers must be expanded when their behavioral ports are
added. Python execution outcomes come from run.py, not this static inventory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

SPARK_COMMIT = "617618396087429b5627a86ce933bb0f5d6645cb"
CALL = re.compile(
    r"\b(test|testQuietly|testWithName|registerTest|gridTest|namedGridTest|booleanGridTest)\s*\("
)


def argument(source: str, start: int) -> tuple[str, int]:
    """Read balanced call arguments, preserving string contents and interpolation."""
    depth = 1
    cursor = start
    quote = None
    while cursor < len(source):
        if quote:
            if source.startswith(quote, cursor):
                cursor += len(quote)
                quote = None
            elif source[cursor] == "\\" and quote != '"""':
                cursor += 2
            else:
                cursor += 1
        elif source.startswith('"""', cursor):
            quote = '"""'
            cursor += 3
        elif source[cursor] == '"':
            quote = '"'
            cursor += 1
        elif source[cursor] == "(":
            depth += 1
            cursor += 1
        elif source[cursor] == ")":
            depth -= 1
            if depth == 0:
                return source[start:cursor].strip(), cursor + 1
            cursor += 1
        else:
            cursor += 1
    raise ValueError(f"Unclosed test call at offset {start}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.spark_source.resolve()
    commit = subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
    ).strip()
    if commit != SPARK_COMMIT:
        parser.error(f"Expected {SPARK_COMMIT}, found {commit}")
    roots = [
        "sql/pipelines/src/test",
        "sql/connect/server/src/test/scala/org/apache/spark/sql/connect/pipelines",
    ]
    subprocess.run(
        ["git", "-C", str(root), "diff", "--exit-code", "HEAD", "--", *roots], check=True
    )
    cases = []
    files = {}
    for path in sorted(path for directory in roots for path in (root / directory).rglob("*.scala")):
        source = path.read_text()
        name = path.relative_to(root).as_posix()
        for match in CALL.finditer(source):
            expression, _ = argument(source, match.end())
            line = source.count("\n", 0, match.start()) + 1
            cases.append(
                {
                    "id": f"{name}:{line}",
                    "source": name,
                    "line": line,
                    "method": match.group(1),
                    "name_expression": expression,
                    "status": "unmapped",
                    "sail_tests": [],
                }
            )
            files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {
                "spark_commit": commit,
                "kind": "static Scala test call sites; parameter expansion outstanding",
                "source_sha256": files,
                "cases": cases,
            },
            indent=2,
        )
        + "\n"
    )
    print(f"{len(cases)} test call sites in {len(files)} files")


if __name__ == "__main__":
    main()
