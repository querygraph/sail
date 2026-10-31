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
CALL = re.compile(r"\b(test|testQuietly|testWithName|registerTest|gridTest|namedGridTest|booleanGridTest)\s*\(")


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
    commit = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    if commit != SPARK_COMMIT:
        parser.error(f"Expected {SPARK_COMMIT}, found {commit}")
    roots = [
        "sql/pipelines/src/test/scala",
        "sql/connect/server/src/test/scala/org/apache/spark/sql/connect/pipelines",
        "sql/hive/src/test/scala/org/apache/spark/sql/hive/pipelines",
    ]
    command = "sql/core/src/test/scala/org/apache/spark/sql/execution/command/"
    service = "sql/connect/server/src/test/scala/org/apache/spark/sql/connect/service/"
    # Shared suites contain unrelated tests. These source-pin-bound declarations
    # select only their SDP obligations; dedicated parser suites retain all calls.
    mixed = {
        command + "DDLSuite.scala": {2522, 2533, 2544},
        service + "SparkConnectSessionHolderSuite.scala": {676},
        service + "SparkConnectSessionManagerSuite.scala": {158},
        "sql/hive/src/test/scala/org/apache/spark/sql/hive/execution/HiveDDLSuite.scala": set(),
    }
    extra = [
        command + "CreatePipelineDatasetAsSelectParserSuiteBase.scala",
        command + "v2/CreateMaterializedViewAsSelectParserSuite.scala",
        command + "v2/CreateStreamingTableAsSelectParserSuite.scala",
        command + "v2/CreateStreamingTableParserSuite.scala",
        command + "v2/CreateFlowCommandSuite.scala",
        *mixed,
    ]
    names = subprocess.check_output(
        ["git", "-C", str(root), "ls-tree", "-r", "--name-only", SPARK_COMMIT, "--", *roots, *extra],
        text=True,
    ).splitlines()
    cases = []
    files = {}
    for name in sorted(name for name in names if name.endswith(".scala")):
        # Read immutable Git blobs, including files absent from a sparse checkout.
        data = subprocess.check_output(["git", "-C", str(root), "show", f"{SPARK_COMMIT}:{name}"])
        source = data.decode()
        files[name] = hashlib.sha256(data).hexdigest()
        selected = set()
        for match in CALL.finditer(source):
            expression, _ = argument(source, match.end())
            line = source.count("\n", 0, match.start()) + 1
            if name in mixed and line not in mixed[name]:
                continue
            selected.add(line)
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
        if name in mixed and selected != mixed[name]:
            raise RuntimeError(f"Pinned SDP declarations not found in {name}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {
                "spark_commit": commit,
                "kind": "static SDP declaration sites including parser/lifecycle scope; runtime expansion separate",
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
