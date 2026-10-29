"""Verify that the minimal shared kernels match their recorded upstream blobs."""

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    for manifest in sorted((ROOT / "vendor").glob("*/SOURCE.json")):
        source = json.loads(manifest.read_text())
        for name, expected in source["files_sha256"].items():
            path = manifest.parent / name
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
            if actual != expected:
                raise RuntimeError(f"{path} differs from {source['repository']}@{source['commit']}")
        print(f"Verified {manifest.parent.name} from {source['commit']}")


if __name__ == "__main__":
    main()
