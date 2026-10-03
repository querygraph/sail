"""Check static extension headers without importing native or Python packages.

Run source checks with unittest, or also check genuinely built wheels:

    python test_extension_metadata.py --wheel path/to/sedona.whl --wheel path/to/nutmeg.whl

The optional wheel checks read ZIP members and RECORD only. They do not install,
import, mutate or repair a wheel. Build wheels with the normal maturin workflow.
"""

import argparse
import ast
import base64
import configparser
import csv
import hashlib
import io
import json
from pathlib import Path
import sys
import tomllib
import unittest
import zipfile


EXTENSIONS = Path(__file__).resolve().parents[2]
PACKAGES = ("sedona", "nutmeg")
IMMUTABLE_FIELDS = (
    "name", "version", "api_version", "datafusion_version", "arrow_version",
)
WHEELS = []


def project(package):
    root = EXTENSIONS / package
    config = tomllib.loads((root / "pyproject.toml").read_text())
    entries = config["project"]["entry-points"]["pysail.extensions"]
    entry_name, target = next(iter(entries.items()))
    module = target.split(":", 1)[0]
    relative = module.replace(".", "/") + "/sail-extension.json"
    source = root / config["tool"]["maturin"]["python-source"] / relative
    return root, config, entries, entry_name, relative, source


def manifest_literals(source):
    """Read the immutable return literals, never execute the package bootstrap."""
    tree = ast.parse(source.read_text())
    method = next(node for node in ast.walk(tree)
                  if isinstance(node, ast.FunctionDef) and node.name == "manifest")
    returned = next(node.value for node in ast.walk(method)
                    if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict))
    return {
        ast.literal_eval(key): ast.literal_eval(value)
        for key, value in zip(returned.keys, returned.values)
        if ast.literal_eval(key) in IMMUTABLE_FIELDS
    }


class ExtensionMetadataTest(unittest.TestCase):
    def test_source_headers_match_entry_points_and_runtime_declarations(self):
        for package in PACKAGES:
            with self.subTest(package=package):
                _, config, entries, entry_name, _, source = project(package)
                header = json.loads(source.read_text())
                self.assertEqual(set(header), {"schema_version", "extensions"})
                self.assertEqual(header["schema_version"], 1)
                declarations = header["extensions"]
                names = [item["entry_point"] for item in declarations]
                self.assertEqual(len(names), len(set(names)), "duplicate static entry point")
                self.assertEqual(set(names), set(entries))
                item = next(item for item in declarations if item["entry_point"] == entry_name)
                self.assertEqual(set(item), {"entry_point", *IMMUTABLE_FIELDS})
                self.assertEqual(item["version"], config["project"]["version"])
                self.assertEqual(
                    {key: item[key] for key in IMMUTABLE_FIELDS},
                    manifest_literals(source.with_name("__init__.py")),
                    "static and runtime declarations must agree",
                )

    def test_source_headers_match_locked_ffi_versions(self):
        for package in PACKAGES:
            with self.subTest(package=package):
                root, _, _, _, _, source = project(package)
                declaration = json.loads(source.read_text())["extensions"][0]
                lock = tomllib.loads((root / "Cargo.lock").read_text())
                versions = {}
                for item in lock["package"]:
                    versions.setdefault(item["name"], set()).add(item["version"])
                self.assertEqual(versions["datafusion-ffi"], {declaration["datafusion_version"]})
                self.assertEqual(versions["arrow-schema"], {declaration["arrow_version"]})

    def test_built_wheels_contain_recorded_static_headers(self):
        if not WHEELS:
            self.skipTest("pass --wheel to verify actual maturin-built artifacts")
        for wheel in WHEELS:
            with self.subTest(wheel=str(wheel)), zipfile.ZipFile(wheel) as archive:
                files = archive.namelist()
                records = [name for name in files if name.endswith(".dist-info/RECORD")]
                self.assertEqual(len(records), 1)
                record = {row[0]: row[1:] for row in csv.reader(io.StringIO(archive.read(records[0]).decode()))}
                matches = []
                for package in PACKAGES:
                    _, _, entries, _, relative, source = project(package)
                    if relative not in files:
                        continue
                    matches.append(package)
                    self.assertEqual(files.count(relative), 1, "duplicate header ZIP member")
                    data = archive.read(relative)
                    self.assertEqual(data, source.read_bytes(), "wheel header differs from source")
                    self.assertIn(relative, record, "static header must belong to the distribution RECORD")
                    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
                    self.assertEqual(record[relative], ["sha256=" + digest, str(len(data))])
                    entry_points = configparser.ConfigParser()
                    entry_points.optionxform = str
                    entry_points.read_string(archive.read(records[0].removesuffix("RECORD") + "entry_points.txt").decode())
                    self.assertEqual(dict(entry_points["pysail.extensions"]), entries)
                self.assertEqual(len(matches), 1, "expected one Sedona or Nutmeg static header in wheel")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--wheel", type=Path, action="append", default=[])
    options, unittest_arguments = parser.parse_known_args()
    WHEELS.extend(options.wheel)
    unittest.main(argv=[sys.argv[0], *unittest_arguments])
