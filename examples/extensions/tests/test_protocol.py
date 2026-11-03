import json
import contextlib
from pathlib import Path

import pytest
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan
from pyspark.sql.connect.session import SparkSession
from sail_nutmeg.client import ENVELOPE_TYPE_URL, _bytes_field, _varint
from sail_nutmeg import TYPE_URL
from conftest import start_server


def manifest(name="loader-fixture", **changes):
    result = dict(name=name, version="1", api_version=1,
                  datafusion_version="55.2.0", arrow_version="59.2.0",
                  placement="driver", relation_types=[])
    result.update(changes)
    return result


def relation_type(url="type.googleapis.com/fixture.v1.Relation", **changes):
    result = dict(type_url=url, accepts_bare=True, min_inputs=0, max_inputs=0)
    result.update(changes)
    return result


def write_loader_fixture(directory, specifications):
    """Publish real Python entry-point metadata without making a native fixture."""
    directory.mkdir()
    events = directory / "events.jsonl"
    source = f'''
import json
from pathlib import Path

SPECIFICATIONS = json.loads({json.dumps(specifications)!r})
EVENTS = Path({str(events)!r})

def record(event, index):
    with EVENTS.open("a") as output:
        output.write(json.dumps(dict(event=event, index=index)) + "\\n")

class NeverReadCapsule:
    def __init__(self, index):
        self.index = index
    def __datafusion_scalar_udf__(self):
        record("capsule", self.index)
        raise RuntimeError("native capsule must not be inspected")

class BoundFixture:
    def __init__(self, specification, index, session):
        self.specification, self.index = specification, index
        self.native_owner = None
        self.functions = []
        if specification.get("scalars") == "sedona_point":
            from sail_sedona import extension
            self.native_owner = extension.bind(session)
            self.functions = [f for f in self.native_owner.scalar_udfs() if f.name() == "st_point"]
            assert len(self.functions) == 1
        elif specification.get("scalars") == "forbidden_capsule":
            self.functions = [NeverReadCapsule(index)]
    def scalar_udfs(self):
        return self.functions
    def plan_relation(self, *args):
        record("plan_relation", self.index)
        raise RuntimeError("relation planning must not happen after a rejected load")

class Extension:
    def __init__(self, specification, index):
        self.specification, self.index = specification, index
    def manifest(self):
        return self.specification["manifest"]
    def bind(self, session):
        record("bind", self.index)
        return BoundFixture(self.specification, self.index, session)
'''
    for index in range(len(specifications)):
        source += f"\nextension_{index} = Extension(SPECIFICATIONS[{index}], {index})\n"
    (directory / "fixture_extensions.py").write_text(source)
    distribution = directory / "sail_loader_fixture-1.dist-info"
    distribution.mkdir()
    (distribution / "RECORD").write_text("fixture_extensions.py,,\n")
    (distribution / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: sail-loader-fixture\nVersion: 1\n")
    (distribution / "entry_points.txt").write_text(
        "[pysail.extensions]\n" + "".join(
            f"000_fixture_{index:02} = fixture_extensions:extension_{index}\n"
            for index in range(len(specifications))))
    return events


def read_events(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def assert_loader_rejected(request, tmp_path, specifications, message):
    fixture = tmp_path / "fixture"
    events = write_loader_fixture(fixture, specifications)
    binary = str(Path(request.config.getoption("--sail-binary")).resolve())
    with start_server(binary, tmp_path / "server", extra_pythonpath=fixture) as endpoint:
        session = SparkSession.builder.remote(endpoint).create()
        try:
            with pytest.raises(Exception, match=message):
                session.sql("SELECT 1").collect()
        finally:
            # A failed first session load also makes ReleaseSession fail.
            with contextlib.suppress(Exception):
                session.stop()
    return read_events(events)


class RawRelation(LogicalPlan):
    def __init__(self, type_url, value):
        super().__init__(None)
        self.type_url, self.value = type_url, value

    def plan(self, session):
        relation = self._create_proto_relation()
        relation.extension.type_url = self.type_url
        relation.extension.value = self.value
        return relation


def test_unknown_url_and_envelope_errors(spark):
    with pytest.raises(Exception, match="missing.library.v1.Request.*registered"):
        DataFrame(RawRelation("type.googleapis.com/missing.library.v1.Request", b""), spark).collect()
    envelope = _bytes_field(1, TYPE_URL.encode()) + _bytes_field(2, b"{}") + _varint(5 << 3) + _varint(2)
    with pytest.raises(Exception, match="envelope version 2"):
        DataFrame(RawRelation(ENVELOPE_TYPE_URL, envelope), spark).collect()
    envelope = _bytes_field(4, b"") + _varint(5 << 3) + _varint(1)
    with pytest.raises(Exception, match="input expressions"):
        DataFrame(RawRelation(ENVELOPE_TYPE_URL, envelope), spark).collect()
    with pytest.raises(Exception, match="payload exceeds"):
        DataFrame(RawRelation(TYPE_URL, b"x" * (1024 * 1024 + 1)), spark).collect()


def test_cluster_mode_runs_installed_native_scalar(request, tmp_path):
    binary = str(Path(request.config.getoption("--sail-binary")).resolve())
    with start_server(binary, tmp_path / "cluster", mode="local-cluster") as endpoint:
        session = SparkSession.builder.remote(endpoint).create()
        try:
            rows = session.sql("SELECT ST_AsText(ST_Point(CAST(id AS DOUBLE), 2.0)) AS wkt FROM range(4) ORDER BY id").collect()
            from shapely import from_wkt
            assert [tuple(from_wkt(row.wkt).coords)[0] for row in rows] == [(float(i), 2.0) for i in range(4)]
        finally:
            try:
                session.stop()
            except Exception:
                pass


def test_build_mismatch_is_rejected_before_native_binding(request, tmp_path):
    events = assert_loader_rejected(request, tmp_path, [dict(manifest=manifest(
        "mismatch-fixture", datafusion_version="54.1.0"))],
        "mismatch-fixture build mismatch.*55.2.0.*54.1.0")
    assert events == [], "metadata rejection must precede bind and native import"


@pytest.mark.parametrize("changes,message", [
    ({"name": ""}, "name and version must not be empty"),
    ({"api_version": 2}, "build mismatch.*host api=1.*package api=2"),
    ({"arrow_version": "58.3.0"}, "build mismatch.*Arrow=59.2.0.*Arrow=58.3.0"),
    ({"placement": "sometimes"}, "unsupported placement sometimes"),
    ({"future_layout": 2}, "unknown field.*future_layout"),
    ({"relation_types": [relation_type(min_inputs=2, max_inputs=1)]},
     "invalid or duplicate relation type"),
    ({"relation_types": [relation_type(), relation_type()]},
     "invalid or duplicate relation type"),
], ids=["empty-name", "api-version", "arrow-version", "placement", "unknown-field",
        "invalid-arity", "duplicate-url-within-manifest"])
def test_invalid_manifest_never_binds(request, tmp_path, changes, message):
    events = assert_loader_rejected(request, tmp_path,
        [dict(manifest=manifest(**changes))], message)
    assert events == [], "invalid metadata must be rejected before native binding"


def test_extension_names_are_unique_after_case_folding(request, tmp_path):
    events = assert_loader_rejected(request, tmp_path, [
        dict(manifest=manifest("casefold-fixture")),
        dict(manifest=manifest("CASEFOLD-FIXTURE")),
    ], "duplicate native extension name: CASEFOLD-FIXTURE")
    assert not any(event["index"] == 1 for event in events), "duplicate factory must not bind"


def test_relation_url_claims_are_unique_across_packages(request, tmp_path):
    events = assert_loader_rejected(request, tmp_path, [
        dict(manifest=manifest("url-first", relation_types=[relation_type()])),
        dict(manifest=manifest("url-second", relation_types=[relation_type()])),
    ], "duplicate Connect extension type URL: type.googleapis.com/fixture.v1.Relation")
    assert not any(event["event"] == "plan_relation" for event in events)


def test_driver_only_scalars_are_rejected_before_capsule_access(request, tmp_path):
    events = assert_loader_rejected(request, tmp_path, [
        dict(manifest=manifest("driver-scalar"), scalars="forbidden_capsule"),
    ], "driver-only extension driver-scalar cannot export scalar functions")
    assert any(event["event"] == "bind" for event in events)
    assert not any(event["event"] == "capsule" for event in events)


def test_duplicate_actual_native_scalar_names_are_rejected(request, tmp_path):
    # Both factories export the real, independently built Sedona ST_Point
    # capsule. No fabricated or mutated ABI memory participates in this test.
    assert_loader_rejected(request, tmp_path, [
        dict(manifest=manifest("scalar-first", placement="any"), scalars="sedona_point"),
        dict(manifest=manifest("scalar-second", placement="any"), scalars="sedona_point"),
    ], "extension scalar-second function name collision: st_point")
