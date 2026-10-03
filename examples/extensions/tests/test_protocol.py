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
                  datafusion_version="55.1.0", arrow_version="59.3.0",
                  placement="driver", relation_types=[])
    result.update(changes)
    return result


def relation_type(url="type.googleapis.com/fixture.v1.Relation", **changes):
    result = dict(type_url=url, accepts_bare=True, min_inputs=0, max_inputs=0)
    result.update(changes)
    return result


AUTO_STATIC = object()
STATIC_FIELDS = ("name", "version", "api_version", "datafusion_version", "arrow_version")


def static_metadata(specifications):
    return {
        "schema_version": 1,
        "extensions": [
            dict(entry_point=f"000_fixture_{index:02}", **{
                field: specification["manifest"][field] for field in STATIC_FIELDS
            })
            for index, specification in enumerate(specifications)
        ],
    }


def write_loader_fixture(directory, specifications, *, static_document=AUTO_STATIC,
                         extra_static_paths=(), record_static_files=True,
                         write_static_files=True, module_name="fixture_extensions",
                         package_module=False, parent_import_error=None):
    """Publish real Python entry-point metadata without making a native fixture."""
    directory.mkdir()
    events = directory / "events.jsonl"
    source = f'''
import json
import os
from pathlib import Path

SPECIFICATIONS = json.loads({json.dumps(specifications)!r})
EVENTS = Path({str(events)!r})

def record(event, index, **details):
    with EVENTS.open("a") as output:
        output.write(json.dumps(dict(event=event, index=index, pid=os.getpid(), **details)) + "\\n")

record("import", -1)
for specification in SPECIFICATIONS:
    if specification.get("import_error"):
        raise RuntimeError(specification["import_error"])

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
        result = dict(self.specification["manifest"])
        if self.specification.get("memory_env"):
            result["memory_bytes"] = int(os.environ[self.specification["memory_env"]])
        record("manifest", self.index)
        return result
    def bind(self, session):
        record("bind", self.index)
        return BoundFixture(self.specification, self.index, session)
    def bind_with_resources(self, session, memory_bytes, host_resource):
        record("bind_with_resources", self.index, memory_bytes=memory_bytes)
        bound = BoundFixture(self.specification, self.index, session)
        bound.host_resource = host_resource
        return bound
'''
    for index in range(len(specifications)):
        source += f'''
def extension_{index}():
    record("factory", {index})
    return Extension(SPECIFICATIONS[{index}], {index})
'''
    module_path = Path(*module_name.split("."))
    source_path = module_path / "__init__.py" if package_module else module_path.with_suffix(".py")
    (directory / source_path).parent.mkdir(parents=True, exist_ok=True)
    (directory / source_path).write_text(source)
    files = [source_path.as_posix()]
    for parent in module_path.parents:
        if parent == Path("."):
            continue
        parent_source = ""
        if parent_import_error:
            parent_source = (
                "import json, os\nfrom pathlib import Path\n"
                f"with Path({str(events)!r}).open('a') as output:\n"
                "    output.write(json.dumps(dict(event='parent_import', index=-1, pid=os.getpid())) + '\\n')\n"
                f"raise RuntimeError({parent_import_error!r})\n"
            )
        parent_path = parent / "__init__.py"
        (directory / parent_path).write_text(parent_source)
        files.append(parent_path.as_posix())
    distribution = directory / "sail_loader_fixture-1.dist-info"
    distribution.mkdir()
    if static_document is AUTO_STATIC:
        static_document = static_metadata(specifications)
    if static_document is not None:
        payload = static_document if isinstance(static_document, str) else json.dumps(static_document)
        static_path = (module_path / "sail-extension.json").as_posix() if package_module \
            else module_path.as_posix() + ".sail-extension.json"
        for name in (static_path, *extra_static_paths):
            if write_static_files:
                (directory / name).parent.mkdir(parents=True, exist_ok=True)
                (directory / name).write_text(payload)
            if record_static_files:
                files.append(name)
    (distribution / "RECORD").write_text("".join(f"{name},,\n" for name in files))
    (distribution / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: sail-loader-fixture\nVersion: 1\n")
    (distribution / "entry_points.txt").write_text(
        "[pysail.extensions]\n" + "".join(
            f"000_fixture_{index:02} = {module_name}:extension_{index}\n"
            for index in range(len(specifications))))
    return events


def read_events(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def assert_loader_rejected(request, tmp_path, specifications, message, **fixture_options):
    fixture = tmp_path / "fixture"
    events = write_loader_fixture(fixture, specifications, **fixture_options)
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
        "mismatch-fixture", datafusion_version="54.1.0"), import_error="IMPORT_MUST_NOT_RUN")],
        "mismatch-fixture build mismatch.*55.1.0.*54.1.0")
    assert events == [], "static rejection must precede module import, factory, manifest and bind"


def test_all_entry_points_are_preflighted_before_any_import(request, tmp_path):
    events = assert_loader_rejected(request, tmp_path, [
        dict(manifest=manifest("first-valid")),
        dict(manifest=manifest("second-invalid", datafusion_version="54.1.0"),
             import_error="EVEN_THE_VALID_ENTRY_MUST_NOT_IMPORT"),
    ], "second-invalid build mismatch.*55.1.0.*54.1.0")
    assert events == [], "a later incompatible entry must prevent earlier compatible imports too"


@pytest.mark.parametrize("case,message", [
    ("missing", "missing static compatibility metadata"),
    ("malformed", "invalid static compatibility metadata"),
    ("unknown-field", "invalid static compatibility metadata"),
    ("missing-build-field", "invalid static compatibility metadata"),
    ("wrong-build-type", "invalid static compatibility metadata"),
    ("schema", "unsupported static compatibility schema"),
    ("duplicate-entry", "invalid static compatibility metadata"),
    ("missing-entry", "missing static compatibility entry"),
    ("ambiguous-paths", "ambiguous static compatibility metadata"),
    ("unrecorded-file", "missing static compatibility metadata"),
    # Some importlib.metadata versions filter missing paths out of dist.files;
    # others retain the RECORD entry and let the subsequent read fail.
    ("recorded-missing-file", "(?:missing|cannot read) static compatibility metadata"),
])
def test_static_metadata_is_required_and_validated_without_import(request, tmp_path, case, message):
    specifications = [dict(manifest=manifest(), import_error="STATIC_FAILURE_MUST_PRECEDE_IMPORT")]
    document = static_metadata(specifications)
    options = {}
    if case == "missing":
        document = None
    elif case == "malformed":
        document = "{not JSON"
    elif case == "unknown-field":
        document["future_layout"] = 2
    elif case == "missing-build-field":
        del document["extensions"][0]["arrow_version"]
    elif case == "wrong-build-type":
        document["extensions"][0]["api_version"] = "1"
    elif case == "schema":
        document["schema_version"] = 2
    elif case == "duplicate-entry":
        document["extensions"].append(document["extensions"][0].copy())
    elif case == "missing-entry":
        document["extensions"][0]["entry_point"] = "not_this_entry"
    elif case == "ambiguous-paths":
        options["extra_static_paths"] = ("fixture_extensions/sail-extension.json",)
    elif case == "unrecorded-file":
        options["record_static_files"] = False
    elif case == "recorded-missing-file":
        options["write_static_files"] = False
    events = assert_loader_rejected(request, tmp_path, specifications, message,
                                    static_document=document, **options)
    assert events == [], "invalid static declarations must never execute package code"


@pytest.mark.parametrize("package_module", [False, True], ids=["nested-module", "nested-package"])
def test_nested_metadata_lookup_never_imports_parent_packages(request, tmp_path, package_module):
    events = assert_loader_rejected(request, tmp_path, [dict(
        manifest=manifest("nested-poison", datafusion_version="54.1.0"),
        import_error="CHILD_IMPORT_MUST_NOT_RUN",
    )], "nested-poison build mismatch.*55.1.0.*54.1.0",
        module_name="poison_parent.extension", package_module=package_module,
        parent_import_error="PARENT_IMPORT_MUST_NOT_RUN")
    assert events == [], "finding nested static metadata must not import even its parent package"


@pytest.mark.parametrize("field,value", [
    ("name", "renamed-fixture"),
    ("version", "2"),
    ("api_version", 2),
    ("datafusion_version", "54.1.0"),
    ("arrow_version", "58.3.0"),
])
def test_runtime_manifest_cannot_change_static_build_identity(request, tmp_path, field, value):
    specifications = [dict(manifest=manifest())]
    document = static_metadata(specifications)
    specifications[0]["manifest"][field] = value
    events = assert_loader_rejected(request, tmp_path, specifications,
                                    "static compatibility metadata disagrees with manifest",
                                    static_document=document)
    stages = [event["event"] for event in events]
    assert stages[0] == "import"
    assert "factory" in stages and "manifest" in stages
    assert set(stages) <= {"import", "factory", "manifest"}, "drift must fail before bind or capsule access"


@pytest.mark.parametrize("memory_bytes", [64, 128])
def test_static_build_metadata_preserves_dynamic_options(request, tmp_path, memory_bytes):
    specifications = [dict(manifest=manifest(
        "dynamic-options", relation_types=[relation_type()]), memory_env="SAIL_FIXTURE_MEMORY_BYTES")]
    fixture = tmp_path / "fixture"
    events_path = write_loader_fixture(fixture, specifications)
    binary = str(Path(request.config.getoption("--sail-binary")).resolve())
    with start_server(binary, tmp_path / "server", extra_pythonpath=fixture,
                      extra_env={"SAIL_FIXTURE_MEMORY_BYTES": str(memory_bytes)}) as endpoint:
        session = SparkSession.builder.remote(endpoint).create()
        try:
            assert session.sql("SELECT 1 AS value").collect()[0].value == 1
        finally:
            session.stop()
    binds = [event for event in read_events(events_path) if event["event"] == "bind_with_resources"]
    assert binds and all(event["memory_bytes"] == memory_bytes for event in binds)
    assert not any(event["event"] == "bind" for event in read_events(events_path))


@pytest.mark.parametrize("changes,message", [
    ({"name": ""}, "name and version must not be empty"),
    ({"api_version": 2}, "build mismatch.*host api=1.*package api=2"),
    ({"arrow_version": "58.3.0"}, "build mismatch.*Arrow=59.3.0.*Arrow=58.3.0"),
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
    assert not any(event["event"] in {"bind", "bind_with_resources", "capsule"} for event in events), \
        "invalid metadata must be rejected before native binding"


def test_extension_names_are_unique_after_case_folding(request, tmp_path):
    events = assert_loader_rejected(request, tmp_path, [
        dict(manifest=manifest("casefold-fixture")),
        dict(manifest=manifest("CASEFOLD-FIXTURE")),
    ], "duplicate native extension name: CASEFOLD-FIXTURE")
    assert not any(event["index"] == 1 and event["event"] == "bind" for event in events), \
        "duplicate factory must not bind"


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
