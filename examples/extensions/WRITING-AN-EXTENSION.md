# Writing a Sail extension

Build a native extension, install its Python wheel beside Sail, and call it from
Spark Connect. No JVM or JAR is required. This page describes the experimental
`work/extensions-datafusion-graphs` prototype, including its historical limitations.

The local `work/extensions-static-preflight` branch additionally requires static
compatibility metadata recorded in the installed wheel before an entry point can
load. Existing wheels without it must be rebuilt and reinstalled. Follow
[the static-preflight schema and migration guide](../../docs/development/extensions/static-compatibility-preflight.md)
alongside the package instructions below. The clone command retained here selects
the historical prototype. The preflight branch is local and unpublished; test it
from this checkout.

## Build and start Sail

Install Rust 1.97.1, Python 3.12 with a shared library, uv, Git, protoc (with
headers), a C/C++ toolchain and GEOS development libraries (3.12 or later).
On macOS, `brew install geos protobuf` supplies the latter dependencies.

```bash
git clone --branch work/extensions-datafusion-graphs https://github.com/querygraph/sail.git
cd sail
bash examples/extensions/scripts/build.sh

export PYTHONHOME=$(.venv/bin/python -c 'import sys; print(sys.base_prefix)')
export PYTHONPATH=$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
export DYLD_LIBRARY_PATH=$(.venv/bin/python -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')
export LD_LIBRARY_PATH="$DYLD_LIBRARY_PATH"
SAIL_EXPERIMENTAL_EXTENSIONS=1 SAIL_MODE=local \
  target/extensions-poc/host/debug/sail spark server --ip 127.0.0.1 --port 50051
```

The script builds Sail and installs the Sedona and Nutmeg wheels into `.venv`.
Run client code below with `.venv/bin/python` in another terminal.
For separate worker processes on this machine, replace `SAIL_MODE=local` with
`SAIL_MODE=local-cluster SAIL_EXPERIMENTAL_PROCESS_WORKERS=1`.
Sedona functions execute on workers; native Nutmeg algorithms execute on the
driver, gathering distributed inputs. This is not distributed PageRank/WCC.
For installation details and multiple hosts, see the [deployment tutorial](TUTORIAL.md).

## Protocol and client versions

Spark Connect defines [three extension points](https://spark.apache.org/docs/latest/app-dev-spark-connect.html).
This branch implements the following paths:

| Operation | Client representation | Sail implementation |
| --- | --- | --- |
| Relation → DataFrame | `Relation.extension` (`google.protobuf.Any`, field 998) | Dispatch by registered `type_url`; return a native table provider. |
| Expression → Column | Ordinary `unresolved_function`, resolved by name | Native scalar UDF registration. Raw `Expression.extension` (field 999) is **not implemented**. |
| Command → receipt | A relation followed by `.collect()` | Execute a mutation while reading its result. Raw `Command.extension` (field 999) is **not implemented**. |

Use the checked-in [Python lock file](requirements.lock): Python 3.12,
PySpark 4.0.1, **protobuf runtime 7.36.2**, grpcio 1.84.0 and PyArrow 21.0.0.
PySpark supplies the generated Spark Connect messages; these examples require no
client-side `protoc`. The Python runtime version is distinct from the `protoc`
compiler used to build Sail. The client uses PySpark's internal Connect plan API,
so keep its version pinned.

1. A bare request sets `Relation.extension.type_url` to the plugin's type URL
   and `.value` to its payload bytes. Its manifest must allow `accepts_bare`.
2. With DataFrame inputs, set the outer type URL to
   `type.googleapis.com/sail.extension.v1.SailExtensionRequest` and serialize
   this [envelope](../../crates/sail-spark-connect/proto/sail/extension/v1/extension.proto):

   ```proto
   message SailExtensionRequest {
     string payload_type_url = 1;
     bytes payload = 2;
     repeated spark.connect.Plan inputs = 3; // Plan.root only
     repeated spark.connect.Expression input_expressions = 4; // rejected
     uint32 envelope_version = 5; // required: 1
   }
   ```

3. Sail resolves input plans and restores their column names before calling
   the plugin. Payload bytes are opaque to Sail; Nutmeg uses UTF-8 JSON with
   `version: 1`. A protobuf payload is also possible.
4. Planning must have no side effects: schema inspection can call the handler.
   Consume inputs and mutate state only during execution. Collect mutation
   receipts explicitly; do not assume exactly-once execution across reconnects.

## Minimal client: all three operation shapes

This complete example calls the installed native extensions. The small custom
plan below implements the bare relation wire format directly.

```python
import json
from pyspark.sql import functions as F
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan
from pyspark.sql.connect.session import SparkSession
from sail_nutmeg import Nutmeg

class GraphRequest(LogicalPlan):
    def __init__(self, verb, **fields):
        super().__init__(None)
        self.request = dict(version=1, verb=verb, graph="demo", **fields)

    def plan(self, session):
        relation = self._create_proto_relation()
        relation.extension.type_url = "type.googleapis.com/nutmeg.v1.NutmegApi"
        relation.extension.value = json.dumps(self.request, allow_nan=False).encode()
        return relation

spark = SparkSession.builder.remote("sc://127.0.0.1:50051").create()
try:
    # Expression: the client emits a normal function call; Sail invokes Sedona.
    point = F.call_function("ST_Point", F.lit(1.0), F.lit(2.0))
    spark.range(1).select(F.call_function("ST_AsText", point).alias("wkt")).show()

    # Stage two input relations. Nutmeg's client constructs the envelope above.
    nm = Nutmeg(spark)
    nodes = spark.createDataFrame([("a",), ("b",), ("c",)], "node_id string")
    edges = spark.createDataFrame([("a", "b"), ("b", "a")], "source string,target string")
    print(nm.stage("demo", nodes, edges))

    # Relation: a lazy DataFrame, composable with normal Sail operations.
    ranks = DataFrame(GraphRequest("run", algorithm="pagerank", options={}), spark)
    ranks.select("nodeId", "score").show()
    nm.run("demo", "wcc").select("nodeId", "componentId").show()

    # Command shape: execute a relation and consume its receipt.
    receipt = DataFrame(GraphRequest("drop"), spark).collect()[0]
    print(receipt.dropped)
finally:
    spark.stop()
```

The complete [Nutmeg client](nutmeg/python/sail_nutmeg/client.py) adds envelope
encoding and session checks for input DataFrames. Use it as the starting point
for a GraphFrames-shaped client: map `id/src/dst` to `node_id/source/target`,
then expose PageRank and connected components as calls to `run`. Reject other
methods explicitly with your own `NotSupportedException` (or Python's
`NotImplementedError`). Nutmeg returns `nodeId/score` and `nodeId/componentId`;
GraphFrames-compatible return objects, options and algorithm semantics still
need an adapter. This example does not claim drop-in GraphFrames compatibility.

## Implement the server package

A plugin is an independent Rust/PyO3 wheel; it does **not** implement a Sail
Rust trait or link Sail crates. Start with the small [Nutmeg package](nutmeg)
for relations, or [Sedona package](sedona) for scalar functions. Each has its
own Cargo manifest, lock file and Python bootstrap.

Declare discovery in `pyproject.toml`:

```toml
[project.entry-points."pysail.extensions"]
nutmeg = "sail_nutmeg:extension"
```

The entry point returns an object with `manifest()` and `bind(session_id)`.
The manifest declares `name`, `version`, `api_version: 1`,
`datafusion_version: "55.1.0"`, `arrow_version: "59.3.0"`, `placement` and
`relation_types`. A relation registration contains `type_url`, `accepts_bare`,
`min_inputs` and `max_inputs`. See the executable
[Nutmeg bootstrap](nutmeg/python/sail_nutmeg/__init__.py).
With `memory_bytes`, Sail instead calls
`bind_with_resources(session_id, memory_bytes, host_resource)`; retain the host
lease for as long as native state or exported buffers use its quota.

The bound object implements these contracts:

| Shape | Server implementation |
| --- | --- |
| Relation | `plan_relation(type_url, payload, inputs)` receives named `datafusion_execution_plan` capsules and returns a `datafusion_table_provider` capsule containing `FFI_TableProvider`. Return a provider describing execution, not eagerly computed results. [Complete implementation](nutmeg/src/lib.rs). |
| Expression | `scalar_udfs()` returns objects whose `__datafusion_scalar_udf__()` returns a `datafusion_scalar_udf` capsule containing `FFI_ScalarUDF`. Use `placement: "any"` for worker execution. [Complete implementation](sedona/src/lib.rs). |
| Command shape | Use the same relation callback with a mutating verb. Its provider performs the mutation during execution and emits a receipt batch. Nutmeg implements `stage` and `drop` this way. [Verb dispatch and providers](nutmeg/src/mutation.rs). |

For example, Sedona exports each existing DataFusion scalar function like this:

```rust
fn __datafusion_scalar_udf__<'py>(
    &self, py: Python<'py>,
) -> PyResult<Bound<'py, PyCapsule>> {
    PyCapsule::new_with_value(
        py,
        FFI_ScalarUDF::from(Arc::clone(&self.inner)),
        c"datafusion_scalar_udf",
    )
}
```

This is a method excerpt from the linked, buildable package; `inner` is an
`Arc<ScalarUDF>`. A relation-only bound object still returns `[]` from
`scalar_udfs()`. Native relation plugins currently use `placement: "driver"`.
Install the wheel in the server environment; install identical scalar-plugin
wheels on every worker. Restart the server after replacing packages. The host
checks API/DataFusion/Arrow versions before binding; this experimental contract
is not a general stable ABI promise.
