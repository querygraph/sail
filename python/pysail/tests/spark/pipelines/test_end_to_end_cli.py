"""EndToEndAPISuite. It defines no test of its own beyond APITest, which test_api_suite.py ports through
start_run. What it adds is the CLI process and its spec file, which these drive against the same server.
"""

import os
import subprocess
import sys
from pathlib import Path

import pyspark
import pytest

from pysail.testing.spark.utils.common import is_jvm_spark

CLI = Path(pyspark.__file__).parent / "pipelines" / "cli.py"


def remote():
    url = os.environ.get("SPARK_REMOTE", "")
    return url if url.startswith("sc://") else "sc://localhost:15002"


def spec(tmp_path, catalog, schema):
    path = tmp_path / "pipeline.yaml"
    path.write_text(
        f"""
name: test-pipeline
catalog: "{catalog}"
database: "{schema}"
storage: "{(tmp_path / "storage").as_uri()}"
configuration:
  "spark.remote": "{remote()}"
libraries:
  - glob:
      include: "transformations/**"
"""
    )
    source = tmp_path / "transformations"
    source.mkdir()
    (source / "st.sql").write_text(
        f"CREATE STREAMING TABLE st AS SELECT * FROM STREAM {schema}.src;\nCREATE MATERIALIZED VIEW mv AS SELECT * FROM st;\n"
    )
    (source / "view.py").write_text(
        'from pyspark import pipelines as dp\n\n@dp.materialized_view\ndef py_mv():\n  return spark.table("mv")\n'
    )
    return path


def cli(*arguments, spec_path):
    env = {**os.environ, "SPARK_REMOTE": remote()}
    process = subprocess.run(  # noqa: S603 the CLI under test
        [sys.executable, str(CLI), *arguments, "--spec", str(spec_path)],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        check=False,
    )
    return process.returncode, process.stdout + process.stderr


# The CLI opens a session of its own, which sees this test's schema when sessions share a catalog,
# as Spark sessions do and as Sail sessions do under this setting.
SHARED_CATALOG = pytest.mark.skipif(
    not is_jvm_spark() and os.environ.get("SAIL_CATALOG__SHARED_MEMORY") != "true",
    reason="needs a server with catalog.shared_memory",
)


@pytest.fixture
def schema(spark, request):
    name = "cl_" + "".join(c if c.isalnum() else "_" for c in request.node.name).lower()[:48]
    spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")
    spark.sql(f"CREATE SCHEMA {name}")
    spark.sql(f"CREATE TABLE {name}.src (id BIGINT) USING parquet")
    spark.sql(f"INSERT INTO {name}.src VALUES (1), (2)")
    yield name
    spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")


def rows(spark, table):
    return sorted(tuple(row) for row in spark.table(table).collect())


@SHARED_CATALOG
def test_a_dry_run_creates_nothing(spark, schema, tmp_path):
    code, output = cli("dry-run", spec_path=spec(tmp_path, spark.catalog.currentCatalog(), schema))
    assert code == 0, output
    assert "Run is COMPLETED" in output
    assert not spark.catalog.tableExists(f"{schema}.st")


@SHARED_CATALOG
def test_a_run_and_a_full_refresh_all(spark, schema, tmp_path):
    path = spec(tmp_path, spark.catalog.currentCatalog(), schema)
    code, output = cli("run", spec_path=path)
    assert code == 0, output
    for table in ["st", "mv", "py_mv"]:
        assert rows(spark, f"{schema}.{table}") == [(1,), (2,)]
    spark.sql(f"INSERT OVERWRITE TABLE {schema}.src VALUES (3)")
    code, output = cli("run", "--full-refresh-all", spec_path=path)
    assert code == 0, output
    for table in ["st", "mv", "py_mv"]:
        # The CLI wrote through its own session, so this one drops its cached file listing.
        spark.catalog.refreshTable(f"{schema}.{table}")
        assert rows(spark, f"{schema}.{table}") == [(3,)]


@SHARED_CATALOG
def test_a_failed_run_exits_nonzero(spark, schema, tmp_path):
    path = spec(tmp_path, spark.catalog.currentCatalog(), schema)
    (tmp_path / "transformations" / "bad.sql").write_text("CREATE MATERIALIZED VIEW bad AS SELECT * FROM missing_table;\n")
    code, output = cli("run", spec_path=path)
    assert code != 0
    assert "Failed to resolve flows in the pipeline" in output


def test_a_self_contained_spec_needs_nothing_from_another_session(spark, tmp_path):
    """No schema or source from the test's session: the CLI's own session has to resolve everything."""
    path = tmp_path / "pipeline.yaml"
    path.write_text(
        f"""
name: test-pipeline
storage: "{(tmp_path / "storage").as_uri()}"
configuration:
  "spark.remote": "{remote()}"
libraries:
  - glob:
      include: "transformations/**"
"""
    )
    source = tmp_path / "transformations"
    source.mkdir()
    (source / "one.sql").write_text("CREATE MATERIALIZED VIEW cli_one AS SELECT 1 AS id;\n")
    (source / "two.py").write_text(
        'from pyspark import pipelines as dp\n\n@dp.materialized_view\ndef cli_two():\n  return spark.table("cli_one")\n'
    )
    try:
        code, output = cli("dry-run", spec_path=path)
        assert code == 0, output
        code, output = cli("run", spec_path=path)
        assert code == 0, output
        assert "Run is COMPLETED" in output
    finally:
        for table in ["cli_one", "cli_two"]:
            spark.sql(f"DROP TABLE IF EXISTS default.{table}")
