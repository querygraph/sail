from __future__ import annotations

import pytest
from pysail.testing.spark.session import spark_session_factory


@pytest.fixture
def pipeline_spark(remote, tmp_path):
    # A separate session keeps pipeline catalogs, registrations, and SQL defaults
    # isolated between scenarios without changing the shared Spark test fixture.
    with spark_session_factory(remote) as sessions:
        session = sessions.create()
        session.conf.set("spark.sql.warehouse.dir", (tmp_path / "warehouse").as_uri())
        yield session


@pytest.fixture
def pipeline(pipeline_spark, tmp_path):
    registry_module = pytest.importorskip("pyspark.pipelines.spark_connect_graph_element_registry")
    client = pytest.importorskip("pyspark.pipelines.spark_connect_pipeline")
    graph_id = client.create_dataflow_graph(pipeline_spark, None, None, None)
    registry = registry_module.SparkConnectGraphElementRegistry(pipeline_spark, graph_id)

    def run(dry=False, refresh=None, full_refresh=None, full_refresh_all=False):
        client.handle_pipeline_events(
            client.start_run(
                pipeline_spark,
                graph_id,
                full_refresh,
                full_refresh_all,
                refresh,
                dry,
                (tmp_path / "state").as_uri(),
            )
        )

    return graph_id, registry, run
