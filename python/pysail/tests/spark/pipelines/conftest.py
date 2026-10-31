from __future__ import annotations

import pytest
from pysail.testing.spark.session import spark_session_factory
from pyspark.errors import AnalysisException


def pytest_configure(config):
    config.pluginmanager.import_plugin("pysail.testing.spark.steps.pipelines")


@pytest.fixture
def pipeline_spark(remote, tmp_path):
    # A separate session keeps pipeline catalogs, registrations, and SQL defaults
    # isolated between scenarios without changing the shared Spark test fixture.
    with spark_session_factory(remote) as sessions:
        session = sessions.create()
        try:
            session.conf.set("spark.sql.warehouse.dir", (tmp_path / "warehouse").as_uri())
        except AnalysisException as error:
            # Spark uses its startup warehouse; Sail permits a session override.
            if error.getCondition() != "CANNOT_MODIFY_STATIC_CONFIG":
                raise
        yield session


@pytest.fixture
def pipeline_factory(pipeline_spark, tmp_path):
    registry_module = pytest.importorskip("pyspark.pipelines.spark_connect_graph_element_registry")
    client = pytest.importorskip("pyspark.pipelines.spark_connect_pipeline")

    def create():
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

    return create


@pytest.fixture
def pipeline(pipeline_factory):
    return pipeline_factory()
