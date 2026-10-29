"""Connect server identifier grids from SparkDeclarativePipelinesServerSuite."""

from contextlib import contextmanager

import pytest
from pyspark.sql.connect import proto as pb

pytest.importorskip("pyspark.pipelines")


@contextmanager
def _graph(spark, database):
    original_database = spark.catalog.currentDatabase()
    catalog = spark.catalog.currentCatalog()
    graph_id = None
    if database is not None:
        assert not spark.catalog.databaseExists(database)
        spark.sql(f"CREATE DATABASE {database}")
    try:
        command = pb.Command()
        create = command.pipeline_command.create_dataflow_graph
        create.SetInParent()
        if database is not None:
            create.default_catalog = catalog
            create.default_database = database
        _, properties, _ = spark.client.execute_command(command)
        result = properties["pipeline_command_result"]
        assert result.HasField("create_dataflow_graph_result")
        graph_id = result.create_dataflow_graph_result.dataflow_graph_id
        assert graph_id
        yield graph_id, catalog, database if database is not None else original_database
    finally:
        if graph_id is not None:
            command = pb.Command()
            command.pipeline_command.drop_dataflow_graph.dataflow_graph_id = graph_id
            spark.client.execute_command(command)
        if database is not None:
            spark.catalog.setCurrentDatabase(original_database)
            spark.sql(f"DROP DATABASE {database}")


@pytest.mark.parametrize(
    ("kind", "name", "database"),
    [
        pytest.param(pb.TEMPORARY_VIEW, "tv", None, id="default-temporary-view"),
        pytest.param(pb.TABLE, "`tb`", None, id="default-table"),
        pytest.param(pb.MATERIALIZED_VIEW, "mv", None, id="default-materialized-view"),
        pytest.param(pb.TEMPORARY_VIEW, "tv", "custom_db", id="custom-temporary-view"),
        pytest.param(pb.TABLE, "`tb`", "`my_db`", id="custom-table"),
        pytest.param(pb.MATERIALIZED_VIEW, "mv", "another_db", id="custom-materialized-view"),
    ],
)
def test_define_output_returns_resolved_identifier(pipeline_spark, kind, name, database):
    with _graph(pipeline_spark, database) as (graph_id, catalog, namespace):
        command = pb.Command()
        output = command.pipeline_command.define_output
        output.dataflow_graph_id = graph_id
        output.output_name = name
        output.output_type = kind
        _, properties, _ = pipeline_spark.client.execute_command(command)
        result = properties["pipeline_command_result"]
        assert result.HasField("define_output_result")
        identifier = result.define_output_result.resolved_identifier
        private = kind == pb.TEMPORARY_VIEW
        assert identifier.catalog_name == ("" if private else catalog)
        assert list(identifier.namespace) == ([] if private else [namespace])
        assert identifier.table_name == name.strip("`")


@pytest.mark.parametrize(
    ("kind", "name", "database"),
    [
        pytest.param(pb.MATERIALIZED_VIEW, "`mv`", None, id="default-materialized-view"),
        pytest.param(pb.TEMPORARY_VIEW, "tv", None, id="default-temporary-view"),
        pytest.param(pb.MATERIALIZED_VIEW, "mv", "custom_db", id="custom-materialized-view"),
        pytest.param(pb.TEMPORARY_VIEW, "tv", "custom_db", id="custom-temporary-view"),
    ],
)
def test_define_flow_returns_resolved_identifier(pipeline_spark, kind, name, database):
    with _graph(pipeline_spark, database) as (graph_id, catalog, namespace):
        private = kind == pb.TEMPORARY_VIEW
        if private:
            command = pb.Command()
            output = command.pipeline_command.define_output
            output.dataflow_graph_id = graph_id
            output.output_name = name
            output.output_type = kind
            _, properties, _ = pipeline_spark.client.execute_command(command)
            assert properties["pipeline_command_result"].HasField("define_output_result")
        command = pb.Command()
        flow = command.pipeline_command.define_flow
        flow.dataflow_graph_id = graph_id
        flow.flow_name = name
        flow.target_dataset_name = name
        function = flow.relation_flow_details.relation.unresolved_table_valued_function
        function.function_name = "range"
        function.arguments.add().literal.integer = 5
        _, properties, _ = pipeline_spark.client.execute_command(command)
        result = properties["pipeline_command_result"]
        assert result.HasField("define_flow_result")
        identifier = result.define_flow_result.resolved_identifier
        assert identifier.catalog_name == ("" if private else catalog)
        assert list(identifier.namespace) == ([] if private else [namespace])
        assert identifier.table_name == name.strip("`")


@pytest.mark.parametrize("explicit_defaults", [False, True])
def test_graph_captures_database_default(pipeline_spark, explicit_defaults):
    original_database = pipeline_spark.catalog.currentDatabase()
    catalog = pipeline_spark.catalog.currentCatalog()
    database = "pipeline_defaults"
    graph_id = None
    assert not pipeline_spark.catalog.databaseExists(database)
    pipeline_spark.sql(f"CREATE DATABASE {database}")
    try:
        if not explicit_defaults:
            pipeline_spark.catalog.setCurrentDatabase(database)
        command = pb.Command()
        create = command.pipeline_command.create_dataflow_graph
        create.SetInParent()
        if explicit_defaults:
            create.default_catalog = catalog
            create.default_database = database
        _, properties, _ = pipeline_spark.client.execute_command(command)
        result = properties["pipeline_command_result"]
        assert result.HasField("create_dataflow_graph_result")
        graph_id = result.create_dataflow_graph_result.dataflow_graph_id
        assert graph_id
        assert pipeline_spark.catalog.currentCatalog() == catalog
        assert pipeline_spark.catalog.currentDatabase() == database
        # The graph keeps its captured default after the session moves elsewhere.
        pipeline_spark.catalog.setCurrentDatabase(original_database)
        command = pb.Command()
        output = command.pipeline_command.define_output
        output.dataflow_graph_id = graph_id
        output.output_name = "captured_output"
        output.output_type = pb.MATERIALIZED_VIEW
        _, properties, _ = pipeline_spark.client.execute_command(command)
        result = properties["pipeline_command_result"]
        assert result.HasField("define_output_result")
        identifier = result.define_output_result.resolved_identifier
        assert identifier.catalog_name == catalog
        assert list(identifier.namespace) == [database]
        assert identifier.table_name == "captured_output"
    finally:
        if graph_id is not None:
            command = pb.Command()
            command.pipeline_command.drop_dataflow_graph.dataflow_graph_id = graph_id
            pipeline_spark.client.execute_command(command)
        pipeline_spark.catalog.setCurrentDatabase(original_database)
        pipeline_spark.sql(f"DROP DATABASE {database}")
