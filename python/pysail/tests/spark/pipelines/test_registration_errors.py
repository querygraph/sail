"""Structured batch graph-registration errors from Spark's SQL pipeline suite."""

import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pyspark.sql.connect import proto as pb

pytest.importorskip("pyspark.pipelines")


@pytest.mark.parametrize("dry", [False, True])
@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("", id="empty-file"),
        pytest.param("CREATE TEMPORARY VIEW a AS SELECT id FROM range(1, 3)", id="temporary-only"),
        pytest.param("CREATE FLOW f AS INSERT INTO a BY NAME SELECT 1", id="flow-only"),
    ],
)
def test_sql_without_persisted_output_has_structured_error(pipeline_spark, pipeline, tmp_path, sql, dry):
    _, registry, run = pipeline
    registry.register_sql(sql, tmp_path / "empty.sql")
    with pytest.raises(AnalysisException) as error:
        run(dry=dry)
    assert error.value.getCondition() == "RUN_EMPTY_PIPELINE"
    assert error.value.getMessageParameters() == {}
    assert error.value.getSqlState() == "42617"
    assert not pipeline_spark.catalog.tableExists("a")


@pytest.mark.parametrize("dry", [False, True])
@pytest.mark.parametrize(
    ("definitions", "private", "types"),
    [
        pytest.param(
            ["CREATE STREAMING TABLE duplicate", "CREATE VIEW duplicate AS SELECT 1 AS value"],
            False,
            ["TABLE", "VIEW"],
            id="table-view",
        ),
        pytest.param(
            ["CREATE VIEW duplicate AS SELECT 1 AS value", "CREATE STREAMING TABLE duplicate"],
            False,
            ["TABLE", "VIEW"],
            id="view-table",
        ),
        pytest.param(
            ["CREATE MATERIALIZED VIEW duplicate AS SELECT 1 AS value"] * 2,
            False,
            ["TABLE", "TABLE"],
            id="materialized-materialized",
        ),
        pytest.param(
            ["CREATE STREAMING TABLE duplicate", "CREATE MATERIALIZED VIEW duplicate AS SELECT 1 AS value"],
            False,
            ["TABLE", "TABLE"],
            id="streaming-materialized",
        ),
        pytest.param(
            ["CREATE VIEW duplicate AS SELECT 1 AS value"] * 2,
            False,
            ["VIEW", "VIEW"],
            id="persisted-persisted",
        ),
        pytest.param(
            ["CREATE TEMPORARY VIEW duplicate AS SELECT 1 AS value"] * 2,
            True,
            ["VIEW", "VIEW"],
            id="temporary-temporary",
        ),
        pytest.param(
            ["CREATE VIEW duplicate AS SELECT 1 AS value"] * 2 + ["CREATE STREAMING TABLE duplicate"],
            False,
            ["TABLE", "VIEW"],
            id="views-before-table",
        ),
    ],
)
def test_duplicate_outputs_have_structured_types(pipeline_spark, pipeline, tmp_path, definitions, private, types, dry):
    _, registry, run = pipeline
    registry.register_sql("CREATE MATERIALIZED VIEW valid_output AS SELECT 1 AS value", tmp_path / "valid.sql")
    for index, definition in enumerate(definitions):
        registry.register_sql(definition, tmp_path / f"duplicate_{index}.sql")
    with pytest.raises(AnalysisException) as error:
        run(dry=dry)
    identifier = "`duplicate`"
    if not private:
        identifier = (
            f"`{pipeline_spark.catalog.currentCatalog()}`.`{pipeline_spark.catalog.currentDatabase()}`.{identifier}"
        )
    assert error.value.getCondition() == "PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT"
    assert error.value.getSqlState() == "42710"
    assert error.value.getMessageParameters() == {
        "outputName": identifier,
        "outputType1": types[0],
        "outputType2": types[1],
    }
    assert not pipeline_spark.catalog.tableExists("valid_output")
    assert not pipeline_spark.catalog.tableExists("duplicate")


@pytest.mark.parametrize("dry", [False, True])
def test_sql_dataset_without_flow_has_structured_error(pipeline_spark, pipeline, tmp_path, dry):
    _, registry, run = pipeline
    registry.register_sql("CREATE STREAMING TABLE st", tmp_path / "empty_table.sql")
    with pytest.raises(AnalysisException) as error:
        run(dry=dry)
    assert error.value.getCondition() == "PIPELINE_DATASET_WITHOUT_FLOW"
    assert error.value.getSqlState() == "0A000"
    assert error.value.getMessageParameters() == {
        "identifier": f"`{pipeline_spark.catalog.currentCatalog()}`.`{pipeline_spark.catalog.currentDatabase()}`.`st`"
    }
    assert not pipeline_spark.catalog.tableExists("st")


@pytest.mark.parametrize("dry", [False, True])
@pytest.mark.parametrize("kind", [pb.TABLE, pb.MATERIALIZED_VIEW, pb.TEMPORARY_VIEW])
def test_protocol_dataset_without_flow_preserves_identifier(pipeline_spark, pipeline, tmp_path, kind, dry):
    graph_id, registry, run = pipeline
    registry.register_sql("CREATE MATERIALIZED VIEW independent AS SELECT 1 AS value", tmp_path / "independent.sql")
    name = "`output``with.dot`"
    command = pb.Command()
    command.pipeline_command.define_output.CopyFrom(
        pb.PipelineCommand.DefineOutput(dataflow_graph_id=graph_id, output_name=name, output_type=kind)
    )
    pipeline_spark.client.execute_command(command)
    with pytest.raises(AnalysisException) as error:
        run(dry=dry)
    identifier = name
    if kind != pb.TEMPORARY_VIEW:
        identifier = f"`{pipeline_spark.catalog.currentCatalog()}`.`{pipeline_spark.catalog.currentDatabase()}`.{name}"
    assert error.value.getCondition() == "PIPELINE_DATASET_WITHOUT_FLOW"
    assert error.value.getSqlState() == "0A000"
    assert error.value.getMessageParameters() == {"identifier": identifier}
    assert not pipeline_spark.catalog.tableExists("independent")
    assert not pipeline_spark.catalog.tableExists(name)


@pytest.mark.parametrize("dry", [False, True])
@pytest.mark.parametrize(
    ("files", "flow_name", "targets"),
    [
        pytest.param(
            [
                "CREATE STREAMING TABLE st; CREATE FLOW f AS INSERT INTO st BY NAME SELECT * FROM STREAM(source)",
                "CREATE FLOW f AS INSERT INTO st BY NAME SELECT * FROM STREAM(source)",
            ],
            "f",
            ["st"],
            id="same-target",
        ),
        pytest.param(
            [
                "CREATE STREAMING TABLE st AS SELECT * FROM STREAM(source); CREATE STREAMING TABLE st2",
                "CREATE FLOW st AS INSERT INTO st2 BY NAME SELECT * FROM STREAM(source2)",
            ],
            "st",
            ["st", "st2"],
            id="implicit-explicit",
        ),
        pytest.param(
            [
                (
                    "CREATE STREAMING TABLE st; CREATE STREAMING TABLE st2; "
                    "CREATE FLOW f AS INSERT INTO st2 BY NAME SELECT * FROM STREAM(source)"
                ),
                "CREATE FLOW f AS INSERT INTO st BY NAME SELECT * FROM STREAM(source)",
            ],
            "f",
            ["st2", "st"],
            id="target-order",
        ),
        pytest.param(
            [
                (
                    "CREATE STREAMING TABLE st; CREATE STREAMING TABLE st2; CREATE STREAMING TABLE st3; "
                    "CREATE FLOW f AS INSERT INTO st BY NAME SELECT * FROM STREAM(source)"
                ),
                "CREATE FLOW f AS INSERT INTO st2 BY NAME SELECT * FROM STREAM(source)",
                "CREATE FLOW f AS INSERT INTO st3 BY NAME SELECT * FROM STREAM(source)",
            ],
            "f",
            ["st", "st2"],
            id="first-two-targets",
        ),
        pytest.param(
            [
                (
                    "CREATE STREAMING TABLE st; CREATE STREAMING TABLE st2; "
                    "CREATE FLOW f AS INSERT INTO st BY NAME SELECT * FROM STREAM(source); "
                    "CREATE FLOW g AS INSERT INTO st BY NAME SELECT * FROM STREAM(source)"
                ),
                (
                    "CREATE FLOW g AS INSERT INTO st2 BY NAME SELECT * FROM STREAM(source); "
                    "CREATE FLOW f AS INSERT INTO st2 BY NAME SELECT * FROM STREAM(source)"
                ),
            ],
            "f",
            ["st", "st2"],
            id="first-registered-identifier",
        ),
        pytest.param(
            [
                (
                    "CREATE STREAMING TABLE `st.with.dot`; "
                    "CREATE FLOW `flow``with.dot` AS INSERT INTO `st.with.dot` BY NAME SELECT * FROM STREAM(source)"
                ),
                "CREATE FLOW `flow``with.dot` AS INSERT INTO `st.with.dot` BY NAME SELECT * FROM STREAM(source)",
            ],
            "flow`with.dot",
            ["st.with.dot"],
            id="escaped-identifiers",
        ),
    ],
)
def test_duplicate_flows_preserve_identifiers_and_targets(
    pipeline_spark, pipeline, tmp_path, files, flow_name, targets, dry
):
    _, registry, run = pipeline
    for index, sql in enumerate(files):
        registry.register_sql(sql, tmp_path / f"flow_{index}.sql")
    with pytest.raises(AnalysisException) as error:
        run(dry=dry)
    catalog = pipeline_spark.catalog.currentCatalog()
    database = pipeline_spark.catalog.currentDatabase()
    assert error.value.getCondition() == "PIPELINE_DUPLICATE_IDENTIFIERS.FLOW"
    assert error.value.getSqlState() == "42710"
    assert error.value.getMessageParameters() == {
        "flowName": f"{catalog}.{database}.{flow_name}",
        "datasetNames": ",".join(f"`{catalog}`.`{database}`.`{target}`" for target in targets),
    }
    for target in targets:
        assert not pipeline_spark.catalog.tableExists(f"`{target}`")
