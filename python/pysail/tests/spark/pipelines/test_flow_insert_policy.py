"""Flow INSERT registration policy and producer ordering from Spark 4.2.1."""

from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException


@pytest.mark.parametrize(
    ("operation", "message"),
    [
        ("INSERT INTO output SELECT 1", "Only INSERT INTO by name flows supported."),
        ("INSERT OVERWRITE output BY NAME SELECT 1", "INSERT OVERWRITE flows not supported."),
        ("INSERT INTO output(a) SELECT 1", "Column schema may not be specified for flow target."),
        ("INSERT INTO output PARTITION(p) BY NAME SELECT 1", "Partition spec may not be specified for flow target."),
        ("INSERT INTO output PARTITION(p) (a) SELECT 1", "Partition spec may not be specified for flow target."),
        ("INSERT OVERWRITE output(a) SELECT 1", "Column schema may not be specified for flow target."),
        (
            "INSERT OVERWRITE output PARTITION(p) BY NAME SELECT 1",
            "Partition spec may not be specified for flow target.",
        ),
        ("INSERT OVERWRITE output SELECT 1", "INSERT OVERWRITE flows not supported."),
        (
            "INSERT OVERWRITE output PARTITION(p=1) IF NOT EXISTS SELECT 1",
            "Partition spec may not be specified for flow target.",
        ),
        ("INSERT OVERWRITE A.b.c.d BY NAME SELECT 1", "INSERT OVERWRITE flows not supported."),
        ("INSERT INTO A.b.c.d(a) SELECT 1", "Column schema may not be specified for flow target."),
        # Same three input shapes retained in the original local BDD outline.
        ("INSERT INTO output SELECT * FROM STREAM(source)", "Only INSERT INTO by name flows supported."),
        ("INSERT OVERWRITE output BY NAME SELECT * FROM STREAM(source)", "INSERT OVERWRITE flows not supported."),
        ("INSERT INTO output(id) SELECT * FROM STREAM(source)", "Column schema may not be specified for flow target."),
    ],
)
def test_flow_insert_policy_has_exact_registration_fields(pipeline_spark, pipeline, operation, message):
    _, registry, _ = pipeline
    offending = f"CREATE FLOW f AS {operation}"
    with pytest.raises(AnalysisException) as captured:
        registry.register_sql(f"CREATE STREAMING TABLE output;\n{offending};", Path("flow.sql"))
    error = captured.value
    assert error.getCondition() == "PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR"
    assert error.getSqlState() == "42000"
    assert error.getMessageParameters() == {
        "message": message,
        "offendingQuery": f"\nOffending query:\n{offending}\n",
        "codeLocation": "Query defined at flow.sql:2",
    }
    assert message in str(error)
    assert not pipeline_spark.catalog.tableExists("output")


def test_valid_by_name_flow_still_registers(pipeline):
    _, registry, _ = pipeline
    registry.register_sql(
        "CREATE STREAMING TABLE output; CREATE FLOW f AS INSERT INTO output BY NAME SELECT 1",
        Path("flow.sql"),
    )
