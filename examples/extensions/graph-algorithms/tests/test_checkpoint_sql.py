"""Real SQL/Parquet controls; the temporary ownership fixture is not GraphUtils."""
from types import SimpleNamespace

import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from pyspark_pecan import CancellationToken
from pyspark_pecan.staging import StagingRun


@pytest.mark.integration
@pytest.mark.parametrize("repartition", [True, False])
@pytest.mark.parametrize("rows", [[], [(-7, None), (0, "a"), (0, "a"), (8, "b")]])
def test_checkpoint_roundtrip_preserves_rows_schema_and_duplicates(spark, tmp_path, repartition, rows):
    # These controls exercise the actual engine writer/readback, independently
    # of a GraphUtils capability. They do not qualify distributed ownership.
    root = tmp_path / "owned"
    root.mkdir()
    utils = SimpleNamespace(allocate=lambda: (root.as_uri(), "test-token"), exists=lambda *_: True)
    schema = StructType([StructField("id", LongType(), False), StructField("value", StringType(), True)])
    frame = spark.createDataFrame(rows, schema)
    run = StagingRun(spark, utils, CancellationToken(), 3, repartition_checkpoints=repartition)
    path, stored = run.materialize(frame, expected_rows=len(rows))
    assert [(f.name, f.dataType) for f in stored.schema] == [(f.name, f.dataType) for f in schema]
    assert sorted(tuple(row) for row in stored.collect()) == sorted(rows)
    assert run._stages[path] is stored and run.write_uncertain is False
