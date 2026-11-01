"""Nested schemas from Spark 4.2.1 MaterializeTablesSuite:735 and :780.

The original internal registerTable/materializeGraph fixture is exercised through
public materialized views and two refreshes of the same catalog tables.
Spark Connect retains execution entries, so each refresh registers a new graph.
"""

import pytest
from pyspark import pipelines as dp
from pyspark.pipelines.graph_element_registry import graph_element_registration_context
from pyspark.sql.types import ArrayType, IntegerType, StructField, StructType


@pytest.fixture(autouse=True)
def drop_tables(pipeline_spark):
    # Spark's catalog and warehouse outlive the session, and a leftover directory fails the next case.
    yield
    for name in ["c", "b", "a"]:
        pipeline_spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.mark.parametrize("nested", [False, True], ids=["arrays-and-maps", "nested-arrays-and-maps"])
def test_nested_table_schemas_after_two_materializations(pipeline_spark, pipeline_factory, nested):
    if nested:
        a_query = "select map(0, map(0, struct('a', 'b'))) m"
        b_query = "select array(array('a', 'b', 'c'), array('d', 'e', 'f')) arr"
        c_query = "select * from a join b where m[0][0].col1 = arr[0][0]"
        a_ddl = "m MAP<int, MAP<int, struct<col1: string, col2: string>>>"
        b_ddl = "arr ARRAY<ARRAY<string>>"
    else:
        a_query = "select map(1, struct('a', 'b')) m"
        b_query = None
        c_query = "select * from a join b where map_entries(m)[0].key = arr[0]"
        a_ddl = "m MAP<int, struct<col1: string, col2: string>>"
        b_ddl = "arr ARRAY<int>"

    for _ in range(2):
        _, registry, run = pipeline_factory()
        with graph_element_registration_context(registry):
            dp.materialized_view(name="a")(lambda: pipeline_spark.sql(a_query))
            if nested:
                dp.materialized_view(name="b")(lambda: pipeline_spark.sql(b_query))
            else:
                # A DDL string would need an analysis RPC, which a query function may not send.
                arr_schema = StructType([StructField("arr", ArrayType(IntegerType()))])
                dp.materialized_view(name="b")(
                    lambda: pipeline_spark.createDataFrame([([1, 3, 5],), ([2, 4, 6],)], arr_schema)
                )
            dp.materialized_view(name="c")(lambda: pipeline_spark.sql(c_query))
        run()

    for name, ddl in [("a", a_ddl), ("b", b_ddl), ("c", a_ddl + ", " + b_ddl)]:
        assert pipeline_spark.table(name).schema == StructType.fromDDL(ddl)
