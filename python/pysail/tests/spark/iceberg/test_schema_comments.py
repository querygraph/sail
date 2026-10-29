import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField


@pytest.mark.parametrize("comment", ["identifier", 'a "quoted" comment'])
def test_iceberg_field_documentation_is_exposed_in_dataframe_schema(spark, sql_catalog, comment):
    table = sql_catalog.create_table(
        "default.documented",
        schema=Schema(NestedField(field_id=1, name="id", field_type=LongType(), required=False, doc=comment)),
    )
    try:
        schema = spark.read.format("iceberg").load(table.location()).schema
        assert schema["id"].metadata == {"comment": comment}
    finally:
        sql_catalog.drop_table("default.documented")
