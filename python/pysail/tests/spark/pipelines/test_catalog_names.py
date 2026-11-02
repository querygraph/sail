"""Catalog name handling the pipelines rely on. These run against the gate's
server and its default memory catalog, which the `catalog` directory's own
tests do not (they start a server of their own and skip under SPARK_REMOTE).
"""


def test_a_table_is_renamed_under_its_mixed_case_name(spark):
    spark.sql("DROP TABLE IF EXISTS renamed_mixed_case")
    spark.sql("CREATE TABLE RenameMixedCase (id INT) USING parquet")
    try:
        spark.sql("ALTER TABLE RenameMixedCase RENAME TO renamed_mixed_case")
        assert spark.catalog.tableExists("renamed_mixed_case")
        assert not spark.catalog.tableExists("RenameMixedCase")
    finally:
        spark.sql("DROP TABLE IF EXISTS RenameMixedCase")
        spark.sql("DROP TABLE IF EXISTS renamed_mixed_case")
