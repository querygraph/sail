"""ALTER TABLE on a Delta table named by its path, as Spark with Delta accepts it."""

from __future__ import annotations


def test_alter_table_by_path_sets_properties_and_adds_columns(spark, tmp_path):
    location = (tmp_path / "events").as_uri()
    spark.createDataFrame([(1, "a")], "id INT, name STRING").write.format("delta").save(location)
    table = f"delta.`{location}`"

    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('team' = 'cdc')")
    properties = {row.key: row.value for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    assert properties["team"] == "cdc"

    spark.sql(f"ALTER TABLE {table} ADD COLUMNS (score DOUBLE)")
    assert [field.name for field in spark.read.format("delta").load(location).schema] == ["id", "name", "score"]

    spark.sql(f"ALTER TABLE {table} UNSET TBLPROPERTIES ('team')")
    properties = {row.key: row.value for row in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    assert "team" not in properties
