@function(nullability)
Feature: ANSI primitive string cast schema

  Scenario: ANSI primitive casts from string literals remain nullable
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST('12' AS TINYINT) AS tiny, CAST('12' AS SMALLINT) AS small,
             CAST('12' AS INT) AS integer_value, CAST('12' AS BIGINT) AS big,
             CAST('12' AS FLOAT) AS single_value, CAST('12' AS DOUBLE) AS double_value,
             CAST('true' AS BOOLEAN) AS boolean_value
      """
    Then query schema
      """
      root
       |-- tiny: byte (nullable = true)
       |-- small: short (nullable = true)
       |-- integer_value: integer (nullable = true)
       |-- big: long (nullable = true)
       |-- single_value: float (nullable = true)
       |-- double_value: double (nullable = true)
       |-- boolean_value: boolean (nullable = true)
      """

  Scenario Outline: ANSI primitive string casts remain nullable for <input> columns
    Given config spark.sql.ansi.enabled = true
    When query
      """
      SELECT CAST(s AS TINYINT) AS tiny, CAST(s AS SMALLINT) AS small,
             CAST(s AS INT) AS integer_value, CAST(s AS BIGINT) AS big,
             CAST(s AS FLOAT) AS single_value, CAST(s AS DOUBLE) AS double_value,
             CAST(b AS BOOLEAN) AS boolean_value
      FROM VALUES <rows> AS t(s, b)
      """
    Then query schema
      """
      root
       |-- tiny: byte (nullable = true)
       |-- small: short (nullable = true)
       |-- integer_value: integer (nullable = true)
       |-- big: long (nullable = true)
       |-- single_value: float (nullable = true)
       |-- double_value: double (nullable = true)
       |-- boolean_value: boolean (nullable = true)
      """

    Examples:
      | input        | rows                                                                  |
      | non-nullable | ('12', 'true')                                                        |
      | nullable     | ('12', 'true'), (CAST(NULL AS STRING), CAST(NULL AS STRING))             |
