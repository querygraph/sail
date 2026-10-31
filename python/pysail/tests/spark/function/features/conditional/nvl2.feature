Feature: nvl2 output schema

  @function(nullability)
  Rule: Output schema

    Scenario: a non-null literal input to nvl2 yields the schema Spark declares
      When query
        """
        SELECT nvl2(NULL, 2, 1) AS result
        """
      Then query schema
        """
        root
         |-- result: integer (nullable = false)
        """


    Scenario: the tested argument does not affect the branch types
      When query
        """
        SELECT nvl2('sentinel', 1, 2) AS text_input,
               nvl2(TRUE, 1, 2) AS boolean_input,
               nvl2(array(1), 1, 2) AS array_input,
               nvl2(CAST(NULL AS STRING), 1, 2) AS null_input
        """
      Then query result
        | text_input | boolean_input | array_input | null_input |
        | 1          | 1             | 1           | 2          |
      Then query schema
        """
        root
         |-- text_input: integer (nullable = false)
         |-- boolean_input: integer (nullable = false)
         |-- array_input: integer (nullable = false)
         |-- null_input: integer (nullable = false)
        """


    Scenario: nvl2 resolves the branch common type before type introspection
      When query
        """
        SELECT typeof(nvl2('sentinel', 1, CAST(2 AS BIGINT))) AS bigint,
               typeof(nvl2('sentinel', 1, 2.5)) AS decimal
        """
      Then query result
        | bigint | decimal       |
        | bigint | decimal(11,1) |
