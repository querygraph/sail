Feature: CREATE TABLE with PARTITIONED BY clause

  Scenario: CREATE TABLE with typed partition column not in column list
    Given variable location for temporary directory create_table_hive_partition
    Given final statement
      """
      DROP TABLE IF EXISTS create_table_hive_partition
      """
    Given statement template
      """
      CREATE TABLE create_table_hive_partition (id INT)
      USING PARQUET
      LOCATION {{ location.sql }}
      PARTITIONED BY (part STRING)
      """
    Given statement template
      """
      INSERT INTO create_table_hive_partition VALUES (1, 'a'), (2, 'b')
      """
    When query
      """
      SELECT * FROM create_table_hive_partition ORDER BY id
      """
    Then query result ordered
      | id | part |
      | 1  | a    |
      | 2  | b    |


  Scenario: CREATE TABLE with typed partition column already in column list
    Given variable location for temporary directory create_table_typed_part_match
    Given final statement
      """
      DROP TABLE IF EXISTS create_table_typed_part_match
      """
    Given statement template
      """
      CREATE TABLE create_table_typed_part_match (id INT, part STRING)
      USING PARQUET
      LOCATION {{ location.sql }}
      PARTITIONED BY (part STRING)
      """
    Given statement template
      """
      INSERT INTO create_table_typed_part_match VALUES (1, 'a')
      """
    When query
      """
      SELECT * FROM create_table_typed_part_match ORDER BY id
      """
    Then query result ordered
      | id | part |
      | 1  | a    |


  Scenario: CREATE TABLE fails when PARTITIONED BY type conflicts with column definition
    Given variable location for temporary directory create_table_type_mismatch
    Given final statement
      """
      DROP TABLE IF EXISTS create_table_type_mismatch
      """
    Given statement template with error (?i)partition column 'part' has incompatible type
      """
      CREATE TABLE create_table_type_mismatch (id INT, part STRING)
      USING PARQUET
      LOCATION {{ location.sql }}
      PARTITIONED BY (part INT)
      """

  Scenario Outline: Invalid partition fields are rejected before creating a table
    Given final statement
      """
      DROP TABLE IF EXISTS invalid_partition
      """
    Given final statement
      """
      RESET spark.sql.caseSensitive
      """
    Given statement
      """
      SET spark.sql.caseSensitive = <sensitive>
      """
    Given statement with error <condition>
      """
      CREATE TABLE invalid_partition(id BIGINT, payload STRING)
      USING parquet PARTITIONED BY (<partition>)
      """
    When query
      """
      SELECT * FROM invalid_partition
      """
    Then query error TABLE_OR_VIEW_NOT_FOUND

    Examples:
      | sensitive | partition | condition                   |
      | false     | missing   | COLUMN_NOT_DEFINED_IN_TABLE |
      | false     | id, id    | COLUMN_ALREADY_EXISTS       |
      | true      | ID        | COLUMN_NOT_DEFINED_IN_TABLE |

  Scenario Outline: Partition fields use canonical and quoted schema names
    Given final statement
      """
      DROP TABLE IF EXISTS valid_partition
      """
    Given final statement
      """
      RESET spark.sql.caseSensitive
      """
    Given statement
      """
      SET spark.sql.caseSensitive = false
      """
    Given statement
      """
      CREATE TABLE valid_partition(<column> BIGINT, payload STRING)
      USING parquet PARTITIONED BY (<partition>)
      """
    Given statement
      """
      INSERT INTO valid_partition (<column>, payload) VALUES (1, 'a')
      """
    When query
      """
      SELECT <column> AS key, payload FROM valid_partition
      """
    Then query result
      | key | payload |
      | 1   | a       |

    Examples:
      | column | partition |
      | id     | ID        |
      | `a.b`  | `a.b`     |

  Scenario: Typed partition columns respect case-sensitive field identity
    Given final statement
      """
      DROP TABLE IF EXISTS typed_partition_case
      """
    Given final statement
      """
      RESET spark.sql.caseSensitive
      """
    Given statement
      """
      SET spark.sql.caseSensitive = true
      """
    Given statement
      """
      CREATE TABLE typed_partition_case(id BIGINT, payload STRING)
      USING parquet PARTITIONED BY (ID BIGINT)
      """
    When query
      """
      SELECT * FROM typed_partition_case
      """
    Then query schema
      """
      root
       |-- id: long (nullable = true)
       |-- payload: string (nullable = true)
       |-- ID: long (nullable = true)
      """
