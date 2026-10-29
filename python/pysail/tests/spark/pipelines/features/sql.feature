@spark-4.1
Feature: Declarative pipeline SQL
  Background:
    Given a declarative pipeline

  Scenario: Forward references through private views
    Given pipeline SQL file definitions.sql
      """
      CREATE MATERIALIZED VIEW result AS SELECT * FROM intermediate;
      CREATE TEMPORARY VIEW intermediate AS SELECT id * 2 AS doubled FROM source WHERE id > 1;
      CREATE MATERIALIZED VIEW source AS SELECT * FROM range(4);
      """
    When the pipeline runs
    When query
      """
      SELECT doubled FROM result ORDER BY doubled
      """
    Then query result ordered
      | doubled |
      | 4       |
      | 6       |
    Then pipeline dataset intermediate is not published

  Scenario: Pipeline dataset can be referenced in a subquery
    Given pipeline SQL file subquery.sql
      """
      CREATE MATERIALIZED VIEW a AS SELECT * FROM range(5);
      CREATE MATERIALIZED VIEW b AS SELECT * FROM range(5)
      WHERE id = (SELECT max(id) FROM a);
      """
    When the pipeline runs
    When query
      """
      SELECT * FROM b
      """
    Then query result
      | id |
      | 4  |

  Scenario: Pipeline dataset can be referenced in a nested CTE
    Given pipeline SQL file cte.sql
      """
      CREATE MATERIALIZED VIEW a AS SELECT 1 AS value;
      CREATE MATERIALIZED VIEW d AS
      WITH c AS (WITH b AS (SELECT * FROM a) SELECT * FROM b)
      SELECT * FROM c;
      """
    When the pipeline runs
    When query
      """
      SELECT * FROM d
      """
    Then query result
      | value |
      | 1     |

  Scenario: Aggregation reads committed materialized results
    Given pipeline SQL file aggregation.sql
      """
      CREATE MATERIALIZED VIEW a AS SELECT id AS value, id % 2 AS isOdd FROM range(1, 10);
      CREATE MATERIALIZED VIEW b AS
      SELECT isOdd, max(value) AS maximum FROM a GROUP BY isOdd LIMIT 2;
      """
    When the pipeline runs
    When query
      """
      SELECT * FROM b ORDER BY isOdd
      """
    Then query result ordered
      | isOdd | maximum |
      | 0     | 8       |
      | 1     | 9       |

  Scenario: Join private pipeline views
    Given pipeline SQL file join.sql
      """
      CREATE TEMPORARY VIEW a AS SELECT id FROM range(1, 3);
      CREATE TEMPORARY VIEW b AS SELECT id FROM range(1, 3);
      CREATE MATERIALIZED VIEW c AS SELECT a.id AS id1, b.id AS id2
      FROM a JOIN b ON a.id = b.id;
      """
    When the pipeline runs
    When query
      """
      SELECT * FROM c ORDER BY id1
      """
    Then query result ordered
      | id1 | id2 |
      | 1   | 1   |
      | 2   | 2   |

  Scenario: Validation publishes no materialized output
    Given pipeline SQL file validation.sql
      """
      CREATE MATERIALIZED VIEW `output.with.dot` AS SELECT * FROM range(2);
      """
    When the pipeline is validated
    Then pipeline dataset `output.with.dot` is not published

  Scenario: Analysis fails before any output is published
    Given pipeline SQL file invalid.sql
      """
      CREATE MATERIALIZED VIEW valid AS SELECT * FROM range(2);
      CREATE MATERIALIZED VIEW invalid AS SELECT missing FROM range(2);
      """
    When the pipeline fails with missing
    Then pipeline dataset valid is not published

  Scenario: A failed SQL file registers no partial definitions
    Given pipeline SQL file bad.sql fails with PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR
      """
      CREATE MATERIALIZED VIEW output AS SELECT 1 AS value;
      CREATE TABLE ordinary AS SELECT 2 AS value;
      """
    Given pipeline SQL file good.sql
      """
      CREATE MATERIALIZED VIEW output AS SELECT 3 AS value;
      """
    When the pipeline runs
    When query
      """
      SELECT * FROM output
      """
    Then query result
      | value |
      | 3     |
    Then pipeline dataset ordinary is not published

  Scenario: Dataset location is rejected during registration
    Given pipeline SQL file location.sql fails with location.sql
      """
      CREATE MATERIALIZED VIEW output LOCATION '/tmp/unused-pipeline-location' AS SELECT 1 AS value;
      """
    Then pipeline dataset output is not published

  Scenario: Nonidentity partitioning is rejected during registration
    Given pipeline SQL file partition.sql fails with Only identity partition columns
      """
      CREATE MATERIALIZED VIEW output PARTITIONED BY (year(id)) AS SELECT id FROM range(2);
      """
    Then pipeline dataset output is not published

  Scenario: Schema mismatches are found before writes
    Given pipeline SQL file schema.sql
      """
      CREATE MATERIALIZED VIEW valid AS SELECT 1 AS value;
      CREATE MATERIALIZED VIEW invalid (id STRING) AS SELECT * FROM range(2);
      """
    When the pipeline fails with USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE
    Then pipeline dataset valid is not published

  Scenario: A declared schema is validated without materialization
    Given pipeline SQL file schema.sql
      """
      CREATE MATERIALIZED VIEW output (id BIGINT COMMENT 'identifier') AS SELECT * FROM range(5);
      """
    When the pipeline is validated
    Then pipeline dataset output is not published

  Scenario: Streaming table definitions require a producer
    Given pipeline SQL file empty.sql
      """
      CREATE STREAMING TABLE output;
      """
    When the pipeline fails with PIPELINE_DATASET_WITHOUT_FLOW
    Then pipeline dataset output is not published

  Scenario: SQL streaming reads cannot back materialized views
    Given pipeline SQL file streaming.sql
      """
      CREATE MATERIALIZED VIEW output AS SELECT * FROM STREAM(source);
      """
    When the pipeline fails with STREAMING_RELATION_FOR_MATERIALIZED_VIEW
    Then pipeline dataset output is not published
