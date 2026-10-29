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

  Scenario: Publication validation precedes dependency-ordered writes
    Given pipeline SQL file invalid_format.sql
      """
      CREATE MATERIALIZED VIEW a_valid AS SELECT * FROM range(2);
      CREATE MATERIALIZED VIEW z_invalid USING missing_format AS SELECT * FROM a_valid;
      """
    When the pipeline fails with No data source found for: missing_format
    Then pipeline dataset a_valid is not published
    Then pipeline dataset z_invalid is not published

  Scenario: Publication validation is part of a dry run
    Given pipeline SQL file invalid_format.sql
      """
      CREATE MATERIALIZED VIEW a_valid AS SELECT * FROM range(2);
      CREATE MATERIALIZED VIEW z_invalid USING missing_format AS SELECT * FROM a_valid;
      """
    When pipeline validation fails with No data source found for: missing_format
    Then pipeline dataset a_valid is not published
    Then pipeline dataset z_invalid is not published

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

  Scenario: Materialized view comments replace existing table comments
    Given statement
      """
      CREATE TABLE output (id BIGINT) COMMENT 'old comment'
      """
    Given pipeline SQL file comment.sql
      """
      CREATE MATERIALIZED VIEW output COMMENT 'daily summary' AS SELECT * FROM range(2);
      """
    When the pipeline runs
    When the pipeline runs
    When query
      """
      SELECT count(*) AS rows FROM output
      """
    Then query result
      | rows |
      | 2    |
    When query
      """
      DESCRIBE EXTENDED output
      """
    Then query result row where "col_name" is "Comment" has "data_type" equal to "daily summary"

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

  Scenario: USE applies to one file and to external reads in its flows
    Given statement
      """
      CREATE DATABASE other
      """
    Given statement
      """
      CREATE TABLE other.input AS SELECT 7 AS value
      """
    Given pipeline SQL file other.sql
      """
      USE DATABASE other;
      CREATE MATERIALIZED VIEW output AS SELECT * FROM input;
      CREATE TEMPORARY VIEW intermediate AS SELECT * FROM output;
      """
    Given pipeline SQL file default.sql
      """
      CREATE MATERIALIZED VIEW output AS SELECT * FROM intermediate;
      """
    When the pipeline runs
    When query
      """
      SELECT * FROM default.output UNION ALL SELECT * FROM other.output
      """
    Then query result
      | value |
      | 7     |
      | 7     |
    Then pipeline dataset intermediate is not published

  Scenario: SET configuration is captured per flow and reset between files
    Given pipeline SQL file timezone.sql
      """
      SET spark.sql.session.timeZone = America/Los_Angeles;
      CREATE MATERIALIZED VIEW pacific AS
      SELECT hour(TIMESTAMP '2024-01-01 12:00:00+00:00') AS hour;
      SET spark.sql.session.timeZone = UTC;
      CREATE MATERIALIZED VIEW utc AS
      SELECT hour(TIMESTAMP '2024-01-01 12:00:00+00:00') AS hour;
      """
    Given pipeline SQL file default.sql
      """
      CREATE MATERIALIZED VIEW default_timezone AS
      SELECT hour(TIMESTAMP '2024-01-01 12:00:00+00:00') AS hour;
      """
    When the pipeline runs
    When query
      """
      SELECT 'pacific' AS zone, hour FROM pacific
      UNION ALL SELECT 'utc', hour FROM utc
      UNION ALL SELECT 'default', hour FROM default_timezone
      """
    Then query result
      | zone    | hour |
      | pacific | 4    |
      | utc     | 12   |
      | default | 12   |

  Scenario: Private views honor declared column names
    Given pipeline SQL file columns.sql
      """
      CREATE TEMPORARY VIEW intermediate(value) AS SELECT id FROM range(2);
      CREATE MATERIALIZED VIEW output AS SELECT value FROM intermediate;
      """
    When the pipeline runs
    When query
      """
      SELECT * FROM output ORDER BY value
      """
    Then query result ordered
      | value |
      | 0     |
      | 1     |

  Scenario Outline: Standalone flow names must have one component
    Given pipeline SQL file flow.sql fails with MULTIPART_FLOW_NAME_NOT_SUPPORTED
      """
      CREATE STREAMING TABLE output;
      CREATE FLOW <name> AS INSERT INTO output BY NAME SELECT * FROM STREAM(source);
      """
    Then pipeline dataset output is not published

    Examples:
      | name  |
      | a.b   |
      | a.b.c |

  Scenario Outline: Standalone flows require INSERT INTO BY NAME
    Given pipeline SQL file flow.sql fails with Only INSERT INTO by name flows supported
      """
      CREATE STREAMING TABLE output;
      CREATE FLOW f AS <query>;
      """
    Then pipeline dataset output is not published

    Examples:
      | query                                                       |
      | INSERT INTO output SELECT * FROM STREAM(source)              |
      | INSERT OVERWRITE output BY NAME SELECT * FROM STREAM(source) |
      | INSERT INTO output(id) SELECT * FROM STREAM(source)          |

  Scenario: Multiple files cannot register the same output
    Given pipeline SQL file first.sql
      """
      CREATE MATERIALIZED VIEW output AS SELECT 1 AS value;
      """
    Given pipeline SQL file second.sql
      """
      CREATE MATERIALIZED VIEW output AS SELECT 2 AS value;
      """
    When the pipeline fails with PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT
    Then pipeline dataset output is not published
