@spark-4.1
Feature: Persistent pipeline views
  Background:
    Given a declarative pipeline

  Scenario: Persistent views are created in dependency order
    Given pipeline SQL file views.sql
      """
      CREATE VIEW pv3 AS SELECT * FROM pv2;
      CREATE VIEW pv2 AS SELECT * FROM pv1;
      CREATE VIEW pv1 AS SELECT * FROM range(1, 4);
      """
    When the pipeline runs
    When query
      """
      SELECT * FROM pv3 ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |
      | 3  |
    When query
      """
      DESCRIBE EXTENDED pv1
      """
    Then query result row where "col_name" is "Type" has "data_type" equal to "VIEW"

  Scenario: Persistent views read current external table data without a refresh
    Given statement
      """
      CREATE TABLE external_input AS SELECT * FROM range(1, 4)
      """
    Given pipeline SQL file external.sql
      """
      CREATE VIEW output AS SELECT * FROM external_input;
      """
    When the pipeline runs
    Given statement
      """
      INSERT INTO external_input VALUES (4)
      """
    When query
      """
      SELECT * FROM output ORDER BY id
      """
    Then query result ordered
      | id |
      | 1  |
      | 2  |
      | 3  |
      | 4  |

  Scenario: Persistent views retain the SQL file namespace and ignore declared columns
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
      USE SCHEMA other;
      CREATE VIEW output(number COMMENT 'identifier') COMMENT 'persisted comment'
      AS SELECT value FROM input;
      """
    Given pipeline SQL file consumer.sql
      """
      CREATE MATERIALIZED VIEW result AS SELECT * FROM other.output;
      """
    When the pipeline runs
    When query
      """
      SELECT * FROM result
      """
    Then query result
      | value |
      | 7     |
    When query
      """
      DESCRIBE other.output
      """
    Then query result
      | col_name | data_type | comment |
      | value    | int       | NULL    |
    When query
      """
      DESCRIBE EXTENDED other.output
      """
    Then query result row where "col_name" is "Comment" has "data_type" equal to "persisted comment"
    Given a new declarative pipeline
    Given pipeline SQL file other.sql
      """
      USE SCHEMA other;
      CREATE VIEW output(number COMMENT 'identifier') COMMENT 'persisted comment'
      AS SELECT value FROM input;
      """
    Given pipeline SQL file consumer.sql
      """
      CREATE MATERIALIZED VIEW result AS SELECT * FROM other.output;
      """
    When the pipeline runs
    When query
      """
      SELECT * FROM other.output
      """
    Then query result
      | value |
      | 7     |

  Scenario: Persistent views cannot refer to private pipeline views
    Given pipeline SQL file private.sql
      """
      CREATE MATERIALIZED VIEW valid AS SELECT 1 AS value;
      CREATE TEMPORARY VIEW intermediate AS SELECT * FROM range(1, 4);
      CREATE VIEW output AS SELECT * FROM intermediate;
      """
    When the pipeline fails with INVALID_TEMP_OBJ_REFERENCE
    Then pipeline error condition is INVALID_TEMP_OBJ_REFERENCE with SQL state 42K0F
    Then pipeline error parameters
      | parameter   | value                               |
      | obj         | view                                |
      | objName     | `<current_catalog>`.`default`.`output` |
      | tempObj     | temporary view                      |
      | tempObjName | `intermediate`                      |
    Then pipeline dataset output is not published
    Then pipeline dataset valid is not published

  Scenario: Persistent views cannot have streaming definitions
    Given pipeline SQL file streaming.sql
      """
      CREATE MATERIALIZED VIEW source AS SELECT 1 AS id;
      CREATE VIEW output AS SELECT * FROM STREAM(source);
      """
    When the pipeline fails with STREAMING_RELATION_FOR_PERSISTED_VIEW
    Then pipeline dataset output is not published

  Scenario: Persistent views are not published by validation
    Given pipeline SQL file dry.sql
      """
      CREATE VIEW output AS SELECT * FROM range(1, 4);
      """
    When the pipeline is validated
    Then pipeline dataset output is not published

  Scenario: Persistent views are published outside a table refresh selection
    Given pipeline SQL file selection.sql
      """
      CREATE MATERIALIZED VIEW chosen AS SELECT 1 AS value;
      CREATE VIEW output AS SELECT 2 AS value;
      """
    When the pipeline refreshes chosen
    When query
      """
      SELECT * FROM output
      """
    Then query result
      | value |
      | 2     |
    When query
      """
      SELECT * FROM chosen
      """
    Then query result
      | value |
      | 1     |

  Scenario: Persistent views are published before a table write fails
    Given pipeline SQL file failure.sql
      """
      CREATE MATERIALIZED VIEW raw(id BIGINT, missing STRING) AS SELECT * FROM range(2);
      CREATE VIEW output AS SELECT * FROM raw;
      """
    When the pipeline fails with (input schema for INSERT has 1 fields|column number of the existing table)
    When query
      """
      SELECT count(*) AS rows FROM output
      """
    Then query result
      | rows |
      | 0    |

  Scenario: Selected tables read fresh data through an unselected persistent view
    Given pipeline SQL file chain.sql
      """
      CREATE MATERIALIZED VIEW raw AS SELECT * FROM range(1, 4);
      CREATE VIEW output AS SELECT * FROM raw;
      CREATE MATERIALIZED VIEW chosen AS SELECT sum(id) AS value FROM output;
      """
    When the pipeline refreshes raw, chosen
    When query
      """
      SELECT * FROM chosen
      """
    Then query result
      | value |
      | 6     |

  Scenario: Persistent view metadata does not require an excluded upstream table
    Given pipeline SQL file excluded.sql
      """
      CREATE MATERIALIZED VIEW raw AS SELECT * FROM range(2);
      CREATE VIEW output AS SELECT * FROM raw;
      CREATE MATERIALIZED VIEW chosen AS SELECT 1 AS value;
      """
    When the pipeline refreshes chosen
    Then pipeline dataset output is published
    Then pipeline dataset raw is not published
    When query
      """
      SELECT * FROM chosen
      """
    Then query result
      | value |
      | 1     |

  Scenario: Refreshing a persistent view does not read its excluded upstream table
    Given pipeline SQL file view_only.sql
      """
      CREATE MATERIALIZED VIEW raw AS SELECT * FROM range(2);
      CREATE VIEW output AS SELECT * FROM raw;
      """
    When the pipeline refreshes output
    Then pipeline dataset output is published
    Then pipeline dataset raw is not published
