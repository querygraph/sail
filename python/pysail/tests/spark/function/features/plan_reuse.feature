Feature: Plan reuse over slot views

  A slot view holds its rows in the session and is read afresh each time a
  plan scans it; with the plan cache on, a repeated query reuses its physical
  plan and still sees the slot's current rows.

  Rule: A cached plan reads a slot's current rows

    Scenario: a repeated query after its slot views are replaced
      Given config spark.sail.slotViews = slot_input, slot_factor
      Given config spark.sail.planCache = true
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW slot_input AS
        SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, v)
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW slot_factor AS SELECT 10 AS k
        """
      Given statement
        """
        SELECT i.id, i.id * f.k AS p FROM slot_input i CROSS JOIN slot_factor f ORDER BY i.id
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW slot_input AS
        SELECT * FROM VALUES (3, 'c'), (4, 'd'), (5, 'e') AS t(id, v)
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW slot_factor AS SELECT 7 AS k
        """
      When query
        """
        SELECT i.id, i.id * f.k AS p FROM slot_input i CROSS JOIN slot_factor f ORDER BY i.id
        """
      Then query result ordered
        | id | p  |
        | 3  | 21 |
        | 4  | 28 |
        | 5  | 35 |

    Scenario: a cached hash join builds its table again on each run
      Given config spark.sail.slotViews = slot_rows
      Given config spark.sail.planCache = true
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW slot_rows AS
        SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, v)
        """
      Given statement
        """
        SELECT a.id, b.v FROM slot_rows a JOIN slot_rows b ON a.id = b.id
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW slot_rows AS
        SELECT * FROM VALUES (9, 'z') AS t(id, v)
        """
      When query
        """
        SELECT a.id, b.v FROM slot_rows a JOIN slot_rows b ON a.id = b.id
        """
      Then query result
        | id | v |
        | 9  | z |

    Scenario: a slot whose schema changes is a new slot
      Given config spark.sail.slotViews = slot_shape
      Given config spark.sail.planCache = true
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW slot_shape AS SELECT 1 AS id
        """
      Given statement
        """
        SELECT * FROM slot_shape
        """
      Given statement
        """
        CREATE OR REPLACE TEMPORARY VIEW slot_shape AS SELECT 2 AS id, 'two' AS name
        """
      When query
        """
        SELECT * FROM slot_shape
        """
      Then query result
        | id | name |
        | 2  | two  |
