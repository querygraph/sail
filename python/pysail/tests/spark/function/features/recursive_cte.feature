Feature: Recursive CTEs

  Rule: A recursive CTE iterates its recursive term until it returns no rows

    Scenario: counting with UNION ALL
      When query
        """
        WITH RECURSIVE r AS (
          SELECT 1 AS n
          UNION ALL
          SELECT n + 1 FROM r WHERE n < 5
        )
        SELECT n FROM r ORDER BY n
        """
      Then query result ordered
        | n |
        | 1 |
        | 2 |
        | 3 |
        | 4 |
        | 5 |

    Scenario: the column list names the output
      When query
        """
        WITH RECURSIVE r(level, label) AS (
          SELECT 0, 'root'
          UNION ALL
          SELECT level + 1, concat(label, '.child') FROM r WHERE level < 2
        )
        SELECT level, label FROM r ORDER BY level
        """
      Then query result ordered
        | level | label            |
        | 0     | root             |
        | 1     | root.child       |
        | 2     | root.child.child |

    Scenario: many iterations
      When query
        """
        WITH RECURSIVE r AS (
          SELECT 0 AS i, 0L AS total
          UNION ALL
          SELECT i + 1, total + i + 1 FROM r WHERE i < 500
        )
        SELECT max(i) AS i, max(total) AS total, count(*) AS rows FROM r
        """
      Then query result
        | i   | total  | rows |
        | 500 | 125250 | 501  |

    Scenario: several rows per iteration, joined with another relation
      When query
        """
        WITH RECURSIVE r AS (
          SELECT 0 AS step, id, id * 10 AS v FROM VALUES (1), (2), (3) AS t(id)
          UNION ALL
          SELECT r.step + 1, r.id, r.v + d.delta
          FROM r JOIN VALUES (1, 1), (2, 2), (3, 3) AS d(id, delta) ON d.id = r.id
          WHERE r.step < 2
        )
        SELECT step, sum(v) AS total FROM r GROUP BY step ORDER BY step
        """
      Then query result ordered
        | step | total |
        | 0    | 60    |
        | 1    | 66    |
        | 2    | 72    |

    Scenario: a window function in the recursive term sees one iteration
      When query
        """
        WITH RECURSIVE r AS (
          SELECT 0 AS step, id, CAST(id AS DOUBLE) AS x FROM VALUES (1), (2) AS t(id)
          UNION ALL
          SELECT step + 1, id, x + max(x) OVER () FROM r WHERE step < 2
        )
        SELECT step, sum(x) AS total FROM r GROUP BY step ORDER BY step
        """
      Then query result ordered
        | step | total |
        | 0    | 3.0   |
        | 1    | 7.0   |
        | 2    | 15.0  |

  Rule: UNION without ALL stops when an iteration adds no new rows

    Scenario: reachability in a graph with a cycle
      When query
        """
        WITH RECURSIVE reach AS (
          SELECT 1 AS node
          UNION
          SELECT e.dst FROM reach JOIN VALUES (1, 2), (2, 3), (3, 1), (4, 5) AS e(src, dst)
            ON e.src = reach.node
        )
        SELECT node FROM reach ORDER BY node
        """
      Then query result ordered
        | node |
        | 1    |
        | 2    |
        | 3    |

  Rule: Recursive CTEs compose with other CTEs and references

    Scenario: a later CTE reads a recursive one
      When query
        """
        WITH RECURSIVE
          r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3),
          s AS (SELECT n * 10 AS m FROM r)
        SELECT m FROM s ORDER BY m
        """
      Then query result ordered
        | m  |
        | 10 |
        | 20 |
        | 30 |

    Scenario: the recursive CTE is read twice by the main query
      When query
        """
        WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3)
        SELECT a.n AS a, b.n AS b FROM r a JOIN r b ON b.n = a.n + 1 ORDER BY a
        """
      Then query result ordered
        | a | b |
        | 1 | 2 |
        | 2 | 3 |

    Scenario: a CTE in a WITH RECURSIVE clause that does not refer to itself
      When query
        """
        WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT 2)
        SELECT n FROM r ORDER BY n
        """
      Then query result ordered
        | n |
        | 1 |
        | 2 |

    Scenario: values from later iterations may be null
      When query
        """
        WITH RECURSIVE r AS (
          SELECT 1 AS n, 'x' AS s
          UNION ALL
          SELECT n + 1, CAST(NULL AS STRING) FROM r WHERE n < 2
        )
        SELECT n, s FROM r ORDER BY n
        """
      Then query result ordered
        | n | s    |
        | 1 | x    |
        | 2 | NULL |
