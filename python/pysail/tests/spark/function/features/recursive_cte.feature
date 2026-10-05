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

  Rule: The recursive term may refer to the CTE more than once

    Scenario: rows of an iteration read other rows of the same iteration
      When query
        """
        WITH RECURSIVE w AS (
          SELECT 0 AS tic, 'p' AS kind, 0 AS id, 0.0D AS x
          UNION ALL
          SELECT 0, 't', CAST(id AS INT), CAST(id * 10 AS DOUBLE) FROM range(3) r(id)
          UNION ALL
          SELECT t.tic + 1, t.kind, t.id,
                 CASE WHEN t.kind = 't' THEN t.x - 1 ELSE t.x + p.x END
          FROM w t CROSS JOIN (SELECT x FROM w WHERE kind = 't' AND id = 1) p
          WHERE t.tic < 2
        )
        SELECT tic, kind, id, x FROM w WHERE kind = 'p' ORDER BY tic
        """
      Then query result ordered
        | tic | kind | id | x    |
        | 0   | p    | 0  | 0.0  |
        | 1   | p    | 0  | 10.0 |
        | 2   | p    | 0  | 19.0 |

    Scenario: an aggregate of the iteration joined back to it
      When query
        """
        WITH RECURSIVE w AS (
          SELECT 0 AS tic, CAST(id AS INT) AS id, CAST(id AS DOUBLE) AS x FROM range(4) r(id)
          UNION ALL
          SELECT w.tic + 1, w.id, w.x + a.s
          FROM w CROSS JOIN (SELECT sum(x) AS s FROM w) a
          WHERE w.tic < 2
        )
        SELECT tic, sum(x) AS total FROM w GROUP BY tic ORDER BY tic
        """
      Then query result ordered
        | tic | total |
        | 0   | 6.0   |
        | 1   | 30.0  |
        | 2   | 150.0 |

  Rule: CTEs defined inside the recursive term are shared, not inlined

    Scenario: a chain of CTEs over the iteration, each read twice
      When query
        """
        WITH RECURSIVE w AS (
          SELECT 0 AS tic, CAST(1 AS BIGINT) AS x
          UNION ALL
          SELECT * FROM (
            WITH c0 AS (SELECT tic, x FROM w),
            c1 AS (SELECT a.tic, a.x + b.x AS x FROM c0 a JOIN c0 b ON a.tic = b.tic),
            c2 AS (SELECT a.tic, a.x + b.x AS x FROM c1 a JOIN c1 b ON a.tic = b.tic),
            c3 AS (SELECT a.tic, a.x + b.x AS x FROM c2 a JOIN c2 b ON a.tic = b.tic),
            c4 AS (SELECT a.tic, a.x + b.x AS x FROM c3 a JOIN c3 b ON a.tic = b.tic),
            c5 AS (SELECT a.tic, a.x + b.x AS x FROM c4 a JOIN c4 b ON a.tic = b.tic),
            c6 AS (SELECT a.tic, a.x + b.x AS x FROM c5 a JOIN c5 b ON a.tic = b.tic),
            c7 AS (SELECT a.tic, a.x + b.x AS x FROM c6 a JOIN c6 b ON a.tic = b.tic),
            c8 AS (SELECT a.tic, a.x + b.x AS x FROM c7 a JOIN c7 b ON a.tic = b.tic),
            c9 AS (SELECT a.tic, a.x + b.x AS x FROM c8 a JOIN c8 b ON a.tic = b.tic),
            c10 AS (SELECT a.tic, a.x + b.x AS x FROM c9 a JOIN c9 b ON a.tic = b.tic),
            c11 AS (SELECT a.tic, a.x + b.x AS x FROM c10 a JOIN c10 b ON a.tic = b.tic),
            c12 AS (SELECT a.tic, a.x + b.x AS x FROM c11 a JOIN c11 b ON a.tic = b.tic),
            c13 AS (SELECT a.tic, a.x + b.x AS x FROM c12 a JOIN c12 b ON a.tic = b.tic),
            c14 AS (SELECT a.tic, a.x + b.x AS x FROM c13 a JOIN c13 b ON a.tic = b.tic),
            c15 AS (SELECT a.tic, a.x + b.x AS x FROM c14 a JOIN c14 b ON a.tic = b.tic),
            c16 AS (SELECT a.tic, a.x + b.x AS x FROM c15 a JOIN c15 b ON a.tic = b.tic),
            c17 AS (SELECT a.tic, a.x + b.x AS x FROM c16 a JOIN c16 b ON a.tic = b.tic),
            c18 AS (SELECT a.tic, a.x + b.x AS x FROM c17 a JOIN c17 b ON a.tic = b.tic),
            c19 AS (SELECT a.tic, a.x + b.x AS x FROM c18 a JOIN c18 b ON a.tic = b.tic),
            c20 AS (SELECT a.tic, a.x + b.x AS x FROM c19 a JOIN c19 b ON a.tic = b.tic),
            c21 AS (SELECT a.tic, a.x + b.x AS x FROM c20 a JOIN c20 b ON a.tic = b.tic),
            c22 AS (SELECT a.tic, a.x + b.x AS x FROM c21 a JOIN c21 b ON a.tic = b.tic),
            c23 AS (SELECT a.tic, a.x + b.x AS x FROM c22 a JOIN c22 b ON a.tic = b.tic),
            c24 AS (SELECT a.tic, a.x + b.x AS x FROM c23 a JOIN c23 b ON a.tic = b.tic)
            SELECT tic + 1 AS tic, x FROM c24
          ) s
          WHERE s.tic <= 2
        )
        SELECT tic, x FROM w ORDER BY tic
        """
      Then query result ordered
        | tic | x               |
        | 0   | 1               |
        | 1   | 16777216        |
        | 2   | 281474976710656 |

    Scenario: a recursive term reads a CTE defined outside it
      When query
        """
        WITH RECURSIVE a AS (SELECT id % 7 AS k, count(*) AS c FROM range(1000) r(id) GROUP BY id % 7),
        r AS (
          SELECT 0 AS n
          UNION ALL
          SELECT r.n + 1 FROM r JOIN (SELECT count(*) AS c FROM a) x ON r.n < x.c
        )
        SELECT (SELECT max(n) FROM r) AS m, (SELECT sum(c) FROM a) AS total
        """
      Then query result
        | m | total |
        | 7 | 1000  |

    Scenario: an aggregate over a table scan is computed again in every iteration
      Given variable location for temporary directory recursive_cte_max
      Given statement template
        """
        INSERT OVERWRITE DIRECTORY {{ location.sql }} USING parquet
        SELECT CAST(id AS DOUBLE) AS r FROM range(129)
        """
      When query template
        """
        WITH RECURSIVE w AS (
          SELECT 0 AS t, CAST(NULL AS DOUBLE) AS m
          UNION ALL
          SELECT w.t + 1, x.m FROM w CROSS JOIN (SELECT MAX(r) AS m FROM parquet.`{{ location.string }}` WHERE r % 2 = 0) x
          WHERE w.t < 3
        )
        SELECT t, m FROM w ORDER BY t
        """
      Then query result ordered
        | t | m     |
        | 0 | NULL  |
        | 1 | 128.0 |
        | 2 | 128.0 |
        | 3 | 128.0 |
