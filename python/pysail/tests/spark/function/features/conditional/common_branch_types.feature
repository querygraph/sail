Feature: Conditional common branch types

  Scenario Outline: IF and CASE expose the common branch type before introspection under ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT typeof(IF(TRUE, 1, CAST(2 AS BIGINT))) AS if_bigint,
             typeof(CASE WHEN TRUE THEN 1 ELSE CAST(2 AS BIGINT) END) AS case_bigint,
             typeof(IF(TRUE, 1, 2.5)) AS if_decimal,
             typeof(CASE WHEN TRUE THEN 1 ELSE 2.5 END) AS case_decimal
      """
    Then query result
      | if_bigint | case_bigint | if_decimal    | case_decimal  |
      | bigint    | bigint      | decimal(11,1) | decimal(11,1) |

    Examples:
      | ansi  |
      | true  |
      | false |

  Scenario Outline: IF and CASE evaluate coerced branches for nullable conditions under ANSI <ansi>
    Given config spark.sql.ansi.enabled = <ansi>
    When query
      """
      SELECT id, IF(flag, 1, CAST(2 AS BIGINT)) AS if_value,
             CASE WHEN flag THEN 1 ELSE CAST(2 AS BIGINT) END AS case_value,
             CASE WHEN flag THEN 1 WHEN id = 1 THEN CAST(3 AS BIGINT) END AS no_else
      FROM VALUES (0, TRUE), (1, FALSE), (2, CAST(NULL AS BOOLEAN)) AS t(id, flag)
      ORDER BY id
      """
    Then query result ordered
      | id | if_value | case_value | no_else |
      | 0  | 1        | 1          | 1       |
      | 1  | 2        | 2          | 3       |
      | 2  | 2        | 2          | NULL    |

    Examples:
      | ansi  |
      | true  |
      | false |
