# SQL pipeline files

A `.sql` file in a project defines datasets and flows with SQL statements. The file is parsed whole, then its statements are registered in order. Registration never runs a query: it records what each dataset is and which query fills it, and the run does the rest.

## Datasets

```sql
CREATE MATERIALIZED VIEW daily_revenue
COMMENT 'Revenue and order count per day'
AS SELECT day, round(sum(amount), 2) AS revenue, count(*) AS orders
FROM orders
GROUP BY day;

CREATE STREAMING TABLE orders
COMMENT 'Orders, read incrementally from demo.orders_raw'
AS SELECT * FROM STREAM demo.orders_raw;

CREATE TEMPORARY VIEW large_orders
AS SELECT * FROM orders WHERE amount >= 100;

CREATE VIEW revenue_report
COMMENT 'Revenue per day, for reporting'
AS SELECT day, revenue FROM daily_revenue;
```

| Statement | What it defines |
|---|---|
| `CREATE MATERIALIZED VIEW name AS query` | A materialized view: a table recomputed by every run. |
| `CREATE STREAMING TABLE name AS query` | A streaming table whose query reads at least one `STREAM` source. |
| `CREATE STREAMING TABLE name;` | A streaming table with no query of its own, filled by flows. |
| `CREATE TEMPORARY VIEW name AS query` | A private view other datasets read; never published. |
| `CREATE VIEW name AS query` | A persisted view, published to the catalog with its SQL text. |

A materialized view and a streaming table accept, between the name and `AS`:

- `COMMENT 'text'`, published as the table's comment;
- `PARTITIONED BY (col, ...)`, identity partitioning on columns of the query;
- `TBLPROPERTIES ('key' = 'value', ...)`;
- a declared column list, `(id INT COMMENT 'the id', value STRING)`, published in that order with those comments. The query's output must match it.

Two things are refused at registration: a `LOCATION`, and a partitioning transform other than a column (`PARTITIONED BY (year(x))` is an "Invalid partitioning transform").

## Reading a stream

A streaming table's query reads its sources with `STREAM`, in either spelling:

```sql
CREATE STREAMING TABLE st AS SELECT * FROM STREAM(src);
CREATE STREAMING TABLE st AS SELECT * FROM STREAM src;
```

`STREAM` marks the read as incremental: each run reads what was added to `src` since the last run. The source may be a table in the catalog or another dataset of the pipeline, including a temporary view over a streaming read. A materialized view may not read a `STREAM` (`SQL streaming reads cannot back materialized views`), and a persisted view may not either.

## Flows

A flow adds rows to a streaming table from a query of its own. Its statement is `CREATE FLOW` with an `INSERT INTO ... BY NAME`:

```sql
CREATE STREAMING TABLE all_events;

CREATE FLOW from_orders AS INSERT INTO all_events BY NAME
SELECT customer_id AS user, 'order' AS kind FROM STREAM orders;

CREATE FLOW from_clicks COMMENT 'click events' AS INSERT INTO all_events BY NAME
SELECT user, 'click' AS kind FROM STREAM clicks;
```

- `BY NAME` is required: a flow matches the target's columns by name, not position.
- A flow has a one-part name. `CREATE FLOW a.f` is refused (`MULTIPART_FLOW_NAME_NOT_SUPPORTED`).
- The target must be a streaming table defined in the pipeline (`FLOW_TARGET_NOT_DEFINED` otherwise), and a flow may not share a name with a table (`FLOW_NAME_CONFLICTS_WITH_TABLE`).
- Only `INSERT INTO` is a flow. `CREATE FLOW f AS SELECT 1`, `INSERT OVERWRITE`, `INSERT ... IF NOT EXISTS`, `CREATE TABLE AS` and `EXPLAIN` under a flow are refused.
- A `WITH` clause goes inside the `INSERT`: `INSERT INTO t BY NAME WITH s AS (...) SELECT * FROM s`.

The `AUTO CDC` form of a flow is described in the AutoCDC chapter.

## Directives

Three statements change how the names in the rest of the file are resolved. They apply to the file they are in and are reset at the next file.

```sql
USE CATALOG sail;
USE SCHEMA demo;
SET CATALOG sail;
```

- `USE SCHEMA name` sets the database that unqualified names belong to.
- `USE CATALOG name` and `SET CATALOG name` set the catalog. `SET CATALOG` takes an expression, as in Spark; a catalog that does not exist is `CATALOG_NOT_FOUND`.
- A bare `USE name` is not a pipeline directive and is refused (`UNSUPPORTED_PIPELINE_SPARK_SQL_COMMAND`).

`SET key = value` sets a Spark SQL configuration for the flows defined after it in the same file. The value is captured per flow; it is not a session setting.

```sql
SET spark.sql.pipelines.maxFlowRetryAttempts = 0;
CREATE STREAMING TABLE st AS SELECT * FROM STREAM src;
```

## Order and references

Statements may refer to datasets defined later in the same file or in another file. The graph is resolved as a whole, so this is fine:

```sql
CREATE MATERIALIZED VIEW b AS SELECT * FROM a;
CREATE MATERIALIZED VIEW a AS SELECT * FROM range(1, 4);
```

A persisted view may read tables and other persisted views, but not a temporary view of the pipeline (`INVALID_TEMP_OBJ_REFERENCE`), since the catalog could not resolve the view later. A persisted view's columns must have names: `CREATE VIEW v AS SELECT 1 + 1` is refused (`CREATE_PERMANENT_VIEW_WITHOUT_ALIAS`); write `SELECT 1 + 1 AS two`.

A dataset defined in two files is an error for both (`PIPELINE_DUPLICATE_IDENTIFIERS.OUTPUT`).

## A file registers all or nothing

If any statement of a file fails to register, none of the file's definitions are kept, and the run fails before anything is written:

```text
[PIPELINE_SQL_GRAPH_ELEMENT_REGISTRATION_ERROR] Failed to register ... in files.sql
```

The error names the file and the statement. This is stricter than Spark, which keeps the definitions registered before the failing statement; since the client aborts the run in either case, the difference is only what is left registered on the server.

## Comments and separators

Statements are separated by `;`. SQL comments (`--` and `/* */`) are allowed anywhere, and a `;` inside a string literal is not a separator. The text of a persisted view is taken from the file as written.
