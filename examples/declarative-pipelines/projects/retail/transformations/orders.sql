-- Batch and streaming datasets defined in SQL.

-- A streaming table: each run reads only the rows added to the source since the last run.
CREATE STREAMING TABLE orders
COMMENT 'Orders, read incrementally from demo.orders_raw'
AS SELECT * FROM STREAM demo.orders_raw;

-- A materialized view over the streaming table, recomputed on each run.
CREATE MATERIALIZED VIEW daily_revenue
COMMENT 'Revenue and order count per day'
AS SELECT day, round(sum(amount), 2) AS revenue, count(*) AS orders
FROM orders
GROUP BY day;

-- A temporary view: part of the graph, never published to the catalog.
CREATE TEMPORARY VIEW large_orders
AS SELECT * FROM orders WHERE amount >= 100;

CREATE MATERIALIZED VIEW large_order_count
AS SELECT count(*) AS n FROM large_orders;

-- A persisted view: published to the catalog with its SQL text.
CREATE VIEW revenue_report
COMMENT 'Revenue per day, for reporting'
AS SELECT day, revenue FROM daily_revenue;
