-- One valid dataset and one that refers to a column that does not exist.
-- The whole graph is analyzed before anything is written, so neither is created.

CREATE MATERIALIZED VIEW fine AS SELECT order_id FROM demo.orders_raw;

CREATE MATERIALIZED VIEW broken AS SELECT no_such_column FROM demo.orders_raw;
