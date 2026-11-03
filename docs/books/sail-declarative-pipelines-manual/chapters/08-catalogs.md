# Catalogs

A pipeline's datasets are tables and views in a catalog. Sail's catalogs are configured on the server; the pipeline names the catalog and database its datasets belong to.

## The memory catalog

By default a Sail server has one catalog, named `sail`, of type `memory`, with a database `default`. It keeps table metadata in memory and table data in the warehouse directory, `spark-warehouse` under the server's working directory unless `spark.sql.warehouse.dir` says otherwise.

A memory catalog belongs to a session: each Spark Connect session starts with its own, empty one. The tables a `spark-pipelines run` creates are visible to the run's session, and their files stay in the warehouse, but another session does not see them in its catalog. For a pipeline whose results are read by other sessions, there are two choices:

- **Share the memory catalog.** With `SAIL_CATALOG__SHARED_MEMORY=true`, every session on the server shares one memory catalog, as sessions of one Spark application share Spark's. Another session's inserts and overwrites are visible at once, without a refresh. The catalog still lives only as long as the server.
- **Use a metastore.** A Hive metastore keeps the catalog across servers and restarts.

`catalog.shared_memory` is off by default because an existing Sail deployment may rely on a fresh catalog per session.

## Configuration

Catalogs are configured with environment variables, or the matching keys in Sail's configuration file:

| Setting | Meaning | Default |
|---|---|---|
| `SAIL_CATALOG__LIST` | The catalogs, as a list of `{name=..., type=..., ...}` entries. | one memory catalog named `sail` |
| `SAIL_CATALOG__DEFAULT_CATALOG` | The catalog a session starts in. | the first of the list |
| `SAIL_CATALOG__DEFAULT_DATABASE` | The database a session starts in. | `["default"]` |
| `SAIL_CATALOG__SHARED_MEMORY` | Share memory catalogs across sessions. | `false` |

The list uses a TOML-like syntax. The default is:

```text
SAIL_CATALOG__LIST='[{name="sail", type="memory", initial_database=["default"], initial_database_comment="default database"}]'
```

A server with two memory catalogs:

```text
SAIL_CATALOG__LIST='[{name="sail", type="memory", initial_database=["default"]}, {name="other", type="memory", initial_database=["default"]}]'
SAIL_CATALOG__DEFAULT_CATALOG=sail
```

A server whose catalog is a Hive metastore:

```text
SAIL_CATALOG__LIST='[{name="sail", type="hms", uris=["127.0.0.1:9083"]}]'
```

The other catalog types, `unity`, `glue` and `iceberg_rest`, are configured with their own fields; AutoCDC's chapter on catalogs covers Unity and Iceberg as targets.

## Naming datasets

A dataset name has up to three parts: `catalog.database.table`. The spec's `catalog` and `database` complete shorter names, and a SQL file's `USE CATALOG`, `SET CATALOG` and `USE SCHEMA` change them for that file. A pipeline may define datasets in several databases and in several catalogs:

```sql
CREATE MATERIALIZED VIEW first.mv1 AS SELECT 1 AS one;
CREATE MATERIALIZED VIEW other.second.mv2 AS SELECT * FROM first.mv1;
```

Names are resolved against the pipeline before the catalog: a dataset the pipeline defines shadows a catalog table of the same name.

A catalog that does not exist is `CATALOG_NOT_FOUND`. A database the pipeline writes to must exist; the run does not create it.

## Hive metastore

With a `hms` catalog, Sail creates the pipeline's tables in the metastore and keeps their data under the metastore's warehouse. Tables Sail creates are external tables marked `external.table.purge`, so `DROP TABLE` and `DROP DATABASE ... CASCADE` delete their data, as Spark's do. A `file:/path` location is read as an absolute path.

Every behavior in this manual was run against a Hive 4.0.1 metastore as well as the memory catalog; the only differences are the metastore's own: it cannot replace a table in place, so a full refresh of an AutoCDC target there drops and recreates it.

## Seeing another session's writes

Sail serves table reads from the files in the table's directory. When sessions share a catalog, a write from one session is visible to a read from another at once. Two caches can delay that:

- `REFRESH TABLE t` is never needed for visibility in the default configuration. It clears every file-listing and statistics cache entry, since those caches are not keyed by table.
- If the optional file-listing cache is enabled (`execution.file_listing_cache.type`), a listing is reused for `execution.file_listing_cache.ttl` seconds, 1800 by default, and a write from another session is seen after that or after `REFRESH TABLE`. `TRUNCATE TABLE` clears its own directory's entry.

## Views and comments

A materialized view and a streaming table are published as tables, with the `COMMENT` and `TBLPROPERTIES` of their definitions and the declared columns in order. A persisted view is published with its SQL text; `SHOW CREATE TABLE` shows it. A temporary view is never published.
