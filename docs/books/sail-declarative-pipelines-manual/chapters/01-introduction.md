# Introduction

Sail Declarative Pipelines is Sail's implementation of Spark Declarative Pipelines, the pipeline framework that ships with Apache Spark 4.2.1. You describe datasets and the queries that fill them, in Python or SQL, and the engine works out the order, runs the queries, publishes the results to the catalog, and remembers how far each streaming source has been read. Sail runs this on its own engine, written in Rust on DataFusion, with no JVM.

The client is the one that ships with Spark: the `pyspark.pipelines` Python API and the `spark-pipelines` command-line tool. Neither is patched. They talk to Sail over Spark Connect, the same way they talk to Spark.

## What a pipeline is

A pipeline is a graph. Its nodes are datasets and its edges are flows.

- A **materialized view** holds the result of a batch query. Every run recomputes it.
- A **streaming table** receives rows from its sources incrementally. Every run reads only what arrived since the last run and appends it.
- A **temporary view** is a named query that other datasets in the pipeline can read. It is never published to the catalog.
- A **persisted view** is published to the catalog as a view, with its SQL text.
- A **sink** is an external destination that a streaming flow writes to.
- A **flow** is a query with a target. A dataset defined with a query has one flow of the same name. A streaming table can have several append flows, and an **AutoCDC** flow applies a change feed to it.

A run takes the whole graph, checks it, and then fills the datasets in dependency order. A dry run stops after the check and writes nothing.

## What this manual covers

This manual describes what works at milestone 1 of Sail Declarative Pipelines, which is the complete batch implementation together with streaming tables, AutoCDC, and Apache Iggy as a streaming source. Everything in it has been run. Where Sail and Spark behave differently on purpose, the chapter on differences says so and why.

The chapters are in the order you would meet things:

1. Getting started: build Sail, install the client, run a first pipeline.
2. Projects and specs: the project layout and the spec file.
3. Python datasets: the decorators and functions of `pyspark.pipelines`.
4. SQL pipeline files: the statements and directives of a `.sql` file.
5. Running: the commands, refresh modes, events and errors.
6. Sources and formats: tables, files, Delta, ORC.
7. Catalogs: the memory catalog, shared memory, a Hive metastore, several catalogs.
8. Streaming tables: how incremental reads and checkpoints work.
9. AutoCDC: applying change feeds.
10. Apache Iggy: reading a topic.
11. Operations: configuration, cluster mode, shutdown.
12. Differences from Spark.
13. The demo: a walkthrough of the example that proves all of the above.

Two appendices list the configuration keys and the error conditions you can see.

## Versions

| Component | Version |
|---|---|
| Apache Spark client (`pyspark[connect]`) | 4.2.1 |
| Delta Lake (as a source and a target) | the protocol Sail's Delta support implements |
| Apache Iggy broker | the revision pinned in the Sail workspace (`153fdf466`) |
| Python | 3.10 or later |

Sail itself is built from the `work/spark-declarative-pipelines` branch at the milestone tag.
