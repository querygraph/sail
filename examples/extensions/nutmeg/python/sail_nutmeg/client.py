"""Version 1 JSON verbs carried in the proposal's Spark Connect Any envelope."""
import json
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan
from . import TYPE_URL

ENVELOPE_TYPE_URL = "type.googleapis.com/sail.extension.v1.SailExtensionRequest"


def _varint(number):
    result = bytearray()
    while number > 127:
        result.append((number & 127) | 128)
        number >>= 7
    result.append(number)
    return bytes(result)


def _bytes_field(number, value):
    return _varint((number << 3) | 2) + _varint(len(value)) + value


class ExtensionRelation(LogicalPlan):
    def __init__(self, request, inputs=()):
        super().__init__(None)
        self.request = request
        self.inputs = tuple(inputs)

    def plan(self, session):
        relation = self._create_proto_relation()
        payload = json.dumps(self.request, separators=(",", ":"), allow_nan=False).encode()
        if self.inputs:
            envelope = _bytes_field(1, TYPE_URL.encode()) + _bytes_field(2, payload)
            for frame in self.inputs:
                envelope += _bytes_field(3, frame._plan.to_proto(session).SerializeToString())
            envelope += _varint(5 << 3) + _varint(1)
            relation.extension.type_url = ENVELOPE_TYPE_URL
            relation.extension.value = envelope
        else:
            relation.extension.type_url = TYPE_URL
            relation.extension.value = payload
        return relation


class Nutmeg:
    """Use with a Spark Connect session connected to the extension-enabled Sail."""
    def __init__(self, spark):
        self.spark = spark

    def _relation(self, verb, graph, *, inputs=(), **kwargs):
        return DataFrame(ExtensionRelation({"version": 1, "verb": verb, "graph": graph, **kwargs}, inputs), self.spark)

    def stage(self, graph, nodes, edges, *, node_mapping=None, edge_mapping=None):
        """Atomically overwrite one graph; eagerly collect its one-row receipt.

        Both DataFrames must belong to this session. Each receipt reports graph,
        nodeCount, edgeCount and revision. All input partitions are consumed.
        """
        if nodes.sparkSession is not self.spark or edges.sparkSession is not self.spark:
            raise ValueError("stage inputs must belong to this Nutmeg Spark session")
        return self._relation("stage", graph, inputs=(nodes, edges), nodeMapping=node_mapping or {}, edgeMapping=edge_mapping or {}).collect()[0]

    def run(self, graph, algorithm, *, column_names="grust", **options):
        """Return a lazy DataFrame, with a graph snapshot pinned during planning."""
        return self._relation("run", graph, algorithm=algorithm, options=options, columnNames=column_names)

    def tables(self, nodes, edges, *, node_id="node_id", source="source", target="target"):
        """Use ordinary Sail relations for graph queries, without staging or CSR.

        Accepts DataFrames or table names. References remain lazy; they follow
        the underlying Sail tables' consistency rules at each execution.
        """
        from .graph import GraphTables
        nodes = self.spark.table(nodes) if isinstance(nodes, str) else nodes
        edges = self.spark.table(edges) if isinstance(edges, str) else edges
        if nodes.sparkSession is not self.spark or edges.sparkSession is not self.spark:
            raise ValueError("graph tables must belong to this Nutmeg Spark session")
        return GraphTables(nodes, edges, node_id=node_id, source=source, target=target)

    def nodes(self, graph):
        """Scan this session's staged node snapshot as an ordinary DataFrame."""
        return self._relation("nodes", graph)

    def edges(self, graph):
        """Scan this session's staged edge snapshot, preserving duplicate edges."""
        return self._relation("edges", graph)

    def checkpointed(self, path, key, partitions, *, graph="__checkpoint__"):
        """Scan a bucketed, sorted Parquet checkpoint and declare its layout.

        `path` is a directory of exactly `partitions` Parquet files, one per
        bucket of `key`, each sorted by `key`, as `checkpoint` writes them.
        The scan declares Hash(key, partitions) and the key order, so joins
        and aggregates on `key` between such scans need no shuffle and no
        sort. The declaration is trusted: only join checkpoints written by
        the same session with the same `partitions`.
        """
        return self._relation("checkpointed", graph, options={"path": path, "key": key, "partitions": int(partitions)})

    BUCKET_COLUMN = "__bucket"

    def checkpoint(self, frame, path, key, partitions, *, graph="__checkpoint__", mode="distributed"):
        """Write `frame` as a checkpoint bucketed by `key`, then scan it declared.

        mode="distributed": `frame` gains a `__bucket` column from the engine's
        own hash (`nutmeg_bucket`, exported by the `nutmeg-bucket` extension),
        is repartitioned and sorted by it, and is written `partitionBy` that
        column through Sail's ordinary writer, on the workers. One directory
        per bucket; the scan declares the hash partitioning, and the order
        when every bucket is one file. Sail's plain writer cannot bucket by
        itself: it spreads batches over files round-robin.

        mode="driver": the extension executes `frame`, repartitions by the same
        hash, sorts, and writes one file per bucket itself, on the driver. It
        needs no scalar function and is attempted once. The receipt is on
        `last_checkpoint`.

        `path` must be absent or empty. Returns the declared scan.
        """
        if frame.sparkSession is not self.spark:
            raise ValueError("checkpoint input must belong to this Nutmeg Spark session")
        if mode == "distributed":
            from pyspark.sql.connect import functions as F
            bucket = F.expr(f"nutmeg_bucket({key}, {int(partitions)})").alias(self.BUCKET_COLUMN)
            (frame.select("*", bucket)
                  .repartition(int(partitions), self.BUCKET_COLUMN)
                  .sortWithinPartitions(self.BUCKET_COLUMN, key)
                  .write.mode("error").partitionBy(self.BUCKET_COLUMN).parquet(path))
            self.last_checkpoint = None
            # An empty frame leaves no directory behind; a scan cannot declare
            # a layout over nothing, and an empty local relation joins the same.
            try:
                return self.checkpointed(path, key, partitions, graph=graph)
            except Exception as error:  # noqa: BLE001 - the server names the missing directory
                if "cannot list" in str(error) or "is missing" in str(error):
                    return self.spark.createDataFrame([], frame.schema)
                raise
        elif mode == "driver":
            self.last_checkpoint = self._relation("checkpoint", graph, inputs=(frame,),
                                                  options={"path": path, "key": key, "partitions": int(partitions)}).collect()[0]
        else:
            raise ValueError("checkpoint mode must be 'distributed' or 'driver'")
        return self.checkpointed(path, key, partitions, graph=graph)

    def status(self):
        """Read native admission, revision/cache counts and actual kernel states."""
        return json.loads(self._relation("diagnostics", "__session__").collect()[0].status)

    def drop(self, graph):
        """Drop this session's graph and return its removal receipt."""
        return self._relation("drop", graph).collect()[0]
