"""Parquet checkpoint directories.

Port of the writing/reading half of ``graphframes-rs/src/memory/parquet_checkpointer.rs``.
The PySpark Connect API does not expose the object-store list/delete calls the Rust
version uses for ``evict``/``purge``, so checkpoints are never removed here: every
pushed directory stays on disk until the caller wipes the run directory. Keep the
checkpoint dir on a large local disk (see the sem_benchmark harness).
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger("gfrs_poc")


class ParquetCheckpointer:
    """Write DataFrames to parquet under ``base_dir`` and read them straight back.

    Reading back truncates the lazy lineage, exactly like the Rust checkpointer:
    every iteration works on a short "read parquet -> transform" plan instead of a
    growing expression tree.
    """

    def __init__(self, spark: SparkSession, base_dir: str, num_partitions: int):
        self.spark = spark
        self.base_dir = base_dir.rstrip("/")
        self.num_partitions = num_partitions
        self.stored: list[str] = []

    def push(self, postfix: str, df: DataFrame, key: str | None = None) -> DataFrame:
        """Persist ``df`` into ``<base_dir>/<postfix>`` and return the read-back frame.

        With ``key``, the frame is hash-repartitioned into ``num_partitions``
        partitions and sorted by ``key`` within each partition before the write.
        This mirrors the Rust ``push_pre_sorted`` (hash partition + per-partition
        sort on disk); PySpark cannot *declare* the partitioning/sortedness back to
        the optimizer, but the layout still keeps the sorted-by-key file-per-partition
        contract on disk.
        """
        path = f"{self.base_dir}/{postfix}"
        out = df
        if key is not None:
            out = out.repartition(self.num_partitions, F.col(key)).sortWithinPartitions(
                F.col(key)
            )
        out.write.mode("error").parquet(path)
        self.stored.append(path)
        logger.info("checkpoint %s written%s", path, f" (sorted by {key})" if key else "")
        return self._read_back(path, df.schema)

    def _read_back(self, path: str, schema) -> DataFrame:
        # An empty frame writes no data files, so schema inference can fail; fall
        # back to the writer-side schema (same trick as Pecan's staging).
        try:
            stored = self.spark.read.parquet(path)
            if stored.schema.fields:
                return stored
        except Exception as exc:  # noqa: BLE001 - engine-specific empty-write behavior
            logger.debug("plain read of %s failed (%s); reading with the writer schema", path, exc)
        return self.spark.read.schema(schema).parquet(path)
