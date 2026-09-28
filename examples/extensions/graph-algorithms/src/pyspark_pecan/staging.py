"""Parquet generations with explicit capability-based cleanup."""

from .lifecycle import GraphResult


LAYOUTS = ("shuffle", "declared")


class StagingRun:
    """One algorithm's Parquet generations.

    `layout="shuffle"` writes each stage `repartition(partitions)` and reads
    it back as an ordinary Parquet scan, so every later join or aggregate on
    it shuffles again. `layout="declared"` writes a stage that names a `key`
    bucketed and sorted by that key, one file per bucket, and reads it back
    through the Nutmeg extension's `checkpointed` relation, whose scan
    declares the bucketing and the order; joins and aggregates on `key`
    between such stages then need no shuffle and no sort. Stages that name
    no key are written and read the shuffle way under either layout.
    """

    def __init__(self, spark, utils, cancellation, partitions, *, layout="shuffle", nutmeg=None):
        if layout not in LAYOUTS:
            raise ValueError(f"layout must be one of {LAYOUTS}")
        if layout == "declared" and nutmeg is None:
            raise ValueError("the declared layout needs the Nutmeg client for checkpointed scans")
        self.spark = spark
        self.utils = utils
        self.cancellation = cancellation
        self.partitions = partitions
        self.layout = layout
        self.nutmeg = nutmeg
        self.path, self.token = utils.allocate()
        self.closed = False
        self.write_uncertain = False
        self.result_path = None
        self._stages = {}
        self._serial = 0

    def touch(self):
        if self.closed:
            raise RuntimeError("graph staging run is closed")
        if not self.utils.exists(self.path, self.token):
            raise RuntimeError("graph staging session expired or its files were removed")

    @property
    def declared(self):
        return self.layout == "declared"

    def materialize(self, frame, *, expected_rows=None, key=None):
        """Write `frame` as the next stage and read it back.

        With `key` under the declared layout the stage is bucketed and sorted
        by `key` and read back declared; otherwise it is written and read the
        shuffle way. `key` must be a column of `frame`.
        """
        self.cancellation.check()
        self.touch()
        self.cancellation.check()
        if key is not None and key not in frame.columns:
            raise ValueError(f"stage key {key!r} is not a column of the frame")
        declared = self.declared and key is not None
        path = self.path.rstrip("/") + f"/stage-{self._serial:05d}"
        self._serial += 1
        # Record before writing, so a partially failed stage is still owned.
        self._stages[path] = None
        # A failed/interrupted write RPC is not a distributed writer-drain
        # barrier. Keep ownership for session teardown if its outcome is unknown.
        self.write_uncertain = True
        if declared:
            frame.repartition(self.partitions, key).sortWithinPartitions(key).write.mode("error").parquet(path)
        else:
            frame.repartition(self.partitions).write.mode("error").parquet(path)
        self.write_uncertain = False
        self.cancellation.check()
        stored = self.spark.read.parquet(path)
        if not stored.schema.fields:
            # Some engines create no data files for an empty write. Preserve
            # the known schema when there is no Parquet footer to infer it from.
            stored = self.spark.read.schema(frame.schema).parquet(path)
        elif declared:
            stored = self.nutmeg.checkpointed(path, key, self.partitions)
        if stored.schema != frame.schema:
            # Parquet readers may widen nullability; column names/types are
            # contractual, while nullability is not a portable file guarantee.
            actual = [(field.name, field.dataType) for field in stored.schema]
            expected = [(field.name, field.dataType) for field in frame.schema]
            if actual != expected:
                raise RuntimeError("materialized graph stage changed its schema")
        if expected_rows is not None:
            self.cancellation.check()
            if stored.count() != expected_rows:
                raise RuntimeError("materialized graph stage changed its vertex count")
        # Successful writes and reads are the commit check. Output file count
        # is deliberately not compared with the requested partition count.
        self.cancellation.check()
        self._stages[path] = stored
        return path, stored

    def remove(self, path):
        self.utils.remove(path, self.token)
        self._stages.pop(path, None)

    def finish(self, path, frame, *, algorithm, iterations, converged):
        self.cancellation.check()
        for obsolete in list(self._stages):
            if obsolete != path:
                self.remove(obsolete)
        self.cancellation.check()
        self.result_path = path
        return GraphResult(self, frame, algorithm=algorithm, iterations=iterations, converged=converged)

    def close(self):
        if not self.closed:
            self.utils.remove(self.path, self.token)
            self.closed = True
            self._stages.clear()
