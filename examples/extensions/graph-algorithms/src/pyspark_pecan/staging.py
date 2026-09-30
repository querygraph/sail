"""Parquet generations with explicit capability-based cleanup."""

from .lifecycle import GraphResult


class StagingRun:
    def __init__(self, spark, utils, cancellation, partitions, *, repartition_checkpoints=True):
        self.spark = spark
        self.utils = utils
        self.cancellation = cancellation
        self.partitions = partitions
        self.repartition_checkpoints = repartition_checkpoints
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

    def materialize(self, frame, *, expected_rows=None):
        self.cancellation.check()
        self.touch()
        self.cancellation.check()
        path = self.path.rstrip("/") + f"/stage-{self._serial:05d}"
        self._serial += 1
        # Record before writing, so a partially failed stage is still owned.
        self._stages[path] = None
        # A failed/interrupted write RPC is not a distributed writer-drain
        # barrier. Keep ownership for session teardown if its outcome is unknown.
        self.write_uncertain = True
        writing = frame.repartition(self.partitions) if self.repartition_checkpoints else frame
        writing.write.mode("error").parquet(path)
        self.write_uncertain = False
        self.cancellation.check()
        stored = self.spark.read.parquet(path)
        if not stored.schema.fields:
            # Some engines create no data files for an empty write. Preserve
            # the known schema when there is no Parquet footer to infer it from.
            stored = self.spark.read.schema(frame.schema).parquet(path)
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
