"""Cancellation and ownership of materialized graph results."""

import threading
import uuid


class GraphCancelledError(RuntimeError):
    """The algorithm was cancelled before producing a retained result."""


class CancellationToken:
    """Request interruption and stop at the next cooperative action boundary.

    Call cancel() from another thread. A token belongs to at most one active
    algorithm. Spark Connect query tags scope interruption to that algorithm.
    InterruptTag covers registered operations; a race between a local check
    and server operation registration is best effort, not atomic cancellation.
    """

    def __init__(self):
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._spark = None
        self._tag = "graph-" + uuid.uuid4().hex

    def check(self):
        if self._event.is_set():
            raise GraphCancelledError("graph algorithm cancelled")

    @property
    def cancelled(self):
        return self._event.is_set()

    def cancel(self):
        self._event.set()
        with self._lock:
            spark = self._spark
        if spark is not None:
            spark.interruptTag(self._tag)

    def attach(self, spark):
        self.check()
        with self._lock:
            if self._spark is not None:
                raise ValueError("a cancellation token can control only one active algorithm")
            self._spark = spark
        try:
            spark.addTag(self._tag)
            self.check()
        except BaseException:
            with self._lock:
                self._spark = None
            spark.removeTag(self._tag)
            raise

    def detach(self):
        with self._lock:
            spark, self._spark = self._spark, None
        if spark is not None:
            spark.removeTag(self._tag)


class GraphResult:
    """A DataFrame plus its server-owned storage lease.

    Use as a context manager. close() removes the run and invalidates the frame.
    touch() keeps its session active. write_parquet() produces an independently owned
    export; cleanup of that caller-selected path is the caller's responsibility.
    """

    def __init__(self, run, frame, *, algorithm, iterations, converged):
        self._run = run
        self._frame = frame
        self.algorithm = algorithm
        self.iterations = iterations
        self.converged = converged

    @property
    def frame(self):
        if self._run.closed:
            raise RuntimeError("graph result is closed")
        return self._frame

    @property
    def path(self):
        return self._run.result_path

    def touch(self):
        self._run.touch()

    def close(self):
        self._run.close()

    def write_parquet(self, path, *, mode="error"):
        self.touch()
        self.frame.write.mode(mode).parquet(path)

    def __enter__(self):
        self.touch()
        return self

    def __exit__(self, *_):
        self.close()
