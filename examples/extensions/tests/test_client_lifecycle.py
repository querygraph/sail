"""Pin the test harness's workaround for PySpark's shared release-pool lifetime."""
import gc
import weakref

from pyspark.sql.connect.client.reattach import ExecutePlanResponseReattachableIterator
from pyspark.sql.connect.session import SparkSession


def test_stopped_session_gc_preserves_active_release_pool(endpoint, retained_spark_sessions):
    stale = SparkSession.builder.remote(endpoint).create()
    assert stale.sql("SELECT 1 AS value").collect()[0].value == 1
    stale.stop()
    assert stale.is_stopped
    # A prior test can leave a session in a reference cycle. Without retention,
    # cyclic GC invokes __del__, closing a newer session's global release pool.
    stale._lifecycle_test_cycle = stale
    reference = weakref.ref(stale)
    del stale

    pool = ExecutePlanResponseReattachableIterator._get_or_create_release_thread_pool()
    gc.collect()
    assert reference() is not None
    assert any(session is reference() for session in retained_spark_sessions)
    assert pool.submit(lambda: "release work accepted").result(timeout=5) == "release work accepted"
