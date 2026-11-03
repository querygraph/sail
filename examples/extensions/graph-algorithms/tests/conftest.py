import os
import functools
import gc

os.environ.setdefault("SPARK_CONNECT_MODE_ENABLED", "1")

import pytest


@pytest.fixture(scope="session", autouse=True)
def retained_spark_sessions():
    """Avoid PySpark 4.0.1 stale finalizers closing the global release pool."""
    from pyspark.sql.connect.session import SparkSession
    sessions = []
    original = SparkSession.__init__

    @functools.wraps(original)
    def initialize(session, *args, **kwargs):
        original(session, *args, **kwargs)
        sessions.append(session)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(SparkSession, "__init__", initialize)
        yield
    sessions.clear()
    gc.collect()


@pytest.fixture(scope="session")
def spark():
    endpoint = os.environ.get("SAIL_GRAPH_TEST_REMOTE")
    if not endpoint:
        pytest.skip("set SAIL_GRAPH_TEST_REMOTE to test an engine with graph utils")
    from pyspark.sql.connect.client.retries import DefaultPolicy
    from pyspark.sql.connect.session import SparkSession

    session = SparkSession.builder.remote(endpoint).create()
    session.client.set_retry_policies([
        DefaultPolicy(max_retries=1, initial_backoff=100, max_backoff=100, jitter=0)
    ])
    yield session
    session.stop()
