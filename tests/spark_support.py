"""Probe for a Spark runtime that can round-trip rows back into Python.

PySpark 4.0 supports Python 3.9-3.13 and Java 17/21. On an interpreter or JVM
outside that range the JVM side still runs but the Python worker dies, so
anything that collects rows fails. Tests that need real DataFrames use
``requires_spark`` and skip with that reason instead of failing the suite.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap

import pytest

_PROBE = textwrap.dedent(
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("probe")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    rows = spark.createDataFrame([(1, "a")], "i int, s string").collect()
    spark.stop()
    assert rows[0].s == "a"
    print("SPARK_OK")
    """
)


def _probe() -> tuple[bool, str]:
    env = dict(os.environ)
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    env.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    try:
        result = subprocess.run(
            [sys.executable, "-c", _PROBE],
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, f"Spark probe could not run: {exc}"
    if "SPARK_OK" in result.stdout:
        return True, ""
    return False, (
        "Spark cannot return rows to Python here "
        f"(python {sys.version_info.major}.{sys.version_info.minor}); "
        "pyspark 4.0 needs Python 3.9-3.13 and Java 17/21"
    )


try:
    import pyspark  # noqa: F401
except ImportError:  # pragma: no cover - exercised only without pyspark
    SPARK_AVAILABLE, SPARK_SKIP_REASON = False, "pyspark is not installed"
else:
    SPARK_AVAILABLE, SPARK_SKIP_REASON = _probe()


requires_spark = pytest.mark.skipif(not SPARK_AVAILABLE, reason=SPARK_SKIP_REASON)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("marketplace-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        # One session serves the whole suite; the default 1g driver heap runs out
        # once the pipeline tests hold every stage's frames (Phase 10 star).
        .config("spark.driver.memory", "2g")
        # SparkSessionFactory pins the session to UTC; a test session on local time
        # would bucket observed_date differently from production.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _release_cached_frames(request):
    """Drop what a test cached once it ends: the session serves the whole suite, and the
    in-memory lake caches every frame a stage writes, so the heap would otherwise fill up.
    """
    yield
    if "spark" in request.fixturenames:
        request.getfixturevalue("spark").catalog.clearCache()
