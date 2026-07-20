import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from speed_layer.speed_layer import windowed_metrics  # noqa: E402

CSV = """event_time,event_type,user_id,price
2019-10-01 10:00:05,view,u1,
2019-10-01 10:00:20,view,u2,
2019-10-01 10:00:30,purchase,u1,100.0
2019-10-01 10:00:45,purchase,u2,50.0
"""


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("SpeedLayerTest")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_windowed_metrics_aggregates_by_type(spark, tmp_path):
    path = tmp_path / "events.csv"
    path.write_text(CSV, encoding="utf-8")
    events = (
        spark.read.option("header", True).csv(str(path))
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("price", F.col("price").cast("double"))
    )
    result = {r["event_type"]: r for r in windowed_metrics(events).collect()}

    assert result["view"]["event_count"] == 2
    assert result["purchase"]["event_count"] == 2
    assert result["purchase"]["amount"] == 150.0
