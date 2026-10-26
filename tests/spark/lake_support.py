"""An in-memory lake for the batch stages.

The host Spark has no filesystem, so Silver and Gold are DataFrames held in
memory: the stage logic and its Spark transforms run for real, the Parquet and
S3A I/O is exercised in the Docker stack.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from ecomdata.domain.records import record_json
from ecomdata.processing.normalize import common


class MemoryLake:
    """The LakeIO port over DataFrames held in memory; records are decoded from the lines handed in."""

    def __init__(self, spark, lines):
        self.spark = spark
        self.lines = list(lines)
        self.written: dict[str, object] = {}
        self.partitions: dict[str, tuple[str, ...]] = {}
        self.gold: dict[tuple[str, str], object] = {}
        # The star: one frame per table, kept across runs, and how many merges each took.
        self.dw: dict[tuple[str, str], object] = {}
        self.dw_versions: dict[tuple[str, str], int] = {}

    def read_records(self, *, since_date=None):
        frame = common.decode_records(self.spark.createDataFrame([(line,) for line in self.lines], "value string"))
        if since_date is not None:
            frame = frame.filter(F.to_date("fetched_at") >= F.lit(since_date))
        return frame

    def write_silver(self, name, frame, *, partition_by=()):
        self.written[name] = frame.cache()
        self.written[name].count()
        self.partitions[name] = tuple(partition_by)
        return f"memory://silver/{name}"

    def read_silver(self, name):
        return self.written[name]

    def write_gold(self, context, name, frame):
        from ecomdata.processing.batch.context import GoldWriteResult

        cached = frame.cache()
        self.gold[(context.run_id, name)] = cached
        return GoldWriteResult(name, context.gold_dataset_uri(name), cached.count())

    def read_gold(self, context, name):
        return self.gold[(context.run_id, name)]

    def merge_dw(self, context, name, frame):
        from ecomdata.domain.datasets import dataset
        from ecomdata.processing.batch.context import DwWriteResult

        key = (context.dw_root_uri, name)
        cached = frame.select(*dataset(name).columns).cache()
        self.dw[key] = cached
        # Like a Delta table, every merge is a new version, numbered from 0.
        self.dw_versions[key] = self.dw_versions.get(key, -1) + 1
        return DwWriteResult(name, context.dw_dataset_uri(name), cached.count(), self.dw_versions[key])

    def read_dw(self, context, name):
        return self.dw[(context.dw_root_uri, name)]


def lines_of(records):
    return [record_json(r) for r in records]
