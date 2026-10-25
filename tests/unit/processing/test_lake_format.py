"""SparkLakeIO writes and reads the table format it was given (Parquet by default, Delta for Databricks)."""
from __future__ import annotations

import pytest

from ecomdata.processing.batch.lake import SparkLakeIO


class Writer:
    def __init__(self, log):
        self.log = log

    def mode(self, value):
        self.log.append(("mode", value))
        return self

    def option(self, key, value):
        self.log.append(("option", key, value))
        return self

    def partitionBy(self, *columns):
        self.log.append(("partitionBy", columns))
        return self

    def format(self, value):
        self.log.append(("format", value))
        return self

    def save(self, uri):
        self.log.append(("save", uri))


class Frame:
    def __init__(self, log):
        self.write = Writer(log)


class Reader:
    def __init__(self, log):
        self.log = log

    def schema(self, schema):
        return self

    def format(self, value):
        self.log.append(("read-format", value))
        return self

    def load(self, uri):
        self.log.append(("load", uri))
        return "frame"


class Spark:
    def __init__(self, log):
        self.read = Reader(log)


def lake(log, **kwargs):
    return SparkLakeIO(lambda: Spark(log), records_uri="s3a://b/records", silver_root_uri="s3a://s/marketplace", **kwargs)


@pytest.mark.parametrize("table_format", ["parquet", "delta"])
def test_silver_is_written_and_read_in_the_configured_format(table_format):
    log = []
    io = lake(log, table_format=table_format)

    uri = io.write_silver("offer_observations_v2", Frame(log), partition_by=["observed_date"])
    io.read_silver("offer_observations_v2")

    assert uri == "s3a://s/marketplace/offer_observations_v2"
    assert ("format", table_format) in log and ("save", uri) in log
    assert ("read-format", table_format) in log and ("load", uri) in log


def test_parquet_is_the_default():
    assert lake([]).table_format == "parquet"


def test_an_unknown_format_is_refused():
    with pytest.raises(ValueError, match="parquet or delta"):
        lake([], table_format="orc")
