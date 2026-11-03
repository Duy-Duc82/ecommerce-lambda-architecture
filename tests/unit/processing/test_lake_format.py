"""SparkLakeIO writes and reads the table format it was given (Delta by default, Parquet on request)."""
from __future__ import annotations

import pytest

from ecomdata.processing.batch.lake import SilverLayoutMismatch, SparkLakeIO


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
        self.log.append(("schema", schema))
        return self

    def format(self, value):
        self.log.append(("read-format", value))
        return self

    def load(self, uri):
        self.log.append(("load", uri))
        return "frame"


class Detail:
    def __init__(self, partitions):
        self.partitions = partitions

    def first(self):
        return {"partitionColumns": self.partitions}


class Spark:
    """``partitions``: the partition columns of the Delta table already there, None when there is none."""

    def __init__(self, log, partitions=None):
        self.read = Reader(log)
        self.partitions = partitions

    def sql(self, statement):
        from pyspark.errors import AnalysisException

        assert statement.startswith("DESCRIBE DETAIL delta.")
        if self.partitions is None:
            raise AnalysisException(message="DELTA_PATH_DOES_NOT_EXIST")
        return Detail(self.partitions)


def lake(log, partitions=None, **kwargs):
    spark = Spark(log, partitions)
    return SparkLakeIO(lambda: spark, records_uri="s3a://b/records", silver_root_uri="s3a://s/marketplace", **kwargs)


@pytest.mark.parametrize("table_format", ["parquet", "delta"])
def test_silver_is_written_and_read_in_the_configured_format(table_format):
    log = []
    io = lake(log, table_format=table_format)

    uri = io.write_silver("offer_observations_v2", Frame(log), partition_by=["observed_date"])
    io.read_silver("offer_observations_v2")

    assert uri == "s3a://s/marketplace/offer_observations_v2"
    assert ("format", table_format) in log and ("save", uri) in log
    assert ("read-format", table_format) in log and ("load", uri) in log


def test_delta_is_the_default():
    assert lake([]).table_format == "delta"


def test_the_star_refuses_parquet():
    from ecomdata.domain.datasets import dataset

    class Context:
        def dw_dataset_uri(self, name):
            return f"s3a://g/dw/{name}"

    class Selectable:
        def select(self, *columns):
            return self

    assert dataset("dim_marketplace")
    with pytest.raises(RuntimeError, match="DATA_LAKE_TABLE_FORMAT=delta"):
        lake([], table_format="parquet").merge_dw(Context(), "dim_marketplace", Selectable())


def test_a_window_rewrites_only_its_partitions_and_a_rebuild_replaces_the_table():
    log = []
    io = lake(log, table_format="delta")

    io.write_silver("page_context", Frame(log), partition_by=["source_code", "observed_date"])
    assert ("option", "partitionOverwriteMode", "dynamic") in log
    log.clear()
    io.write_silver("page_context", Frame(log), partition_by=["source_code", "observed_date"], whole=True)
    assert ("option", "partitionOverwriteMode", "static") in log
    assert ("option", "overwriteSchema", "true") in log


def test_an_unknown_format_is_refused():
    with pytest.raises(ValueError, match="parquet or delta"):
        lake([], table_format="orc")


@pytest.mark.parametrize(("table_format", "schema_given"), [("parquet", True), ("delta", False)])
def test_only_parquet_reads_carry_the_declared_schema(table_format, schema_given):
    # A Delta table carries its own schema and refuses a user-specified one.
    log = []
    io = lake(log, table_format=table_format, silver_schemas={"offer_observations_v2": "declared"})

    io.read_silver("offer_observations_v2")

    assert (("schema", "declared") in log) is schema_given


def test_a_partial_write_refuses_a_table_partitioned_otherwise():
    log = []
    io = lake(log, partitions=["source_code"])

    with pytest.raises(SilverLayoutMismatch, match="marketplace_reprocess"):
        io.write_silver("quarantine", Frame(log), partition_by=["source_code", "observed_date"])
    assert not any(entry[0] == "save" for entry in log)


def test_a_rebuild_may_change_the_partitioning():
    log = []
    io = lake(log, partitions=["source_code"])

    io.write_silver("quarantine", Frame(log), partition_by=["source_code", "observed_date"], whole=True)

    assert ("option", "overwriteSchema", "true") in log


def test_a_partial_write_to_a_matching_or_new_table_goes_ahead():
    for partitions in (["source_code", "observed_date"], None):
        log = []
        lake(log, partitions=partitions).write_silver(
            "quarantine", Frame(log), partition_by=["source_code", "observed_date"]
        )
        assert ("save", "s3a://s/marketplace/quarantine") in log
