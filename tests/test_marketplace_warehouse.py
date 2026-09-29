"""Batch context and Gold layout tests, items 34 and 46 of the Phase 6 plan."""
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest

from batch_layer.marketplace_postgres import DATASETS, MarketplaceCachePublisher
from batch_layer.marketplace_warehouse import (
    MarketplaceBatchContext,
    write_run_scoped_gold,
)

AS_OF = datetime(2026, 9, 5, 10, tzinfo=timezone.utc)


class FakeWriter:
    def __init__(self, log, name):
        self.log = log
        self.name = name
        self.partitions = None

    def mode(self, mode):
        self.mode_value = mode
        return self

    def partitionBy(self, *columns):
        self.partitions = columns
        return self

    def parquet(self, uri):
        self.log.append((self.name, uri, self.mode_value, self.partitions))


class FakeFrame:
    def __init__(self, name, log, columns, rows=1):
        self.name = name
        self.log = log
        self.columns = columns
        self.rows = rows

    def count(self):
        return self.rows

    @property
    def write(self):
        return FakeWriter(self.log, self.name)


def _marts(log):
    daily_columns = {
        "offer_price_history_daily": ["marketplace", "observed_date"],
        "offer_change_daily": ["marketplace", "observed_date"],
        "category_price_daily": ["marketplace", "observed_date"],
        "source_coverage_daily": ["marketplace", "observed_date"],
        "counter_delta_daily": ["marketplace", "observed_date"],
        "crawl_reliability_daily": ["marketplace", "request_date"],
    }
    return {
        name: FakeFrame(name, log, daily_columns.get(name, ["marketplace"]))
        for name in DATASETS
    }


def test_context_normalizes_utc_and_validates_run_id():
    context = MarketplaceBatchContext(
        "run_1", AS_OF, "file:///silver", "file:///gold"
    )

    assert context.as_of.tzinfo == timezone.utc


def test_context_converts_an_offset_as_of_to_utc():
    tehran = timezone(timedelta(hours=3, minutes=30))

    context = MarketplaceBatchContext(
        "run_1", AS_OF.astimezone(tehran), "file:///silver", "file:///gold"
    )

    assert context.as_of == AS_OF
    assert context.as_of.tzinfo == timezone.utc


@pytest.mark.parametrize(
    "run_id", ["", " ", "run 1", "run/1", "run;drop", "x" * 65, "réun"]
)
def test_context_rejects_an_unsafe_run_id(run_id):
    with pytest.raises(ValueError, match="run_id"):
        MarketplaceBatchContext(run_id, AS_OF, "file:///silver", "file:///gold")


def test_context_rejects_a_naive_as_of():
    with pytest.raises(ValueError, match="as_of"):
        MarketplaceBatchContext(
            "run-1", datetime(2026, 9, 5, 10), "file:///silver", "file:///gold"
        )


@pytest.mark.parametrize(
    "field", ["silver_uri", "gold_root_uri", "freshness_rule_version", "counter_rule_version"]
)
def test_context_rejects_blank_required_text(field):
    values = {
        "run_id": "run-1",
        "as_of": AS_OF,
        "silver_uri": "file:///silver",
        "gold_root_uri": "file:///gold",
    }
    values[field] = "   "

    with pytest.raises(ValueError, match=field):
        MarketplaceBatchContext(**values)


@pytest.mark.parametrize("field", ["freshness_seconds", "counter_max_gap_seconds"])
def test_context_rejects_non_positive_windows(field):
    with pytest.raises(ValueError, match=field):
        MarketplaceBatchContext(
            "run-1", AS_OF, "file:///silver", "file:///gold", **{field: 0}
        )


# 34
def test_gold_paths_are_scoped_to_the_run_and_never_overwrite_another():
    log = []
    context = MarketplaceBatchContext("run-1", AS_OF, "file:///silver", "file:///gold")
    other = MarketplaceBatchContext("run-2", AS_OF, "file:///silver", "file:///gold")

    results = write_run_scoped_gold(_marts(log), context)
    first_uris = {name: result.uri for name, result in results.items()}
    other_log = []
    other_uris = {
        name: result.uri
        for name, result in write_run_scoped_gold(_marts(other_log), other).items()
    }

    assert set(results) == set(DATASETS)
    for name, uri in first_uris.items():
        assert uri.startswith("file:///gold/runs/run_id=run-1/")
        assert uri.endswith(f"/{name}")
        assert uri != other_uris[name]


def test_gold_root_trailing_slash_does_not_double_the_separator():
    log = []
    context = MarketplaceBatchContext("run-1", AS_OF, "file:///silver", "file:///gold/")

    results = write_run_scoped_gold(_marts(log), context)

    for result in results.values():
        assert "//runs" not in result.uri.replace("file://", "")


def test_daily_marts_are_partitioned_by_marketplace_and_their_date_column():
    log = []
    context = MarketplaceBatchContext("run-1", AS_OF, "file:///silver", "file:///gold")

    write_run_scoped_gold(_marts(log), context)

    partitions = {name: parts for name, _, _, parts in log}
    assert partitions["crawl_reliability_daily"] == ("marketplace", "request_date")
    assert partitions["offer_price_history_daily"] == ("marketplace", "observed_date")
    assert partitions["offer_current"] is None
    assert {mode for _, _, mode, _ in log} == {"overwrite"}


def test_gold_write_requires_exactly_the_nine_datasets():
    log = []
    context = MarketplaceBatchContext("run-1", AS_OF, "file:///silver", "file:///gold")
    incomplete = _marts(log)
    del incomplete["offer_freshness"]

    with pytest.raises(ValueError, match="exactly the nine"):
        write_run_scoped_gold(incomplete, context)


def test_staging_table_uses_safe_hash_and_allowlist():
    first = MarketplaceCachePublisher.staging_table("offer_current", "same-run")

    assert first == MarketplaceCachePublisher.staging_table("offer_current", "same-run")
    assert "same-run" not in first


# 46
def test_importing_the_batch_modules_opens_no_client_or_session():
    probe = (
        "import batch_layer.marketplace_warehouse as warehouse;"
        "import batch_layer.marketplace_postgres as publisher;"
        "from pyspark.sql import SparkSession;"
        "assert SparkSession._instantiatedSession is None;"
        "import sys;"
        "assert 'minio' not in sys.modules;"
        "assert 'redis' not in sys.modules;"
        "assert 'elasticsearch' not in sys.modules;"
        "assert len(publisher.DATASETS) == 9;"
        "print('CLEAN')"
    )

    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, timeout=300
    )

    assert "CLEAN" in result.stdout, result.stderr
