"""Staging and publication tests, items 33-45 of the Phase 6 plan section 15.

Connections are fakes; no PostgreSQL socket is opened.
"""
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from batch_layer.marketplace_postgres import (
    DATASET_COLUMNS,
    DATASETS,
    MarketplaceBatchAudit,
    MarketplaceCachePublisher,
    StagedDataset,
)
from batch_layer.marketplace_warehouse import MarketplaceBatchContext

AS_OF = datetime(2026, 9, 29, tzinfo=timezone.utc)


class FakeCursor:
    def __init__(self, counts=None, rows=None, fail_on=None):
        self.counts = counts or {}
        self.rows = rows
        self.fail_on = fail_on
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("duplicate key value violates unique constraint")

    def fetchone(self):
        last = self.executed[-1][0]
        if last.startswith("SELECT COUNT(*) FROM "):
            return (self.counts.get(last.rsplit(" ", 1)[-1], 0),)
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConnection:
    def __init__(self, cursor):
        self.cur = cursor
        self.entered = 0

    def cursor(self):
        return self.cur

    def __enter__(self):
        self.entered += 1
        return self

    def __exit__(self, *exc):
        return False


def _factory(cursor):
    connection = FakeConnection(cursor)

    @contextmanager
    def factory():
        connection.entered += 1
        yield connection

    factory.connection = connection
    return factory


class FakeWriter:
    def __init__(self, log):
        self.log = log

    def jdbc(self, url, table, mode, properties):
        self.log.append(("jdbc", table, mode))


class FakeFrame:
    """Stands in for a mart DataFrame: columns, count and a jdbc writer."""

    def __init__(self, name, rows=1, log=None, columns=None):
        self.columns = columns if columns is not None else list(DATASET_COLUMNS[name])
        self.rows = rows
        self.log = log if log is not None else []

    def count(self):
        return self.rows

    def select(self, *columns):
        return self

    @property
    def write(self):
        return FakeWriter(self.log)


def _marts(rows=1, log=None, **overrides):
    marts = {name: FakeFrame(name, rows, log) for name in DATASETS}
    marts.update(overrides)
    return marts


def _staged(publisher, run_id="run-1", rows=1):
    return tuple(
        StagedDataset(name, publisher.staging_table(name, run_id), rows)
        for name in DATASETS
    )


def _counts(staged, **overrides):
    counts = {item.staging_table: item.row_count for item in staged}
    counts.update(overrides)
    return counts


# 33
def test_exactly_nine_datasets_with_explicit_columns_are_declared():
    assert len(DATASETS) == 9
    assert set(DATASETS) == set(DATASET_COLUMNS)
    for name in DATASETS:
        assert DATASET_COLUMNS[name], name
        assert len(set(DATASET_COLUMNS[name])) == len(DATASET_COLUMNS[name])


def test_staging_requires_every_dataset():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    incomplete = _marts()
    del incomplete["counter_delta_daily"]

    with pytest.raises(ValueError, match="all nine datasets"):
        publisher.stage(incomplete, run_id="run-1")


def test_staging_rejects_a_mart_missing_declared_columns():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    marts = _marts(
        seller_current=FakeFrame("seller_current", columns=["seller_id"])
    )

    with pytest.raises(ValueError, match="seller_current missing columns"):
        publisher.stage(marts, run_id="run-1")


# 36
def test_stage_table_names_derive_from_a_safe_hash_token():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))

    table = publisher.staging_table("offer_current", "run-1")

    assert table.startswith("staging.mp_offer_current_")
    token = table.rsplit("_", 1)[-1]
    assert len(token) == 16
    assert all(char in "0123456789abcdef" for char in token)
    assert publisher.staging_table("offer_current", "run-1") == table
    assert publisher.staging_table("offer_current", "run-2") != table


def test_stage_table_rejects_a_dataset_outside_the_allowlist():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))

    with pytest.raises(ValueError, match="unsupported marketplace dataset"):
        publisher.staging_table("offer_current; DROP TABLE x", "run-1")


def test_staging_writes_every_dataset_with_its_declared_columns():
    log = []
    publisher = MarketplaceCachePublisher(
        _factory(FakeCursor()), jdbc_url="jdbc:postgresql://db/test"
    )

    staged = publisher.stage(_marts(rows=3, log=log), run_id="run-1")

    assert [item.dataset_name for item in staged] == list(DATASETS)
    assert all(item.row_count == 3 for item in staged)
    assert [entry[1] for entry in log] == [item.staging_table for item in staged]
    assert {entry[2] for entry in log} == {"overwrite"}


# 37
def test_all_staging_counts_are_checked_before_any_cache_mutation():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher)
    last = staged[-1]
    cursor = FakeCursor(counts=_counts(staged, **{last.staging_table: 99}))
    publisher = MarketplaceCachePublisher(_factory(cursor))

    with pytest.raises(ValueError, match="staging count mismatch"):
        publisher.publish(staged, run_id="run-1", published_at=AS_OF)

    assert not any(sql.startswith("TRUNCATE") for sql, _ in cursor.executed)


# 38
def test_staged_count_mismatch_blocks_publish():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher)
    cursor = FakeCursor(counts=_counts(staged, **{staged[0].staging_table: 0}))
    publisher = MarketplaceCachePublisher(_factory(cursor))

    with pytest.raises(ValueError, match="staging count mismatch for offer_current"):
        publisher.publish(staged, run_id="run-1", published_at=AS_OF)


def test_publish_rejects_staging_tables_from_another_run():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher, run_id="run-2")

    with pytest.raises(ValueError, match="run-scoped identity"):
        publisher.publish(staged, run_id="run-1", published_at=AS_OF)


def test_publish_requires_all_nine_staged_datasets():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher)[:-1]

    with pytest.raises(ValueError, match="all nine staged datasets"):
        publisher.publish(staged, run_id="run-1", published_at=AS_OF)


# 39
def test_duplicate_cache_primary_key_blocks_publish():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher)
    cursor = FakeCursor(
        counts=_counts(staged), fail_on="INSERT INTO cache.marketplace_seller_current"
    )
    publisher = MarketplaceCachePublisher(_factory(cursor))

    with pytest.raises(RuntimeError, match="duplicate key"):
        publisher.publish(staged, run_id="run-1", published_at=AS_OF)

    assert not any(
        "marketplace_cache_version" in sql for sql, _ in cursor.executed
    )


# 40, 42
def test_publish_uses_one_transaction_with_explicit_columns():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher)
    cursor = FakeCursor(counts=_counts(staged))
    factory = _factory(cursor)
    publisher = MarketplaceCachePublisher(factory)

    publisher.publish(staged, run_id="run-1", published_at=AS_OF)

    statements = [sql for sql, _ in cursor.executed]
    # One connection means one transaction around the whole publication.
    assert factory.connection.entered == 1
    assert statements[0].startswith("SELECT pg_advisory_xact_lock")
    truncate_at = next(i for i, sql in enumerate(statements) if sql.startswith("TRUNCATE"))
    version_at = next(
        i for i, sql in enumerate(statements) if "marketplace_cache_version" in sql
    )
    succeeded_at = next(
        i for i, sql in enumerate(statements) if "status='SUCCEEDED'" in sql
    )
    assert truncate_at < version_at < succeeded_at
    for name in DATASETS:
        insert = next(
            sql for sql in statements if f"INSERT INTO cache.marketplace_{name} (" in sql
        )
        assert ",".join(DATASET_COLUMNS[name]) in insert
        assert "SELECT *" not in insert


def test_publish_truncates_every_cache_table_once():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher)
    cursor = FakeCursor(counts=_counts(staged))
    publisher = MarketplaceCachePublisher(_factory(cursor))

    publisher.publish(staged, run_id="run-1", published_at=AS_OF)

    truncates = [sql for sql, _ in cursor.executed if sql.startswith("TRUNCATE")]
    assert len(truncates) == 1
    for name in DATASETS:
        assert f"cache.marketplace_{name}" in truncates[0]


# 41
def test_a_failure_on_any_table_leaves_no_cache_version_row():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher)
    cursor = FakeCursor(counts=_counts(staged), fail_on="TRUNCATE")
    publisher = MarketplaceCachePublisher(_factory(cursor))

    with pytest.raises(RuntimeError):
        publisher.publish(staged, run_id="run-1", published_at=AS_OF)

    assert not any("marketplace_cache_version" in sql for sql, _ in cursor.executed)
    assert not any("status='SUCCEEDED'" in sql for sql, _ in cursor.executed)


def test_cleanup_never_raises():
    class Exploding:
        def __call__(self):
            raise RuntimeError("connection refused")

    publisher = MarketplaceCachePublisher(Exploding())

    publisher.cleanup(_staged(publisher))


# 44
def test_a_succeeded_run_id_cannot_restart():
    cursor = FakeCursor(rows=("SUCCEEDED", "run-1", AS_OF, "s3a://silver"))
    audit = MarketplaceBatchAudit(_factory(cursor))
    context = MarketplaceBatchContext("run-1", AS_OF, "s3a://silver", "s3a://gold")

    with pytest.raises(ValueError, match="already SUCCEEDED"):
        audit.start_run(context, AS_OF)


def test_an_existing_run_requires_explicit_resume():
    cursor = FakeCursor(rows=("RUNNING", "run-1", AS_OF, "s3a://silver"))
    audit = MarketplaceBatchAudit(_factory(cursor))
    context = MarketplaceBatchContext("run-1", AS_OF, "s3a://silver", "s3a://gold")

    with pytest.raises(ValueError, match="explicit resume"):
        audit.start_run(context, AS_OF)


@pytest.mark.parametrize(
    "stored_as_of, stored_silver",
    [
        (datetime(2026, 9, 28, tzinfo=timezone.utc), "s3a://silver"),
        (AS_OF, "s3a://other-silver"),
    ],
)
def test_resume_requires_an_identical_context(stored_as_of, stored_silver):
    cursor = FakeCursor(rows=("RUNNING", "run-1", stored_as_of, stored_silver))
    audit = MarketplaceBatchAudit(_factory(cursor))
    context = MarketplaceBatchContext("run-1", AS_OF, "s3a://silver", "s3a://gold")

    with pytest.raises(ValueError, match="resume context does not match"):
        audit.start_run(context, AS_OF, resume=True)


def test_resume_with_an_identical_context_restarts_the_run():
    cursor = FakeCursor(rows=("FAILED", "run-1", AS_OF, "s3a://silver"))
    audit = MarketplaceBatchAudit(_factory(cursor))
    context = MarketplaceBatchContext("run-1", AS_OF, "s3a://silver", "s3a://gold")

    audit.start_run(context, AS_OF, resume=True)

    insert = cursor.executed[1][0]
    assert "ON CONFLICT(run_id) DO UPDATE" in insert
    assert "status='RUNNING'" in insert


def test_a_fresh_run_id_starts_without_resume():
    cursor = FakeCursor(rows=None)
    audit = MarketplaceBatchAudit(_factory(cursor))
    context = MarketplaceBatchContext("run-1", AS_OF, "s3a://silver", "s3a://gold")

    audit.start_run(context, AS_OF)

    assert "INSERT INTO audit.marketplace_batch_run" in cursor.executed[1][0]


# 45
def test_a_failed_run_is_recorded_with_a_truncated_error():
    cursor = FakeCursor()
    audit = MarketplaceBatchAudit(_factory(cursor))

    audit.mark_failed(run_id="run-1", completed_at=AS_OF, error=RuntimeError("x" * 5000))

    sql, params = cursor.executed[0]
    assert "status='FAILED'" in sql
    assert len(params[1]) == 2000


def test_gold_written_records_dataset_counts_deterministically():
    cursor = FakeCursor()
    audit = MarketplaceBatchAudit(_factory(cursor))

    audit.mark_gold_written(
        run_id="run-1",
        gold_run_uri="s3a://gold/runs/run_id=run-1",
        silver_rows=10,
        dataset_counts={"seller_current": 2, "offer_current": 3},
        completed_at=AS_OF,
    )

    params = cursor.executed[0][1]
    assert params[2] == 5
    assert params[3] == '{"offer_current": 3, "seller_current": 2}'
