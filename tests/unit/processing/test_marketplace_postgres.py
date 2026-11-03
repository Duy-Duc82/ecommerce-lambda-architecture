"""Staging and publication tests, items 33-45 of the Phase 6 plan section 15.

Connections are fakes; no PostgreSQL socket is opened.
"""

from contextlib import contextmanager
from datetime import UTC, datetime

import pytest

from ecomdata.domain.batch_run import BatchRunStatus, InvalidTransition
from ecomdata.domain.quality_rules import mandatory_rule_names
from ecomdata.processing.batch.context import MarketplaceBatchContext
from ecomdata.processing.batch.quality import QualityDecision, QualityGateFailure, QualityResult
from ecomdata.processing.batch.repositories import (
    DATASET_COLUMNS,
    DATASETS,
    MarketplaceBatchAudit,
    MarketplaceCachePublisher,
    MarketplaceQualityRepository,
    StagedDataset,
)

AS_OF = datetime(2026, 9, 29, tzinfo=UTC)
MANIFEST_URI = "s3a://gold/marketplace/manifests/run_id=run-1/manifest.json"


def passing(run_id="run-1"):
    return QualityDecision(
        run_id=run_id,
        passed=True,
        rule_version="quality-rules.v1",
        evaluated_at=AS_OF,
        mandatory_total=13,
        mandatory_failures=0,
        advisory_failures=0,
        skipped=0,
    )


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
    return tuple(StagedDataset(name, publisher.staging_table(name, run_id), rows) for name in DATASETS)


def _counts(staged, **overrides):
    counts = {item.staging_table: item.row_count for item in staged}
    counts.update(overrides)
    return counts


# 33
def test_exactly_ten_datasets_with_explicit_columns_are_declared():
    assert len(DATASETS) == 10
    assert set(DATASETS) == set(DATASET_COLUMNS)
    for name in DATASETS:
        assert DATASET_COLUMNS[name], name
        assert len(set(DATASET_COLUMNS[name])) == len(DATASET_COLUMNS[name])


def test_staging_requires_every_dataset():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    incomplete = _marts()
    del incomplete["counter_delta_daily"]

    with pytest.raises(ValueError, match="all ten datasets"):
        publisher.stage(incomplete, run_id="run-1")


def test_staging_rejects_a_mart_missing_declared_columns():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    marts = _marts(seller_current=FakeFrame("seller_current", columns=["seller_id"]))

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
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()), jdbc_url="jdbc:postgresql://db/test")

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
        publisher.publish(staged, run_id="run-1", published_at=AS_OF, quality=passing(), manifest_uri=MANIFEST_URI)

    assert not any(sql.startswith("TRUNCATE") for sql, _ in cursor.executed)


# 38
def test_staged_count_mismatch_blocks_publish():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher)
    cursor = FakeCursor(counts=_counts(staged, **{staged[0].staging_table: 0}))
    publisher = MarketplaceCachePublisher(_factory(cursor))

    with pytest.raises(ValueError, match="staging count mismatch for offer_current"):
        publisher.publish(staged, run_id="run-1", published_at=AS_OF, quality=passing(), manifest_uri=MANIFEST_URI)


def test_publish_rejects_staging_tables_from_another_run():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher, run_id="run-2")

    with pytest.raises(ValueError, match="run-scoped identity"):
        publisher.publish(staged, run_id="run-1", published_at=AS_OF, quality=passing(), manifest_uri=MANIFEST_URI)


def test_publish_requires_all_ten_staged_datasets():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher)[:-1]

    with pytest.raises(ValueError, match="all ten staged datasets"):
        publisher.publish(staged, run_id="run-1", published_at=AS_OF, quality=passing(), manifest_uri=MANIFEST_URI)


# 39
def test_duplicate_cache_primary_key_blocks_publish():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher)
    cursor = FakeCursor(counts=_counts(staged), fail_on="INSERT INTO cache.marketplace_seller_current")
    publisher = MarketplaceCachePublisher(_factory(cursor))

    with pytest.raises(RuntimeError, match="duplicate key"):
        publisher.publish(staged, run_id="run-1", published_at=AS_OF, quality=passing(), manifest_uri=MANIFEST_URI)

    assert not any("marketplace_cache_version" in sql for sql, _ in cursor.executed)


# 40, 42
def test_publish_uses_one_transaction_with_explicit_columns():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher)
    cursor = FakeCursor(counts=_counts(staged))
    factory = _factory(cursor)
    publisher = MarketplaceCachePublisher(factory)

    publisher.publish(staged, run_id="run-1", published_at=AS_OF, quality=passing(), manifest_uri=MANIFEST_URI)

    statements = [sql for sql, _ in cursor.executed]
    # One connection means one transaction around the whole publication.
    assert factory.connection.entered == 1
    assert statements[0].startswith("SELECT pg_advisory_xact_lock")
    truncate_at = next(i for i, sql in enumerate(statements) if sql.startswith("TRUNCATE"))
    version_at = next(i for i, sql in enumerate(statements) if "marketplace_cache_version" in sql)
    assert truncate_at < version_at
    # The audit row belongs to the runner, which advances it after the cache is published.
    assert not any("marketplace_batch_run" in sql for sql in statements)
    for name in DATASETS:
        insert = next(sql for sql in statements if f"INSERT INTO cache.marketplace_{name} (" in sql)
        assert ",".join(DATASET_COLUMNS[name]) in insert
        assert "SELECT *" not in insert


def test_publish_truncates_every_cache_table_once():
    publisher = MarketplaceCachePublisher(_factory(FakeCursor()))
    staged = _staged(publisher)
    cursor = FakeCursor(counts=_counts(staged))
    publisher = MarketplaceCachePublisher(_factory(cursor))

    publisher.publish(staged, run_id="run-1", published_at=AS_OF, quality=passing(), manifest_uri=MANIFEST_URI)

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
        publisher.publish(staged, run_id="run-1", published_at=AS_OF, quality=passing(), manifest_uri=MANIFEST_URI)

    assert not any("marketplace_cache_version" in sql for sql, _ in cursor.executed)
    assert not any("marketplace_batch_run" in sql for sql, _ in cursor.executed)


def test_cleanup_never_raises():
    class Exploding:
        def __call__(self):
            raise RuntimeError("connection refused")

    publisher = MarketplaceCachePublisher(Exploding())

    publisher.cleanup(_staged(publisher))


def run_row(status="CREATED", last="CREATED", *, as_of=AS_OF, silver="s3a://silver", expected=None, counts=None):
    return (
        "run-1", as_of, silver, status, last, expected, None, None, counts, None, None, None, False, False, None, None,
    )  # fmt: skip


def audit_ctx():
    return MarketplaceBatchContext("run-1", AS_OF, "s3a://silver", "s3a://gold")


# 44
def test_a_new_run_is_inserted_as_created_with_the_pointer_it_expects():
    cursor = FakeCursor(rows=None)
    audit = MarketplaceBatchAudit(_factory(cursor))

    audit.open_run(audit_ctx(), AS_OF, expected_current_run_id="run-0")

    insert, params = next((sql, p) for sql, p in cursor.executed if sql.lstrip().startswith("INSERT INTO audit"))
    assert "'CREATED','CREATED'" in insert
    assert params == ("run-1", AS_OF, "s3a://silver", AS_OF, "run-0")


def test_an_existing_run_is_returned_not_overwritten():
    cursor = FakeCursor(rows=run_row("SILVER_BUILT", "SILVER_BUILT", expected="run-0"))
    audit = MarketplaceBatchAudit(_factory(cursor))

    record = audit.open_run(audit_ctx(), AS_OF, expected_current_run_id="run-9")

    assert record.status is BatchRunStatus.SILVER_BUILT
    assert record.expected_current_run_id == "run-0"
    assert not any(sql.lstrip().startswith("INSERT") for sql, _ in cursor.executed)


@pytest.mark.parametrize(
    "stored_as_of, stored_silver",
    [(datetime(2026, 9, 28, tzinfo=UTC), "s3a://silver"), (AS_OF, "s3a://other-silver")],
)
def test_an_existing_run_of_another_context_is_refused(stored_as_of, stored_silver):
    cursor = FakeCursor(rows=run_row(as_of=stored_as_of, silver=stored_silver))
    audit = MarketplaceBatchAudit(_factory(cursor))

    with pytest.raises(ValueError, match="another context"):
        audit.open_run(audit_ctx(), AS_OF, expected_current_run_id=None)


def test_a_row_written_before_stages_reads_with_the_new_status_names():
    cursor = FakeCursor(rows=run_row("SUCCEEDED", None))

    record = MarketplaceBatchAudit(_factory(cursor)).get("run-1")

    assert record.status is BatchRunStatus.PROMOTED
    assert record.last_completed_status is BatchRunStatus.PROMOTED


def test_advance_checks_the_state_machine_under_a_row_lock():
    cursor = FakeCursor(rows=run_row("CREATED", "CREATED"))
    audit = MarketplaceBatchAudit(_factory(cursor))

    with pytest.raises(InvalidTransition):
        audit.advance("run-1", BatchRunStatus.PUBLISHED, AS_OF)

    assert cursor.executed[0][0].endswith("FOR UPDATE")
    assert not any(sql.startswith("UPDATE") for sql, _ in cursor.executed)


def test_advance_records_the_status_the_milestone_and_the_columns():
    cursor = FakeCursor(rows=run_row("FACTS_BUILT", "FACTS_BUILT"))
    audit = MarketplaceBatchAudit(_factory(cursor))

    audit.advance(
        "run-1", BatchRunStatus.GOLD_WRITTEN, AS_OF, gold_run_uri="s3a://gold/runs/run_id=run-1",
        dataset_counts={"seller_current": 2, "offer_current": 3}, silver_rows=10,
    )  # fmt: skip

    sql, params = next((sql, p) for sql, p in cursor.executed if sql.startswith("UPDATE"))
    assert "status=%s" in sql and "last_completed_status=%s" in sql and "held_reason=%s" in sql
    assert params[0] == "GOLD_WRITTEN"
    assert '{"offer_current": 3, "seller_current": 2}' in params
    assert 5 in params  # gold_rows, summed from the counts
    assert params[-1] == "run-1"


def test_a_held_run_keeps_its_last_milestone():
    cursor = FakeCursor(rows=run_row("QUALITY_PASSED", "QUALITY_PASSED"))
    audit = MarketplaceBatchAudit(_factory(cursor))

    audit.advance("run-1", BatchRunStatus.HELD, AS_OF, held_reason="BACKFILL_REFUSED")

    sql, params = next((sql, p) for sql, p in cursor.executed if sql.startswith("UPDATE"))
    assert "last_completed_status" not in sql
    assert params[0] == "HELD" and params[1] == AS_OF and "BACKFILL_REFUSED" in params


def test_an_unknown_audit_column_is_refused():
    cursor = FakeCursor(rows=run_row())

    with pytest.raises(ValueError, match="unknown audit column"):
        MarketplaceBatchAudit(_factory(cursor)).set_columns("run-1", status="PROMOTED")


# 45
def test_a_failed_run_is_recorded_with_a_truncated_error():
    cursor = FakeCursor(rows=run_row("GOLD_WRITTEN", "GOLD_WRITTEN"))
    audit = MarketplaceBatchAudit(_factory(cursor))

    audit.fail("run-1", AS_OF, RuntimeError("x" * 5000))

    sql, params = next((sql, p) for sql, p in cursor.executed if sql.startswith("UPDATE"))
    assert "status='FAILED'" in sql
    assert len(params[1]) == 2000


def test_a_finished_run_cannot_be_failed():
    cursor = FakeCursor(rows=run_row("PROMOTED", "PROMOTED"))

    with pytest.raises(InvalidTransition):
        MarketplaceBatchAudit(_factory(cursor)).fail("run-1", AS_OF, RuntimeError("late"))


@pytest.mark.parametrize("status", ["FAILED", "QUALITY_FAILED"])
def test_reopen_returns_a_failed_run_to_its_last_milestone(status):
    cursor = FakeCursor(rows=run_row(status, "GOLD_WRITTEN"))

    MarketplaceBatchAudit(_factory(cursor)).reopen("run-1", AS_OF)

    sql = next(sql for sql, _ in cursor.executed if sql.startswith("UPDATE"))
    assert "status=last_completed_status" in sql and "error_message=NULL" in sql


def test_reopen_leaves_a_run_that_did_not_fail_alone():
    cursor = FakeCursor(rows=run_row("HELD", "QUALITY_PASSED"))

    MarketplaceBatchAudit(_factory(cursor)).reopen("run-1", AS_OF)

    assert not any(sql.startswith("UPDATE") for sql, _ in cursor.executed)


# ----------------------------------------------------------------------------
# Quality result persistence, Phase 7 plan section 9.
# ----------------------------------------------------------------------------
def quality_result(
    check_name="observation_id_unique",
    *,
    run_id="run-1",
    severity="MANDATORY",
    status="PASS",
    observed=0.0,
    sample=None,
):
    return QualityResult(
        run_id=run_id,
        check_name=check_name,
        severity=severity,
        dataset_name="silver",
        status=status,
        observed_value=observed,
        expectation="an expectation",
        rule_version="quality-rules.v1",
        failure_sample_json=sample,
        checked_at=AS_OF,
    )


def quality_decision(*, run_id="run-1", passed=False, failures=2):
    return QualityDecision(
        run_id=run_id,
        passed=passed,
        rule_version="quality-rules.v1",
        evaluated_at=AS_OF,
        mandatory_total=13,
        mandatory_failures=failures,
        advisory_failures=0,
        skipped=1,
    )


def test_quality_results_upsert_one_row_per_check():
    cursor = FakeCursor()
    repository = MarketplaceQualityRepository(_factory(cursor))

    repository.record([quality_result("first_check"), quality_result("second_check")], run_id="run-1")

    assert len(cursor.executed) == 2
    sql, params = cursor.executed[1]
    assert "ON CONFLICT (run_id,check_name) DO UPDATE" in sql
    assert params[1] == "second_check"


# 54
def test_failing_results_are_persisted_with_their_evidence():
    cursor = FakeCursor()
    repository = MarketplaceQualityRepository(_factory(cursor))

    repository.record(
        [quality_result("currency_valid", status="FAIL", observed=3.0, sample='{"keys":["obs-1"]}')],
        run_id="run-1",
    )

    params = cursor.executed[0][1]
    assert params[4] == "FAIL"
    assert params[5] == 3.0
    assert params[8] == '{"keys":["obs-1"]}'


def test_results_from_another_run_are_refused():
    repository = MarketplaceQualityRepository(_factory(FakeCursor()))

    with pytest.raises(ValueError, match="belong to another run"):
        repository.record([quality_result(run_id="run-2")], run_id="run-1")


def test_an_empty_result_set_is_refused():
    repository = MarketplaceQualityRepository(_factory(FakeCursor()))

    with pytest.raises(ValueError, match="empty quality result set"):
        repository.record([], run_id="run-1")


def test_latest_decision_rebuilds_the_stored_verdict():
    cursor = FakeCursor(rows=(len(mandatory_rule_names()), 0, 1, 0, "quality-rules.v1", AS_OF))

    decision = MarketplaceQualityRepository(_factory(cursor)).latest_decision(run_id="run-1")

    assert decision.passed is True
    assert decision.mandatory_failures == 0
    assert decision.advisory_failures == 1


def test_a_partially_recorded_run_does_not_reload_as_passing():
    # All but one mandatory row stored. The missing one was never evaluated,
    # which decide() counts as a failure; a reload must agree.
    cursor = FakeCursor(rows=(len(mandatory_rule_names()) - 1, 0, 0, 0, "quality-rules.v1", AS_OF))

    decision = MarketplaceQualityRepository(_factory(cursor)).latest_decision(run_id="run-1")

    assert decision.passed is False
    assert decision.mandatory_failures == 1


def test_a_run_with_no_stored_results_has_no_decision():
    cursor = FakeCursor(rows=(0, 0, 0, 0, None, None))

    assert MarketplaceQualityRepository(_factory(cursor)).latest_decision(run_id="run-1") is None


def test_a_failed_decision_blocks_publication_before_any_lock_or_truncate():
    cursor = FakeCursor()
    publisher = MarketplaceCachePublisher(_factory(cursor))
    staged = _staged(publisher)

    with pytest.raises(QualityGateFailure):
        publisher.publish(
            staged, run_id="run-1", published_at=AS_OF, quality=quality_decision(), manifest_uri=MANIFEST_URI
        )

    # Nothing reached the database at all: no advisory lock, no TRUNCATE.
    assert cursor.executed == []


# 52
def test_a_decision_from_another_run_blocks_publication():
    cursor = FakeCursor()
    publisher = MarketplaceCachePublisher(_factory(cursor))
    staged = _staged(publisher)

    with pytest.raises(QualityGateFailure, match="belongs to run run-2"):
        publisher.publish(
            staged, run_id="run-1", published_at=AS_OF, quality=passing("run-2"), manifest_uri=MANIFEST_URI
        )

    assert cursor.executed == []


def test_a_published_cache_must_name_its_manifest():
    cursor = FakeCursor()
    publisher = MarketplaceCachePublisher(_factory(cursor))
    staged = _staged(publisher)

    with pytest.raises(ValueError, match="name the manifest"):
        publisher.publish(staged, run_id="run-1", published_at=AS_OF, quality=passing(), manifest_uri="  ")

    assert cursor.executed == []


def test_publication_records_the_manifest_and_rule_version_it_published_under():
    staged = _staged(MarketplaceCachePublisher(_factory(FakeCursor())))
    cursor = FakeCursor(counts=_counts(staged))
    publisher = MarketplaceCachePublisher(_factory(cursor))

    publisher.publish(staged, run_id="run-1", published_at=AS_OF, quality=passing(), manifest_uri=MANIFEST_URI)

    version_sql, version_params = next(
        (sql, params) for sql, params in cursor.executed if "marketplace_cache_version" in sql
    )

    assert version_params[3] == "quality-rules.v1"
    assert version_params[4] == MANIFEST_URI
    assert not any("marketplace_batch_run" in sql for sql, _ in cursor.executed)
