"""Phase 6 GOLD-05: transactional PostgreSQL cache."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from batch_layer.marketplace_gold_contracts import MARKETPLACE_MART_SPECS
from batch_layer.marketplace_postgres_cache import (
    apply_marketplace_ddl,
    generate_ddl,
    publish_marts,
)

AS_OF = datetime(2026, 9, 8, tzinfo=timezone.utc)
RUN = "goldrun_abc"
RULE = "marketplace-gold-rules.v1"


class FakeCursor:
    def __init__(self, log, fail_on=None):
        self._log = log
        self._fail_on = fail_on

    def execute(self, sql, params=None):
        statement = " ".join(sql.split())
        if self._fail_on and self._fail_on in statement:
            raise RuntimeError("database error")
        self._log.append(statement)

    def close(self):
        self._log.append("CURSOR CLOSE")


class FakeConnection:
    def __init__(self, fail_on=None):
        self.log: list[str] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0
        self._fail_on = fail_on

    def cursor(self):
        return FakeCursor(self.log, self._fail_on)

    def commit(self):
        self.commits += 1
        self.log.append("COMMIT")

    def rollback(self):
        self.rollbacks += 1
        self.log.append("ROLLBACK")

    def close(self):
        self.closed += 1


def _run_row(status="SUCCEEDED"):
    return {
        "as_of": AS_OF,
        "window_days": 30,
        "rule_version": RULE,
        "started_at": AS_OF,
        "completed_at": AS_OF,
        "observations_read": 10,
        "quarantined_rows": 1,
        "status": status,
        "failure_stage": None,
        "skipped_marts": {"crawl_reliability_daily": "audit source unavailable"},
    }


# --- 39: exactly one commit -------------------------------------------------


def test_full_refresh_issues_exactly_one_commit():
    connection = FakeConnection()
    published = publish_marts(gold_run_id=RUN, connect=lambda: connection, run_row=_run_row())
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.log.count("COMMIT") == 1
    assert len(published) == len(MARKETPLACE_MART_SPECS) + 1  # marts + gold_run


def test_every_mart_is_deleted_then_inserted_inside_the_transaction():
    connection = FakeConnection()
    publish_marts(gold_run_id=RUN, connect=lambda: connection)
    for spec in MARKETPLACE_MART_SPECS:
        delete = f"DELETE FROM marketplace_gold.{spec.name} WHERE gold_run_id = %s"
        insert = (
            f"INSERT INTO marketplace_gold.{spec.name} "
            f"SELECT * FROM marketplace_gold_staging.{spec.name}"
        )
        assert delete in connection.log
        assert insert in connection.log
        assert connection.log.index(delete) < connection.log.index(insert)
        assert connection.log.index(insert) < connection.log.index("COMMIT")


def test_the_run_row_is_written_before_the_marts():
    connection = FakeConnection()
    publish_marts(gold_run_id=RUN, connect=lambda: connection, run_row=_run_row())
    run_statement = next(s for s in connection.log if "INSERT INTO marketplace_gold.gold_run" in s)
    first_mart = next(s for s in connection.log if "DELETE FROM marketplace_gold.offer_current" in s)
    assert connection.log.index(run_statement) < connection.log.index(first_mart)
    assert "ON CONFLICT (gold_run_id) DO UPDATE" in run_statement


def test_assertions_are_replaced_for_the_run_not_appended():
    connection = FakeConnection()
    publish_marts(
        gold_run_id=RUN,
        connect=lambda: connection,
        run_row=_run_row(),
        assertions=[
            {
                "assertion_name": "offer_current_unique",
                "observed_value": "0 duplicates",
                "expectation": "at most one row per offer_id",
                "status": "PASSED",
                "rule_version": RULE,
            }
        ],
    )
    assert "DELETE FROM marketplace_gold.gold_assertion WHERE gold_run_id = %s" in connection.log
    assert any("INSERT INTO marketplace_gold.gold_assertion" in s for s in connection.log)


# --- 40: rollback on error --------------------------------------------------


def test_an_error_mid_publish_rolls_back_and_re_raises():
    connection = FakeConnection(fail_on="INSERT INTO marketplace_gold.offer_freshness")
    with pytest.raises(RuntimeError, match="database error"):
        publish_marts(gold_run_id=RUN, connect=lambda: connection)
    assert connection.rollbacks == 1
    assert connection.commits == 0
    assert "COMMIT" not in connection.log


def test_the_connection_is_always_closed():
    connection = FakeConnection(fail_on="DELETE FROM marketplace_gold.offer_current")
    with pytest.raises(RuntimeError):
        publish_marts(gold_run_id=RUN, connect=lambda: connection)
    assert connection.closed == 1


# --- 41: idempotent re-publish ---------------------------------------------


def test_republishing_the_same_run_deletes_its_own_rows_first():
    connection = FakeConnection()
    for _ in range(2):
        publish_marts(gold_run_id=RUN, connect=lambda: connection)
    deletes = [s for s in connection.log if s.startswith("DELETE FROM marketplace_gold.offer_current")]
    assert len(deletes) == 2, "each publish must clear its own run before inserting"


def test_delete_is_scoped_to_the_run_and_never_truncates():
    connection = FakeConnection()
    publish_marts(gold_run_id=RUN, connect=lambda: connection)
    assert all("TRUNCATE" not in statement for statement in connection.log)
    assert all(
        "WHERE gold_run_id = %s" in statement
        for statement in connection.log
        if statement.startswith("DELETE FROM marketplace_gold.")
    )


# --- 42: publish is opt-in --------------------------------------------------


def test_unknown_mart_names_are_rejected_before_any_call():
    calls = []

    def connect():
        calls.append(1)
        raise AssertionError("must not connect")

    with pytest.raises(ValueError, match="unknown marts"):
        publish_marts(gold_run_id=RUN, connect=connect, mart_names=["not_a_mart"])
    assert calls == []


def test_blank_run_id_is_rejected_before_any_call():
    with pytest.raises(ValueError, match="gold_run_id"):
        publish_marts(gold_run_id="  ", connect=lambda: (_ for _ in ()).throw(AssertionError()))


def test_a_subset_of_marts_can_be_published():
    connection = FakeConnection()
    published = publish_marts(
        gold_run_id=RUN, connect=lambda: connection, mart_names=["offer_current"]
    )
    assert published == ["offer_current"]
    assert not any("offer_freshness" in statement for statement in connection.log)


# --- 43: DDL has no float --------------------------------------------------


def test_generated_ddl_contains_no_floating_point_type():
    # Comment lines are excluded: the DDL header explains *why* there is no
    # DOUBLE PRECISION, and that explanation must not trip its own check.
    statements = "\n".join(
        line
        for line in generate_ddl().splitlines()
        if not line.strip().startswith("--")
    ).upper()
    for banned in ("DOUBLE PRECISION", " FLOAT", " REAL", "NUMERIC(53"):
        assert banned not in statements


def test_generated_ddl_uses_numeric_for_money():
    ddl = generate_ddl()
    assert "NUMERIC(38, 6)" in ddl
    assert "current_price" in ddl


def test_generated_ddl_is_idempotent_and_covers_both_schemas():
    ddl = generate_ddl()
    assert ddl.count("CREATE SCHEMA IF NOT EXISTS") == 2
    for spec in MARKETPLACE_MART_SPECS:
        assert f"CREATE TABLE IF NOT EXISTS marketplace_gold.{spec.name}" in ddl
        assert f"CREATE TABLE IF NOT EXISTS marketplace_gold_staging.{spec.name}" in ddl
    assert "CREATE TABLE IF NOT EXISTS marketplace_gold.gold_run" in ddl
    assert "CREATE TABLE IF NOT EXISTS marketplace_gold.gold_assertion" in ddl


def test_served_tables_are_keyed_by_run_and_grain_but_staging_is_not():
    ddl = generate_ddl()
    served = ddl.split("-- staging mirror")[0]
    staging = ddl.split("-- staging mirror")[1]
    assert "PRIMARY KEY (gold_run_id, offer_id)" in served
    assert "PRIMARY KEY" not in staging


def test_ddl_mentions_no_legacy_table():
    ddl = generate_ddl()
    for legacy in ("cache.funnel_daily", "cache.product_daily", "audit.pipeline_run"):
        assert legacy not in ddl


def test_apply_ddl_commits_once_and_rolls_back_on_error():
    connection = FakeConnection()
    apply_marketplace_ddl(lambda: connection)
    assert connection.commits == 1
    failing = FakeConnection(fail_on="CREATE SCHEMA IF NOT EXISTS marketplace_gold")
    with pytest.raises(RuntimeError):
        apply_marketplace_ddl(lambda: failing)
    assert failing.rollbacks == 1 and failing.commits == 0


def test_module_imports_without_psycopg2_installed():
    import importlib

    with pytest.raises(ImportError):
        importlib.import_module("psycopg2")
    module = importlib.import_module("batch_layer.marketplace_postgres_cache")
    assert hasattr(module, "publish_marts")
