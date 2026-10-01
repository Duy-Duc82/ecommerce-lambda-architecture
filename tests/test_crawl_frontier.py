"""Frontier tests, items 12-21 of the Phase 3 plan section 12.

The connection is a fake that records SQL and parameters; no PostgreSQL
socket is opened.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from config.marketplace_schema import ResourceType
from crawler.frontier import LeaseLostError, PostgresCrawlFrontier
from crawler.scheduling import (
    CrawlTask,
    CrawlTaskStatus,
    CrawlTier,
    FailureKind,
    RetryPolicy,
    decide_retry,
    make_crawl_task_id,
)
from crawler.contracts import HttpResponseError

NOW = datetime(2026, 9, 29, 8, 0, tzinfo=timezone.utc)
POLICY = RetryPolicy(5, 30, 1800, Decimal("0"))


def _task(**overrides):
    values = {
        "task_id": make_crawl_task_id("tiki", ResourceType.LISTING_PAGE, "1846:1", NOW),
        "marketplace_code": "tiki",
        "marketplace_id": "marketplace-tiki",
        "target": "1846:1",
        "resource_type": ResourceType.LISTING_PAGE,
        "tier": CrawlTier.NORMAL,
        "priority": 10,
        "scheduled_for": NOW,
        "status": CrawlTaskStatus.READY,
        "attempts": 0,
        "max_attempts": 5,
    }
    values.update(overrides)
    return CrawlTask(**values)


def _row(task, *, status="LEASED", attempts=1, owner="worker-1", expires=None):
    return (
        task.task_id,
        task.marketplace_code,
        task.marketplace_id,
        task.target,
        task.resource_type.value,
        task.tier.value,
        task.priority,
        task.scheduled_for,
        status,
        attempts,
        task.max_attempts,
        owner,
        expires or NOW + timedelta(seconds=300),
        None,
        None,
        None,
        None,
    )


class FakeCursor:
    def __init__(self, rows=None, rowcount=1):
        self.rows = list(rows or [])
        self.rowcount = rowcount
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConnection:
    def __init__(self, cursor):
        self.cur = cursor

    def cursor(self):
        return self.cur

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _factory(cursor):
    connection = FakeConnection(cursor)
    opened = {"count": 0}

    @contextmanager
    def factory():
        opened["count"] += 1
        yield connection

    factory.cursor = cursor
    factory.opened = opened
    return factory


def _sql(cursor, index=0):
    return cursor.executed[index][0]


def _params(cursor, index=0):
    return cursor.executed[index][1]


# 12
def test_enqueue_is_idempotent():
    cursor = FakeCursor(rowcount=1)
    frontier = PostgresCrawlFrontier(_factory(cursor))

    inserted = frontier.enqueue(_task())

    assert inserted is True
    assert "ON CONFLICT (task_id) DO NOTHING" in _sql(cursor)

    cursor.rowcount = 0
    assert frontier.enqueue(_task()) is False


def test_enqueue_refuses_a_task_that_is_not_ready():
    frontier = PostgresCrawlFrontier(_factory(FakeCursor()))

    with pytest.raises(ValueError, match="READY"):
        frontier.enqueue(
            _task(
                status=CrawlTaskStatus.LEASED,
                lease_owner="worker-1",
                lease_expires_at=NOW,
            )
        )


def test_enqueue_never_writes_a_lease():
    cursor = FakeCursor()
    frontier = PostgresCrawlFrontier(_factory(cursor))

    frontier.enqueue(_task())

    params = _params(cursor)
    assert params[11] is None
    assert params[12] is None


# 13
def test_lease_uses_skip_locked_and_increments_attempts():
    task = _task()
    cursor = FakeCursor(rows=[_row(task)])
    frontier = PostgresCrawlFrontier(_factory(cursor))

    leased = frontier.lease_due(
        worker_id="worker-1", now=NOW, lease_seconds=300, limit=10
    )

    sql = _sql(cursor)
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "attempts = f.attempts + 1" in sql
    assert "status = 'LEASED'" in sql
    assert len(leased) == 1
    assert leased[0].status is CrawlTaskStatus.LEASED
    assert leased[0].attempts == 1
    assert leased[0].lease_owner == "worker-1"


def test_lease_passes_the_expiry_it_computed():
    cursor = FakeCursor(rows=[])
    frontier = PostgresCrawlFrontier(_factory(cursor))

    frontier.lease_due(worker_id="worker-1", now=NOW, lease_seconds=300, limit=5)

    params = _params(cursor)
    assert NOW + timedelta(seconds=300) in params
    assert 5 in params


# 14
def test_due_ordering_is_priority_then_time():
    cursor = FakeCursor(rows=[])
    frontier = PostgresCrawlFrontier(_factory(cursor))

    frontier.lease_due(worker_id="worker-1", now=NOW, lease_seconds=300, limit=10)

    assert "ORDER BY priority DESC, scheduled_for ASC" in _sql(cursor)


def test_lease_selects_ready_retry_wait_and_expired_leases():
    cursor = FakeCursor(rows=[])
    frontier = PostgresCrawlFrontier(_factory(cursor))

    frontier.lease_due(worker_id="worker-1", now=NOW, lease_seconds=300, limit=10)

    sql = _sql(cursor)
    assert "status IN ('READY', 'RETRY_WAIT') AND scheduled_for <= %s" in sql
    assert "status = 'LEASED' AND lease_expires_at <= %s" in sql


def test_the_lease_returns_columns_qualified_by_the_frontier_table():
    # The UPDATE joins the "due" CTE, which also has task_id. An unqualified
    # RETURNING task_id is ambiguous, and PostgreSQL refuses the statement:
    # no task had ever been leased on a real database until Phase 8 started
    # the worker for the first time (2026-10-01).
    cursor = FakeCursor(rows=[])
    frontier = PostgresCrawlFrontier(_factory(cursor))

    frontier.lease_due(worker_id="worker-1", now=NOW, lease_seconds=300, limit=10)

    returning = _sql(cursor).split("RETURNING", 1)[1]
    columns = [column.strip() for column in returning.split(",")]
    assert len(columns) == 17
    assert [column for column in columns if not column.startswith("f.")] == []


# 15, 16
def test_only_an_expired_lease_is_eligible_to_be_taken_again():
    cursor = FakeCursor(rows=[])
    frontier = PostgresCrawlFrontier(_factory(cursor))

    frontier.lease_due(worker_id="worker-2", now=NOW, lease_seconds=300, limit=10)

    # A live lease has lease_expires_at > now, so the <= predicate excludes it:
    # the comparison is what stops a second worker stealing it.
    assert "lease_expires_at <= %s" in _sql(cursor)
    assert "lease_expires_at >" not in _sql(cursor)


def test_expired_leases_are_recovered_to_retry_wait():
    cursor = FakeCursor(rowcount=3)
    frontier = PostgresCrawlFrontier(_factory(cursor))

    recovered = frontier.recover_expired_leases(now=NOW)

    sql = _sql(cursor)
    assert recovered == 3
    assert "status = 'RETRY_WAIT'" in sql
    assert "lease_owner = NULL" in sql
    assert "WHERE status = 'LEASED' AND lease_expires_at <= %s" in sql


# 17
@pytest.mark.parametrize(
    "call",
    [
        lambda f: f.mark_succeeded(
            task_id="t1",
            worker_id="worker-2",
            completed_at=NOW,
            next_scheduled_for=NOW + timedelta(hours=4),
        ),
        lambda f: f.mark_retry(
            task_id="t1",
            worker_id="worker-2",
            decision=decide_retry(
                error=HttpResponseError(503),
                http_status=None,
                retry_after=None,
                attempts=1,
                now=NOW,
                policy=POLICY,
                jitter_unit=Decimal("0.5"),
            ),
            http_status=503,
            error_message="boom",
        ),
        lambda f: f.mark_failed(
            task_id="t1",
            worker_id="worker-2",
            completed_at=NOW,
            failure_kind=FailureKind.CLIENT_ERROR,
            http_status=404,
            error_message="gone",
        ),
    ],
)
def test_a_wrong_worker_completion_raises_lease_lost(call):
    frontier = PostgresCrawlFrontier(_factory(FakeCursor(rows=[], rowcount=0)))

    with pytest.raises(LeaseLostError):
        call(frontier)


def test_every_completion_is_scoped_to_the_holding_worker():
    task = _task()
    for call in (
        lambda f: f.mark_succeeded(
            task_id=task.task_id,
            worker_id="worker-1",
            completed_at=NOW,
            next_scheduled_for=NOW + timedelta(hours=4),
        ),
        lambda f: f.mark_failed(
            task_id=task.task_id,
            worker_id="worker-1",
            completed_at=NOW,
            failure_kind=FailureKind.PARSE_ERROR,
            http_status=None,
            error_message="drift",
        ),
    ):
        cursor = FakeCursor(rows=[_row(task)])
        call(PostgresCrawlFrontier(_factory(cursor)))
        assert "status = 'LEASED' AND lease_owner = %s" in _sql(cursor)


# 18
def test_success_marks_the_occurrence_and_enqueues_exactly_one_next_task():
    task = _task()
    cursor = FakeCursor(rows=[_row(task)])
    factory = _factory(cursor)
    frontier = PostgresCrawlFrontier(factory)
    next_at = NOW + timedelta(hours=4)

    frontier.mark_succeeded(
        task_id=task.task_id,
        worker_id="worker-1",
        completed_at=NOW,
        next_scheduled_for=next_at,
    )

    assert len(cursor.executed) == 2
    assert factory.opened["count"] == 1
    assert "status = 'SUCCEEDED'" in _sql(cursor, 0)
    insert_sql, insert_params = cursor.executed[1]
    assert "INSERT INTO audit.crawl_frontier" in insert_sql
    assert "ON CONFLICT (task_id) DO NOTHING" in insert_sql
    assert insert_params[0] == make_crawl_task_id(
        task.marketplace_code, task.resource_type, task.target, next_at
    )
    assert insert_params[7] == next_at
    assert insert_params[8] == "READY"
    assert insert_params[9] == 0


def test_the_next_task_is_the_deterministic_identity_of_that_bucket():
    task = _task()
    next_at = NOW + timedelta(hours=4)
    ids = []
    for _ in range(2):
        cursor = FakeCursor(rows=[_row(task)])
        PostgresCrawlFrontier(_factory(cursor)).mark_succeeded(
            task_id=task.task_id,
            worker_id="worker-1",
            completed_at=NOW,
            next_scheduled_for=next_at,
        )
        ids.append(cursor.executed[1][1][0])

    assert ids[0] == ids[1]
    assert ids[0] != task.task_id


# 19
def test_retry_clears_the_lease_and_stores_the_next_attempt_and_error():
    decision = decide_retry(
        error=HttpResponseError(503),
        http_status=None,
        retry_after=None,
        attempts=1,
        now=NOW,
        policy=POLICY,
        jitter_unit=Decimal("0.5"),
    )
    cursor = FakeCursor(rowcount=1)
    frontier = PostgresCrawlFrontier(_factory(cursor))

    frontier.mark_retry(
        task_id="t1",
        worker_id="worker-1",
        decision=decision,
        http_status=503,
        error_message="service unavailable",
    )

    sql, params = cursor.executed[0]
    assert "status = 'RETRY_WAIT'" in sql
    assert "lease_owner = NULL" in sql
    assert "lease_expires_at = NULL" in sql
    assert decision.next_attempt_at in params
    assert "SERVER_ERROR" in params
    assert "service unavailable" in params


def test_retry_refuses_a_terminal_decision():
    decision = decide_retry(
        error=HttpResponseError(404),
        http_status=None,
        retry_after=None,
        attempts=1,
        now=NOW,
        policy=POLICY,
        jitter_unit=Decimal("0.5"),
    )
    frontier = PostgresCrawlFrontier(_factory(FakeCursor()))

    with pytest.raises(ValueError, match="retryable"):
        frontier.mark_retry(
            task_id="t1",
            worker_id="worker-1",
            decision=decision,
            http_status=404,
            error_message="not found",
        )


# 20
def test_terminal_failure_clears_the_lease():
    cursor = FakeCursor(rowcount=1)
    frontier = PostgresCrawlFrontier(_factory(cursor))

    frontier.mark_failed(
        task_id="t1",
        worker_id="worker-1",
        completed_at=NOW,
        failure_kind=FailureKind.ROBOTS_DENIED,
        http_status=None,
        error_message="disallowed",
    )

    sql, params = cursor.executed[0]
    assert "status = 'FAILED'" in sql
    assert "lease_owner = NULL" in sql
    assert "lease_expires_at = NULL" in sql
    assert "ROBOTS_DENIED" in params


# 21
def test_hostile_target_and_error_values_are_passed_as_parameters():
    hostile = "1846'); DROP TABLE audit.crawl_frontier; --"
    cursor = FakeCursor(rowcount=1)
    frontier = PostgresCrawlFrontier(_factory(cursor))

    frontier.enqueue(_task(target=hostile))

    sql, params = cursor.executed[0]
    assert hostile not in sql
    assert hostile in params


def test_a_hostile_error_message_is_a_parameter_and_is_truncated():
    hostile = "x'; DELETE FROM audit.crawl_frontier; --" + "y" * 5000
    cursor = FakeCursor(rowcount=1)
    frontier = PostgresCrawlFrontier(_factory(cursor))

    frontier.mark_failed(
        task_id="t1",
        worker_id="worker-1",
        completed_at=NOW,
        failure_kind=FailureKind.UNKNOWN,
        http_status=None,
        error_message=hostile,
    )

    sql, params = cursor.executed[0]
    stored = next(p for p in params if isinstance(p, str) and p.startswith("x';"))
    assert "DELETE FROM" not in sql
    assert len(stored) == 2000


# Guards that keep the repository honest about its inputs.
def test_importing_the_module_opens_no_connection():
    import subprocess
    import sys

    probe = (
        "import crawler.frontier as f;"
        "assert hasattr(f, 'PostgresCrawlFrontier');"
        "import sys;"
        "assert 'psycopg2' not in sys.modules;"
        "print('CLEAN')"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, timeout=300
    )

    assert "CLEAN" in result.stdout, result.stderr


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"worker_id": "  "}, "worker_id"),
        ({"lease_seconds": 0}, "lease_seconds"),
        ({"limit": 0}, "limit"),
        ({"lease_seconds": True}, "lease_seconds"),
    ],
)
def test_lease_rejects_invalid_arguments(kwargs, message):
    frontier = PostgresCrawlFrontier(_factory(FakeCursor(rows=[])))
    values = {"worker_id": "worker-1", "now": NOW, "lease_seconds": 300, "limit": 10}
    values.update(kwargs)

    with pytest.raises(ValueError, match=message):
        frontier.lease_due(**values)


def test_lease_rejects_a_naive_now():
    frontier = PostgresCrawlFrontier(_factory(FakeCursor(rows=[])))

    with pytest.raises(ValueError, match="timezone-aware"):
        frontier.lease_due(
            worker_id="worker-1",
            now=datetime(2026, 9, 29, 8, 0),
            lease_seconds=300,
            limit=10,
        )


def test_the_factory_must_be_callable():
    with pytest.raises(TypeError, match="callable"):
        PostgresCrawlFrontier(object())
