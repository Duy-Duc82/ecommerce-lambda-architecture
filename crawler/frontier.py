"""PostgreSQL-backed crawl frontier: durable scheduling and leasing.

Importing this module opens no connection. Every method owns one
transaction, and every value reaches SQL as a parameter — a crawl target and
an error message are remote text and are never interpolated into a statement.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from config.marketplace_schema import ResourceType
from crawler.scheduling import (
    CrawlTask,
    CrawlTaskStatus,
    CrawlTier,
    FailureKind,
    RetryDecision,
    make_crawl_task_id,
)

_ERROR_LIMIT = 2000

_COLUMNS = (
    "task_id, marketplace_code, marketplace_id, target, resource_type, tier, "
    "priority, scheduled_for, status, attempts, max_attempts, lease_owner, "
    "lease_expires_at, last_http_status, last_error_kind, last_error, "
    "last_success_at"
)


class LeaseLostError(RuntimeError):
    """A completion was attempted for a task this worker no longer holds."""


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _worker(worker_id: str) -> str:
    if not isinstance(worker_id, str) or not worker_id.strip():
        raise ValueError("worker_id is required")
    return worker_id.strip()


def _truncate(message: str | None) -> str | None:
    if message is None:
        return None
    return str(message)[:_ERROR_LIMIT]


def _row_to_task(row: tuple) -> CrawlTask:
    return CrawlTask(
        task_id=row[0],
        marketplace_code=row[1],
        marketplace_id=row[2],
        target=row[3],
        resource_type=ResourceType(row[4]),
        tier=CrawlTier(row[5]),
        priority=row[6],
        scheduled_for=row[7],
        status=CrawlTaskStatus(row[8]),
        attempts=row[9],
        max_attempts=row[10],
        lease_owner=row[11],
        lease_expires_at=row[12],
        last_http_status=row[13],
        last_error_kind=FailureKind(row[14]) if row[14] else None,
        last_error=row[15],
        last_success_at=row[16],
    )


class PostgresCrawlFrontier:
    def __init__(self, connection_factory: Callable[[], Any]):
        if not callable(connection_factory):
            raise TypeError("connection_factory must be callable")
        self.connection_factory = connection_factory

    def enqueue(self, task: CrawlTask) -> bool:
        """Insert one scheduled occurrence. Returns False if it already existed."""
        if not isinstance(task, CrawlTask):
            raise TypeError("task must be a CrawlTask")
        if task.status is not CrawlTaskStatus.READY:
            raise ValueError("only a READY task can be enqueued")
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO audit.crawl_frontier ({_COLUMNS}) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (task_id) DO NOTHING",
                (
                    task.task_id,
                    task.marketplace_code.lower(),
                    task.marketplace_id,
                    task.target,
                    task.resource_type.value,
                    task.tier.value,
                    task.priority,
                    task.scheduled_for,
                    task.status.value,
                    task.attempts,
                    task.max_attempts,
                    None,
                    None,
                    task.last_http_status,
                    task.last_error_kind.value if task.last_error_kind else None,
                    _truncate(task.last_error),
                    task.last_success_at,
                ),
            )
            return getattr(cur, "rowcount", 0) == 1

    def lease_due(
        self,
        *,
        worker_id: str,
        now: datetime,
        lease_seconds: int,
        limit: int,
    ) -> list[CrawlTask]:
        """Claim up to `limit` due tasks for this worker.

        One statement selects and updates: the CTE takes row locks with
        SKIP LOCKED so a second worker steps over them instead of blocking,
        and the UPDATE increments attempts exactly once per lease.
        """
        worker_id = _worker(worker_id)
        now = _utc(now, "now")
        if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive integer")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be a positive integer")
        expires_at = now + timedelta(seconds=lease_seconds)
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(
                "WITH due AS ("
                "  SELECT task_id FROM audit.crawl_frontier"
                "  WHERE (status IN ('READY', 'RETRY_WAIT') AND scheduled_for <= %s)"
                "     OR (status = 'LEASED' AND lease_expires_at <= %s)"
                "  ORDER BY priority DESC, scheduled_for ASC"
                "  LIMIT %s"
                "  FOR UPDATE SKIP LOCKED"
                ") "
                "UPDATE audit.crawl_frontier AS f "
                "SET status = 'LEASED', lease_owner = %s, lease_expires_at = %s,"
                "    attempts = f.attempts + 1, updated_at = %s "
                "FROM due WHERE f.task_id = due.task_id "
                f"RETURNING {_COLUMNS}",
                (now, now, limit, worker_id, expires_at, now),
            )
            rows = cur.fetchall() or []
        return [_row_to_task(row) for row in rows]

    def mark_succeeded(
        self,
        *,
        task_id: str,
        worker_id: str,
        completed_at: datetime,
        next_scheduled_for: datetime,
    ) -> None:
        """Close this occurrence and enqueue the next one in one transaction."""
        worker_id = _worker(worker_id)
        completed_at = _utc(completed_at, "completed_at")
        next_scheduled_for = _utc(next_scheduled_for, "next_scheduled_for")
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE audit.crawl_frontier SET status = 'SUCCEEDED', lease_owner = NULL,"
                " lease_expires_at = NULL, last_success_at = %s, last_error = NULL,"
                " last_error_kind = NULL, updated_at = %s"
                " WHERE task_id = %s AND status = 'LEASED' AND lease_owner = %s"
                f" RETURNING {_COLUMNS}",
                (completed_at, completed_at, task_id, worker_id),
            )
            row = cur.fetchone() if hasattr(cur, "fetchone") else None
            if getattr(cur, "rowcount", 0) != 1 or row is None:
                raise LeaseLostError(f"task {task_id} is not leased by {worker_id}")
            done = _row_to_task(row)
            follow_up = CrawlTask(
                task_id=make_crawl_task_id(
                    done.marketplace_code,
                    done.resource_type,
                    done.target,
                    next_scheduled_for,
                ),
                marketplace_code=done.marketplace_code,
                marketplace_id=done.marketplace_id,
                target=done.target,
                resource_type=done.resource_type,
                tier=done.tier,
                priority=done.priority,
                scheduled_for=next_scheduled_for,
                status=CrawlTaskStatus.READY,
                attempts=0,
                max_attempts=done.max_attempts,
                last_success_at=completed_at,
            )
            cur.execute(
                f"INSERT INTO audit.crawl_frontier ({_COLUMNS}) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (task_id) DO NOTHING",
                (
                    follow_up.task_id,
                    follow_up.marketplace_code.lower(),
                    follow_up.marketplace_id,
                    follow_up.target,
                    follow_up.resource_type.value,
                    follow_up.tier.value,
                    follow_up.priority,
                    follow_up.scheduled_for,
                    follow_up.status.value,
                    0,
                    follow_up.max_attempts,
                    None,
                    None,
                    None,
                    None,
                    None,
                    completed_at,
                ),
            )

    def mark_retry(
        self,
        *,
        task_id: str,
        worker_id: str,
        decision: RetryDecision,
        http_status: int | None,
        error_message: str,
    ) -> None:
        """Park the same row in RETRY_WAIT until the decided time."""
        worker_id = _worker(worker_id)
        if not isinstance(decision, RetryDecision):
            raise TypeError("decision must be a RetryDecision")
        if not decision.retryable:
            raise ValueError("mark_retry needs a retryable decision")
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE audit.crawl_frontier SET status = 'RETRY_WAIT', lease_owner = NULL,"
                " lease_expires_at = NULL, scheduled_for = %s, last_http_status = %s,"
                " last_error_kind = %s, last_error = %s, updated_at = %s"
                " WHERE task_id = %s AND status = 'LEASED' AND lease_owner = %s",
                (
                    decision.next_attempt_at,
                    http_status,
                    decision.failure_kind.value,
                    _truncate(error_message),
                    decision.next_attempt_at,
                    task_id,
                    worker_id,
                ),
            )
            if getattr(cur, "rowcount", 0) != 1:
                raise LeaseLostError(f"task {task_id} is not leased by {worker_id}")

    def mark_failed(
        self,
        *,
        task_id: str,
        worker_id: str,
        completed_at: datetime,
        failure_kind: FailureKind,
        http_status: int | None,
        error_message: str,
    ) -> None:
        """Close this occurrence as terminal; no further attempt is scheduled."""
        worker_id = _worker(worker_id)
        completed_at = _utc(completed_at, "completed_at")
        if not isinstance(failure_kind, FailureKind):
            raise ValueError("failure_kind must be a FailureKind")
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE audit.crawl_frontier SET status = 'FAILED', lease_owner = NULL,"
                " lease_expires_at = NULL, last_http_status = %s, last_error_kind = %s,"
                " last_error = %s, updated_at = %s"
                " WHERE task_id = %s AND status = 'LEASED' AND lease_owner = %s",
                (
                    http_status,
                    failure_kind.value,
                    _truncate(error_message),
                    completed_at,
                    task_id,
                    worker_id,
                ),
            )
            if getattr(cur, "rowcount", 0) != 1:
                raise LeaseLostError(f"task {task_id} is not leased by {worker_id}")

    def recover_expired_leases(self, *, now: datetime) -> int:
        """Return tasks whose lease outlived their worker to RETRY_WAIT.

        A crashed worker leaves LEASED rows behind; without this a restart
        would never touch them again.
        """
        now = _utc(now, "now")
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE audit.crawl_frontier SET status = 'RETRY_WAIT', lease_owner = NULL,"
                " lease_expires_at = NULL, scheduled_for = %s, updated_at = %s"
                " WHERE status = 'LEASED' AND lease_expires_at <= %s",
                (now, now, now),
            )
            return getattr(cur, "rowcount", 0)
