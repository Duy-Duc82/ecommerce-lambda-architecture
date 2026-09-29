"""Crawl run/attempt audit and the persistent source circuit breaker.

Operational evidence only. A raw body, cookie, authorization header or
traceback never reaches PostgreSQL: the attempt row holds counts, coordinates
and a truncated error message, and the body itself already lives in Bronze.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from config.marketplace_schema import CrawlRun
from crawler.scheduling import FailureKind

_ERROR_LIMIT = 2000
_ATTEMPT_STATUS = frozenset({"SUCCEEDED", "PARTIAL", "FAILED"})


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _count(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


def _code(marketplace_code: str) -> str:
    if not isinstance(marketplace_code, str) or not marketplace_code.strip():
        raise ValueError("marketplace_code is required")
    return marketplace_code.strip().lower()


@dataclass(frozen=True)
class CrawlAttemptAudit:
    crawl_run_id: str
    task_id: str
    attempt_number: int
    started_at: datetime
    completed_at: datetime
    status: str
    http_status: int | None = None
    latency_ms: int | None = None
    raw_artifact_id: str | None = None
    raw_uri: str | None = None
    raw_bytes: int = 0
    parsed_count: int = 0
    rejected_count: int = 0
    error_kind: FailureKind | None = None
    error_message: str | None = None

    def __post_init__(self) -> None:
        for name in ("crawl_run_id", "task_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} is required")
        if self.status not in _ATTEMPT_STATUS:
            raise ValueError("status must be SUCCEEDED, PARTIAL or FAILED")
        object.__setattr__(
            self, "attempt_number", _count(self.attempt_number, "attempt_number")
        )
        if self.attempt_number < 1:
            raise ValueError("attempt_number must be at least 1")
        object.__setattr__(self, "started_at", _utc(self.started_at, "started_at"))
        object.__setattr__(self, "completed_at", _utc(self.completed_at, "completed_at"))
        if self.completed_at < self.started_at:
            raise ValueError("completed_at cannot precede started_at")
        for name in ("raw_bytes", "parsed_count", "rejected_count"):
            object.__setattr__(self, name, _count(getattr(self, name), name))
        if self.latency_ms is not None:
            object.__setattr__(self, "latency_ms", _count(self.latency_ms, "latency_ms"))
        if self.http_status is not None:
            status = _count(self.http_status, "http_status")
            if not 100 <= status <= 599:
                raise ValueError("http_status must be a valid HTTP status")
        if self.error_kind is not None and not isinstance(self.error_kind, FailureKind):
            raise ValueError("error_kind must be a FailureKind")
        if self.error_message is not None:
            object.__setattr__(
                self, "error_message", str(self.error_message)[:_ERROR_LIMIT]
            )


class CrawlAuditRepository:
    def __init__(self, connection_factory: Callable[[], Any]):
        if not callable(connection_factory):
            raise TypeError("connection_factory must be callable")
        self.connection_factory = connection_factory

    def start_run(self, run: CrawlRun) -> None:
        if not isinstance(run, CrawlRun):
            raise TypeError("run must be a CrawlRun")
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO audit.crawl_run (crawl_run_id, marketplace_id, started_at,"
                " completed_at, status, requested, succeeded, failed, adapter_version)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                " ON CONFLICT (crawl_run_id) DO NOTHING",
                (
                    run.crawl_run_id,
                    run.marketplace_id,
                    run.started_at,
                    run.completed_at,
                    run.status.value,
                    run.requested,
                    run.succeeded,
                    run.failed,
                    run.adapter_version,
                ),
            )

    def record_attempt(self, attempt: CrawlAttemptAudit) -> None:
        if not isinstance(attempt, CrawlAttemptAudit):
            raise TypeError("attempt must be a CrawlAttemptAudit")
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO audit.crawl_request_attempt (crawl_run_id, task_id,"
                " attempt_number, started_at, completed_at, status, http_status,"
                " latency_ms, raw_artifact_id, raw_uri, raw_bytes, parsed_count,"
                " rejected_count, error_kind, error_message)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    attempt.crawl_run_id,
                    attempt.task_id,
                    attempt.attempt_number,
                    attempt.started_at,
                    attempt.completed_at,
                    attempt.status,
                    attempt.http_status,
                    attempt.latency_ms,
                    attempt.raw_artifact_id,
                    attempt.raw_uri,
                    attempt.raw_bytes,
                    attempt.parsed_count,
                    attempt.rejected_count,
                    attempt.error_kind.value if attempt.error_kind else None,
                    attempt.error_message,
                ),
            )

    def finish_run(self, run: CrawlRun, *, error_summary: dict | None = None) -> None:
        if not isinstance(run, CrawlRun):
            raise TypeError("run must be a CrawlRun")
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE audit.crawl_run SET completed_at = %s, status = %s,"
                " requested = %s, succeeded = %s, failed = %s, error_summary = %s"
                " WHERE crawl_run_id = %s",
                (
                    run.completed_at,
                    run.status.value,
                    run.requested,
                    run.succeeded,
                    run.failed,
                    json.dumps(error_summary, sort_keys=True) if error_summary else None,
                    run.crawl_run_id,
                ),
            )

    def source_is_open(self, marketplace_code: str, now: datetime) -> bool:
        """True while the breaker is holding this source back.

        There is no boolean column: the circuit is open exactly while
        opened_until is still in the future, so a stale row heals by itself.
        """
        code = _code(marketplace_code)
        now = _utc(now, "now")
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT opened_until FROM audit.crawl_source_state WHERE marketplace_code = %s",
                (code,),
            )
            row = cur.fetchone() if hasattr(cur, "fetchone") else None
        if not row or row[0] is None:
            return False
        return _utc(row[0], "opened_until") > now

    def record_source_success(self, marketplace_code: str, at: datetime) -> None:
        """Reset the failure streak and close the circuit."""
        code = _code(marketplace_code)
        at = _utc(at, "at")
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO audit.crawl_source_state (marketplace_code,"
                " consecutive_failures, opened_until, last_success_at, updated_at)"
                " VALUES (%s, 0, NULL, %s, %s)"
                " ON CONFLICT (marketplace_code) DO UPDATE SET consecutive_failures = 0,"
                " opened_until = NULL, last_success_at = EXCLUDED.last_success_at,"
                " updated_at = EXCLUDED.updated_at",
                (code, at, at),
            )

    def record_source_failure(
        self,
        marketplace_code: str,
        at: datetime,
        *,
        threshold: int,
        open_seconds: int,
    ) -> None:
        """Extend the failure streak, opening the circuit at the threshold.

        Only retryable failures reach here. A parser or validation error is a
        data-quality problem with this response, not evidence the source is
        unhealthy, so it must not hold the whole source back.
        """
        code = _code(marketplace_code)
        at = _utc(at, "at")
        if isinstance(threshold, bool) or not isinstance(threshold, int) or threshold < 1:
            raise ValueError("threshold must be a positive integer")
        if isinstance(open_seconds, bool) or not isinstance(open_seconds, int) or open_seconds < 1:
            raise ValueError("open_seconds must be a positive integer")
        opened_until = at + timedelta(seconds=open_seconds)
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO audit.crawl_source_state (marketplace_code,"
                " consecutive_failures, opened_until, last_failure_at, updated_at)"
                " VALUES (%s, 1, NULL, %s, %s)"
                " ON CONFLICT (marketplace_code) DO UPDATE SET"
                " consecutive_failures = audit.crawl_source_state.consecutive_failures + 1,"
                " last_failure_at = EXCLUDED.last_failure_at,"
                " updated_at = EXCLUDED.updated_at,"
                " opened_until = CASE WHEN"
                "   audit.crawl_source_state.consecutive_failures + 1 >= %s"
                "   THEN %s ELSE audit.crawl_source_state.opened_until END",
                (code, at, at, threshold, opened_until),
            )
