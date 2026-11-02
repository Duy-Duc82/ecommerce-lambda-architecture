"""Copy the crawl audit from PostgreSQL into the lake, in the layout the crawler writes.

The crawler writes each finished crawl run to the lake itself
(``ecomdata.ingestion.audit.LakeCrawlAuditLog``). Runs crawled before it did, or
whose lake write failed (the worker logs and goes on), exist only in
PostgreSQL; this puts them in the lake. It rewrites a run's object with the same
path and the same content, so running it twice changes nothing.

A run still RUNNING is left out, as the crawler leaves it out: it reaches the
lake when it finishes.
"""
from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from ecomdata.domain.observations import CrawlRun, CrawlRunStatus
from ecomdata.ingestion.audit import CrawlAttemptAudit, LakeCrawlAuditLog
from ecomdata.ingestion.scheduling import FailureKind

_RUNS = (
    "SELECT crawl_run_id, marketplace_id, started_at, completed_at, status, requested, succeeded, failed,"
    " adapter_version, error_summary FROM audit.crawl_run"
    " WHERE completed_at IS NOT NULL AND (%s::timestamptz IS NULL OR started_at >= %s::timestamptz)"
    " ORDER BY started_at"
)
_ATTEMPTS = (
    "SELECT a.crawl_run_id, a.task_id, a.attempt_number, a.started_at, a.completed_at, a.status, a.http_status,"
    " a.latency_ms, a.raw_artifact_id, a.raw_uri, a.raw_bytes, a.parsed_count, a.rejected_count, a.error_kind,"
    " a.error_message, f.marketplace_code, f.target, f.resource_type"
    " FROM audit.crawl_request_attempt a JOIN audit.crawl_run r USING (crawl_run_id)"
    " LEFT JOIN audit.crawl_frontier f USING (task_id)"
    " WHERE r.completed_at IS NOT NULL AND (%s::timestamptz IS NULL OR r.started_at >= %s::timestamptz)"
)


def _summary(value: Any) -> dict | None:
    if value is None or isinstance(value, dict):
        return value
    return json.loads(value)


def export_crawl_audit(
    connection_factory: Callable[[], Any],
    writer: Callable[[str, str, bytes], str],
    *,
    since: datetime | None = None,
) -> dict[str, int]:
    """Write every finished crawl run (started at or after ``since``) with its attempts; the counts written."""
    with connection_factory() as conn, conn.cursor() as cur:
        cur.execute(_RUNS, (since, since))
        runs = cur.fetchall()
        cur.execute(_ATTEMPTS, (since, since))
        attempts = cur.fetchall()

    by_run: dict[str, list[tuple[CrawlAttemptAudit, SimpleNamespace]]] = defaultdict(list)
    for row in attempts:
        (run_id, task_id, number, started, completed, status, http, latency, artifact, uri, size, parsed,
         rejected, kind, message, code, target, resource_type) = row
        attempt = CrawlAttemptAudit(
            crawl_run_id=run_id,
            task_id=task_id,
            attempt_number=number,
            started_at=started,
            completed_at=completed,
            status=status,
            http_status=http,
            latency_ms=latency,
            raw_artifact_id=artifact,
            raw_uri=uri,
            raw_bytes=size or 0,
            parsed_count=parsed or 0,
            rejected_count=rejected or 0,
            error_kind=FailureKind(kind) if kind else None,
            error_message=message,
        )
        by_run[run_id].append((attempt, SimpleNamespace(marketplace_code=code, target=target, resource_type=resource_type)))

    log = LakeCrawlAuditLog(writer)
    written = attempt_count = 0
    for run_id, marketplace_id, started, completed, status, requested, succeeded, failed, adapter, summary in runs:
        run = CrawlRun(
            run_id,
            marketplace_id,
            started,
            completed,
            CrawlRunStatus(status),
            requested=requested,
            succeeded=succeeded,
            failed=failed,
            adapter_version=adapter,
            error_summary=_summary(summary),
        )
        pairs = by_run.get(run_id, [])
        log.write(run, tuple(pairs))
        written += 1
        attempt_count += len(pairs)
    return {"runs": written, "attempts": attempt_count}


__all__ = ["export_crawl_audit"]
