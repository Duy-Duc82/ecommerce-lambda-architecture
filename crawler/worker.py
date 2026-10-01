"""The scheduled crawl worker: one polling cycle, no source-specific parsing.

The worker owns no clock and no acquisition code of its own. Both are
injected, which is what lets a full cycle — lease, acquire, audit, retry,
circuit — run offline in a unit test with zero sleep.

No database transaction is held open across acquisition: the frontier and
audit calls each own their transaction, and the network call happens between
them.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Protocol

from config.marketplace_schema import CrawlRun, CrawlRunStatus
from crawler.audit import CrawlAttemptAudit, CrawlAuditRepository
from crawler.contracts import AcquisitionStatus, ObservationPublishError, Phase2AcquisitionReport
from crawler.frontier import LeaseLostError, PostgresCrawlFrontier
from crawler.scheduling import (
    CrawlTask,
    FailureKind,
    RetryPolicy,
    classify_failure,
    decide_retry,
    next_scheduled_for,
)

# A parser or validation failure says this response was bad, not that the
# source is down, so it never counts towards opening the circuit.
_CIRCUIT_KINDS = frozenset(
    {
        FailureKind.RATE_LIMITED,
        FailureKind.TRANSIENT_NETWORK,
        FailureKind.SERVER_ERROR,
        FailureKind.STORAGE_ERROR,
    }
)


class AcquisitionExecutor(Protocol):
    def __call__(
        self, *, task: CrawlTask, crawl_run_id: str
    ) -> Phase2AcquisitionReport: ...


@dataclass(frozen=True)
class CycleResult:
    leased: int
    succeeded: int
    retried: int
    failed: int
    skipped_circuit_open: int
    recovered_leases: int


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _retry_after(report: Phase2AcquisitionReport | None, error: Exception) -> str | None:
    if report is not None and getattr(report, "retry_after", None):
        return report.retry_after
    return getattr(error, "retry_after", None)


def _http_status(report: Phase2AcquisitionReport | None, error: Exception) -> int | None:
    if report is not None and getattr(report, "http_status", None) is not None:
        return report.http_status
    return getattr(error, "status_code", None)


class CrawlWorker:
    def __init__(
        self,
        *,
        frontier: PostgresCrawlFrontier,
        audit: CrawlAuditRepository,
        executor: AcquisitionExecutor,
        clock: Callable[[], datetime],
        policy: RetryPolicy,
        worker_id: str,
        lease_seconds: int,
        batch_size: int,
        circuit_threshold: int,
        circuit_open_seconds: int,
        jitter: Callable[[], Decimal] = lambda: Decimal("0.5"),
    ):
        if not callable(executor):
            raise TypeError("executor must be callable")
        if not callable(clock):
            raise TypeError("clock must be callable")
        if not isinstance(policy, RetryPolicy):
            raise TypeError("policy must be a RetryPolicy")
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValueError("worker_id is required")
        self.frontier = frontier
        self.audit = audit
        self.executor = executor
        self.clock = clock
        self.policy = policy
        self.worker_id = worker_id.strip()
        self.lease_seconds = lease_seconds
        self.batch_size = batch_size
        self.circuit_threshold = circuit_threshold
        self.circuit_open_seconds = circuit_open_seconds
        self.jitter = jitter

    def run_once(self, *, crawl_run_id_for: Callable[[CrawlTask], str]) -> CycleResult:
        """Run one polling cycle. Never sleeps; an outer CLI owns the interval."""
        now = _utc(self.clock(), "clock")
        recovered = self.frontier.recover_expired_leases(now=now)
        tasks = self.frontier.lease_due(
            worker_id=self.worker_id,
            now=now,
            lease_seconds=self.lease_seconds,
            limit=self.batch_size,
        )
        succeeded = retried = failed = skipped = 0
        for task in tasks:
            # One task's failure must not abandon the rest of the batch.
            try:
                outcome = self._run_task(task, crawl_run_id_for(task))
            except LeaseLostError:
                continue
            if outcome == "SKIPPED":
                skipped += 1
            elif outcome == "SUCCEEDED":
                succeeded += 1
            elif outcome == "RETRY":
                retried += 1
            else:
                failed += 1
        return CycleResult(
            leased=len(tasks),
            succeeded=succeeded,
            retried=retried,
            failed=failed,
            skipped_circuit_open=skipped,
            recovered_leases=recovered,
        )

    def _run_task(self, task: CrawlTask, crawl_run_id: str) -> str:
        started_at = _utc(self.clock(), "clock")
        if self.audit.source_is_open(task.marketplace_code, started_at):
            # Hand the task back untouched: a held-back source is not a failure
            # of this task, so neither its attempts nor the source streak move.
            self.frontier.mark_retry(
                task_id=task.task_id,
                worker_id=self.worker_id,
                decision=self._circuit_decision(started_at),
                http_status=None,
                error_message="source circuit open",
            )
            return "SKIPPED"

        run = CrawlRun(
            crawl_run_id,
            task.marketplace_id,
            started_at,
            None,
            CrawlRunStatus.RUNNING,
            requested=1,
            adapter_version="phase-3-worker",
        )
        self.audit.start_run(run)

        report: Phase2AcquisitionReport | None = None
        error: Exception | None = None
        try:
            report = self.executor(task=task, crawl_run_id=crawl_run_id)
        except Exception as exc:  # the taxonomy decides what this means
            error = exc

        completed_at = _utc(self.clock(), "clock")
        if error is None and report is not None and report.status is not AcquisitionStatus.FAILED:
            return self._on_success(task, run, report, started_at, completed_at)
        return self._on_failure(task, run, report, error, started_at, completed_at)

    def _circuit_decision(self, now: datetime):
        from crawler.scheduling import RetryDecision

        return RetryDecision(
            True,
            FailureKind.RATE_LIMITED,
            self.circuit_open_seconds,
            now.fromtimestamp(
                now.timestamp() + self.circuit_open_seconds, tz=timezone.utc
            ),
        )

    def _on_success(self, task, run, report, started_at, completed_at) -> str:
        self.audit.record_attempt(
            CrawlAttemptAudit(
                crawl_run_id=run.crawl_run_id,
                task_id=task.task_id,
                attempt_number=task.attempts,
                started_at=started_at,
                completed_at=completed_at,
                status=report.status.value,
                http_status=report.http_status,
                latency_ms=int((completed_at - started_at).total_seconds() * 1000),
                raw_artifact_id=getattr(report.raw_artifact, "raw_artifact_id", None),
                raw_uri=getattr(report.raw_artifact, "raw_uri", None),
                raw_bytes=getattr(report.raw_artifact, "raw_bytes", 0) or 0,
                parsed_count=len(report.observations),
                rejected_count=len(report.rejections),
            )
        )
        self.frontier.mark_succeeded(
            task_id=task.task_id,
            worker_id=self.worker_id,
            completed_at=completed_at,
            next_scheduled_for=next_scheduled_for(task.tier, completed_at),
        )
        self.audit.record_source_success(task.marketplace_code, completed_at)
        self._finish(run, completed_at, CrawlRunStatus.COMPLETED, succeeded=1, failed=0)
        return "SUCCEEDED"

    def _on_failure(self, task, run, report, error, started_at, completed_at) -> str:
        error = error or RuntimeError(
            getattr(getattr(report, "failure", None), "message", "acquisition failed")
        )
        parsed_count = len(report.observations) if report else 0
        if isinstance(error, ObservationPublishError):
            # The fetch, raw write and parse all happened; only publication
            # failed part-way. The report rides on the error so the audit can
            # still name the raw artifact, and only the observations Kafka
            # acknowledged count as parsed: nothing else can reach Silver, and
            # Phase 7 check 8 reconciles exactly this number against it.
            report = error.report
            parsed_count = error.acknowledged
        http_status = _http_status(report, error)
        kind = classify_failure(error, http_status)
        decision = decide_retry(
            error=error,
            http_status=http_status,
            retry_after=_retry_after(report, error),
            attempts=task.attempts,
            now=completed_at,
            policy=self.policy,
            jitter_unit=self.jitter(),
        )
        self.audit.record_attempt(
            CrawlAttemptAudit(
                crawl_run_id=run.crawl_run_id,
                task_id=task.task_id,
                attempt_number=task.attempts,
                started_at=started_at,
                completed_at=completed_at,
                status="FAILED",
                http_status=http_status,
                latency_ms=int((completed_at - started_at).total_seconds() * 1000),
                raw_artifact_id=getattr(getattr(report, "raw_artifact", None), "raw_artifact_id", None),
                raw_uri=getattr(getattr(report, "raw_artifact", None), "raw_uri", None),
                parsed_count=parsed_count,
                rejected_count=len(report.rejections) if report else 0,
                error_kind=kind,
                error_message=str(error),
            )
        )
        if kind in _CIRCUIT_KINDS:
            self.audit.record_source_failure(
                task.marketplace_code,
                completed_at,
                threshold=self.circuit_threshold,
                open_seconds=self.circuit_open_seconds,
            )
        if decision.retryable:
            self.frontier.mark_retry(
                task_id=task.task_id,
                worker_id=self.worker_id,
                decision=decision,
                http_status=http_status,
                error_message=str(error),
            )
            self._finish(run, completed_at, CrawlRunStatus.PARTIAL, succeeded=0, failed=1)
            return "RETRY"
        self.frontier.mark_failed(
            task_id=task.task_id,
            worker_id=self.worker_id,
            completed_at=completed_at,
            failure_kind=kind,
            http_status=http_status,
            error_message=str(error),
        )
        self._finish(run, completed_at, CrawlRunStatus.FAILED, succeeded=0, failed=1)
        return "FAILED"

    def _finish(self, run, completed_at, status, *, succeeded, failed) -> None:
        self.audit.finish_run(
            CrawlRun(
                run.crawl_run_id,
                run.marketplace_id,
                run.started_at,
                completed_at,
                status,
                requested=1,
                succeeded=succeeded,
                failed=failed,
                adapter_version=run.adapter_version,
            )
        )
