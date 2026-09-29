"""Audit and worker tests, items 22-30 of the Phase 3 plan section 12.

Fake repositories and a fake executor throughout: no network, Kafka, MinIO
or PostgreSQL, and no sleep.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from config.marketplace_schema import CrawlRun, CrawlRunStatus, ResourceType
from crawler.audit import CrawlAttemptAudit
from crawler.contracts import (
    AcquisitionStatus,
    CanonicalRecordError,
    FetchTransportError,
    HttpResponseError,
    ListingPageParseError,
    RawPersistenceError,
)
from crawler.scheduling import (
    CrawlTask,
    CrawlTaskStatus,
    CrawlTier,
    FailureKind,
    RetryPolicy,
    cadence_minutes,
    make_crawl_task_id,
)
from crawler.worker import CrawlWorker

NOW = datetime(2026, 9, 29, 8, 0, tzinfo=timezone.utc)
POLICY = RetryPolicy(5, 30, 1800, Decimal("0"))


def _task(attempts=1, **overrides):
    values = {
        "task_id": make_crawl_task_id("tiki", ResourceType.LISTING_PAGE, "1846:1", NOW),
        "marketplace_code": "tiki",
        "marketplace_id": "marketplace-tiki",
        "target": "1846:1",
        "resource_type": ResourceType.LISTING_PAGE,
        "tier": CrawlTier.NORMAL,
        "priority": 10,
        "scheduled_for": NOW,
        "status": CrawlTaskStatus.LEASED,
        "attempts": attempts,
        "max_attempts": 5,
        "lease_owner": "worker-1",
        "lease_expires_at": NOW + timedelta(seconds=300),
    }
    values.update(overrides)
    return CrawlTask(**values)


class FakeReport:
    """Stands in for Phase2AcquisitionReport with only what the worker reads."""

    def __init__(self, status=AcquisitionStatus.SUCCEEDED, http_status=200,
                 observations=(), rejections=(), retry_after=None, failure=None):
        self.status = status
        self.http_status = http_status
        self.observations = observations
        self.rejections = rejections
        self.retry_after = retry_after
        self.failure = failure
        self.raw_artifact = None


class FakeFrontier:
    def __init__(self, tasks=(), recovered=0):
        self.tasks = list(tasks)
        self.recovered = recovered
        self.calls = []

    def recover_expired_leases(self, *, now):
        self.calls.append(("recover", now))
        return self.recovered

    def lease_due(self, *, worker_id, now, lease_seconds, limit):
        self.calls.append(("lease", worker_id, limit))
        return self.tasks

    def mark_succeeded(self, *, task_id, worker_id, completed_at, next_scheduled_for):
        self.calls.append(("succeeded", task_id, next_scheduled_for))

    def mark_retry(self, *, task_id, worker_id, decision, http_status, error_message):
        self.calls.append(("retry", task_id, decision.failure_kind, error_message))

    def mark_failed(self, *, task_id, worker_id, completed_at, failure_kind,
                    http_status, error_message):
        self.calls.append(("failed", task_id, failure_kind, error_message))

    def kinds(self):
        return [call[0] for call in self.calls]


class FakeAudit:
    def __init__(self, circuit_open=False):
        self.circuit_open = circuit_open
        self.calls = []
        self.attempts = []

    def source_is_open(self, marketplace_code, now):
        self.calls.append(("source_is_open", marketplace_code))
        return self.circuit_open

    def start_run(self, run):
        self.calls.append(("start_run", run.crawl_run_id, run.status))

    def record_attempt(self, attempt):
        self.calls.append(("attempt", attempt.status))
        self.attempts.append(attempt)

    def finish_run(self, run, **kwargs):
        self.calls.append(("finish_run", run.crawl_run_id, run.status))

    def record_source_success(self, marketplace_code, at):
        self.calls.append(("source_success", marketplace_code))

    def record_source_failure(self, marketplace_code, at, *, threshold, open_seconds):
        self.calls.append(("source_failure", marketplace_code, threshold))

    def kinds(self):
        return [call[0] for call in self.calls]


def _worker(frontier, audit, executor, clock=None):
    times = iter([NOW, NOW, NOW + timedelta(seconds=2)] * 20)
    return CrawlWorker(
        frontier=frontier,
        audit=audit,
        executor=executor,
        clock=clock or (lambda: next(times)),
        policy=POLICY,
        worker_id="worker-1",
        lease_seconds=300,
        batch_size=10,
        circuit_threshold=5,
        circuit_open_seconds=900,
        jitter=lambda: Decimal("0.5"),
    )


def _run(frontier, audit, executor):
    return _worker(frontier, audit, executor).run_once(
        crawl_run_id_for=lambda task: f"run-{task.task_id[:8]}"
    )


# 22
def test_attempt_audit_rejects_naive_times_negative_counts_and_reversed_times():
    base = {
        "crawl_run_id": "run-1",
        "task_id": "t1",
        "attempt_number": 1,
        "started_at": NOW,
        "completed_at": NOW + timedelta(seconds=1),
        "status": "SUCCEEDED",
    }

    with pytest.raises(ValueError, match="timezone-aware"):
        CrawlAttemptAudit(**{**base, "started_at": datetime(2026, 9, 29, 8, 0)})
    with pytest.raises(ValueError, match="cannot precede"):
        CrawlAttemptAudit(**{**base, "completed_at": NOW - timedelta(seconds=1)})
    with pytest.raises(ValueError, match="parsed_count"):
        CrawlAttemptAudit(**{**base, "parsed_count": -1})
    with pytest.raises(ValueError, match="status"):
        CrawlAttemptAudit(**{**base, "status": "DONE"})
    with pytest.raises(ValueError, match="attempt_number"):
        CrawlAttemptAudit(**{**base, "attempt_number": 0})
    with pytest.raises(ValueError, match="http_status"):
        CrawlAttemptAudit(**{**base, "http_status": 99})


def test_attempt_audit_truncates_an_oversized_error_message():
    attempt = CrawlAttemptAudit(
        crawl_run_id="run-1",
        task_id="t1",
        attempt_number=1,
        started_at=NOW,
        completed_at=NOW,
        status="FAILED",
        error_kind=FailureKind.SERVER_ERROR,
        error_message="x" * 5000,
    )

    assert len(attempt.error_message) == 2000


# 23
def test_a_successful_execution_writes_counts_and_completes_the_run():
    frontier = FakeFrontier(tasks=[_task()])
    audit = FakeAudit()
    report = FakeReport(observations=(1, 2, 3), rejections=(4,))

    result = _run(frontier, audit, lambda *, task, crawl_run_id: report)

    assert result.succeeded == 1
    assert result.failed == 0
    attempt = audit.attempts[0]
    assert attempt.status == "SUCCEEDED"
    assert attempt.parsed_count == 3
    assert attempt.rejected_count == 1
    assert attempt.latency_ms == 2000
    assert ("finish_run", "run-" + _task().task_id[:8], CrawlRunStatus.COMPLETED) in audit.calls
    assert "source_success" in audit.kinds()


def test_success_schedules_the_next_occurrence_one_cadence_later():
    frontier = FakeFrontier(tasks=[_task()])

    _run(frontier, FakeAudit(), lambda *, task, crawl_run_id: FakeReport())

    call = next(c for c in frontier.calls if c[0] == "succeeded")
    completed_at = NOW + timedelta(seconds=2)
    assert call[2] == completed_at + timedelta(minutes=cadence_minutes(CrawlTier.NORMAL))


# 24
@pytest.mark.parametrize(
    "error",
    [
        RawPersistenceError("minio down", write_stage="BODY"),
        FetchTransportError("connection reset"),
        HttpResponseError(503),
    ],
)
def test_storage_and_network_failures_follow_the_retry_policy(error):
    frontier = FakeFrontier(tasks=[_task()])
    audit = FakeAudit()

    def boom(*, task, crawl_run_id):
        raise error

    result = _run(frontier, audit, boom)

    assert result.retried == 1
    assert "retry" in frontier.kinds()
    assert "source_failure" in audit.kinds()
    assert audit.attempts[0].status == "FAILED"


def test_an_exhausted_task_becomes_terminal_instead_of_retrying():
    frontier = FakeFrontier(tasks=[_task(attempts=POLICY.max_attempts)])
    audit = FakeAudit()

    def boom(*, task, crawl_run_id):
        raise HttpResponseError(503)

    result = _run(frontier, audit, boom)

    assert result.failed == 1
    assert "failed" in frontier.kinds()
    # The source is still unhealthy even though this task has run out of tries.
    assert "source_failure" in audit.kinds()


# 25
@pytest.mark.parametrize(
    "error, kind",
    [
        (ListingPageParseError("schema drift"), FailureKind.PARSE_ERROR),
        (CanonicalRecordError("bad price"), FailureKind.VALIDATION_ERROR),
    ],
)
def test_parser_and_validation_failures_are_terminal_and_spare_the_circuit(error, kind):
    frontier = FakeFrontier(tasks=[_task()])
    audit = FakeAudit()

    def boom(*, task, crawl_run_id):
        raise error

    result = _run(frontier, audit, boom)

    assert result.failed == 1
    failed = next(c for c in frontier.calls if c[0] == "failed")
    assert failed[2] is kind
    assert "source_failure" not in audit.kinds()


# 26
def test_repeated_retryable_failures_reach_the_circuit_threshold():
    audit = FakeAudit()
    for _ in range(5):
        frontier = FakeFrontier(tasks=[_task()])

        def boom(*, task, crawl_run_id):
            raise HttpResponseError(503)

        _run(frontier, audit, boom)

    failures = [c for c in audit.calls if c[0] == "source_failure"]
    assert len(failures) == 5
    assert {call[2] for call in failures} == {5}


# 27
def test_a_source_success_records_the_reset():
    frontier = FakeFrontier(tasks=[_task()])
    audit = FakeAudit()

    _run(frontier, audit, lambda *, task, crawl_run_id: FakeReport())

    assert ("source_success", "tiki") in audit.calls


# 28
def test_an_open_circuit_prevents_the_acquisition_call():
    frontier = FakeFrontier(tasks=[_task()])
    audit = FakeAudit(circuit_open=True)
    called = []

    def executor(*, task, crawl_run_id):
        called.append(task)
        return FakeReport()

    result = _run(frontier, audit, executor)

    assert called == []
    assert result.skipped_circuit_open == 1
    assert result.succeeded == 0
    assert frontier.kinds() == ["recover", "lease", "retry"]
    assert "start_run" not in audit.kinds()
    assert "source_failure" not in audit.kinds()


# 29
def test_one_task_failure_does_not_prevent_another_leased_task_from_running():
    good = _task()
    bad = _task(
        task_id=make_crawl_task_id("tiki", ResourceType.LISTING_PAGE, "1846:2", NOW),
        target="1846:2",
    )
    frontier = FakeFrontier(tasks=[bad, good])
    audit = FakeAudit()

    def executor(*, task, crawl_run_id):
        if task.target == "1846:2":
            raise HttpResponseError(503)
        return FakeReport()

    result = _run(frontier, audit, executor)

    assert result.leased == 2
    assert result.succeeded == 1
    assert result.retried == 1
    assert "succeeded" in frontier.kinds()
    assert "retry" in frontier.kinds()


def test_a_lost_lease_skips_that_task_and_keeps_the_batch_going():
    class LosingFrontier(FakeFrontier):
        def mark_succeeded(self, **kwargs):
            from crawler.frontier import LeaseLostError

            self.calls.append(("succeeded-lost", kwargs["task_id"]))
            raise LeaseLostError("stolen")

    frontier = LosingFrontier(tasks=[_task()])
    audit = FakeAudit()

    result = _run(frontier, audit, lambda *, task, crawl_run_id: FakeReport())

    assert result.leased == 1
    assert result.succeeded == 0


def test_expired_leases_are_recovered_before_leasing():
    frontier = FakeFrontier(tasks=[], recovered=4)

    result = _run(frontier, FakeAudit(), lambda *, task, crawl_run_id: FakeReport())

    assert result.recovered_leases == 4
    assert frontier.kinds()[:2] == ["recover", "lease"]


def test_a_report_marked_failed_is_treated_as_a_failure():
    frontier = FakeFrontier(tasks=[_task()])
    audit = FakeAudit()
    report = FakeReport(status=AcquisitionStatus.FAILED, http_status=500)

    result = _run(frontier, audit, lambda *, task, crawl_run_id: report)

    assert result.succeeded == 0
    assert result.retried == 1


def test_a_partial_report_still_counts_as_progress():
    frontier = FakeFrontier(tasks=[_task()])
    audit = FakeAudit()
    report = FakeReport(status=AcquisitionStatus.PARTIAL, observations=(1,))

    result = _run(frontier, audit, lambda *, task, crawl_run_id: report)

    assert result.succeeded == 1
    assert audit.attempts[0].status == "PARTIAL"


def test_a_retry_after_header_from_the_report_reaches_the_decision():
    frontier = FakeFrontier(tasks=[_task()])
    audit = FakeAudit()

    def boom(*, task, crawl_run_id):
        raise HttpResponseError(429, "45")

    _run(frontier, audit, boom)

    retry = next(c for c in frontier.calls if c[0] == "retry")
    assert retry[2] is FailureKind.RATE_LIMITED


# 30
def test_no_default_test_opens_network_kafka_minio_or_postgresql():
    import subprocess
    import sys

    probe = (
        "import crawler.worker, crawler.frontier, crawler.audit;"
        "import sys;"
        "assert 'psycopg2' not in sys.modules;"
        "assert 'kafka' not in sys.modules;"
        "assert 'minio' not in sys.modules;"
        "assert 'requests' not in sys.modules;"
        "print('CLEAN')"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, timeout=300
    )

    assert "CLEAN" in result.stdout, result.stderr


def test_the_worker_validates_its_collaborators():
    frontier, audit = FakeFrontier(), FakeAudit()
    base = {
        "frontier": frontier,
        "audit": audit,
        "executor": lambda **kwargs: None,
        "clock": lambda: NOW,
        "policy": POLICY,
        "worker_id": "worker-1",
        "lease_seconds": 300,
        "batch_size": 10,
        "circuit_threshold": 5,
        "circuit_open_seconds": 900,
    }

    with pytest.raises(TypeError, match="executor"):
        CrawlWorker(**{**base, "executor": "not callable"})
    with pytest.raises(TypeError, match="clock"):
        CrawlWorker(**{**base, "clock": None})
    with pytest.raises(TypeError, match="policy"):
        CrawlWorker(**{**base, "policy": object()})
    with pytest.raises(ValueError, match="worker_id"):
        CrawlWorker(**{**base, "worker_id": "  "})
