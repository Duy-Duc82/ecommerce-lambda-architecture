"""H5: the crawl audit's copy of record in the lake, written by the worker and by ``ops audit-export``."""
from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta

import pytest

from ecomdata.domain import crawl_audit as layout
from ecomdata.domain.observations import CrawlRun, CrawlRunStatus
from ecomdata.ingestion.audit import CrawlAttemptAudit, LakeCrawlAuditLog
from ecomdata.ingestion.scheduling import FailureKind
from ecomdata.ops.audit_export import export_crawl_audit
from tests.unit.ingestion.test_crawl_worker import NOW, FakeAudit, FakeFrontier, FakeReport, _task, _worker


class Store:
    def __init__(self, fail=False):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.fail = fail

    def put(self, zone, path, data):
        if self.fail:
            raise OSError("minio down")
        self.objects[(zone, path)] = data
        return f"s3a://{zone}/{path}"

    def lines(self):
        ((key, data),) = self.objects.items()
        return key, [json.loads(line) for line in gzip.decompress(data).decode("utf-8").splitlines()]


def finished_run(run_id="run-1", status=CrawlRunStatus.COMPLETED):
    return CrawlRun(run_id, "marketplace-tiki", NOW, NOW + timedelta(seconds=2), status, requested=1, succeeded=1,
                    adapter_version="phase-3-worker")  # fmt: skip


def attempt(run_id="run-1", **overrides):
    values = dict(crawl_run_id=run_id, task_id="task-1", attempt_number=1, started_at=NOW,
                  completed_at=NOW + timedelta(seconds=2), status="SUCCEEDED", http_status=200, latency_ms=2000,
                  parsed_count=40)  # fmt: skip
    values.update(overrides)
    return CrawlAttemptAudit(**values)


def test_every_line_field_has_one_type_and_kind_comes_first():
    names = [name for name, _ in layout.line_fields()]
    assert names[:2] == ["kind", "schema_version"]
    assert len(names) == len(set(names))
    assert {name for name, _ in layout.ATTEMPT_FIELDS} >= {"marketplace_code", "request_target", "resource_type"}


def test_the_log_writes_one_object_per_run_under_its_start_day_and_hour():
    store = Store()

    uri = LakeCrawlAuditLog(store.put).write(finished_run(), ((attempt(), _task()),))

    (zone, path), lines = store.lines()
    assert zone == "bronze"
    assert path == "marketplace/crawl_audit/observed_date=2026-09-29/hour=08/crawl_run_id=run-1.jsonl.gz"
    assert uri.endswith(path)
    run_line, attempt_line = lines
    assert run_line["kind"] == "run" and run_line["status"] == "COMPLETED"
    assert run_line["completed_at"] == "2026-09-29T08:00:02+00:00"
    assert attempt_line["kind"] == "attempt"
    assert (attempt_line["marketplace_code"], attempt_line["request_target"], attempt_line["resource_type"]) == (
        "tiki", "1846:1", "LISTING_PAGE",
    )  # fmt: skip
    assert attempt_line["parsed_count"] == 40 and attempt_line["error_kind"] is None
    # Every line holds only declared fields.
    declared = {name for name, _ in layout.line_fields()}
    assert set(run_line) <= declared and set(attempt_line) <= declared


def test_writing_a_run_again_gives_the_same_object():
    store = Store()
    log = LakeCrawlAuditLog(store.put)
    log.write(finished_run(), ((attempt(), _task()),))
    first = dict(store.objects)

    log.write(finished_run(), ((attempt(), _task()),))

    assert store.objects == first


def test_a_run_still_running_is_not_written():
    running = CrawlRun("run-1", "marketplace-tiki", NOW, None, CrawlRunStatus.RUNNING, requested=1, adapter_version="x")

    with pytest.raises(ValueError, match="finished"):
        LakeCrawlAuditLog(Store().put).write(running, ())


def test_the_worker_writes_each_finished_task_to_the_lake():
    store = Store()
    worker = _worker(FakeFrontier(tasks=[_task()]), FakeAudit(), lambda *, task, crawl_run_id: FakeReport(records=(1, 2)),
                     audit_log=LakeCrawlAuditLog(store.put))  # fmt: skip

    worker.run_once(crawl_run_id_for=lambda task: "run-1")

    _, (run_line, attempt_line) = store.lines()
    assert run_line["status"] == "COMPLETED" and attempt_line["parsed_count"] == 2


def test_a_failed_task_reaches_the_lake_with_its_error_kind():
    from ecomdata.domain.acquisition import HttpResponseError

    store = Store()

    def boom(*, task, crawl_run_id):
        raise HttpResponseError(503)

    worker = _worker(FakeFrontier(tasks=[_task()]), FakeAudit(), boom, audit_log=LakeCrawlAuditLog(store.put))
    worker.run_once(crawl_run_id_for=lambda task: "run-1")

    _, (run_line, attempt_line) = store.lines()
    assert run_line["status"] == "PARTIAL"
    assert (attempt_line["status"], attempt_line["error_kind"], attempt_line["http_status"]) == (
        "FAILED", "SERVER_ERROR", 503,
    )  # fmt: skip


def test_a_lake_write_that_fails_does_not_fail_the_task():
    frontier, audit = FakeFrontier(tasks=[_task()]), FakeAudit()
    worker = _worker(frontier, audit, lambda *, task, crawl_run_id: FakeReport(),
                     audit_log=LakeCrawlAuditLog(Store(fail=True).put))  # fmt: skip

    result = worker.run_once(crawl_run_id_for=lambda task: "run-1")

    assert result.succeeded == 1
    assert "succeeded" in frontier.kinds() and "finish_run" in audit.kinds()


class Cursor:
    def __init__(self, results):
        self.results = list(results)
        self.queries = []

    def execute(self, sql, params):
        self.queries.append((sql, params))

    def fetchall(self):
        return self.results.pop(0)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_the_export_copies_postgres_runs_and_attempts_in_the_crawler_layout():
    done = NOW + timedelta(seconds=2)
    runs = [("run-1", "marketplace-tiki", NOW, done, "PARTIAL", 1, 0, 1, "phase-3-worker", {"x": 1})]
    attempts = [
        ("run-1", "task-1", 1, NOW, done, "FAILED", 503, 2000, None, None, 0, 0, 0, "SERVER_ERROR", "503",
         "tiki", "1846:1", "LISTING_PAGE"),
    ]  # fmt: skip
    cursor = Cursor([runs, attempts])
    store = Store()

    counts = export_crawl_audit(lambda: Connection(cursor), store.put, since=datetime(2026, 9, 1, tzinfo=UTC))

    assert counts == {"runs": 1, "attempts": 1}
    _, (run_line, attempt_line) = store.lines()
    assert run_line["error_summary"] == '{"x": 1}'
    assert attempt_line["error_kind"] == FailureKind.SERVER_ERROR.value
    assert attempt_line["marketplace_code"] == "tiki"

    # The worker writes the same bytes for the same run.
    worker_store = Store()
    LakeCrawlAuditLog(worker_store.put).write(
        CrawlRun("run-1", "marketplace-tiki", NOW, done, CrawlRunStatus.PARTIAL, requested=1, failed=1,
                 adapter_version="phase-3-worker", error_summary={"x": 1}),
        ((attempt(status="FAILED", http_status=503, parsed_count=0, error_kind=FailureKind.SERVER_ERROR,
                  error_message="503"), _task()),),
    )  # fmt: skip
    assert attempt_line == worker_store.lines()[1][1] | {"task_id": "task-1"}
