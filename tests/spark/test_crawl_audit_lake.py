"""H5: the batch reads the crawl audit from the lake and builds the same marts as from PostgreSQL.

The PostgreSQL side is the frames ``read_crawl_audit`` hands the marts (the
real column sets, attempts resolved through the frontier); the lake side is
what the crawler's ``LakeCrawlAuditLog`` writes for the same runs, read back by
``read_crawl_audit_lake``.
"""
from __future__ import annotations

import gzip
from datetime import date
from types import SimpleNamespace

import pytest

from ecomdata.domain.observations import CrawlRun, CrawlRunStatus
from ecomdata.ingestion.audit import CrawlAttemptAudit, LakeCrawlAuditLog
from ecomdata.ingestion.scheduling import FailureKind
from ecomdata.processing.batch.facts import build_fact_crawl_attempt
from ecomdata.processing.batch.lake import crawl_audit_from_lines
from ecomdata.processing.batch.marts import build_crawl_reliability_daily, build_source_coverage_daily
from tests.spark.test_marketplace_marts import AS_OF, _audit, observation, observations
from tests.spark_support import requires_spark

pytestmark = requires_spark

ROWS = [
    ("run-1", "tiki", date(2026, 9, 4), "SUCCEEDED", 100, 10, 7, 3, None),
    ("run-2", "tiki", date(2026, 9, 4), "FAILED", 200, 0, 0, 2, "SERVER_ERROR"),
    ("run-3", "lazada", date(2026, 9, 4), "SUCCEEDED", 300, 20, 5, 0, None),
]


class Store:
    """The objects the log writes; the host Spark has no filesystem, so their lines are handed to Spark."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put(self, zone, path, data):
        self.objects[path] = data
        return f"memory://{zone}/{path}"

    def lines(self, spark):
        text = [line for data in self.objects.values() for line in gzip.decompress(data).decode("utf-8").splitlines()]
        return spark.createDataFrame([(line,) for line in text], "value string")


def write_to_lake(store: Store, postgres_attempts, postgres_runs) -> None:
    """Write what PostgreSQL holds the way the crawler writes it."""
    log = LakeCrawlAuditLog(store.put)
    runs = {row["crawl_run_id"]: row for row in postgres_runs.collect()}
    for row in postgres_attempts.collect():
        run = runs[row["crawl_run_id"]]
        attempt = CrawlAttemptAudit(
            crawl_run_id=row["crawl_run_id"],
            task_id=row["task_id"],
            attempt_number=row["attempt_number"],
            started_at=row["started_at"].astimezone(),
            completed_at=row["completed_at"].astimezone(),
            status=row["status"],
            http_status=row["http_status"],
            latency_ms=row["latency_ms"],
            raw_artifact_id=row["raw_artifact_id"],
            raw_uri=row["raw_uri"],
            raw_bytes=row["raw_bytes"],
            parsed_count=row["parsed_count"],
            rejected_count=row["rejected_count"],
            error_kind=FailureKind(row["error_kind"]) if row["error_kind"] else None,
        )
        task = SimpleNamespace(
            marketplace_code=row["marketplace_code"], target=row["request_target"], resource_type=row["resource_type"]
        )
        finished = CrawlRun(
            run["crawl_run_id"],
            run["marketplace_id"],
            run["started_at"].astimezone(),
            run["completed_at"].astimezone(),
            CrawlRunStatus(run["status"]),
            requested=run["requested"],
            succeeded=run["succeeded"],
            failed=run["failed"],
            adapter_version=run["adapter_version"],
        )
        log.write(finished, ((attempt, task),))


def rows(frame):
    columns = sorted(frame.columns)
    return sorted((tuple(row[c] for c in columns) for row in frame.select(*columns).collect()), key=repr)


@pytest.fixture
def both(spark):
    postgres = _audit(spark, ROWS)
    store = Store()
    write_to_lake(store, *postgres)
    assert len(store.objects) == 3
    return postgres, crawl_audit_from_lines(store.lines(spark))


def test_the_lake_audit_reads_back_every_run_and_attempt(both):
    (pg_attempts, pg_runs), (attempts, runs) = both

    assert attempts.count() == pg_attempts.count() == 3
    assert runs.count() == pg_runs.count() == 3
    assert {row["marketplace_code"] for row in attempts.collect()} == {"tiki", "lazada"}
    assert dict(attempts.dtypes)["started_at"] == "timestamp"


def test_crawl_reliability_from_the_lake_matches_postgres(both):
    (pg_attempts, pg_runs), (attempts, runs) = both

    assert rows(build_crawl_reliability_daily(attempts, runs)) == rows(build_crawl_reliability_daily(pg_attempts, pg_runs))


def test_source_coverage_from_the_lake_matches_postgres(spark, both):
    (pg_attempts, pg_runs), (attempts, runs) = both
    frame = observations(spark, [observation(offer="offer-1", minute=0), observation(offer="offer-2", minute=0)])

    def coverage(a, r):
        return build_source_coverage_daily(frame, a, r, as_of=AS_OF, stale_after_seconds=3600, rule_version="v1")

    assert rows(coverage(attempts, runs)) == rows(coverage(pg_attempts, pg_runs))


def test_fact_crawl_attempt_from_the_lake_matches_postgres(both):
    (pg_attempts, _), (attempts, _) = both

    assert rows(build_fact_crawl_attempt(attempts)) == rows(build_fact_crawl_attempt(pg_attempts))


def test_a_lake_without_audit_objects_reads_as_no_rows(spark):
    attempts, runs = crawl_audit_from_lines(spark.createDataFrame([], "value string"))

    assert attempts.count() == 0 and runs.count() == 0
    assert "marketplace_code" in attempts.columns
