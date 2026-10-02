"""The long-running crawl service: lease, fetch, store raw, parse, publish.

Phase 8 plan section 6.1. ``CrawlWorker.run_once`` owns one polling cycle and
never sleeps; this module owns the loop around it, the wait between cycles,
graceful shutdown and the one step Phase 3 left out — publishing every parsed
observation to ``marketplace.observations.v1``.

The loop decides only *when* to work. Every timestamp that reaches an
observation, an audit row or a crawl run ID still comes from the injected
clock and the task itself, never from the loop.

Importing this module opens nothing: PostgreSQL, Kafka and the HTTP adapter
are all built inside :func:`main`.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import os
import random
import socket
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable

from common.identity import deterministic_id
# Re-exported: the crawl service was their first home, and callers import them here.
from common.lifecycle import StopSignal, install_signal_handlers  # noqa: F401
from common.postgres import postgres_connection_factory  # noqa: F401
from crawler.contracts import AcquisitionStatus, ObservationPublishError
from crawler.scheduling import CrawlTask


def crawl_run_id_for(task: CrawlTask) -> str:
    """One crawl run per attempt.

    The attempt number separates retries; the lease expiry separates a
    re-lease after a crash, which can carry the same attempt number. A reused
    run ID would merge two attempts' parsed counts in Phase 7 check 8.
    """
    return deterministic_id("crawl_run", task.task_id, task.attempts, task.lease_expires_at)


def default_worker_id() -> str:
    # The PID keeps two processes on one host from sharing a lease owner,
    # which would let one complete a task the other is still working on.
    return f"{socket.gethostname()}:{os.getpid()}"


def publishing_executor(inner: Callable[..., Any], *, publish: Callable[[Any], Any]) -> Callable[..., Any]:
    """Wrap an acquisition executor so every parsed observation is published.

    ``inner`` keeps the Phase 2 order — raw Bronze is persisted before
    parsing — and this adds the step after it. Observations are published one
    at a time, each waiting for its acknowledgement, in report order, so a
    failure at position ``k`` means exactly ``k`` observations are in Kafka.
    A failed acquisition is returned untouched and publishes nothing.
    """

    def execute(*, task: CrawlTask, crawl_run_id: str):
        report = inner(task=task, crawl_run_id=crawl_run_id)
        if report.status is AcquisitionStatus.FAILED:
            return report
        acknowledged = 0
        for event in report.observations:
            try:
                publish(event)
            except Exception as error:
                raise ObservationPublishError(report=report, acknowledged=acknowledged, cause=error) from error
            acknowledged += 1
        return report

    return execute


def run_service(
    worker: Any,
    *,
    stop: Any,
    idle_seconds: float,
    poll_seconds: float,
    max_cycles: int | None = None,
    log: Callable[[str], None] = print,
    run_id_for: Callable[[CrawlTask], str] = crawl_run_id_for,
) -> int:
    """Run cycles until stopped, or until ``max_cycles``; return the cycle count.

    A stop request is honoured between cycles, never inside one: the cycle
    leased its tasks up front, and a task leased but never started would sit
    ``LEASED`` until its lease expired.
    """
    if idle_seconds <= 0:
        raise ValueError("idle_seconds must be positive")
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")
    if max_cycles is not None and max_cycles <= 0:
        raise ValueError("max_cycles must be positive")
    cycles = 0
    while not stop.is_set():
        result = worker.run_once(crawl_run_id_for=run_id_for)
        cycles += 1
        log(json.dumps({"event": "crawl_cycle", "cycle": cycles, **dataclasses.asdict(result)}, sort_keys=True))
        if max_cycles is not None and cycles >= max_cycles:
            break
        if stop.is_set():
            break
        if stop.wait(idle_seconds if result.leased == 0 else poll_seconds):
            break
    return cycles


def build_worker(*, site: str, worker_id: str, publish: Callable[[Any], Any], connection_factory: Callable[[], Any]):
    from config.settings import (
        CRAWL_CIRCUIT_FAILURE_THRESHOLD, CRAWL_CIRCUIT_OPEN_SECONDS, CRAWL_LEASE_SECONDS,
        CRAWL_MAX_ATTEMPTS, CRAWL_RETRY_BASE_SECONDS, CRAWL_RETRY_JITTER_RATIO,
        CRAWL_RETRY_MAX_SECONDS, CRAWL_WORKER_BATCH_SIZE,
    )
    from crawler.audit import CrawlAuditRepository
    from crawler.frontier import PostgresCrawlFrontier
    from crawler.runner import listing_page_executor
    from crawler.scheduling import RetryPolicy
    from crawler.worker import CrawlWorker

    clock = lambda: datetime.now(timezone.utc)  # noqa: E731 - the one wall clock, injected once
    return CrawlWorker(
        frontier=PostgresCrawlFrontier(connection_factory),
        audit=CrawlAuditRepository(connection_factory),
        executor=publishing_executor(listing_page_executor(site=site, clock=clock), publish=publish),
        clock=clock,
        policy=RetryPolicy(CRAWL_MAX_ATTEMPTS, CRAWL_RETRY_BASE_SECONDS, CRAWL_RETRY_MAX_SECONDS, CRAWL_RETRY_JITTER_RATIO),
        worker_id=worker_id,
        lease_seconds=CRAWL_LEASE_SECONDS,
        batch_size=CRAWL_WORKER_BATCH_SIZE,
        circuit_threshold=CRAWL_CIRCUIT_FAILURE_THRESHOLD,
        circuit_open_seconds=CRAWL_CIRCUIT_OPEN_SECONDS,
        jitter=lambda: Decimal(str(random.random())),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Long-running marketplace crawl service")
    parser.add_argument("--site", default="tiki")
    parser.add_argument("--worker-id", default=None, help="lease owner; defaults to CRAWL_SERVICE_WORKER_ID, then <hostname>:<pid>")
    parser.add_argument("--max-cycles", type=int, default=None, help="stop after N cycles (tests and smoke only)")
    args = parser.parse_args()

    from config.settings import CRAWL_SERVICE_IDLE_SECONDS, CRAWL_SERVICE_WORKER_ID, CRAWL_WORKER_POLL_SECONDS
    from data_ingestion.marketplace_producer import create_marketplace_producer, publish_observation

    stop = StopSignal()
    install_signal_handlers(stop)
    producer = create_marketplace_producer()
    try:
        worker = build_worker(
            site=args.site,
            worker_id=args.worker_id or CRAWL_SERVICE_WORKER_ID or default_worker_id(),
            publish=lambda event: publish_observation(producer, event),
            connection_factory=postgres_connection_factory(),
        )
        run_service(worker, stop=stop, idle_seconds=CRAWL_SERVICE_IDLE_SECONDS,
                    poll_seconds=CRAWL_WORKER_POLL_SECONDS, max_cycles=args.max_cycles,
                    log=lambda line: print(line, flush=True))
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
