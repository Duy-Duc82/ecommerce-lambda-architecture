"""The marketplace batch scheduler of Phase 8 (plan section 6.4), kept as profile ``legacy-scheduler``.

Airflow's DAG ``marketplace_batch`` replaced it (ADR-0002); this loop remains
for a stack without Airflow. Every tick cuts the newest closed window, runs the
batch stages on it unless that window already succeeded, and sleeps until the
next window closes. ``run_id_for`` is shared with the DAG.

Windows close on interval boundaries counted from the Unix epoch in UTC, and
a window is cut only ``lag`` after it closes, so crawl runs inside it have
time to settle before Phase 7 check 8 reconciles them. The scheduler wakes at
boundary plus lag for the same reason: waking at the bare boundary would
still see the previous window and do nothing for a whole interval.

``as_of`` and ``run_id`` depend only on the clock, the interval and the lag.
A restart therefore finds the same run in the audit table and resumes it
rather than inventing a second run for the same window.

Importing this module opens nothing; PostgreSQL and Spark are reached only
from :func:`main` and the batch itself.
"""
from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _check(interval_seconds: int, lag_seconds: int) -> None:
    # Run IDs carry minutes. Below a minute two windows would share one.
    if interval_seconds < 60:
        raise ValueError("interval_seconds must be at least 60")
    if lag_seconds < 0:
        raise ValueError("lag_seconds must not be negative")


def window_as_of(now: datetime, *, interval_seconds: int, lag_seconds: int) -> datetime:
    """The end of the newest window that closed at least ``lag`` ago."""
    _check(interval_seconds, lag_seconds)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    elapsed = int((now.astimezone(UTC) - _EPOCH).total_seconds()) - lag_seconds
    return _EPOCH + timedelta(seconds=elapsed - elapsed % interval_seconds)


def run_id_for(as_of: datetime) -> str:
    return "mp-" + as_of.astimezone(UTC).strftime("%Y%m%dT%H%MZ")


def next_wake(now: datetime, *, interval_seconds: int, lag_seconds: int) -> datetime:
    """When the window after the current one becomes due."""
    return window_as_of(now, interval_seconds=interval_seconds, lag_seconds=lag_seconds) + timedelta(
        seconds=interval_seconds + lag_seconds)


class PostgresRunStatus:
    """The audit status of one batch run, or None if it never started."""

    def __init__(self, connection_factory: Callable[[], Any]):
        self.connection_factory = connection_factory

    def __call__(self, run_id: str) -> str | None:
        with self.connection_factory() as conn, conn.cursor() as cur:
            cur.execute("SELECT status FROM audit.marketplace_batch_run WHERE run_id=%s", (run_id,))
            row = cur.fetchone()
        return None if row is None else row[0]


def tick(
    now: datetime,
    *,
    run_status: Callable[[str], str | None],
    run_batch: Callable[..., Any],
    silver_uri: str,
    gold_root_uri: str,
    interval_seconds: int,
    lag_seconds: int,
) -> dict:
    """Run the window due at ``now``; return what happened, for the log.

    Never raises for a failed batch. A refused or crashed window must not stop
    the windows after it; its evidence is already in PostgreSQL.
    """
    from ecomdata.processing.batch.context import MarketplaceBatchContext
    from ecomdata.processing.batch.lock import BatchAlreadyRunning
    from ecomdata.processing.batch.quality import QualityGateFailure

    as_of = window_as_of(now, interval_seconds=interval_seconds, lag_seconds=lag_seconds)
    run_id = run_id_for(as_of)
    report = {"event": "batch_tick", "run_id": run_id, "as_of": as_of.isoformat()}
    context = MarketplaceBatchContext(run_id, as_of, silver_uri, gold_root_uri)
    try:
        # Inside the try: PostgreSQL restarting as a tick fires is one failed
        # window, not the end of the scheduler.
        previous = run_status(run_id)
        if previous in ("PROMOTED", "SUCCEEDED"):
            return {**report, "outcome": "SKIPPED_SUCCEEDED"}
        # Never allow_backfill: a backfill is a deliberate operator command.
        result = run_batch(context, resume=previous is not None, serving_gold_root_uri=gold_root_uri)
    except BatchAlreadyRunning:
        return {**report, "outcome": "ALREADY_RUNNING"}
    except QualityGateFailure as refusal:
        return {**report, "outcome": "QUALITY_FAILED", "mandatory_failure_count": refusal.decision.mandatory_failures,
                "failing_checks": list(refusal.failing)}
    except Exception as error:  # noqa: BLE001 - the loop outlives any one window
        return {**report, "outcome": "FAILED", "error": f"{type(error).__name__}: {error}"[:1000]}
    return {**report, "outcome": result.status, "resumed": previous is not None,
            "manifest_promoted": result.manifest_promoted, "promotion_reason": result.promotion_reason}


def run_scheduler(
    run_tick: Callable[[datetime], dict],
    *,
    clock: Callable[[], datetime],
    stop: Any,
    interval_seconds: int,
    lag_seconds: int,
    max_ticks: int | None = None,
    log: Callable[[str], None] = print,
) -> int:
    """Tick now, then at every boundary plus lag, until stopped; return the tick count.

    A stop request is honoured between ticks. A batch in progress runs to its
    end, because killing it half-way leaves a RUNNING audit row that only the
    next tick for the same window would resume.
    """
    _check(interval_seconds, lag_seconds)
    if max_ticks is not None and max_ticks <= 0:
        raise ValueError("max_ticks must be positive")
    ticks = 0
    while not stop.is_set():
        log(json.dumps(run_tick(clock()), sort_keys=True))
        ticks += 1
        if max_ticks is not None and ticks >= max_ticks:
            break
        now = clock()
        wait = (next_wake(now, interval_seconds=interval_seconds, lag_seconds=lag_seconds) - now).total_seconds()
        if stop.wait(max(wait, 0.0)):
            break
    return ticks


def main(argv: list[str] | None = None) -> None:
    import sys

    from ecomdata.processing.batch.cli import main as cli_main

    cli_main(["scheduler", *(sys.argv[1:] if argv is None else argv)])


if __name__ == "__main__":
    main()
