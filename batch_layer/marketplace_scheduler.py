"""The marketplace batch scheduler — Phase 8 plan section 6.4.

A plain loop, not Airflow (decision D2): every tick cuts the newest closed
window, runs the temporal warehouse on it unless that window already
succeeded, and sleeps until the next window closes.

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

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from common.lifecycle import StopSignal, install_signal_handlers

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


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
    elapsed = int((now.astimezone(timezone.utc) - _EPOCH).total_seconds()) - lag_seconds
    return _EPOCH + timedelta(seconds=elapsed - elapsed % interval_seconds)


def run_id_for(as_of: datetime) -> str:
    return "mp-" + as_of.astimezone(timezone.utc).strftime("%Y%m%dT%H%MZ")


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
    from batch_layer.marketplace_lock import BatchAlreadyRunning
    from batch_layer.marketplace_quality import QualityGateFailure
    from batch_layer.marketplace_warehouse import MarketplaceBatchContext

    as_of = window_as_of(now, interval_seconds=interval_seconds, lag_seconds=lag_seconds)
    run_id = run_id_for(as_of)
    report = {"event": "batch_tick", "run_id": run_id, "as_of": as_of.isoformat()}
    context = MarketplaceBatchContext(run_id, as_of, silver_uri, gold_root_uri)
    try:
        # Inside the try: PostgreSQL restarting as a tick fires is one failed
        # window, not the end of the scheduler.
        previous = run_status(run_id)
        if previous == "SUCCEEDED":
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
    ticks, due = 0, None
    while not stop.is_set():
        now = clock()
        # The wait sleeps on the monotonic clock, and windows are cut by the
        # wall clock; the two drift apart over a day's sleep. A wake short of
        # the due time by the wall clock would rerun the old window.
        if due is not None and now < due:
            if stop.wait((due - now).total_seconds()):
                break
            continue
        ran = window_as_of(now, interval_seconds=interval_seconds, lag_seconds=lag_seconds)
        log(json.dumps(run_tick(now), sort_keys=True))
        ticks += 1
        if max_ticks is not None and ticks >= max_ticks:
            break
        # From the window that ran, not the clock after the batch: a batch
        # that ends past the next boundary would otherwise skip that window.
        due = ran + timedelta(seconds=interval_seconds + lag_seconds)
    return ticks


def main() -> None:
    parser = argparse.ArgumentParser(description="Marketplace batch scheduler")
    parser.add_argument("--max-ticks", type=int, default=None, help="stop after N ticks (tests and smoke only)")
    args = parser.parse_args()
    from batch_layer.marketplace_warehouse import run_marketplace_warehouse
    from common.postgres import postgres_connection_factory
    from config.settings import (
        MARKETPLACE_BATCH_AS_OF_LAG_SECONDS, MARKETPLACE_BATCH_INTERVAL_SECONDS,
        MARKETPLACE_GOLD_DATASET, MARKETPLACE_SILVER_DATASET, data_lake_uri,
    )

    stop = StopSignal()
    install_signal_handlers(stop)
    run_status = PostgresRunStatus(postgres_connection_factory())
    silver_uri = data_lake_uri("silver", MARKETPLACE_SILVER_DATASET)
    gold_root_uri = data_lake_uri("gold", MARKETPLACE_GOLD_DATASET)
    run_scheduler(
        lambda now: tick(now, run_status=run_status, run_batch=run_marketplace_warehouse,
                         silver_uri=silver_uri, gold_root_uri=gold_root_uri,
                         interval_seconds=MARKETPLACE_BATCH_INTERVAL_SECONDS,
                         lag_seconds=MARKETPLACE_BATCH_AS_OF_LAG_SECONDS),
        clock=lambda: datetime.now(timezone.utc),
        stop=stop,
        interval_seconds=MARKETPLACE_BATCH_INTERVAL_SECONDS,
        lag_seconds=MARKETPLACE_BATCH_AS_OF_LAG_SECONDS,
        max_ticks=args.max_ticks,
        log=lambda line: print(line, flush=True),
    )


if __name__ == "__main__":
    main()
