"""Phase 8 plan section 6.4: the marketplace batch scheduler (tests 14-16, 21)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from batch_layer import marketplace_scheduler as scheduler
from batch_layer.marketplace_lock import BatchAlreadyRunning
from batch_layer.marketplace_quality import QualityDecision, QualityGateFailure
from batch_layer.marketplace_warehouse import MarketplaceBatchResult

DAY, LAG = 86400, 1800
UTC = timezone.utc


def at(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


# 14
@pytest.mark.parametrize("now, as_of", [
    ("2026-10-02T00:31:00", "2026-10-02T00:00:00"),
    ("2026-10-02T00:30:00", "2026-10-02T00:00:00"),
    # Within the lag the newest window is still settling: cut the one before.
    ("2026-10-02T00:29:59", "2026-10-01T00:00:00"),
    ("2026-10-02T23:59:59", "2026-10-02T00:00:00"),
])
def test_as_of_is_the_newest_window_closed_at_least_the_lag_ago(now, as_of):
    assert scheduler.window_as_of(at(now), interval_seconds=DAY, lag_seconds=LAG) == at(as_of)


# 14
def test_as_of_and_run_id_depend_only_on_the_clock_interval_and_lag():
    first = scheduler.window_as_of(at("2026-10-02T07:15:00"), interval_seconds=DAY, lag_seconds=LAG)
    again = scheduler.window_as_of(at("2026-10-02T21:40:00"), interval_seconds=DAY, lag_seconds=LAG)
    hourly = scheduler.window_as_of(at("2026-10-02T07:15:00"), interval_seconds=3600, lag_seconds=600)

    assert first == again
    assert scheduler.run_id_for(first) == "mp-20261002T0000Z"
    assert scheduler.run_id_for(hourly) == "mp-20261002T0700Z"
    # Another offset, same instant: same window.
    plus7 = at("2026-10-02T07:15:00").astimezone(timezone(timedelta(hours=7)))
    assert scheduler.window_as_of(plus7, interval_seconds=DAY, lag_seconds=LAG) == first


# 14: the wake-up is boundary plus lag, or every window would run a day late.
def test_the_next_wake_is_the_next_boundary_plus_the_lag():
    wake = scheduler.next_wake(at("2026-10-02T00:31:00"), interval_seconds=DAY, lag_seconds=LAG)

    assert wake == at("2026-10-03T00:30:00")
    assert scheduler.window_as_of(wake, interval_seconds=DAY, lag_seconds=LAG) == at("2026-10-03T00:00:00")


@pytest.mark.parametrize("interval, lag", [(59, 0), (DAY, -1)])
def test_unusable_intervals_and_lags_are_refused(interval, lag):
    with pytest.raises(ValueError):
        scheduler.window_as_of(at("2026-10-02T00:00:00"), interval_seconds=interval, lag_seconds=lag)


def test_a_naive_clock_is_refused():
    with pytest.raises(ValueError, match="timezone-aware"):
        scheduler.window_as_of(datetime(2026, 10, 2), interval_seconds=DAY, lag_seconds=LAG)


class RecordingBatch:
    def __init__(self, error: Exception | None = None):
        self.calls, self.error = [], error

    def __call__(self, context, **kwargs):
        self.calls.append((context, kwargs))
        if self.error:
            raise self.error
        return MarketplaceBatchResult(context.run_id, "SUCCEEDED", 3, {}, manifest_promoted=True, promotion_reason="PROMOTED")


def run_tick(now, *, status, batch):
    return scheduler.tick(at(now), run_status=lambda run_id: status, run_batch=batch,
                          silver_uri="file:///silver", gold_root_uri="file:///gold",
                          interval_seconds=DAY, lag_seconds=LAG)


# 15
def test_a_succeeded_window_is_skipped():
    batch = RecordingBatch()

    report = run_tick("2026-10-02T01:00:00", status="SUCCEEDED", batch=batch)

    assert report["outcome"] == "SKIPPED_SUCCEEDED"
    assert batch.calls == []


# 15
@pytest.mark.parametrize("status, resume", [
    (None, False), ("RUNNING", True), ("FAILED", True), ("QUALITY_FAILED", True), ("GOLD_WRITTEN", True),
])
def test_an_absent_window_starts_and_any_other_status_resumes(status, resume):
    batch = RecordingBatch()

    report = run_tick("2026-10-02T01:00:00", status=status, batch=batch)

    (context, kwargs), = batch.calls
    assert context.run_id == "mp-20261002T0000Z"
    assert context.as_of == at("2026-10-02T00:00:00")
    assert (context.silver_uri, context.gold_root_uri) == ("file:///silver", "file:///gold")
    assert kwargs == {"resume": resume, "serving_gold_root_uri": "file:///gold"}
    assert "allow_backfill" not in kwargs
    assert report["outcome"] == "SUCCEEDED"


def refusal():
    decision = QualityDecision(run_id="mp-20261002T0000Z", passed=False, rule_version="quality-rules.v2",
                               evaluated_at=at("2026-10-02T00:00:00"), mandatory_total=13, mandatory_failures=2,
                               advisory_failures=0, skipped=0)
    return QualityGateFailure(decision, [])


# 16
@pytest.mark.parametrize("error, outcome", [
    (refusal(), "QUALITY_FAILED"),
    (BatchAlreadyRunning(820801), "ALREADY_RUNNING"),
    (RuntimeError("spark died"), "FAILED"),
])
def test_a_failed_window_is_reported_not_raised(error, outcome):
    report = run_tick("2026-10-02T01:00:00", status=None, batch=RecordingBatch(error))

    assert report["outcome"] == outcome
    if outcome == "QUALITY_FAILED":
        assert report["mandatory_failure_count"] == 2


# 16: PostgreSQL restarting when a tick fires must not end the scheduler.
def test_an_unreachable_audit_table_is_reported_not_raised():
    def unreachable(run_id):
        raise ConnectionError("could not connect to server")
    batch = RecordingBatch()

    report = scheduler.tick(at("2026-10-02T01:00:00"), run_status=unreachable, run_batch=batch,
                            silver_uri="file:///silver", gold_root_uri="file:///gold",
                            interval_seconds=DAY, lag_seconds=LAG)

    assert report["outcome"] == "FAILED"
    assert "could not connect" in report["error"]
    assert batch.calls == []


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


class FakeStop:
    """Never set; each wait moves the clock forward by what was asked."""

    def __init__(self, clock, stop_after_waits=None):
        self.clock, self.waits, self.stop_after_waits = clock, [], stop_after_waits

    def is_set(self):
        return False

    def wait(self, seconds):
        self.waits.append(seconds)
        self.clock.now += timedelta(seconds=seconds)
        return self.stop_after_waits is not None and len(self.waits) >= self.stop_after_waits


# 16
def test_a_quality_failure_in_one_tick_does_not_stop_the_next():
    clock = Clock(at("2026-10-02T00:31:00"))
    stop = FakeStop(clock)
    seen, lines = [], []
    outcomes = iter(["QUALITY_FAILED", "SUCCEEDED"])

    def fake_tick(now):
        seen.append(now)
        return {"run_id": scheduler.run_id_for(scheduler.window_as_of(now, interval_seconds=DAY, lag_seconds=LAG)),
                "outcome": next(outcomes)}

    ticks = scheduler.run_scheduler(fake_tick, clock=clock, stop=stop, interval_seconds=DAY, lag_seconds=LAG,
                                    max_ticks=2, log=lines.append)

    assert ticks == 2
    assert [json.loads(line)["run_id"] for line in lines] == ["mp-20261002T0000Z", "mp-20261003T0000Z"]
    assert seen[1] == at("2026-10-03T00:30:00")
    assert stop.waits == [(at("2026-10-03T00:30:00") - at("2026-10-02T00:31:00")).total_seconds()]


# 16, end to end through tick(): a refused window, then the next one runs.
def test_the_loop_runs_the_next_window_after_a_refused_one():
    clock = Clock(at("2026-10-02T00:31:00"))
    stop = FakeStop(clock)
    batch = RecordingBatch(refusal())
    lines = []

    def real_tick(now):
        report = scheduler.tick(now, run_status=lambda run_id: None, run_batch=batch, silver_uri="file:///silver",
                                gold_root_uri="file:///gold", interval_seconds=DAY, lag_seconds=LAG)
        batch.error = None
        return report

    scheduler.run_scheduler(real_tick, clock=clock, stop=stop, interval_seconds=DAY, lag_seconds=LAG,
                            max_ticks=2, log=lines.append)

    assert [json.loads(line)["outcome"] for line in lines] == ["QUALITY_FAILED", "SUCCEEDED"]
    assert [context.run_id for context, _ in batch.calls] == ["mp-20261002T0000Z", "mp-20261003T0000Z"]


def test_a_stop_during_the_wait_ends_the_loop():
    clock = Clock(at("2026-10-02T00:31:00"))
    stop = FakeStop(clock, stop_after_waits=1)

    ticks = scheduler.run_scheduler(lambda now: {"outcome": "SUCCEEDED"}, clock=clock,
                                    stop=stop, interval_seconds=DAY, lag_seconds=LAG, log=lambda line: None)

    assert ticks == 1


class EarlyStop(FakeStop):
    """The first wait ends ``early`` seconds short by the wall clock.

    On the collection machine the scheduler slept 14 hours on the monotonic
    clock and woke ~50 s before 00:30 by the wall clock (PROGRESS 28.1).
    """

    def __init__(self, clock, early):
        super().__init__(clock)
        self.early = early

    def wait(self, seconds):
        self.waits.append(seconds)
        self.clock.now += timedelta(seconds=seconds - (self.early if len(self.waits) == 1 else 0))
        return False


def _slow_tick(clock, seconds, seen):
    """A tick that cuts its window from ``now`` and takes ``seconds``, like a batch does."""
    def run(now):
        seen.append((now, scheduler.run_id_for(scheduler.window_as_of(now, interval_seconds=DAY, lag_seconds=LAG))))
        clock.now += timedelta(seconds=seconds)
        return {"run_id": seen[-1][1], "outcome": "QUALITY_FAILED"}
    return run


def test_a_wake_early_by_the_wall_clock_waits_out_the_rest_and_runs_the_due_window():
    clock = Clock(at("2026-10-04T10:22:00"))
    stop = EarlyStop(clock, early=50)
    seen = []

    scheduler.run_scheduler(_slow_tick(clock, 20, seen), clock=clock, stop=stop, interval_seconds=DAY,
                            lag_seconds=LAG, max_ticks=2, log=lambda line: None)

    assert [run_id for _, run_id in seen] == ["mp-20261004T0000Z", "mp-20261005T0000Z"]
    assert seen[1][0] >= at("2026-10-05T00:30:00")


def test_a_tick_that_ends_past_the_next_boundary_does_not_skip_that_window():
    # 28.1: the 10-04 window started a hair before 00:30 and finished after.
    clock = Clock(at("2026-10-05T00:29:59.990000"))
    stop = FakeStop(clock)
    seen = []

    scheduler.run_scheduler(_slow_tick(clock, 20, seen), clock=clock, stop=stop, interval_seconds=DAY,
                            lag_seconds=LAG, max_ticks=2, log=lambda line: None)

    assert [run_id for _, run_id in seen] == ["mp-20261004T0000Z", "mp-20261005T0000Z"]
    assert seen[1][0] < at("2026-10-05T00:31:00")


def test_the_next_wake_follows_the_window_that_ran_not_the_clock_after_it():
    clock = Clock(at("2026-10-05T00:29:59.990000"))
    stop = FakeStop(clock)
    seen = []

    scheduler.run_scheduler(_slow_tick(clock, 20, seen), clock=clock, stop=stop, interval_seconds=DAY,
                            lag_seconds=LAG, max_ticks=3, log=lambda line: None)

    assert [run_id for _, run_id in seen] == ["mp-20261004T0000Z", "mp-20261005T0000Z", "mp-20261006T0000Z"]
    assert seen[2][0] == at("2026-10-06T00:30:00")


def test_the_run_status_reads_the_audit_row():
    executed = []

    class Cursor:
        def __init__(self, row): self.row = row
        def __enter__(self): return self
        def __exit__(self, *exc): return False
        def execute(self, sql, params): executed.append((sql, params))
        def fetchone(self): return self.row

    class Connection:
        def __init__(self, row): self.row = row
        def __enter__(self): return self
        def __exit__(self, *exc): return False
        def cursor(self): return Cursor(self.row)

    assert scheduler.PostgresRunStatus(lambda: Connection(("FAILED",)))("mp-1") == "FAILED"
    assert scheduler.PostgresRunStatus(lambda: Connection(None))("mp-2") is None
    assert executed[0][1] == ("mp-1",)
    assert "audit.marketplace_batch_run" in executed[0][0]


def test_the_entrypoint_answers_help():
    import subprocess
    import sys

    result = subprocess.run([sys.executable, "-m", "batch_layer.marketplace_scheduler", "--help"],
                            capture_output=True, text=True, timeout=300)

    assert result.returncode == 0, result.stderr
    assert "--max-ticks" in result.stdout


# 21
def test_settings_refuse_a_lookback_not_exceeding_interval_plus_settle(monkeypatch):
    from config import settings

    monkeypatch.setattr(settings, "MARKETPLACE_BATCH_INTERVAL_SECONDS", 86400)
    monkeypatch.setattr(settings, "MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS", 900)
    monkeypatch.setattr(settings, "MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS", 86400 + 900)
    with pytest.raises(ValueError, match="MARKETPLACE_BATCH_INTERVAL_SECONDS"):
        settings.validate_marketplace_settings()

    monkeypatch.setattr(settings, "MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS", 86400 + 901)
    settings.validate_marketplace_settings()


# 21
@pytest.mark.parametrize("name", [
    "MARKETPLACE_BATCH_INTERVAL_SECONDS", "MARKETPLACE_BATCH_AS_OF_LAG_SECONDS", "MARKETPLACE_BATCH_LOCK_KEY",
])
def test_settings_refuse_non_positive_scheduler_values(monkeypatch, name):
    from config import settings

    monkeypatch.setattr(settings, name, 0)
    with pytest.raises(ValueError, match=name):
        settings.validate_marketplace_settings()
