"""Crawl reliability, freshness and coverage — Phase 9 plan section 9 (tests 8-10)."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from ops import evaluate
from ops.evaluate import EvaluationRefused, Window

T0 = datetime(2026, 10, 4, 10, 0, tzinfo=timezone.utc)
HOUR = 3600


def _attempt(minutes, *, task="t1", status="SUCCEEDED", latency=3000, error=None, http=200, parsed=40):
    started = T0 + timedelta(minutes=minutes)
    return {"task_id": task, "status": status, "started_at": started, "completed_at": started + timedelta(seconds=4),
            "http_status": http, "latency_ms": latency, "raw_bytes": 150_000, "parsed_count": parsed,
            "rejected_count": 0, "error_kind": error}


def test_the_universe_check_refuses_a_category_outside_it():
    evaluate.check_universe(["1846", "931"], ["1846", "8322", "931"])
    with pytest.raises(EvaluationRefused, match="9001"):
        evaluate.check_universe(["1846", "9001"], ["1846", "931"])


def test_the_universe_check_refuses_an_empty_universe():
    with pytest.raises(EvaluationRefused, match="no frozen universe"):
        evaluate.check_universe(["1846"], [""])


def test_a_gap_is_any_stretch_over_twice_the_cadence_including_one_across_midnight():
    window = Window(T0, T0 + timedelta(hours=20))
    starts = [T0, T0 + timedelta(hours=1), T0 + timedelta(hours=2),         # hourly
              T0 + timedelta(hours=16),                                     # 14 h later, past midnight
              T0 + timedelta(hours=17)]

    gaps = evaluate.collection_gaps(starts, cadence_seconds=HOUR, window=window)

    assert [(g["start"], g["end"], g["hours"]) for g in gaps] == [
        ((T0 + timedelta(hours=2)).isoformat(), (T0 + timedelta(hours=16)).isoformat(), 14.0),
        ((T0 + timedelta(hours=17)).isoformat(), (T0 + timedelta(hours=20)).isoformat(), 3.0)]
    assert {g["cause"] for g in gaps} == {"unknown"}


def test_an_hourly_collection_has_no_gap():
    starts = [T0 + timedelta(hours=h) for h in range(10)]

    assert evaluate.collection_gaps(starts, cadence_seconds=HOUR, window=Window(T0, starts[-1])) == []


def test_reliability_counts_success_errors_retries_and_lateness():
    attempts = [_attempt(0, task="a"), _attempt(1, task="b", status="FAILED", error="SERVER_ERROR", http=500, parsed=0),
                _attempt(3, task="b"), _attempt(60, task="c", latency=5000),
                _attempt(61, task="d", status="FAILED", error="RATE_LIMITED", http=429, parsed=0),
                _attempt(62, task="d", status="FAILED", error="RATE_LIMITED", http=429, parsed=0),
                _attempt(64, task="d")]
    tasks = [{"task_id": t, "scheduled_for": T0 + timedelta(minutes=m)}
             for t, m in (("a", 0), ("b", 0), ("c", 59), ("d", 60))]

    report = evaluate.reliability_report(attempts, tasks, [], [], cadence_seconds=HOUR)

    total = report["total"]
    assert (total["attempts"], total["succeeded"]) == (7, 4)
    assert total["error_kinds"] == {"RATE_LIMITED": 2, "SERVER_ERROR": 1}
    assert total["http_statuses"] == {"200": 4, "429": 2, "500": 1}
    assert report["attempts_per_task"] == {"1": 2, "2": 1, "3+": 1, "tasks": 4}
    assert report["schedule_lateness_seconds"]["max"] == 60
    assert report["dataset"] == "live"
    assert report["window"]["threshold_met"] is False
    assert "not recorded" in report["circuit_breaker"]["history"]


def test_seeded_occurrences_are_left_out_of_lateness_and_pages_are_timed_between_crawls():
    from crawler.seed_frontier import SEED_SCHEDULED_FOR

    attempts = [dict(_attempt(0, task="s1"), target="p1"), dict(_attempt(61, task="n1"), target="p1"),
                dict(_attempt(130, task="n2"), target="p1"), dict(_attempt(0, task="s2"), target="p2")]
    tasks = [{"task_id": "s1", "scheduled_for": SEED_SCHEDULED_FOR}, {"task_id": "s2", "scheduled_for": SEED_SCHEDULED_FOR},
             {"task_id": "n1", "scheduled_for": T0 + timedelta(minutes=60)},
             {"task_id": "n2", "scheduled_for": T0 + timedelta(minutes=121)}]

    report = evaluate.reliability_report(attempts, tasks, [], [], cadence_seconds=HOUR)

    lateness = report["schedule_lateness_seconds"]
    assert (lateness["tasks"], lateness["seeded_occurrences_excluded"], lateness["max"]) == (2, 2, 540)
    interval = report["crawl_interval_per_page_seconds"]
    assert (interval["intervals"], interval["p50"], interval["max"]) == (2, 3660, 4140)
    assert interval["share_within_110pct_of_cadence"] == 0.5


def test_a_window_of_thirty_days_meets_the_threshold():
    assert Window(T0, T0 + timedelta(days=30)).describe()["threshold_met"] is True
    assert Window(T0, T0 + timedelta(days=29, hours=23)).describe()["threshold_met"] is False


def test_drill_recovery_is_read_from_the_step_timestamps():
    record = {"drill": "d3", "passed": True, "steps": [
        {"step": "baseline", "at": "2026-10-03T14:35:53+00:00"}, {"step": "inject", "at": "2026-10-03T14:36:16+00:00"},
        {"step": "observe", "at": "2026-10-03T14:37:18+00:00"}, {"step": "recover", "at": "2026-10-03T14:37:25+00:00"},
        {"step": "verify", "at": "2026-10-03T14:37:27+00:00"}]}

    assert evaluate.drill_recovery([record, {"drill": "d10", "passed": True, "steps": []}]) == [
        {"drill": "d3", "passed": True, "fault_seconds": 69.0, "recovery_seconds": 2.0},
        {"drill": "d10", "passed": True, "fault_seconds": None, "recovery_seconds": None}]


def test_no_attempt_is_refused_rather_than_reported_as_zero():
    with pytest.raises(EvaluationRefused):
        evaluate.reliability_report([], [], [], [], cadence_seconds=HOUR)


def test_expected_observations_follow_the_window_on_partial_days():
    window = Window(datetime(2026, 10, 4, 10, 16, tzinfo=timezone.utc), datetime(2026, 10, 6, 12, 0, tzinfo=timezone.utc))

    assert evaluate.expected_per_day(date(2026, 10, 4), window, HOUR) == pytest.approx(13 + 44 / 60)
    assert evaluate.expected_per_day(date(2026, 10, 5), window, HOUR) == 24
    assert evaluate.expected_per_day(date(2026, 10, 6), window, HOUR) == 12
    assert evaluate.expected_per_day(date(2026, 10, 7), window, HOUR) == 0


def test_freshness_shares_ratios_and_crawl_to_kafka_delay():
    window = Window(datetime(2026, 10, 4, 0, 0, tzinfo=timezone.utc), datetime(2026, 10, 6, 0, 0, tzinfo=timezone.utc))
    as_of = datetime(2026, 10, 6, tzinfo=timezone.utc)
    freshness = [{"as_of": as_of, "freshness_status": s, "age_seconds": a}
                 for s, a in (("FRESH", 100), ("FRESH", 200), ("FRESH", 300), ("STALE", 30000))]
    coverage = [{"observed_date": date(2026, 10, 4), "observed_offer_count": 600, "coverage_rate": 1}]
    offer_days = [{"observed_date": date(2026, 10, 5), "observation_count": n} for n in (24, 24, 22, 12)]

    report = evaluate.freshness_report(freshness, coverage, offer_days, [120.0, 80.0, 400.0],
                                       window=window, cadence_seconds=HOUR)

    assert report["latest_freshness"]["shares"] == {"FRESH": 0.75, "STALE": 0.25}
    assert report["latest_freshness"]["age_seconds"]["p50"] == 200
    day = report["observations_per_offer_day_vs_cadence"]["2026-10-05"]
    assert day["offers"] == 4 and day["ratio_median"] == pytest.approx(23 / 24, abs=1e-4)
    assert day["share_at_least_90pct"] == 0.75
    assert report["universe_per_day"] == {"2026-10-04": 600}
    assert report["crawl_to_kafka_ms"]["p50"] == 120.0 and report["crawl_to_kafka_ms"]["samples"] == 3


def test_both_reports_render_to_markdown_with_their_threshold():
    attempts = [_attempt(0), _attempt(60, task="t2")]
    md = evaluate.to_markdown(evaluate.reliability_report(attempts, [], [], [], cadence_seconds=HOUR))

    assert "30-day threshold NOT met" in md and "| 2026-10-04 | 2 | 100.00% |" in md
