"""Crawl reliability, freshness and coverage — Phase 9 plan section 9 (P2-02, P2-03).

Read-only. Runs in the ``ops`` container against the live collection stack:

    .\\scripts\\mp.ps1 -EnvFile env/live.env evaluate reliability
    .\\scripts\\mp.ps1 -EnvFile env/live.env evaluate freshness

Each report names its window, its day count, how many attempts and
observations it rests on, and whether the Brief section 23 threshold of 30
days is met. It is labelled ``dataset: live``, and it refuses to run when the
crawl frontier holds any category outside the frozen universe
(``--universe``, which ``mp evaluate`` takes from ``TIKI_CATEGORIES`` in
``env/live.env``): the stub answers any category, so
a target outside the universe is how stub traffic shows in the data.

What the data cannot say is said, not invented:

- the circuit breaker keeps only its *current* state, so past circuit
  episodes are not counted;
- the cause of a collection gap is not recorded; a gap is listed with its
  start and end, and its cause stays ``unknown`` unless a person adds one.
"""
from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

THRESHOLD_DAYS = 30
DATASET = "live"
REPORTS = Path("/reports/evaluation")


class EvaluationRefused(RuntimeError):
    pass


def percentile(values: Sequence[float], q: float) -> float | None:
    import math

    ordered = sorted(v for v in values if v is not None)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)] if ordered else None


def _day(instant: datetime) -> date:
    return instant.astimezone(timezone.utc).date()


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime

    @property
    def days(self) -> float:
        return (self.end - self.start).total_seconds() / 86400

    def describe(self) -> dict[str, Any]:
        return {"start": self.start.isoformat(), "end": self.end.isoformat(), "days": round(self.days, 3),
                "threshold_days": THRESHOLD_DAYS, "threshold_met": self.days >= THRESHOLD_DAYS}


def check_universe(targets: Iterable[str], universe: Iterable[str]) -> None:
    """Every frontier target's category must lie inside the frozen universe."""
    allowed = {u.strip() for u in universe if u.strip()}
    if not allowed:
        raise EvaluationRefused("no frozen universe given; run mp.ps1 -EnvFile env/live.env evaluate ...")
    foreign = sorted({t for t in targets if t not in allowed})
    if foreign:
        raise EvaluationRefused(f"the frontier holds categories outside the frozen universe {sorted(allowed)}: "
                                f"{foreign[:10]}; this is not the live collection stack, or stub traffic reached it")


# -- P2-02 reliability -------------------------------------------------------

def collection_gaps(starts: Sequence[datetime], *, cadence_seconds: int, window: Window) -> list[dict[str, Any]]:
    """Every stretch longer than twice the cadence with no attempt, including
    one at either end of the window."""
    limit = timedelta(seconds=2 * cadence_seconds)
    points = [window.start, *sorted(starts), window.end]
    gaps = []
    for before, after in zip(points, points[1:]):
        if after - before > limit:
            gaps.append({"start": before.isoformat(), "end": after.isoformat(),
                         "hours": round((after - before).total_seconds() / 3600, 2), "cause": "unknown"})
    return gaps


def drill_recovery(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Fault duration (inject to recover) and recovery (recover to verify) per
    drill record, from the step timestamps. Measured on the stub stack."""
    rows = []
    for record in records:
        steps: dict[str, datetime] = {}
        for step in record.get("steps", []):
            steps.setdefault(step["step"], datetime.fromisoformat(step["at"]))
        row = {"drill": record.get("drill"), "passed": record.get("passed"),
               "fault_seconds": None, "recovery_seconds": None}
        if "inject" in steps and "recover" in steps:
            row["fault_seconds"] = round((steps["recover"] - steps["inject"]).total_seconds(), 1)
        if "recover" in steps and "verify" in steps:
            row["recovery_seconds"] = round((steps["verify"] - steps["recover"]).total_seconds(), 1)
        rows.append(row)
    return sorted(rows, key=lambda r: int(str(r["drill"]).lstrip("d") or 0))


def reliability_report(attempts: Sequence[Mapping[str, Any]], tasks: Sequence[Mapping[str, Any]],
                       source_state: Sequence[Mapping[str, Any]], drills: Sequence[Mapping[str, Any]],
                       *, cadence_seconds: int) -> dict[str, Any]:
    if not attempts:
        raise EvaluationRefused("no crawl attempt in the window")
    window = Window(min(a["started_at"] for a in attempts), max(a["completed_at"] for a in attempts))
    by_day: dict[date, list[Mapping[str, Any]]] = defaultdict(list)
    for attempt in attempts:
        by_day[_day(attempt["started_at"])].append(attempt)

    def summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        latencies = [r["latency_ms"] for r in rows if r.get("latency_ms") is not None]
        succeeded = sum(r["status"] == "SUCCEEDED" for r in rows)
        return {"attempts": len(rows), "succeeded": succeeded, "success_rate": round(succeeded / len(rows), 4),
                "error_kinds": dict(sorted(Counter(r["error_kind"] for r in rows if r.get("error_kind")).items())),
                "http_statuses": dict(sorted(Counter(str(r["http_status"]) for r in rows
                                                     if r.get("http_status") is not None).items())),
                "latency_p50_ms": percentile(latencies, 0.50), "latency_p95_ms": percentile(latencies, 0.95),
                "latency_p99_ms": percentile(latencies, 0.99),
                "parsed": sum(r["parsed_count"] for r in rows), "rejected": sum(r["rejected_count"] for r in rows),
                "raw_bytes": sum(r["raw_bytes"] for r in rows)}

    per_task = Counter(a["task_id"] for a in attempts)
    first_start: dict[str, datetime] = {}
    for a in attempts:
        if a["task_id"] not in first_start or a["started_at"] < first_start[a["task_id"]]:
            first_start[a["task_id"]] = a["started_at"]
    # A seeded occurrence is scheduled at the epoch on purpose (idempotent
    # seeding, "due now"), so it has no lateness to measure.
    from crawler.seed_frontier import SEED_SCHEDULED_FOR

    seeded = sum(t["scheduled_for"] == SEED_SCHEDULED_FOR for t in tasks if t["task_id"] in first_start)
    lateness = [(first_start[t["task_id"]] - t["scheduled_for"]).total_seconds()
                for t in tasks if t["task_id"] in first_start and t["scheduled_for"] != SEED_SCHEDULED_FOR]
    # Cadence adherence: between two successful crawls of the same page.
    by_target: dict[str, list[datetime]] = defaultdict(list)
    for a in attempts:
        if a["status"] == "SUCCEEDED" and a.get("target"):
            by_target[a["target"]].append(a["started_at"])
    intervals = [(later - earlier).total_seconds() for starts in by_target.values()
                 for earlier, later in zip(sorted(starts), sorted(starts)[1:])]
    return {
        "report": "crawl_reliability", "dataset": DATASET, "window": window.describe(),
        "total": summary(attempts),
        "per_day": {d.isoformat(): summary(rows) for d, rows in sorted(by_day.items())},
        "attempts_per_task": {"1": sum(n == 1 for n in per_task.values()), "2": sum(n == 2 for n in per_task.values()),
                              "3+": sum(n >= 3 for n in per_task.values()), "tasks": len(per_task)},
        "schedule_lateness_seconds": {"p50": percentile(lateness, 0.50), "p95": percentile(lateness, 0.95),
                                      "max": max(lateness) if lateness else None, "tasks": len(lateness),
                                      "seeded_occurrences_excluded": seeded},
        "crawl_interval_per_page_seconds": {
            "p50": percentile(intervals, 0.50), "p95": percentile(intervals, 0.95),
            "max": max(intervals) if intervals else None, "intervals": len(intervals),
            "share_within_110pct_of_cadence": (round(sum(i <= 1.1 * cadence_seconds for i in intervals) / len(intervals), 4)
                                               if intervals else None)},
        "gaps": collection_gaps([a["started_at"] for a in attempts], cadence_seconds=cadence_seconds, window=window),
        "cadence_seconds": cadence_seconds,
        "circuit_breaker": {"current": [dict(s) for s in source_state],
                            "history": "not recorded: crawl_source_state keeps only the current state"},
        "drill_recovery": {"dataset": "stub stack, fault injected", "drills": drill_recovery(drills)},
    }


# -- P2-03 freshness and coverage ----------------------------------------------

def expected_per_day(day: date, window: Window, cadence_seconds: int) -> float:
    """Observations one offer could have had on ``day`` inside the window."""
    day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    overlap = min(day_start + timedelta(days=1), window.end) - max(day_start, window.start)
    return max(overlap.total_seconds(), 0) / cadence_seconds


def freshness_report(freshness: Sequence[Mapping[str, Any]], coverage: Sequence[Mapping[str, Any]],
                     offer_days: Sequence[Mapping[str, Any]], produce_delays_ms: Sequence[float],
                     *, window: Window, cadence_seconds: int) -> dict[str, Any]:
    statuses = Counter(r["freshness_status"] for r in freshness)
    ages = [r["age_seconds"] for r in freshness]
    ratios_by_day: dict[date, list[float]] = defaultdict(list)
    for row in offer_days:
        expected = expected_per_day(row["observed_date"], window, cadence_seconds)
        if expected >= 1:
            ratios_by_day[row["observed_date"]].append(row["observation_count"] / expected)
    return {
        "report": "freshness_coverage", "dataset": DATASET, "window": window.describe(),
        "cadence_seconds": cadence_seconds,
        "latest_freshness": {
            "as_of": freshness[0]["as_of"].isoformat() if freshness else None, "offers": len(freshness),
            "shares": {k: round(v / len(freshness), 4) for k, v in sorted(statuses.items())} if freshness else {},
            "age_seconds": {"p50": percentile(ages, 0.5), "p95": percentile(ages, 0.95),
                            "max": max(ages) if ages else None}},
        "coverage_per_day": [{k: (v.isoformat() if isinstance(v, (date, datetime)) else
                                  float(v) if hasattr(v, "is_finite") else v) for k, v in row.items()}
                             for row in sorted(coverage, key=lambda r: r["observed_date"])],
        "observations_per_offer_day_vs_cadence": {
            day.isoformat(): {"offers": len(r), "ratio_median": round(statistics.median(r), 4),
                              "ratio_p5": round(percentile(r, 0.05), 4),
                              "share_at_least_90pct": round(sum(x >= 0.9 for x in r) / len(r), 4)}
            for day, r in sorted(ratios_by_day.items())},
        "universe_per_day": {row["observed_date"].isoformat(): row["observed_offer_count"]
                             for row in sorted(coverage, key=lambda r: r["observed_date"])},
        "crawl_to_kafka_ms": {"p50": percentile(produce_delays_ms, 0.5), "p95": percentile(produce_delays_ms, 0.95),
                              "max": max(produce_delays_ms) if produce_delays_ms else None,
                              "samples": len(produce_delays_ms),
                              "note": "produced_at - fetched_at: the only pipeline interval live data records"},
    }


def to_markdown(report: Mapping[str, Any]) -> str:
    w = report["window"]
    lines = [f"# {report['report']} ({report['dataset']})", "",
             f"Window {w['start']} to {w['end']}: {w['days']} days; 30-day threshold "
             f"{'met' if w['threshold_met'] else 'NOT met'}.", ""]
    if report["report"] == "crawl_reliability":
        lines += ["| day | attempts | success rate | p50 ms | p95 ms | p99 ms | parsed | errors |", "|---|---:|---:|---:|---:|---:|---:|---|"]
        for day, s in report["per_day"].items():
            lines.append(f"| {day} | {s['attempts']} | {s['success_rate']:.2%} | {s['latency_p50_ms']} | "
                         f"{s['latency_p95_ms']} | {s['latency_p99_ms']} | {s['parsed']} | {s['error_kinds'] or ''} |")
        lines += ["", f"Gaps longer than twice the cadence: {len(report['gaps'])}."]
        lines += [f"- {g['start']} to {g['end']} ({g['hours']} h), cause {g['cause']}" for g in report["gaps"]]
    else:
        lines += ["| day | offers | ratio median | ratio p5 | share >= 90% |", "|---|---:|---:|---:|---:|"]
        for day, s in report["observations_per_offer_day_vs_cadence"].items():
            lines.append(f"| {day} | {s['offers']} | {s['ratio_median']} | {s['ratio_p5']} | {s['share_at_least_90pct']:.2%} |")
    return "\n".join(lines) + "\n"


# -- the live sources ------------------------------------------------------------

def _rows(cur, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    cur.execute(sql, params)
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def _universe_check(cur, universe: Sequence[str]) -> None:
    targets = [json.loads(r["target"]).get("target") for r in _rows(cur, "SELECT DISTINCT target FROM audit.crawl_frontier")]
    check_universe(targets, universe)


def _produce_delays(limit: int = 500) -> list[float]:
    """``produced_at - fetched_at`` over a spread sample of Silver objects."""
    from common.object_store import _client
    from config.settings import MARKETPLACE_SILVER_DATASET, MINIO_BUCKET_SILVER
    from config.storage import active_profile

    client = _client(active_profile())
    names = sorted(o.object_name for o in client.list_objects(
        MINIO_BUCKET_SILVER, prefix=MARKETPLACE_SILVER_DATASET.strip("/") + "/", recursive=True))
    delays = []
    for name in names[::max(1, len(names) // limit)][:limit]:
        response = client.get_object(MINIO_BUCKET_SILVER, name)
        try:
            event = json.loads(response.read())
        finally:
            response.close()
            response.release_conn()
        produced = datetime.fromisoformat(event["produced_at"].replace("Z", "+00:00"))
        fetched = datetime.fromisoformat(event["payload"]["observation"]["fetched_at"].replace("Z", "+00:00"))
        delays.append((produced - fetched).total_seconds() * 1000)
    return delays


def live_reliability(cadence_seconds: int, universe: Sequence[str]) -> dict[str, Any]:
    from common.postgres import postgres_connection_factory

    drills_dir = Path(__file__).resolve().parents[1] / "data" / "ops" / "drills"
    drills = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(drills_dir.glob("d*.json"))]
    with postgres_connection_factory()() as conn, conn.cursor() as cur:
        _universe_check(cur, universe)
        attempts = _rows(cur, "SELECT a.task_id, a.status, a.started_at, a.completed_at, a.http_status, a.latency_ms, "
                              "a.raw_bytes, a.parsed_count, a.rejected_count, a.error_kind, f.target "
                              "FROM audit.crawl_request_attempt a JOIN audit.crawl_frontier f USING (task_id)")
        tasks = _rows(cur, "SELECT task_id, scheduled_for FROM audit.crawl_frontier")
        state = _rows(cur, "SELECT marketplace_code, consecutive_failures, opened_until, last_failure_at, "
                           "last_success_at FROM audit.crawl_source_state")
    for row in state:
        for key, value in list(row.items()):
            if isinstance(value, datetime):
                row[key] = value.isoformat()
    return reliability_report(attempts, tasks, state, drills, cadence_seconds=cadence_seconds)


def live_freshness(cadence_seconds: int, universe: Sequence[str]) -> dict[str, Any]:
    from common.postgres import postgres_connection_factory

    with postgres_connection_factory()() as conn, conn.cursor() as cur:
        _universe_check(cur, universe)
        bounds = _rows(cur, "SELECT min(started_at) AS start, max(completed_at) AS end FROM audit.crawl_request_attempt")[0]
        if bounds["start"] is None:
            raise EvaluationRefused("no crawl attempt yet")
        freshness = _rows(cur, "SELECT as_of, freshness_status, age_seconds FROM cache.marketplace_offer_freshness")
        coverage = _rows(cur, "SELECT * FROM cache.marketplace_source_coverage_daily")
        offer_days = _rows(cur, "SELECT observed_date, observation_count FROM cache.marketplace_offer_price_history_daily")
    return freshness_report(freshness, coverage, offer_days, _produce_delays(),
                            window=Window(bounds["start"], bounds["end"]), cadence_seconds=cadence_seconds)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops evaluate")
    parser.add_argument("kind", choices=("reliability", "freshness", "storage"))
    parser.add_argument("--cadence-seconds", type=int, default=None, help="default: CRAWL_ACTIVE_CADENCE_MINUTES")
    parser.add_argument("--out", default=str(REPORTS))
    parser.add_argument("--universe", default="", help="comma-separated frozen categories (mp passes TIKI_CATEGORIES)")
    args = parser.parse_args(argv)
    from config.settings import CRAWL_ACTIVE_CADENCE_MINUTES

    cadence = args.cadence_seconds or CRAWL_ACTIVE_CADENCE_MINUTES * 60
    if args.kind == "storage":
        # P2-04, plan section 10: from audit.storage_snapshot only.
        from ops.storage import run_growth_report
        return run_growth_report(args.out)
    try:
        universe = args.universe.split(",")
        report = (live_reliability(cadence, universe) if args.kind == "reliability"
                  else live_freshness(cadence, universe))
    except EvaluationRefused as error:
        print(json.dumps({"event": "evaluation_refused", "reason": str(error)}), flush=True)
        return 2
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (out / f"{args.kind}-{stamp}.json").write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n",
                                                   encoding="utf-8")
    (out / f"{args.kind}-{stamp}.md").write_text(to_markdown(report), encoding="utf-8")
    print(json.dumps({"event": "evaluation_written", "kind": args.kind, "window": report["window"],
                      "path": str(out / f"{args.kind}-{stamp}.json")}, default=str), flush=True)
    return 0
