"""The report evidence bundle — Phase 9 plan section 12.1 (P2-07).

    .\\scripts\\mp.ps1 -EnvFile env/live.env evidence [--skip-tests]

Collects, never measures anew: every number in the bundle comes from a file
another command wrote (benchmarks, evaluations, drill records) or from a
read-only query of the live stack (samples). Each run writes a new directory
``data/ops/evidence/<UTC stamp>/`` and never touches an older one.

``INDEX.md`` lists Brief section 25's checklist. Each item links its file, or
reads ``MISSING`` (the command exits 1), or ``MANUAL`` for what a person
supplies: screenshots, the demo recording, the limitations prose. Each item
carries its dataset label, so a replayed number is never read as a live one.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping

ROOT = Path(__file__).resolve().parents[1]
OPS = ROOT / "data" / "ops"
EVIDENCE = OPS / "evidence"


@dataclass(frozen=True)
class Item:
    """One line of Brief section 25, and where its evidence lives in the bundle."""
    title: str
    paths: tuple[str, ...]           # relative to the bundle; any one present satisfies the item
    dataset: str                     # live, replayed_fixture, stub stack, repository, manual
    manual: bool = False
    note: str = ""


ITEMS: tuple[Item, ...] = (
    Item("Source feasibility matrix", ("docs/SOURCE_FEASIBILITY.md",), "repository",
         note="Brief §3/P0-01; the evidence so far is spread over PROGRESS.md §4a"),
    Item("Monitored-universe definition and volume projection", ("universe/live.env", "evaluation/storage.json"), "live"),
    Item("Crawl-run audit samples", ("samples/crawl_runs.json",), "live"),
    Item("Raw artifact and raw-to-Silver lineage example", ("samples/lineage.json",), "live"),
    Item("Adapter version / schema-drift example", ("integration/drills/d7.json",), "stub stack, fault injected"),
    Item("Data quality and quarantine samples", ("samples/quality_results.json",), "live"),
    Item("Offer/observation counts by source and day", ("evaluation/freshness.json",), "live"),
    Item("Fresh/stale coverage", ("evaluation/freshness.json",), "live"),
    Item("Request success/error/latency distributions", ("evaluation/reliability.json",), "live"),
    Item("Observation and Kafka throughput", ("bench/ingest",), "replayed_fixture"),
    Item("Spark streaming p50/p95 latency", ("bench/speed",), "replayed_fixture"),
    Item("Batch runtime and rows/second", ("bench/batch",), "replayed_fixture"),
    Item("Raw/Parquet storage growth", ("evaluation/storage.json",), "live"),
    Item("Replay/idempotency/restart test results", ("tests/junit.xml", "integration/drills/d5.json"), "repository, stub stack"),
    Item("Price history/change/anomaly examples", ("samples/price_examples.json",), "live"),
    Item("Superset/Kibana screenshots", (), "manual", manual=True),
    Item("Hardware/software/test configuration", ("environment.json",), "this machine"),
    Item("Limitations and failed experiments", (), "manual", manual=True,
         note="report prose; PROGRESS.md records each failed run and production bug"),
    Item("Recorded fallback demo", (), "manual", manual=True, note="docs/DEMO_SCRIPT.md; the video stays outside git"),
)


def item_status(item: Item, bundle: Path) -> str:
    if item.manual:
        return "MANUAL"
    return "PRESENT" if any((bundle / p).exists() for p in item.paths) else "MISSING"


def render_index(bundle: Path, items: tuple[Item, ...] = ITEMS, *, header: Mapping[str, Any]) -> tuple[str, list[str]]:
    lines = ["# Evidence bundle", "", *(f"- **{k}:** {v}" for k, v in header.items()), "",
             "| # | Brief §25 item | status | dataset | evidence | note |", "|---:|---|---|---|---|---|"]
    missing = []
    for n, item in enumerate(items, 1):
        status = item_status(item, bundle)
        if status == "MISSING":
            missing.append(item.title)
        links = ", ".join(f"[{p}]({p})" for p in item.paths if (bundle / p).exists()) or ", ".join(item.paths)
        lines.append(f"| {n} | {item.title} | {status} | {item.dataset} | {links} | {item.note} |")
    return "\n".join(lines) + "\n", missing


def _jsonable(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _latest(folder: Path, pattern: str) -> Path | None:
    files = sorted(folder.glob(pattern)) if folder.is_dir() else []
    return files[-1] if files else None


# -- collectors: each writes into the bundle and may fail on its own -----------

def copy_repository_docs(bundle: Path) -> None:
    for rel in ("docs/SOURCE_FEASIBILITY.md",):
        if (ROOT / rel).exists():
            (bundle / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / rel, bundle / rel)
    (bundle / "universe").mkdir(exist_ok=True)
    shutil.copy2(ROOT / "env" / "live.env", bundle / "universe" / "live.env")


def copy_runs(bundle: Path) -> None:
    drills_out = bundle / "integration" / "drills"
    drills_out.mkdir(parents=True, exist_ok=True)
    for record in sorted((OPS / "drills").glob("d*.json")):
        shutil.copy2(record, drills_out / record.name)
    for name in ("smoke-validate.json",):
        if (OPS / name).exists():
            shutil.copy2(OPS / name, bundle / "integration" / name)
    for scenario in ("crawl", "ingest", "speed", "batch"):
        files = sorted((OPS / "bench").glob(f"{scenario}-*.json")) if (OPS / "bench").is_dir() else []
        if files:
            (bundle / "bench" / scenario).mkdir(parents=True, exist_ok=True)
            for f in files:
                shutil.copy2(f, bundle / "bench" / scenario / f.name)
    if (bundle / "bench").exists():
        from ops.bench import render_report

        results = [json.loads(p.read_text(encoding="utf-8")) for p in sorted((bundle / "bench").rglob("*.json"))]
        (bundle / "bench" / "REPORT.md").write_text(render_report(results), encoding="utf-8")
    for kind in ("reliability", "freshness", "storage"):
        latest = _latest(OPS / "evaluation", f"{kind}-*.json")
        if latest:
            (bundle / "evaluation").mkdir(exist_ok=True)
            shutil.copy2(latest, bundle / "evaluation" / f"{kind}.json")
            md = latest.with_suffix(".md")
            if md.exists():
                shutil.copy2(md, bundle / "evaluation" / f"{kind}.md")


SAMPLES: dict[str, str] = {
    "crawl_runs.json": "SELECT * FROM audit.crawl_run ORDER BY started_at DESC LIMIT 20",
    "quality_results.json": "SELECT * FROM audit.marketplace_quality_result ORDER BY checked_at DESC LIMIT 50",
    "price_examples.json": ("SELECT * FROM cache.marketplace_offer_change_daily WHERE price_change_count > 0 "
                            "ORDER BY observed_date DESC, max_price_drop DESC NULLS LAST LIMIT 30"),
    "anomaly_examples.json": ("SELECT * FROM cache.marketplace_price_anomaly_daily "
                              "ORDER BY observed_date DESC, robust_score DESC NULLS LAST LIMIT 30"),
}


def collect_samples(bundle: Path, query: Callable[[str], list[dict[str, Any]]]) -> None:
    out = bundle / "samples"
    out.mkdir(exist_ok=True)
    for name, sql in SAMPLES.items():
        rows = query(sql)
        if rows:
            (out / name).write_text(json.dumps([{k: _jsonable(v) for k, v in r.items()} for r in rows],
                                               indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    lineage = query("SELECT a.crawl_run_id, a.task_id, a.raw_uri, a.raw_bytes, a.parsed_count, a.started_at "
                    "FROM audit.crawl_request_attempt a WHERE a.parsed_count > 0 ORDER BY a.started_at DESC LIMIT 1")
    if lineage:
        (out / "lineage.json").write_text(json.dumps({"attempt": {k: _jsonable(v) for k, v in lineage[0].items()},
                                                      "how_to_follow": "the Silver observations carrying this raw_uri; "
                                                                       "crawler.reparse re-derives them from Bronze"},
                                                     indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def run_tests(bundle: Path) -> int:
    (bundle / "tests").mkdir(exist_ok=True)
    result = subprocess.run([sys.executable, "-m", "pytest", "tests", "-q", "-p", "no:cacheprovider",
                             f"--junitxml={bundle / 'tests' / 'junit.xml'}"], cwd=ROOT, capture_output=True, text=True)
    (bundle / "tests" / "pytest.txt").write_text(result.stdout[-20000:], encoding="utf-8")
    return result.returncode


def live_query(sql: str) -> list[dict[str, Any]]:
    from common.postgres import postgres_connection_factory

    with postgres_connection_factory()() as conn, conn.cursor() as cur:
        cur.execute(sql)
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.evidence", description="Assemble the report evidence bundle")
    parser.add_argument("--skip-tests", action="store_true", help="leave the test run out (its item then reads MISSING)")
    parser.add_argument("--out", default=str(EVIDENCE))
    args = parser.parse_args(argv)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bundle = Path(args.out) / stamp
    bundle.mkdir(parents=True, exist_ok=False)

    from ops.bench import environment

    problems: dict[str, str] = {}
    steps: list[tuple[str, Callable[[], Any]]] = [
        ("environment", lambda: (bundle / "environment.json").write_text(
            json.dumps(environment(), indent=2, sort_keys=True) + "\n", encoding="utf-8")),
        ("repository", lambda: copy_repository_docs(bundle)),
        ("runs", lambda: copy_runs(bundle)),
        ("samples", lambda: collect_samples(bundle, live_query)),
    ]
    if not args.skip_tests:
        steps.append(("tests", lambda: run_tests(bundle) == 0 or problems.setdefault("tests", "the suite failed")))
    for name, step in steps:
        try:
            step()
        except Exception as error:  # noqa: BLE001 - one missing source must not cost the rest
            problems[name] = f"{type(error).__name__}: {error}"[:300]

    env = json.loads((bundle / "environment.json").read_text(encoding="utf-8")) if (bundle / "environment.json").exists() else {}
    header = {"assembled": stamp, "commit": env.get("git_head"), "dirty tree": env.get("git_dirty"),
              "collection problems": problems or "none"}
    index, missing = render_index(bundle, header=header)
    (bundle / "INDEX.md").write_text(index, encoding="utf-8")
    print(json.dumps({"event": "evidence_written", "bundle": str(bundle), "missing": missing, "problems": problems}),
          flush=True)
    return 1 if missing or problems else 0


if __name__ == "__main__":
    sys.exit(main())
