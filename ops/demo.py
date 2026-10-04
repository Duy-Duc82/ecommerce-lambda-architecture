"""The scripted thesis demo — Phase 9 plan section 11.2 (P2-06).

``mp demo`` first runs the smoke in the demo project, which brings the
pipeline up against the offline stub, crawls, streams and publishes one
batch. This module then walks the audience through what that run proves,
one step at a time. It never touches the network beyond the demo project,
and never ``mp-live``.

The two fault steps are drills D3 and D8, narrated: the same functions, the
same assertions, so the demo cannot show a recovery the drills do not prove.
Their records are printed, never written to ``data/ops/drills``, where they
would overwrite the drill evidence the evaluation reads.

``docs/DEMO_SCRIPT.md`` holds the spoken script and the timing for each step.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Callable

from ops import drills
from ops.drills import Record, Stack

ROOT = Path(__file__).resolve().parents[1]
DEMO_BACKUP = ROOT / "data" / "ops" / "demo-backup"


class DemoRecord(Record):
    """A drill record that is shown, not filed with the drill evidence."""

    def write(self) -> Path:  # noqa: D401 - deliberately writes nothing
        return Path(os.devnull)


def say(title: str, lines: list[str]) -> None:
    print(f"\n=== {title} ===", flush=True)
    for line in lines:
        print(f"  {line}", flush=True)


def pause(auto: bool) -> None:
    if not auto:
        input("\n  [Enter] for the next step ")


def kibana_url() -> str:
    return f"http://localhost:{os.environ.get('KIBANA_HOST_PORT', '5601')}"


def superset_url() -> str:
    return f"http://localhost:{os.environ.get('SUPERSET_HOST_PORT', '8088')}"


def step_running(stack: Stack) -> list[str]:
    failing = drills._failing(stack.validate())
    serving = stack.serving_version()
    return [f"validate: {'all checks pass' if not failing else f'FAILING {failing}'}",
            f"serving version: pointer {serving['pointer_run_id']}, cache {serving['cache_run_id']}, "
            f"{serving['cache_offer_rows']} offers in the cache"]


def step_raw_first(stack: Stack) -> list[str]:
    """One attempt's raw artifact, and the Silver observation that names it."""
    rows = stack.query("SELECT crawl_run_id, raw_uri, raw_bytes, parsed_count FROM audit.crawl_request_attempt "
                       "WHERE raw_uri IS NOT NULL AND parsed_count > 0 ORDER BY started_at LIMIT 1")
    if not rows:
        return ["no attempt with a raw artifact yet"]
    crawl_run_id, raw_uri, raw_bytes, parsed = rows[0]
    from config.settings import MARKETPLACE_SILVER_DATASET, data_lake_uri

    names = stack.src.list_objects(data_lake_uri("silver", MARKETPLACE_SILVER_DATASET))
    match = None
    for uri in names:
        event = json.loads(stack.src.read_object(uri) or b"{}")
        if event.get("raw_uri") == raw_uri:
            match = (uri, event)
            break
    lines = [f"1. the crawler stored the response first: {raw_uri}",
             f"   {raw_bytes} bytes, crawl run {crawl_run_id}, {parsed} observations parsed from it",
             f"   present in Bronze: {stack.src.object_exists(raw_uri)}"]
    if match:
        uri, event = match
        obs = event["payload"]["observation"]
        lines += [f"2. a Silver observation names it: {uri}",
                  f"   observation_id {obs['observation_id']}",
                  f"   raw_sha256 {obs['raw_sha256']} (the checksum of those bytes)"]
    return lines


def step_realtime(stack: Stack) -> list[str]:
    from elasticsearch import Elasticsearch

    from config.settings import ES_HOST, ES_INDEX_MARKETPLACE_CHANGES

    es = Elasticsearch(ES_HOST)
    try:
        es.indices.refresh(index=ES_INDEX_MARKETPLACE_CHANGES)
        hits = es.search(index=ES_INDEX_MARKETPLACE_CHANGES, size=1000, query={"match_all": {}})["hits"]["hits"]
    finally:
        es.close()
    kinds = Counter(hit["_source"].get("change_type") for hit in hits)
    return [f"change events in Elasticsearch: {dict(sorted(kinds.items()))}",
            "the stub cuts each page's price by 5 %, then 40 %, then restores it, on successive serves:",
            "PRICE_CHANGED, then LARGE_PRICE_DROP, as a real price would move",
            f"Kibana realtime dashboard: {kibana_url()}  (Dashboards -> Marketplace realtime changes)"]


def step_batch(stack: Stack) -> list[str]:
    serving = stack.serving_version()
    results = stack.query("SELECT check_name, status FROM audit.marketplace_quality_result WHERE run_id = %s "
                          "ORDER BY check_name", (serving["pointer_run_id"],)) if serving["pointer_run_id"] else []
    passed = sum(1 for _, status in results if status == "PASS")
    return [f"batch run {serving['pointer_run_id']} is the serving version: the pointer and the cache agree",
            f"quality gate: {passed}/{len(results)} rules PASS for that run",
            f"Superset price history: {superset_url()}"]


def narrated(stack: Stack, name: str, run: Callable[[Stack, Record], None]) -> list[str]:
    """A drill exactly as `mp drill` runs it, shown instead of filed."""
    rec = DemoRecord(f"demo-{name}")
    error = None
    try:
        drills.baseline(stack, rec)
        run(stack, rec)
    except Exception as exc:  # noqa: BLE001 - shown, then the stack is restored
        error = f"{type(exc).__name__}: {exc}"
    finally:
        failing = drills.restore(stack, rec)
    lines = [f"{s['step']:>9}: " + json.dumps({k: v for k, v in s.items() if k not in ("step", "at")},
                                               default=str)[:160] for s in rec.steps]
    lines.append("PASSED" if error is None and not failing else f"FAILED: {error or failing}")
    return lines


def step_evaluation() -> list[str]:
    lines = []
    for label, folder in (("benchmarks (replayed fixtures)", ROOT / "data" / "ops" / "bench"),
                          ("live evaluation", ROOT / "data" / "ops" / "evaluation")):
        files = sorted(folder.glob("*.json")) if folder.is_dir() else []
        lines.append(f"{label}: {len(files)} file(s)" + (f", latest {files[-1].name}" if files else ""))
    lines.append("python -m ops.bench report renders the benchmark tables; each row carries its dataset label")
    return lines


def snapshot(stack: Stack) -> list[str]:
    """Plan section 11.3: a restorable copy of the state the demo reached."""
    code, reports = stack.ops_in(None, "backup")
    written = next((r for r in reports if r.get("event") == "backup_written"), None)
    if code != 0 or written is None:
        return [f"backup FAILED (exit {code})"]
    DEMO_BACKUP.mkdir(parents=True, exist_ok=True)
    (DEMO_BACKUP / "latest.json").write_text(json.dumps(written, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return [f"backup {written['backup_id']} recorded in {DEMO_BACKUP / 'latest.json'}",
            "replay it offline: .\\scripts\\mp.ps1 -EnvFile env/demo.env restore "
            f"-BackupId {written['backup_id']} -Project mp-demo-replay"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ops.demo", description="The scripted thesis demo (offline)")
    parser.add_argument("--auto", action="store_true", help="no pauses between steps")
    parser.add_argument("--snapshot", action="store_true", help="take a restorable backup at the end")
    parser.add_argument("--skip-faults", action="store_true", help="leave out the D3 and D8 steps")
    args = parser.parse_args(argv)
    if os.environ.get("COMPOSE_PROJECT_NAME") == drills.LIVE_PROJECT or not os.environ.get("MP_CONTAINER_PREFIX"):
        print("the demo runs only in its own project: .\\scripts\\mp.ps1 -EnvFile env/demo.env demo", file=sys.stderr)
        return 2
    stack = Stack()
    drills.refuse_live(stack)
    steps: list[tuple[str, Callable[[], list[str]]]] = [
        ("1. The pipeline is running, offline", lambda: step_running(stack)),
        ("2. Raw first: every observation traces to stored bytes", lambda: step_raw_first(stack)),
        ("3. Realtime: price changes reach Elasticsearch and Kibana", lambda: step_realtime(stack)),
        ("4. Batch truth: quality-gated, versioned serving", lambda: step_batch(stack)),
    ]
    if not args.skip_faults:
        steps += [("5. Failure: MinIO goes down under the Silver sink (drill D3)",
                   lambda: narrated(stack, "d3", drills.d3_minio_down_during_sink)),
                  ("6. Quality gate: a bad run never reaches the cache (drill D8)",
                   lambda: narrated(stack, "d8", drills.d8_quality_failure))]
    steps.append(("7. Evaluation", step_evaluation))
    failed = False
    for title, run in steps:
        lines = run()
        failed = failed or any(line.startswith("FAILED") or "FAILING" in line for line in lines)
        say(title, lines)
        pause(args.auto)
    if args.snapshot:
        say("Offline fallback", snapshot(stack))
    print(json.dumps({"event": "demo_finished", "passed": not failed}), flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
