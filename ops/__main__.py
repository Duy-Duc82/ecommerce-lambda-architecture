"""`python -m ops` — the operations commands behind scripts/mp.ps1 (plan section 9).

Run inside the ``ops`` container, so every command reaches the stack by its
service names. What needs Docker itself (``up``, ``down``, the one-shot Spark
batch) stays in mp.ps1 on the host; everything that reads or writes the stack
lives here.

    migrate                       re-apply scripts/init_postgres.sql
    seed [seed_frontier args]     crawler.seed_frontier, unchanged
    seed-smoke                    seed the smoke universe on the ACTIVE tier, re-enabling parked tasks
    park-smoke                    disable the smoke's pending tasks once it ends
    smoke-wait [--timeout S]      section 9.1 steps 2-3
    wait-quiet [--timeout S]      wait for Silver lag 0 once the crawler is stopped
    batch-plan [--settle S]       the as_of and run_id for a one-shot batch
    validate [--json PATH]        section 9.2
    status                        the last run of each component
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

INIT_SQL = Path(__file__).resolve().parents[1] / "scripts" / "init_postgres.sql"


def migrate() -> int:
    """Every statement in the script is idempotent, so re-applying it is safe."""
    from common.postgres import postgres_connection_factory

    sql = INIT_SQL.read_text(encoding="utf-8")
    with postgres_connection_factory()() as conn, conn.cursor() as cur:
        cur.execute(sql)
    print(json.dumps({"event": "migrated", "script": INIT_SQL.name}))
    return 0


def seed_smoke() -> int:
    from config.settings import CRAWL_MAX_ATTEMPTS
    from crawler.frontier import PostgresCrawlFrontier
    from crawler.scheduling import CrawlTier
    from crawler.seed_frontier import MARKETPLACE_IDS, seed
    from common.postgres import postgres_connection_factory
    from ops.smoke import SMOKE_CATEGORIES, SMOKE_PAGES

    from ops.smoke import activate

    factory = postgres_connection_factory()
    created = seed(PostgresCrawlFrontier(factory), marketplace_code="tiki",
                   marketplace_id=MARKETPLACE_IDS["tiki"], categories=SMOKE_CATEGORIES, pages=SMOKE_PAGES,
                   tier=CrawlTier.ACTIVE, priority=0, max_attempts=CRAWL_MAX_ATTEMPTS)
    # A smoke run before this one parked its tasks; seeding alone is a no-op then.
    reactivated = activate(factory)
    print(json.dumps({"event": "seeded_smoke", "created": created, "reactivated": reactivated,
                      "tasks": len(SMOKE_CATEGORIES) * SMOKE_PAGES}))
    return 0


def park_smoke() -> int:
    from common.postgres import postgres_connection_factory
    from ops.smoke import park

    print(json.dumps({"event": "parked_smoke", "disabled": park(postgres_connection_factory())}))
    return 0


def smoke_wait(timeout: float) -> int:
    from ops.smoke import smoke_targets, wait
    from ops.validate import LiveSources

    met = wait(LiveSources(), targets=smoke_targets(), timeout_seconds=timeout, log=lambda line: print(line, flush=True))
    return 0 if met else 1


def wait_quiet(timeout: float) -> int:
    from ops.smoke import wait_for_quiet
    from ops.validate import LiveSources

    return 0 if wait_for_quiet(LiveSources(), timeout_seconds=timeout, log=lambda line: print(line, flush=True)) else 1


def batch_plan(settle: int, *, after_last_crawl: bool = False, sleep=None, now=None) -> int:
    """The as_of and run_id of a one-shot batch.

    By default the newest window that has settled: now minus the settle
    delay. With ``after_last_crawl`` (the smoke), the first minute boundary at
    least one settle delay after the newest crawl run finished, waiting until
    the clock has passed it: every run the smoke made is then inside the
    window and settled, which "now minus settle" cannot promise when all of
    them are minutes old.
    """
    import time

    from batch_layer.marketplace_scheduler import run_id_for

    clock = now or (lambda: datetime.now(timezone.utc))
    if after_last_crawl:
        from ops.validate import LiveSources

        (last,), = LiveSources().query("SELECT max(completed_at) FROM audit.crawl_run")
        if last is None:
            raise SystemExit("no finished crawl run to anchor the window on")
        target = last + timedelta(seconds=settle)
        as_of = target.replace(second=0, microsecond=0) + (timedelta(minutes=1) if target.second or target.microsecond else timedelta())
        # as_of must not lie in the future: the gate refuses observations past it.
        while clock() <= as_of:
            (sleep or time.sleep)(max(1.0, (as_of - clock()).total_seconds() + 1))
    else:
        as_of = (clock() - timedelta(seconds=settle)).replace(second=0, microsecond=0)
    print(json.dumps({"run_id": run_id_for(as_of), "as_of": as_of.isoformat().replace("+00:00", "Z")}))
    return 0


def run_validate(json_path: str | None) -> int:
    from ops.validate import PASS, LiveSources, default_limits, report, validate

    results = validate(LiveSources(), default_limits())
    text = report(results)
    print(text)
    if json_path:
        Path(json_path).write_text(text + "\n", encoding="utf-8")
    return 0 if all(r.status == PASS for r in results) else 1


def status() -> int:
    from ops.validate import LiveSources

    src = LiveSources()
    last = {
        "crawl_attempt": src.query("SELECT task_id, status, started_at FROM audit.crawl_request_attempt ORDER BY started_at DESC LIMIT 1"),
        "speed_batch": src.query("SELECT batch_id, status, started_at FROM audit.marketplace_speed_batch ORDER BY started_at DESC LIMIT 1"),
        "batch_run": src.query("SELECT run_id, status, as_of FROM audit.marketplace_batch_run ORDER BY started_at DESC LIMIT 1"),
    }
    pointer = src.current_pointer()
    print(json.dumps({**{k: (list(v[0]) if v else None) for k, v in last.items()},
                      "pointer": pointer and {"run_id": pointer["run_id"], "as_of": pointer["as_of"]}},
                     sort_keys=True, default=str, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["seed"]:
        # Pass straight through: seed_frontier owns its own arguments.
        from crawler import seed_frontier
        sys.argv = ["crawler.seed_frontier", *argv[1:]]
        seed_frontier.main()
        return 0
    parser = argparse.ArgumentParser(prog="python -m ops", description="Marketplace operations commands")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate")
    sub.add_parser("seed", help="crawler.seed_frontier arguments")
    sub.add_parser("seed-smoke")
    sub.add_parser("park-smoke")
    wait_parser = sub.add_parser("smoke-wait")
    wait_parser.add_argument("--timeout", type=float, default=900)
    quiet_parser = sub.add_parser("wait-quiet")
    quiet_parser.add_argument("--timeout", type=float, default=180)
    plan_parser = sub.add_parser("batch-plan")
    plan_parser.add_argument("--settle", type=int, default=None, help="defaults to MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS")
    plan_parser.add_argument("--after-last-crawl", action="store_true", help="anchor on the newest finished crawl run (the smoke)")
    validate_parser = sub.add_parser("validate")
    validate_parser.add_argument("--json", dest="json_path", default=None)
    sub.add_parser("status")
    args = parser.parse_args(argv)
    if args.command == "migrate":
        return migrate()
    if args.command == "seed-smoke":
        return seed_smoke()
    if args.command == "park-smoke":
        return park_smoke()
    if args.command == "smoke-wait":
        return smoke_wait(args.timeout)
    if args.command == "wait-quiet":
        return wait_quiet(args.timeout)
    if args.command == "batch-plan":
        from config.settings import MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS
        return batch_plan(MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS if args.settle is None else args.settle,
                          after_last_crawl=args.after_last_crawl)
    if args.command == "validate":
        return run_validate(args.json_path)
    return status()


if __name__ == "__main__":
    sys.exit(main())
