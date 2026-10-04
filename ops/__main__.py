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
    validate [--json PATH] [--restored]  section 9.2
    status                        the last run of each component
    backup [--path DIR]           section 12.2, under the batch lock
    restore --from ID [--path DIR] section 12.3, into a fresh project only
    backups [--path DIR]          list what is in the backup directory
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

INIT_SQL = Path(__file__).resolve().parents[1] / "scripts" / "init_postgres.sql"
# The ops container's one writable bind (./data/ops on the host), so a second
# Compose project can read a backup the first one wrote.
DEFAULT_BACKUP_ROOT = "/reports/backups"


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


def _store(path: str):
    from ops.backup import BackupStore

    return BackupStore(Path(path))


def run_backup(path: str, backup_id: str | None) -> int:
    from ops.backup import LiveLake, LivePostgres, backup, tool_versions

    manifest = backup(LiveLake(), LivePostgres(), _store(path), backup_id=backup_id,
                      tools=tool_versions(), log=lambda line: print(line, flush=True))
    print(json.dumps({"event": "backup_written", "backup_id": manifest["backup_id"],
                      "pointer_run_id": manifest["pointer_run_id"], "counts": manifest["counts"],
                      "files": len(manifest["files"]),
                      "path": str(Path(path) / manifest["backup_id"])}, sort_keys=True))
    return 0


def run_restore(path: str, backup_id: str, sample_size: int) -> int:
    from ops.backup import LiveLake, LivePostgres, PASS, report, restore, restore_checks

    lake, db = LiveLake(), LivePostgres()
    manifest = restore(lake, db, _store(path), backup_id=backup_id,
                       log=lambda line: print(line, flush=True))
    results = restore_checks(lake, db, manifest, sample_size=sample_size)
    print(report(results))
    # The fourth check of plan 12.3 needs Spark, so mp.ps1 runs it next; this
    # prints what it needs rather than leaving the operator to look it up.
    print(json.dumps({"event": "quality_only_batch_next", "as_of": manifest["pointer_as_of"],
                      "pointer_run_id": manifest["pointer_run_id"]}, sort_keys=True))
    return 0 if all(result.status == PASS for result in results) else 1


def list_backups(path: str) -> int:
    from ops.backup import MANIFEST_NAME

    root = Path(path)
    found = []
    for manifest_path in sorted(root.glob(f"*/{MANIFEST_NAME}")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        found.append({"backup_id": manifest["backup_id"], "created_at": manifest["created_at"],
                      "pointer_run_id": manifest["pointer_run_id"], "counts": manifest["counts"]})
    print(json.dumps({"root": str(root), "backups": found}, indent=2, sort_keys=True))
    return 0


def run_validate(json_path: str | None, *, restored: bool = False) -> int:
    from ops.validate import PASS, LiveSources, default_limits, report, validate

    results = validate(LiveSources(), default_limits())
    text = report(results)
    print(text)
    if json_path:
        Path(json_path).write_text(text + "\n", encoding="utf-8")
    if restored:
        from ops.backup import judge_restored_validate

        ok, tolerated, unexpected = judge_restored_validate(results)
        print(json.dumps({"event": "validate_restored", "passed": ok, "tolerated": tolerated,
                          "unexpected": unexpected,
                          "why": "Kafka, Elasticsearch and Redis are not backed up (plan 12.1); "
                                 "they refill from new crawls"},
                         sort_keys=True))
        return 0 if ok else 1
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
    validate_parser.add_argument("--restored", action="store_true",
                                 help="tolerate the empty Elasticsearch and Redis of a just-restored stack")
    sub.add_parser("status")
    backup_parser = sub.add_parser("backup")
    backup_parser.add_argument("--path", default=DEFAULT_BACKUP_ROOT)
    backup_parser.add_argument("--id", dest="backup_id", default=None, help="defaults to bk-<UTC timestamp>")
    restore_parser = sub.add_parser("restore")
    restore_parser.add_argument("--from", dest="backup_id", required=True)
    restore_parser.add_argument("--path", default=DEFAULT_BACKUP_ROOT)
    restore_parser.add_argument("--sample", type=int, default=None,
                                help="raw artifacts to reparse (default 5)")
    sub.add_parser("storage-snapshot", help="record what each store holds, once per UTC day (read-only)")
    backups_parser = sub.add_parser("backups")
    backups_parser.add_argument("--path", default=DEFAULT_BACKUP_ROOT)
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
        return run_validate(args.json_path, restored=args.restored)
    if args.command == "backup":
        return run_backup(args.path, args.backup_id)
    if args.command == "restore":
        from ops.backup import DEFAULT_REPARSE_SAMPLE
        return run_restore(args.path, args.backup_id,
                           DEFAULT_REPARSE_SAMPLE if args.sample is None else args.sample)
    if args.command == "storage-snapshot":
        from ops.storage import run_snapshot
        return 0 if run_snapshot()["measured"] else 1
    if args.command == "backups":
        return list_backups(args.path)
    return status()


if __name__ == "__main__":
    sys.exit(main())
