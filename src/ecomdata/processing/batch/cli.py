"""Command line of the marketplace batch: ``python -m ecomdata.processing.batch <command>``.

``run`` runs one stage or all of them over one batch run; ``scheduler`` is the
polling loop kept for the ``legacy-scheduler`` profile. This is a composition
root: it is where the loaded settings meet the real adapters.
"""
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from typing import Any

from ecomdata.settings import Settings, load_settings

STAGE_NAMES = ("silver", "dimensions", "marts", "quality", "publish", "promote")


def build_parser(silver_root: str, gold_root: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m ecomdata.processing.batch", description="Marketplace batch")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="run one stage or all of them over one batch run")
    run.add_argument("--run-id", required=True)
    run.add_argument("--as-of", required=True, help="ISO-8601 instant with a timezone")
    scope = run.add_mutually_exclusive_group(required=True)
    scope.add_argument("--stage", action="append", choices=STAGE_NAMES, help="a stage to run (repeatable)")
    scope.add_argument("--all", action="store_true", help="every stage, in order, in this process")
    run.add_argument("--silver-uri", default=silver_root)
    run.add_argument(
        "--gold-root-uri", default=gold_root, help="a root other than the default is a scratch run that never publishes"
    )
    run.add_argument(
        "--skip-postgres", action="store_true", help="no audit row and no cache; the crawl audit is still read"
    )
    run.add_argument(
        "--resume", action="store_true", help="continue a FAILED or QUALITY_FAILED run from where it stopped"
    )
    run.add_argument("--allow-backfill", action="store_true", help="let an older as-of become the serving version")
    run.add_argument("--quality-only", action="store_true", help="write Gold and the manifest, then hold the run")
    run.add_argument("--force", action="store_true", help="run a stage again although the run already passed it")
    run.add_argument("--silver-since", help="rebuild Silver day partitions from this date (YYYY-MM-DD) on")

    scheduler = commands.add_parser("scheduler", help="the polling loop (profile legacy-scheduler)")
    scheduler.add_argument("--max-ticks", type=int, default=None, help="stop after N ticks (tests and smoke only)")
    return parser


def main(argv: list[str] | None = None, *, settings: Settings | None = None) -> None:
    settings = settings or load_settings(dotenv=True)
    from ecomdata.processing.batch.bootstrap import default_uris

    silver_root, gold_root = default_uris(settings)
    args = build_parser(silver_root, gold_root).parse_args(argv)
    if args.command == "scheduler":
        scheduler_main(args, settings)
        return
    run_main(args, settings, serving_gold_root_uri=gold_root)


def run_main(args: argparse.Namespace, settings: Settings, *, serving_gold_root_uri: str) -> None:
    from datetime import date

    from ecomdata.processing.batch.bootstrap import batch_dependencies, context_for
    from ecomdata.processing.batch.lock import ALREADY_RUNNING_EXIT_CODE, BatchAlreadyRunning
    from ecomdata.processing.batch.quality import QualityGateFailure
    from ecomdata.processing.batch.runner import run_batch, select_stages
    from ecomdata.processing.batch.stages.base import RunOptions

    context = context_for(
        settings,
        run_id=args.run_id,
        as_of=datetime.fromisoformat(args.as_of.replace("Z", "+00:00")),
        silver_uri=args.silver_uri,
        gold_root_uri=args.gold_root_uri,
    )
    options = RunOptions(
        allow_backfill=args.allow_backfill,
        quality_only=args.quality_only,
        publish_cache=not args.skip_postgres,
        serving_gold_root_uri=serving_gold_root_uri,
        silver_since=date.fromisoformat(args.silver_since) if args.silver_since else None,
        force=args.force,
    )
    deps = batch_dependencies(settings, publish_cache=not args.skip_postgres)
    try:
        report = run_batch(
            context,
            deps=deps,
            stages=select_stages(None if args.all else args.stage),
            options=options,
            resume=args.resume,
        )
    except QualityGateFailure as refusal:
        # A CI log alone should be enough to see why publication was refused.
        print(
            json.dumps(
                {
                    "run_id": context.run_id,
                    "status": "QUALITY_FAILED",
                    "quality_status": "FAIL",
                    "mandatory_failure_count": refusal.decision.mandatory_failures,
                    "failing_checks": list(refusal.failing),
                },
                sort_keys=True,
            )
        )
        raise SystemExit(1) from None
    except BatchAlreadyRunning as refusal:
        # Nothing was started, so there is nothing to clean up or audit.
        print(
            json.dumps({"run_id": context.run_id, "status": "ALREADY_RUNNING", "lock_key": refusal.key}, sort_keys=True)
        )
        raise SystemExit(ALREADY_RUNNING_EXIT_CODE) from None
    result = report.result()
    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "status": result.status,
                "stages": {item.stage: ("skipped" if item.skipped else item.status.value) for item in report.stages},
                "silver_rows": result.silver_rows,
                "dataset_counts": result.dataset_counts,
                "quality_status": result.quality_status,
                "mandatory_failure_count": result.mandatory_failure_count,
                "manifest_uri": result.manifest_uri,
                "manifest_promoted": result.manifest_promoted,
                "promotion_reason": result.promotion_reason,
            },
            sort_keys=True,
        )
    )


def scheduler_main(args: argparse.Namespace, settings: Settings) -> None:
    from ecomdata.infra.lifecycle import StopSignal, install_signal_handlers
    from ecomdata.infra.postgres import connection_factory
    from ecomdata.processing.batch.bootstrap import batch_dependencies, context_for, default_uris
    from ecomdata.processing.batch.runner import run_batch
    from ecomdata.processing.batch.scheduler import PostgresRunStatus, run_scheduler, tick
    from ecomdata.processing.batch.stages.base import RunOptions

    stop = StopSignal()
    install_signal_handlers(stop)
    run_status = PostgresRunStatus(connection_factory(settings.postgres))
    silver_uri, gold_root_uri = default_uris(settings)
    interval, lag = settings.batch.interval_seconds, settings.batch.as_of_lag_seconds

    def run_batch_window(context: Any, *, resume: bool, serving_gold_root_uri: str) -> Any:
        configured = context_for(
            settings,
            run_id=context.run_id,
            as_of=context.as_of,
            silver_uri=context.silver_uri,
            gold_root_uri=context.gold_root_uri,
        )
        # Never allow_backfill: a backfill is a deliberate operator command.
        options = RunOptions(serving_gold_root_uri=serving_gold_root_uri)
        return run_batch(configured, deps=batch_dependencies(settings), options=options, resume=resume).result()

    run_scheduler(
        lambda now: tick(
            now,
            run_status=run_status,
            run_batch=run_batch_window,
            silver_uri=silver_uri,
            gold_root_uri=gold_root_uri,
            interval_seconds=interval,
            lag_seconds=lag,
        ),
        clock=lambda: datetime.now(UTC),
        stop=stop,
        interval_seconds=interval,
        lag_seconds=lag,
        max_ticks=args.max_ticks,
        log=lambda line: print(line, flush=True),
    )
