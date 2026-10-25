"""Composition root of the marketplace batch: settings -> real adapters.

Logic modules take their collaborators as arguments; this module is where the
loaded settings become a Spark session factory, the PostgreSQL repositories,
the object store and the batch lock.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any

from ecomdata.domain.datasets import GOLD_DATASET
from ecomdata.settings import Settings, load_settings


def spark_factory(settings: Settings, app_name: str | None = None):
    from ecomdata.infra.spark import SparkSessionFactory

    return SparkSessionFactory.for_storage(
        settings.storage,
        app_name=app_name or settings.spark.batch_app_name,
        shuffle_partitions=settings.spark.batch_shuffle_partitions,
        driver_memory=settings.spark.driver_memory,
        master=settings.spark.master,
    )


def context_for(settings: Settings, *, run_id: str, as_of: datetime, silver_uri: str, gold_root_uri: str):
    from ecomdata.processing.batch.warehouse import MarketplaceBatchContext

    batch, quality = settings.batch, settings.quality
    return MarketplaceBatchContext(
        run_id,
        as_of,
        silver_uri,
        gold_root_uri,
        freshness_seconds=batch.freshness_seconds,
        freshness_rule_version=batch.freshness_rule_version,
        counter_rule_version=batch.counter_rule_version,
        counter_max_gap_seconds=batch.counter_max_gap_seconds,
        anomaly_window_days=batch.anomaly_window_days,
        anomaly_min_samples=batch.anomaly_min_samples,
        anomaly_mad_threshold=batch.anomaly_mad_threshold,
        anomaly_iqr_multiplier=batch.anomaly_iqr_multiplier,
        anomaly_rule_version=batch.anomaly_rule_version,
        quality_rule_version=quality.rule_version,
        future_tolerance_seconds=quality.future_tolerance_seconds,
        reconciliation_settle_seconds=quality.reconciliation_settle_seconds,
        reconciliation_lookback_seconds=quality.reconciliation_lookback_seconds,
        allowed_currencies=quality.allowed_currencies,
        percentile_accuracy=batch.percentile_accuracy,
        sample_limit=quality.sample_limit,
    )


def warehouse_dependencies(settings: Settings, *, publish_cache: bool = True):
    from ecomdata.infra.object_store import object_store_for
    from ecomdata.infra.postgres import connection_factory, jdbc_properties, jdbc_url
    from ecomdata.processing.batch.lock import exclusive_batch
    from ecomdata.processing.batch.repositories import (
        MarketplaceBatchAudit,
        MarketplaceCachePublisher,
        MarketplaceQualityRepository,
    )
    from ecomdata.processing.batch.warehouse import WarehouseDependencies, read_crawl_audit

    pg = settings.postgres
    factory = connection_factory(pg)
    store = object_store_for(settings.storage)
    url, props = jdbc_url(pg), jdbc_properties(pg)
    return WarehouseDependencies(
        spark_factory=spark_factory(settings).create,
        read_crawl_audit=lambda spark: read_crawl_audit(spark, url, props),
        writer=store.put,
        reader=store.get,
        audit=MarketplaceBatchAudit(factory) if publish_cache else None,
        quality_repository=MarketplaceQualityRepository(factory) if publish_cache else None,
        cache_publisher=MarketplaceCachePublisher(factory, url, props) if publish_cache else None,
        lock=lambda: exclusive_batch(connection_factory(pg), key=settings.batch.lock_key),
    )


def default_uris(settings: Settings) -> tuple[str, str]:
    """(Silver URI, serving Gold root URI)."""
    from ecomdata.infra.object_store import object_store_for

    store = object_store_for(settings.storage)
    return store.uri("silver", settings.batch.silver_dataset), store.uri("gold", GOLD_DATASET)


def warehouse_main(argv: list[str] | None = None, *, settings: Settings | None = None) -> None:
    settings = settings or load_settings(dotenv=True)
    silver_uri, serving_gold_root_uri = default_uris(settings)
    parser = argparse.ArgumentParser(description="Marketplace temporal warehouse")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--silver-uri", default=silver_uri)
    parser.add_argument(
        "--gold-root-uri",
        default=serving_gold_root_uri,
        help="a root other than the default is a scratch run that never publishes",
    )
    parser.add_argument("--skip-postgres", action="store_true")
    parser.add_argument(
        "--resume", action="store_true", help="resume an existing non-SUCCEEDED run with the same context"
    )
    parser.add_argument("--allow-backfill", action="store_true", help="let an older as-of become the published version")
    parser.add_argument(
        "--quality-only",
        action="store_true",
        help="write Gold and the run manifest, then stop; never promote or publish",
    )
    args = parser.parse_args(argv)
    from ecomdata.processing.batch.lock import ALREADY_RUNNING_EXIT_CODE, BatchAlreadyRunning
    from ecomdata.processing.batch.quality import QualityGateFailure
    from ecomdata.processing.batch.warehouse import run_marketplace_warehouse

    context = context_for(
        settings,
        run_id=args.run_id,
        as_of=datetime.fromisoformat(args.as_of.replace("Z", "+00:00")),
        silver_uri=args.silver_uri,
        gold_root_uri=args.gold_root_uri,
    )
    try:
        result = run_marketplace_warehouse(
            context,
            deps=warehouse_dependencies(settings, publish_cache=not args.skip_postgres),
            publish_cache=not args.skip_postgres,
            resume=args.resume,
            allow_backfill=args.allow_backfill,
            quality_only=args.quality_only,
            serving_gold_root_uri=serving_gold_root_uri,
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
    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "status": result.status,
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


def scheduler_main(argv: list[str] | None = None, *, settings: Settings | None = None) -> None:
    from datetime import UTC

    from ecomdata.infra.lifecycle import StopSignal, install_signal_handlers
    from ecomdata.infra.postgres import connection_factory
    from ecomdata.processing.batch.scheduler import PostgresRunStatus, run_scheduler, tick
    from ecomdata.processing.batch.warehouse import run_marketplace_warehouse

    parser = argparse.ArgumentParser(description="Marketplace batch scheduler")
    parser.add_argument("--max-ticks", type=int, default=None, help="stop after N ticks (tests and smoke only)")
    args = parser.parse_args(argv)
    settings = settings or load_settings(dotenv=True)
    stop = StopSignal()
    install_signal_handlers(stop)
    run_status = PostgresRunStatus(connection_factory(settings.postgres))
    silver_uri, gold_root_uri = default_uris(settings)
    interval, lag = settings.batch.interval_seconds, settings.batch.as_of_lag_seconds

    def run_batch(context: Any, **options: Any) -> Any:
        configured = context_for(
            settings,
            run_id=context.run_id,
            as_of=context.as_of,
            silver_uri=context.silver_uri,
            gold_root_uri=context.gold_root_uri,
        )
        return run_marketplace_warehouse(configured, deps=warehouse_dependencies(settings), **options)

    run_scheduler(
        lambda now: tick(
            now,
            run_status=run_status,
            run_batch=run_batch,
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
