"""Composition root of the marketplace batch: settings -> real adapters.

Logic modules take their collaborators as arguments; this module is where the
loaded settings become a Spark session factory, the PostgreSQL repositories,
the object store and the batch lock.
"""
from __future__ import annotations

from datetime import datetime

from ecomdata.domain.crawl_audit import CRAWL_AUDIT_ROOT
from ecomdata.domain.datasets import GOLD_DATASET
from ecomdata.settings import Settings


def spark_factory(settings: Settings, app_name: str | None = None):
    from ecomdata.infra.spark import SparkSessionFactory

    return SparkSessionFactory.for_storage(
        settings.storage,
        app_name=app_name or settings.spark.batch_app_name,
        shuffle_partitions=settings.spark.batch_shuffle_partitions,
        driver_memory=settings.spark.driver_memory,
        master=settings.spark.master,
        runtime=settings.spark.runtime,
    )


def context_for(settings: Settings, *, run_id: str, as_of: datetime, silver_uri: str, gold_root_uri: str):
    from ecomdata.processing.batch.context import MarketplaceBatchContext

    batch, quality, speed = settings.batch, settings.quality, settings.speed
    return MarketplaceBatchContext(
        run_id,
        as_of,
        silver_uri,
        gold_root_uri,
        freshness_seconds=batch.freshness_seconds,
        freshness_rule_version=batch.freshness_rule_version,
        counter_rule_version=batch.counter_rule_version,
        counter_max_gap_seconds=batch.counter_max_gap_seconds,
        change_rule_version=speed.change_rule_version,
        large_drop_absolute=speed.large_drop_absolute,
        large_drop_relative=speed.large_drop_relative,
        anomaly_window_days=batch.anomaly_window_days,
        anomaly_min_samples=batch.anomaly_min_samples,
        anomaly_mad_threshold=batch.anomaly_mad_threshold,
        anomaly_iqr_multiplier=batch.anomaly_iqr_multiplier,
        anomaly_rule_version=batch.anomaly_rule_version,
        discount_rules=batch.discount_rules,
        quality_rule_version=quality.rule_version,
        future_tolerance_seconds=quality.future_tolerance_seconds,
        reconciliation_settle_seconds=quality.reconciliation_settle_seconds,
        reconciliation_lookback_seconds=quality.reconciliation_lookback_seconds,
        allowed_currencies=quality.allowed_currencies,
        percentile_accuracy=batch.percentile_accuracy,
        sample_limit=quality.sample_limit,
    )


def default_uris(settings: Settings) -> tuple[str, str]:
    """(Silver root URI, serving Gold root URI)."""
    from ecomdata.infra.object_store import object_store_for

    store = object_store_for(settings.storage)
    return store.uri("silver", settings.batch.silver_root), store.uri("gold", GOLD_DATASET)


def star_snapshot(settings: Settings | None = None) -> dict:
    """Rows, Delta version and last operation of every star table (read-only)."""
    from ecomdata.domain.datasets import STAR_NAMES
    from ecomdata.processing.batch.dw_inspect import describe_table
    from ecomdata.settings import load_settings

    settings = load_settings(dotenv=True) if settings is None else settings
    _, gold_root = default_uris(settings)
    spark = spark_factory(settings, app_name="dw-inspect").create()
    try:
        tables = {name: describe_table(spark, f"{gold_root.rstrip('/')}/dw/{name}") for name in STAR_NAMES}
    finally:
        spark.stop()
    return {"gold_root": gold_root, "tables": tables}


def batch_dependencies(settings: Settings, *, publish_cache: bool = True):
    """The real adapters of a batch run: PostgreSQL audit, object store, Spark lake, advisory lock.

    ``publish_cache=False`` (``--skip-postgres``) keeps the audit and the quality results in memory and
    publishes no cache. The crawl audit is read from the lake, or from PostgreSQL when
    ``MARKETPLACE_CRAWL_AUDIT_SOURCE=postgres`` (a lake written before the crawler kept its audit there).
    """
    from ecomdata.infra.object_store import object_store_for
    from ecomdata.infra.postgres import connection_factory, jdbc_properties, jdbc_url
    from ecomdata.processing.batch.lake import SparkLakeIO, read_crawl_audit, read_crawl_audit_lake, silver_schemas
    from ecomdata.processing.batch.lock import exclusive_batch
    from ecomdata.processing.batch.memory import MemoryAudit, MemoryQuality
    from ecomdata.processing.batch.repositories import (
        MarketplaceBatchAudit,
        MarketplaceCachePublisher,
        MarketplaceQualityRepository,
    )
    from ecomdata.processing.batch.runner import BatchDependencies

    pg = settings.postgres
    factory = connection_factory(pg)
    store = object_store_for(settings.storage)
    url, props = jdbc_url(pg), jdbc_properties(pg)
    provider = spark_factory(settings).create
    io = SparkLakeIO(
        provider,
        records_uri=store.uri("bronze", settings.batch.records_dataset),
        silver_root_uri=default_uris(settings)[0],
        silver_schemas=silver_schemas(),
        table_format=settings.storage.table_format,
    )
    if settings.batch.crawl_audit_source == "postgres":
        crawl_audit = lambda spark: read_crawl_audit(spark, url, props)  # noqa: E731
    else:
        audit_root = store.uri("bronze", CRAWL_AUDIT_ROOT)
        crawl_audit = lambda spark: read_crawl_audit_lake(spark, audit_root)  # noqa: E731
    return BatchDependencies(
        audit=MarketplaceBatchAudit(factory) if publish_cache else MemoryAudit(),
        quality=MarketplaceQualityRepository(factory) if publish_cache else MemoryQuality(),
        store=store,
        io=io,
        spark_provider=provider,
        crawl_audit=crawl_audit,
        publisher=MarketplaceCachePublisher(factory, url, props) if publish_cache else None,
        lock=lambda: exclusive_batch(connection_factory(pg), key=settings.batch.lock_key),
    )
