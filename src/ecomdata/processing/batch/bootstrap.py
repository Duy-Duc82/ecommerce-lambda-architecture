"""Composition root of the marketplace batch: settings -> real adapters.

Logic modules take their collaborators as arguments; this module is where the
loaded settings become a Spark session factory, the PostgreSQL repositories,
the object store and the batch lock.
"""
from __future__ import annotations

from datetime import datetime

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
    )


def context_for(settings: Settings, *, run_id: str, as_of: datetime, silver_uri: str, gold_root_uri: str):
    from ecomdata.processing.batch.context import MarketplaceBatchContext

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


def default_uris(settings: Settings) -> tuple[str, str]:
    """(Silver root URI, serving Gold root URI)."""
    from ecomdata.infra.object_store import object_store_for

    store = object_store_for(settings.storage)
    return store.uri("silver", settings.batch.silver_root), store.uri("gold", GOLD_DATASET)


def batch_dependencies(settings: Settings, *, publish_cache: bool = True):
    """The real adapters of a batch run: PostgreSQL audit, object store, Spark lake, advisory lock.

    ``publish_cache=False`` (``--skip-postgres``) keeps the audit and the quality results in memory and
    publishes no cache, but the crawl audit is still read from PostgreSQL.
    """
    from ecomdata.infra.object_store import object_store_for
    from ecomdata.infra.postgres import connection_factory, jdbc_properties, jdbc_url
    from ecomdata.processing.batch.lake import SparkLakeIO, read_crawl_audit, silver_schemas
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
    )
    return BatchDependencies(
        audit=MarketplaceBatchAudit(factory) if publish_cache else MemoryAudit(),
        quality=MarketplaceQualityRepository(factory) if publish_cache else MemoryQuality(),
        store=store,
        io=io,
        spark_provider=provider,
        crawl_audit=lambda spark: read_crawl_audit(spark, url, props),
        publisher=MarketplaceCachePublisher(factory, url, props) if publish_cache else None,
        lock=lambda: exclusive_batch(connection_factory(pg), key=settings.batch.lock_key),
    )
