"""Independent batch entrypoint for the marketplace temporal warehouse."""
from __future__ import annotations
import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Mapping
from urllib.parse import quote

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import (
    MARKETPLACE_ALLOWED_CURRENCIES,
    MARKETPLACE_ANOMALY_IQR_MULTIPLIER, MARKETPLACE_ANOMALY_MAD_THRESHOLD,
    MARKETPLACE_ANOMALY_MIN_SAMPLES, MARKETPLACE_ANOMALY_RULE_VERSION,
    MARKETPLACE_ANOMALY_WINDOW_DAYS, MARKETPLACE_BATCH_APP_NAME,
    MARKETPLACE_BATCH_SHUFFLE_PARTITIONS,
    MARKETPLACE_QUALITY_FUTURE_TOLERANCE_SECONDS, MARKETPLACE_QUALITY_RULE_VERSION,
    MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS, MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS,
    MARKETPLACE_COUNTER_MAX_GAP_SECONDS, MARKETPLACE_COUNTER_RULE_VERSION,
    MARKETPLACE_FRESHNESS_RULE_VERSION, MARKETPLACE_FRESHNESS_SECONDS,
    MARKETPLACE_GOLD_DATASET, MARKETPLACE_PERCENTILE_ACCURACY,
    MARKETPLACE_SILVER_DATASET, data_lake_uri,
)
from data_ingestion.schemas import MARKETPLACE_OBSERVATION_WIRE_SCHEMA


RUN_ID_RE = re.compile(r"[a-zA-Z0-9_-]{1,64}\Z")


@dataclass(frozen=True)
class MarketplaceBatchContext:
    run_id: str
    as_of: datetime
    silver_uri: str
    gold_root_uri: str
    freshness_seconds: int = MARKETPLACE_FRESHNESS_SECONDS
    freshness_rule_version: str = MARKETPLACE_FRESHNESS_RULE_VERSION
    counter_rule_version: str = MARKETPLACE_COUNTER_RULE_VERSION
    counter_max_gap_seconds: int = MARKETPLACE_COUNTER_MAX_GAP_SECONDS
    anomaly_window_days: int = MARKETPLACE_ANOMALY_WINDOW_DAYS
    anomaly_min_samples: int = MARKETPLACE_ANOMALY_MIN_SAMPLES
    anomaly_mad_threshold: Decimal = MARKETPLACE_ANOMALY_MAD_THRESHOLD
    anomaly_iqr_multiplier: Decimal = MARKETPLACE_ANOMALY_IQR_MULTIPLIER
    anomaly_rule_version: str = MARKETPLACE_ANOMALY_RULE_VERSION
    quality_rule_version: str = MARKETPLACE_QUALITY_RULE_VERSION
    future_tolerance_seconds: int = MARKETPLACE_QUALITY_FUTURE_TOLERANCE_SECONDS
    reconciliation_settle_seconds: int = MARKETPLACE_QUALITY_RECONCILIATION_SETTLE_SECONDS
    reconciliation_lookback_seconds: int = MARKETPLACE_QUALITY_RECONCILIATION_LOOKBACK_SECONDS
    allowed_currencies: tuple[str, ...] = MARKETPLACE_ALLOWED_CURRENCIES

    def __post_init__(self) -> None:
        if not RUN_ID_RE.fullmatch(self.run_id): raise ValueError("run_id must match [a-zA-Z0-9_-]{1,64}")
        if self.as_of.tzinfo is None or self.as_of.utcoffset() is None: raise ValueError("as_of must be timezone-aware")
        object.__setattr__(self, "as_of", self.as_of.astimezone(timezone.utc))
        for name in ("silver_uri", "gold_root_uri", "freshness_rule_version", "counter_rule_version", "anomaly_rule_version", "quality_rule_version"):
            if not getattr(self, name).strip(): raise ValueError(f"{name} must be non-empty")
        for name in ("freshness_seconds", "counter_max_gap_seconds", "anomaly_window_days", "anomaly_min_samples", "anomaly_mad_threshold", "anomaly_iqr_multiplier", "future_tolerance_seconds", "reconciliation_settle_seconds", "reconciliation_lookback_seconds"):
            if getattr(self, name) <= 0: raise ValueError(f"{name} must be positive")
        # A minimum above the frame size would make every row report
        # INSUFFICIENT_HISTORY while nothing looked broken.
        if self.anomaly_min_samples > self.anomaly_window_days: raise ValueError("anomaly_min_samples cannot exceed anomaly_window_days")
        # An empty reconciliation window would reconcile nothing and pass.
        if self.reconciliation_lookback_seconds <= self.reconciliation_settle_seconds: raise ValueError("reconciliation_lookback_seconds must exceed reconciliation_settle_seconds")
        # Normalised and sorted so a manifest built from this context is
        # byte-stable regardless of how the environment spelled the list.
        codes = tuple(sorted({str(code).strip().upper() for code in self.allowed_currencies if str(code).strip()}))
        if not codes: raise ValueError("allowed_currencies must list at least one code")
        if any(len(code) != 3 or not code.isalpha() for code in codes): raise ValueError("allowed_currencies must hold ISO-4217 style codes")
        object.__setattr__(self, "allowed_currencies", codes)


class ConflictingObservationError(ValueError): pass


@dataclass(frozen=True)
class GoldWriteResult:
    dataset_name: str
    uri: str
    row_count: int


# A passing run deliberately kept off the serving version for inspection.
QUALITY_ONLY = "QUALITY_ONLY"
# A run into a Gold root other than the serving one. Manifests and the pointer
# live at one fixed place, so such a run must not write either.
SCRATCH_GOLD_ROOT = "SCRATCH_GOLD_ROOT"


@dataclass(frozen=True)
class MarketplaceBatchResult:
    run_id: str
    status: str
    silver_rows: int
    dataset_counts: Mapping[str, int]
    quality_status: str = "PASS"
    mandatory_failure_count: int = 0
    manifest_uri: str | None = None
    manifest_promoted: bool = False
    promotion_reason: str | None = None


def build_spark() -> SparkSession:
    from config.storage import spark_hadoop_options
    builder = (SparkSession.builder.appName(MARKETPLACE_BATCH_APP_NAME)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", str(MARKETPLACE_BATCH_SHUFFLE_PARTITIONS))
               .config("spark.sql.sources.partitionOverwriteMode", "dynamic"))
    # Empty for the local profile. For any other the S3A credentials and
    # endpoint come from the profile, which is what lets this job read and
    # write MinIO or a cloud bucket at all (Phase 8 plan section 6.6).
    for key, value in spark_hadoop_options().items():
        builder = builder.config(f"spark.hadoop.{key}", value)
    return builder.getOrCreate()


def read_marketplace_silver(spark: SparkSession, silver_uri: str) -> DataFrame:
    return spark.read.schema(MARKETPLACE_OBSERVATION_WIRE_SCHEMA).option("recursiveFileLookup", "true").json(silver_uri)


def _decimal(col): return F.col(col).cast("decimal(38,6)")


def flatten_marketplace_observations(wire: DataFrame) -> DataFrame:
    p, o = F.col("payload.offer"), F.col("payload.observation")
    result = wire.select(
        "event_id", "schema_version", "event_type", F.to_timestamp("occurred_at").alias("occurred_at"), F.to_timestamp("produced_at").alias("produced_at"), "marketplace", "partition_key", "crawl_run_id", "raw_uri",
        p.offer_id.alias("offer_id"), p.marketplace_id.alias("marketplace_id"), p.platform_listing_id.alias("platform_listing_id"), p.seller_id.alias("seller_id"), p.product_title.alias("product_title"), p.brand.alias("brand"), p.category_path.alias("category_path"), p.source_url.alias("source_url"), p.currency.alias("currency"), F.to_timestamp(p.first_seen_at).alias("first_seen_at"), F.to_timestamp(p.last_seen_at).alias("last_seen_at"), p.active_status.alias("active_status"),
        o.observation_id.alias("observation_id"), o.offer_id.alias("observation_offer_id"), o.raw_uri.alias("observation_raw_uri"), o.crawl_run_id.alias("observation_crawl_run_id"), F.to_timestamp(o.observed_at).alias("observed_at"), F.to_timestamp(o.fetched_at).alias("fetched_at"), _decimal("payload.observation.current_price").alias("current_price"), _decimal("payload.observation.list_price").alias("list_price"), _decimal("payload.observation.shipping_price").alias("shipping_price"), _decimal("payload.observation.discount_amount").alias("discount_amount"), _decimal("payload.observation.discount_percent").alias("discount_percent"), _decimal("payload.observation.rating_value").alias("rating_value"), _decimal("payload.observation.rating_scale").alias("rating_scale"), o.rating_count.alias("rating_count"), o.review_count.alias("review_count"), o.sold_count.alias("sold_count"), o.availability.alias("availability"), F.to_json(o.promotion).alias("promotion_json"), o.ranking_position.alias("ranking_position"), o.raw_sha256.alias("raw_sha256"), o.adapter_version.alias("adapter_version"), F.to_date(F.to_timestamp(o.observed_at)).alias("observed_date"),
    )
    # The nested offer/observation equality is checked explicitly because
    # malformed rows must fail the batch rather than disappear in a filter.
    bad = result.filter(F.col("event_id").isNull() | F.col("observation_id").isNull() | (F.col("event_id") != F.col("observation_id")) | (F.col("offer_id") != F.col("observation_offer_id")) | (F.col("raw_uri") != F.col("observation_raw_uri")) | (F.col("crawl_run_id") != F.col("observation_crawl_run_id")) | F.col("current_price").isNull() | (F.col("current_price") < 0) | F.col("observed_at").isNull() | F.col("fetched_at").isNull() | F.col("raw_sha256").isNull() | F.col("raw_uri").isNull() | F.col("crawl_run_id").isNull())
    if bad.limit(1).count(): raise ValueError("invalid marketplace Silver row: missing lineage or non-negative numeric field")
    return result.drop("observation_offer_id", "observation_raw_uri", "observation_crawl_run_id")


def _require_aware(as_of: datetime) -> datetime:
    if as_of.tzinfo is None or as_of.utcoffset() is None: raise ValueError("as_of must be timezone-aware")
    return as_of.astimezone(timezone.utc)


def observations_as_of(flat: DataFrame, as_of: datetime) -> DataFrame:
    """Silver as it stood at ``as_of``: nothing observed after it, inclusive.

    A run is a view of one instant. Without this, Silver that kept growing
    after as_of leaks into a rerun or a backfill, so the same context stops
    producing the same Gold and an older window can never pass its gates.
    """
    cutoff = F.lit(_require_aware(as_of)).cast("timestamp")
    return flat.filter(F.col("observed_at") <= cutoff)


def crawl_audit_as_of(attempts: DataFrame, runs: DataFrame, as_of: datetime) -> tuple[DataFrame, DataFrame]:
    """The crawl audit as it stood at ``as_of``.

    An attempt is known once it completed, so one still in flight at as_of
    reported nothing yet. A crawl run exists once it started, finished or not;
    whether it had settled is check 8's question, not this cut's.
    """
    cutoff = F.lit(_require_aware(as_of)).cast("timestamp")
    return attempts.filter(F.col("completed_at") <= cutoff), runs.filter(F.col("started_at") <= cutoff)


def deduplicate_marketplace_observations(flat: DataFrame) -> DataFrame:
    semantic_cols = [name for name in flat.columns if name != "_content_hash"]
    marked = flat.withColumn("_content_hash", F.sha2(F.to_json(F.struct(*[F.col(x) for x in semantic_cols])), 256))
    conflicts = marked.groupBy("observation_id").agg(F.countDistinct("_content_hash").alias("hashes")).filter("hashes > 1")
    conflict_ids = [row.observation_id for row in conflicts.collect()]
    if conflict_ids: raise ConflictingObservationError(f"conflicting canonical content for observation IDs: {sorted(conflict_ids)[:10]}")
    return marked.drop("_content_hash").dropDuplicates(["observation_id"])


def read_crawl_audit(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    from batch_layer.marketplace_marts import attach_marketplace_code
    from config.settings import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER
    jdbc = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    props = {"user": POSTGRES_USER, "password": POSTGRES_PASSWORD, "driver": "org.postgresql.Driver"}
    attempts = spark.read.jdbc(jdbc, "audit.crawl_request_attempt", properties=props)
    runs = spark.read.jdbc(jdbc, "audit.crawl_run", properties=props)
    # Neither attempt nor run rows name a marketplace code; the frontier does.
    # Resolving it here keeps the marts free of a third audit frame.
    frontier = spark.read.jdbc(jdbc, "audit.crawl_frontier", properties=props)
    return attach_marketplace_code(attempts, frontier), runs


def _join_uri(root: str, *parts: str) -> str:
    return root.rstrip("/") + "/" + "/".join(parts)


def write_run_scoped_gold(marts: Mapping[str, DataFrame], context: MarketplaceBatchContext) -> dict[str, GoldWriteResult]:
    if set(marts) != {"offer_current", "seller_current", "offer_price_history_daily", "offer_change_daily", "offer_freshness", "category_price_daily", "source_coverage_daily", "crawl_reliability_daily", "counter_delta_daily", "price_anomaly_daily"}:
        raise ValueError("marts must contain exactly the ten marketplace datasets")
    run_root = _join_uri(context.gold_root_uri, "runs", f"run_id={quote(context.run_id, safe='')}")
    results = {}
    daily = {"offer_price_history_daily", "offer_change_daily", "category_price_daily", "source_coverage_daily", "crawl_reliability_daily", "counter_delta_daily", "price_anomaly_daily"}
    for name, frame in marts.items():
        uri = _join_uri(run_root, name)
        # Static, whatever the session says: a run directory belongs wholly to
        # this run, so a rerun must replace it, not only the partitions it
        # still has rows for.
        writer = frame.write.mode("overwrite").option("partitionOverwriteMode", "static")
        if name in daily:
            date_col = "observed_date" if "observed_date" in frame.columns else "request_date"
            writer = writer.partitionBy("marketplace", date_col)
        writer.parquet(uri)
        results[name] = GoldWriteResult(name, uri, frame.count())
    return results


def default_batch_lock():
    from batch_layer.marketplace_lock import exclusive_batch
    from common.postgres import postgres_connection_factory
    return exclusive_batch(postgres_connection_factory())


def run_marketplace_warehouse(context: MarketplaceBatchContext, *, lock=None, **options) -> MarketplaceBatchResult:
    """Run one batch under the exclusive batch lock (Phase 8 plan section 6.5).

    The lock is taken before anything else, even under ``publish_cache=False``,
    which still reads the crawl audit from PostgreSQL. A run refused with
    ``BatchAlreadyRunning`` has therefore started nothing: no Spark session,
    no audit row, no Gold. ``lock`` is a zero-argument callable returning a
    context manager; the default is the PostgreSQL advisory lock.
    """
    with (lock or default_batch_lock)():
        return _run_marketplace_warehouse(context, **options)


def _run_marketplace_warehouse(context: MarketplaceBatchContext, *, publish_cache: bool = True, resume: bool = False, spark: SparkSession | None = None, allow_backfill: bool = False, quality_only: bool = False, writer=None, reader=None, serving_gold_root_uri: str | None = None) -> MarketplaceBatchResult:
    """Read Silver, build Gold, judge it, and publish only if it earns it.

    The order below is the contract, not an implementation detail. Quality
    results are persisted and the run manifest is written *before* a refusal
    propagates, so a blocked run is fully documented in both PostgreSQL and
    object storage rather than leaving a log line and nothing else.

    ``serving_gold_root_uri`` names the root the manifests and the pointer
    describe. A run into any other root is a scratch run: it is judged and its
    quality results recorded, but it writes no manifest, publishes nothing and
    promotes nothing.
    """
    from batch_layer.marketplace_manifest import ALREADY_CURRENT, BACKFILL_REFUSED, build_gold_manifest, promote_manifest, promotion_refusal, read_current_manifest, write_run_manifest
    from batch_layer.marketplace_marts import build_marketplace_marts
    from batch_layer.marketplace_quality import QualityGateFailure, decide, evaluate_quality_gates
    if writer is None or reader is None:
        from common.object_store import get_bytes, put_bytes
        writer, reader = writer or put_bytes, reader or get_bytes
    own_spark = spark is None
    spark = spark or build_spark()
    observations = None
    audit = None
    try:
        if publish_cache:
            from batch_layer.marketplace_postgres import MarketplaceBatchAudit
            audit = MarketplaceBatchAudit.from_settings()
            audit.start_run(context, datetime.now(timezone.utc), resume=resume)
        wire = read_marketplace_silver(spark, context.silver_uri)
        # Flattening still validates every Silver row, so corruption fails the
        # run whatever its window. The cut comes before deduplication, so a
        # row past as_of can never collide with one inside it.
        observations = deduplicate_marketplace_observations(observations_as_of(flatten_marketplace_observations(wire), context.as_of)).cache()
        attempts, runs = crawl_audit_as_of(*read_crawl_audit(spark), context.as_of)
        marts = build_marketplace_marts(observations, attempts, runs, context)
        writes = write_run_scoped_gold(marts, context)
        silver_rows = observations.count()
        counts = {name: result.row_count for name, result in writes.items()}
        gold_run_uri = _join_uri(context.gold_root_uri, "runs", f"run_id={quote(context.run_id, safe='')}")
        if audit:
            audit.mark_gold_written(run_id=context.run_id, gold_run_uri=gold_run_uri, silver_rows=silver_rows, dataset_counts=counts, completed_at=None if publish_cache else context.as_of)

        results = evaluate_quality_gates(observations, marts, attempts, runs, context)
        decision = decide(results, context)
        if audit:
            # Its own transaction, before the decision is acted on. Evidence
            # for a refused run is the whole point of the gate.
            from batch_layer.marketplace_postgres import MarketplaceQualityRepository
            MarketplaceQualityRepository.from_settings().record(results, run_id=context.run_id)

        previous = read_current_manifest(reader=reader)
        # A pointer that already names this run (a rerun, or a promotion whose
        # reply was lost) is not its own predecessor: keep the one it recorded.
        previous_run_id = None if previous is None else previous.previous_run_id if previous.run_id == context.run_id else previous.run_id
        manifest = build_gold_manifest(writes, decision, context, previous_run_id=previous_run_id)
        scratch = serving_gold_root_uri is not None and context.gold_root_uri.rstrip("/") != serving_gold_root_uri.rstrip("/")

        if not decision.passed:
            manifest_uri = None if scratch else write_run_manifest(manifest, writer=writer)
            if audit:
                audit.mark_quality_failed(run_id=context.run_id, completed_at=datetime.now(timezone.utc), decision=decision, manifest_uri=manifest_uri)
            raise QualityGateFailure(decision, results)

        if scratch:
            # Its manifest would land beside production's, under a run_id that
            # may be production's too, naming datasets nobody serves.
            if audit:
                audit.mark_held(run_id=context.run_id, completed_at=datetime.now(timezone.utc), reason=SCRATCH_GOLD_ROOT, manifest_uri=None)
            return MarketplaceBatchResult(context.run_id, "GOLD_WRITTEN", silver_rows, counts, "PASS", 0, None, False, SCRATCH_GOLD_ROOT)

        manifest_uri = write_run_manifest(manifest, writer=writer)
        if quality_only:
            # Inspect a suspect window without touching the serving version.
            if audit:
                audit.mark_held(run_id=context.run_id, completed_at=datetime.now(timezone.utc), reason=QUALITY_ONLY, manifest_uri=manifest_uri)
            return MarketplaceBatchResult(context.run_id, "GOLD_WRITTEN", silver_rows, counts, "PASS", 0, manifest_uri, False, QUALITY_ONLY)

        # Asked before publication, not after: the cache has no notion of time,
        # so publishing a window the pointer then refuses would leave the cache
        # serving older data than the pointer names.
        if promotion_refusal(manifest, previous, allow_backfill=allow_backfill) == BACKFILL_REFUSED:
            if audit:
                audit.mark_held(run_id=context.run_id, completed_at=datetime.now(timezone.utc), reason=BACKFILL_REFUSED, manifest_uri=manifest_uri)
            return MarketplaceBatchResult(context.run_id, "GOLD_WRITTEN", silver_rows, counts, "PASS", 0, manifest_uri, False, BACKFILL_REFUSED)

        if publish_cache:
            from batch_layer.marketplace_postgres import MarketplaceCachePublisher
            publisher = MarketplaceCachePublisher.from_settings()
            staged = publisher.stage(marts, run_id=context.run_id)
            publisher.publish(staged, run_id=context.run_id, published_at=context.as_of, quality=decision, manifest_uri=manifest_uri)
            publisher.cleanup(staged)
        # The pointer moves last, and only once everything it would advertise
        # actually exists. Promoting before publication leaves a serving
        # manifest naming a Gold run whose cache was never written, which is a
        # worse state than either the cache or the pointer failing alone.
        # Compare-and-swap against the pointer read above: if another run
        # promoted in between, this one must not overwrite it.
        promotion = promote_manifest(manifest, writer=writer, reader=reader, allow_backfill=allow_backfill,
                                     expected_current_run_id=None if previous is None else previous.run_id)
        if audit:
            audit.mark_promotion(run_id=context.run_id, promoted=promotion.promoted or promotion.reason == ALREADY_CURRENT)
        return MarketplaceBatchResult(context.run_id, "SUCCEEDED" if publish_cache else "GOLD_WRITTEN", silver_rows, counts, "PASS", 0, promotion.manifest_uri or manifest_uri, promotion.promoted, promotion.reason)
    except QualityGateFailure:
        raise
    except Exception as error:
        if audit:
            try: audit.mark_failed(run_id=context.run_id, completed_at=datetime.now(timezone.utc), error=error)
            except Exception: pass
        raise
    finally:
        if observations is not None: observations.unpersist()
        if own_spark: spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Marketplace temporal warehouse")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--silver-uri", default=data_lake_uri("silver", MARKETPLACE_SILVER_DATASET))
    serving_gold_root_uri = data_lake_uri("gold", MARKETPLACE_GOLD_DATASET)
    parser.add_argument("--gold-root-uri", default=serving_gold_root_uri, help="a root other than the default is a scratch run that never publishes")
    parser.add_argument("--skip-postgres", action="store_true")
    parser.add_argument("--resume", action="store_true", help="resume an existing non-SUCCEEDED run with the same context")
    parser.add_argument("--allow-backfill", action="store_true", help="let an older as-of become the published version")
    parser.add_argument("--quality-only", action="store_true", help="write Gold and the run manifest, then stop; never promote or publish")
    args = parser.parse_args()
    from batch_layer.marketplace_lock import ALREADY_RUNNING_EXIT_CODE, BatchAlreadyRunning
    from batch_layer.marketplace_quality import QualityGateFailure
    context = MarketplaceBatchContext(args.run_id, datetime.fromisoformat(args.as_of.replace("Z", "+00:00")), args.silver_uri, args.gold_root_uri)
    try:
        result = run_marketplace_warehouse(context, publish_cache=not args.skip_postgres, resume=args.resume, allow_backfill=args.allow_backfill, quality_only=args.quality_only, serving_gold_root_uri=serving_gold_root_uri)
    except QualityGateFailure as refusal:
        # A CI log alone should be enough to see why publication was refused.
        print(json.dumps({"run_id": context.run_id, "status": "QUALITY_FAILED", "quality_status": "FAIL",
                          "mandatory_failure_count": refusal.decision.mandatory_failures,
                          "failing_checks": list(refusal.failing)}, sort_keys=True))
        raise SystemExit(1)
    except BatchAlreadyRunning as refusal:
        # Nothing was started, so there is nothing to clean up or audit.
        print(json.dumps({"run_id": context.run_id, "status": "ALREADY_RUNNING", "lock_key": refusal.key}, sort_keys=True))
        raise SystemExit(ALREADY_RUNNING_EXIT_CODE)
    print(json.dumps({"run_id": result.run_id, "status": result.status, "silver_rows": result.silver_rows,
                      "dataset_counts": result.dataset_counts, "quality_status": result.quality_status,
                      "mandatory_failure_count": result.mandatory_failure_count,
                      "manifest_uri": result.manifest_uri, "manifest_promoted": result.manifest_promoted,
                      "promotion_reason": result.promotion_reason}, sort_keys=True))


if __name__ == "__main__": main()
