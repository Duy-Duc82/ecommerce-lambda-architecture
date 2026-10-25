"""The pre-ELT Silver path (canonical observation events as JSON), kept for the AC7 parity test.

Before the ELT refactor the ``silver-sink`` service wrote canonical observation
events and the batch flattened them. That path no longer runs in production; the
parity test builds the old marts through it to compare them with the new ones.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ecomdata.processing.batch.context import ConflictingObservationError
from ecomdata.processing.schemas import MARKETPLACE_OBSERVATION_WIRE_SCHEMA


def read_marketplace_silver(spark: SparkSession, silver_uri: str) -> DataFrame:
    return spark.read.schema(MARKETPLACE_OBSERVATION_WIRE_SCHEMA).option("recursiveFileLookup", "true").json(silver_uri)


def _decimal(col):
    return F.col(col).cast("decimal(38,6)")


def flatten_marketplace_observations(wire: DataFrame) -> DataFrame:
    p, o = F.col("payload.offer"), F.col("payload.observation")
    result = wire.select(
        "event_id",
        "schema_version",
        "event_type",
        F.to_timestamp("occurred_at").alias("occurred_at"),
        F.to_timestamp("produced_at").alias("produced_at"),
        "marketplace",
        "partition_key",
        "crawl_run_id",
        "raw_uri",
        p.offer_id.alias("offer_id"),
        p.marketplace_id.alias("marketplace_id"),
        p.platform_listing_id.alias("platform_listing_id"),
        p.seller_id.alias("seller_id"),
        p.product_title.alias("product_title"),
        p.brand.alias("brand"),
        p.category_path.alias("category_path"),
        p.source_url.alias("source_url"),
        p.currency.alias("currency"),
        F.to_timestamp(p.first_seen_at).alias("first_seen_at"),
        F.to_timestamp(p.last_seen_at).alias("last_seen_at"),
        p.active_status.alias("active_status"),
        o.observation_id.alias("observation_id"),
        o.offer_id.alias("observation_offer_id"),
        o.raw_uri.alias("observation_raw_uri"),
        o.crawl_run_id.alias("observation_crawl_run_id"),
        F.to_timestamp(o.observed_at).alias("observed_at"),
        F.to_timestamp(o.fetched_at).alias("fetched_at"),
        _decimal("payload.observation.current_price").alias("current_price"),
        _decimal("payload.observation.list_price").alias("list_price"),
        _decimal("payload.observation.shipping_price").alias("shipping_price"),
        _decimal("payload.observation.discount_amount").alias("discount_amount"),
        _decimal("payload.observation.discount_percent").alias("discount_percent"),
        _decimal("payload.observation.rating_value").alias("rating_value"),
        _decimal("payload.observation.rating_scale").alias("rating_scale"),
        o.rating_count.alias("rating_count"),
        o.review_count.alias("review_count"),
        o.sold_count.alias("sold_count"),
        o.availability.alias("availability"),
        F.to_json(o.promotion).alias("promotion_json"),
        o.ranking_position.alias("ranking_position"),
        o.raw_sha256.alias("raw_sha256"),
        o.adapter_version.alias("adapter_version"),
        F.to_date(F.to_timestamp(o.observed_at)).alias("observed_date"),
    )
    # The nested offer/observation equality is checked explicitly because
    # malformed rows must fail the batch rather than disappear in a filter.
    bad = result.filter(
        F.col("event_id").isNull()
        | F.col("observation_id").isNull()
        | (F.col("event_id") != F.col("observation_id"))
        | (F.col("offer_id") != F.col("observation_offer_id"))
        | (F.col("raw_uri") != F.col("observation_raw_uri"))
        | (F.col("crawl_run_id") != F.col("observation_crawl_run_id"))
        | F.col("current_price").isNull()
        | (F.col("current_price") < 0)
        | F.col("observed_at").isNull()
        | F.col("fetched_at").isNull()
        | F.col("raw_sha256").isNull()
        | F.col("raw_uri").isNull()
        | F.col("crawl_run_id").isNull()
    )
    if bad.limit(1).count():
        raise ValueError("invalid marketplace Silver row: missing lineage or non-negative numeric field")
    return result.drop("observation_offer_id", "observation_raw_uri", "observation_crawl_run_id")


def deduplicate_marketplace_observations(flat: DataFrame) -> DataFrame:
    semantic_cols = [name for name in flat.columns if name != "_content_hash"]
    marked = flat.withColumn("_content_hash", F.sha2(F.to_json(F.struct(*[F.col(x) for x in semantic_cols])), 256))
    conflicts = (
        marked.groupBy("observation_id").agg(F.countDistinct("_content_hash").alias("hashes")).filter("hashes > 1")
    )
    conflict_ids = [row.observation_id for row in conflicts.collect()]
    if conflict_ids:
        raise ConflictingObservationError(
            f"conflicting canonical content for observation IDs: {sorted(conflict_ids)[:10]}"
        )
    return marked.drop("_content_hash").dropDuplicates(["observation_id"])
