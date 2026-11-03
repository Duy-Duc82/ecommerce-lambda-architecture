"""Canonical Silver columns as the ``marketplace-observation.v1`` wire event.

The speed layer keeps its stateful change detection over canonical observation
events. Those events are derived from the source records with the same
normalisers the batch uses, so a row means the same thing on both sides.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ecomdata.processing.normalize.common import iso_microseconds

_OFFER_FIELDS = (
    "offer_id",
    "marketplace_id",
    "platform_listing_id",
    "seller_id",
    "product_title",
    "brand",
    "category_path",
    "source_url",
    "currency",
    "active_status",
)
_MONEY_FIELDS = (
    "current_price",
    "list_price",
    "shipping_price",
    "discount_amount",
    "discount_percent",
    "rating_value",
    "rating_scale",
)
_COUNT_FIELDS = ("rating_count", "review_count", "sold_count")


def _text(name: str) -> Column:
    return F.col(name).alias(name)


def observation_event_json(canonical: DataFrame) -> DataFrame:
    """``key`` (the partition key) and ``value`` (the event JSON) for each canonical row."""
    offer = F.struct(
        *[_text(name) for name in _OFFER_FIELDS[:9]],
        iso_microseconds(F.col("first_seen_at")).alias("first_seen_at"),
        iso_microseconds(F.col("last_seen_at")).alias("last_seen_at"),
        _text("active_status"),
    )
    observation = F.struct(
        _text("observation_id"),
        F.col("offer_id").alias("offer_id"),
        iso_microseconds(F.col("observed_at")).alias("observed_at"),
        iso_microseconds(F.col("fetched_at")).alias("fetched_at"),
        *[F.col(name).cast("string").alias(name) for name in _MONEY_FIELDS],
        *[F.col(name).cast("long").alias(name) for name in _COUNT_FIELDS],
        _text("availability"),
        F.lit(None).cast("map<string,string>").alias("promotion"),
        F.col("ranking_position").cast("long").alias("ranking_position"),
        _text("raw_uri"),
        F.col("raw_sha256").alias("raw_sha256"),
        _text("adapter_version"),
        _text("crawl_run_id"),
    )
    event = F.struct(
        _text("event_id"),
        _text("schema_version"),
        _text("event_type"),
        iso_microseconds(F.col("occurred_at")).alias("occurred_at"),
        iso_microseconds(F.col("produced_at")).alias("produced_at"),
        _text("marketplace"),
        _text("partition_key"),
        _text("crawl_run_id"),
        _text("raw_uri"),
        F.struct(offer.alias("offer"), observation.alias("observation")).alias("payload"),
    )
    return canonical.select(
        F.col("partition_key").alias("key"),
        F.to_json(event, {"ignoreNullFields": "false"}).alias("value"),
    )
