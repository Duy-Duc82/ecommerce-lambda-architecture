"""Spark schema for the canonical behavioral-event contract.

Mirrors `config.schema` (the row-level contract) for use by Spark Structured
Streaming and batch. There is exactly one event shape across the platform:
view / cart / purchase behavioral events from the Kaggle store dataset.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_OFFER_WIRE_SCHEMA = StructType([
    StructField("offer_id", StringType(), False),
    StructField("marketplace_id", StringType(), False),
    StructField("platform_listing_id", StringType(), False),
    StructField("seller_id", StringType(), True),
    StructField("product_title", StringType(), False),
    StructField("brand", StringType(), True),
    StructField("category_path", StringType(), True),
    StructField("source_url", StringType(), False),
    StructField("currency", StringType(), False),
    StructField("first_seen_at", StringType(), False),
    StructField("last_seen_at", StringType(), False),
    StructField("active_status", StringType(), False),
])
_OBSERVATION_WIRE_SCHEMA = StructType([
    StructField("observation_id", StringType(), False),
    StructField("offer_id", StringType(), False),
    StructField("observed_at", StringType(), False),
    StructField("fetched_at", StringType(), False),
    StructField("current_price", StringType(), False),
    StructField("list_price", StringType(), True),
    StructField("shipping_price", StringType(), True),
    StructField("discount_amount", StringType(), True),
    StructField("discount_percent", StringType(), True),
    StructField("rating_value", StringType(), True),
    StructField("rating_scale", StringType(), True),
    StructField("rating_count", LongType(), True),
    StructField("review_count", LongType(), True),
    StructField("sold_count", LongType(), True),
    StructField("availability", StringType(), False),
    StructField("promotion", MapType(StringType(), StringType(), True), True),
    StructField("ranking_position", LongType(), True),
    StructField("raw_uri", StringType(), False),
    StructField("raw_sha256", StringType(), False),
    StructField("adapter_version", StringType(), False),
    StructField("crawl_run_id", StringType(), False),
])

MARKETPLACE_OBSERVATION_WIRE_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("schema_version", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("occurred_at", StringType(), False),
    StructField("produced_at", StringType(), False),
    StructField("marketplace", StringType(), False),
    StructField("partition_key", StringType(), False),
    StructField("crawl_run_id", StringType(), False),
    StructField("raw_uri", StringType(), False),
    StructField("payload", StructType([
        StructField("offer", _OFFER_WIRE_SCHEMA, False),
        StructField("observation", _OBSERVATION_WIRE_SCHEMA, False),
    ]), False),
])

MARKETPLACE_CHANGE_WIRE_SCHEMA = StructType([
    StructField("event_id", StringType(), False), StructField("schema_version", StringType(), False),
    StructField("change_type", StringType(), False), StructField("detected_at", StringType(), False),
    StructField("marketplace", StringType(), False), StructField("offer_id", StringType(), False),
    StructField("previous_observation_id", StringType(), True), StructField("current_observation_id", StringType(), False),
    StructField("field_name", StringType(), True), StructField("previous_value", StringType(), True),
    StructField("current_value", StringType(), True), StructField("rule_version", StringType(), False),
])

MARKETPLACE_SILVER_SCHEMA = StructType([
    StructField("event_id", StringType(), False), StructField("schema_version", StringType(), False),
    StructField("event_type", StringType(), False), StructField("occurred_at", TimestampType(), False),
    StructField("produced_at", TimestampType(), False), StructField("marketplace", StringType(), False),
    StructField("partition_key", StringType(), False), StructField("crawl_run_id", StringType(), False),
    StructField("raw_uri", StringType(), False), StructField("offer_id", StringType(), False),
    StructField("marketplace_id", StringType(), False), StructField("platform_listing_id", StringType(), False),
    StructField("seller_id", StringType(), True), StructField("product_title", StringType(), False),
    StructField("brand", StringType(), True), StructField("category_path", StringType(), True),
    StructField("source_url", StringType(), False), StructField("currency", StringType(), False),
    StructField("first_seen_at", TimestampType(), False), StructField("last_seen_at", TimestampType(), False),
    StructField("active_status", StringType(), False), StructField("observation_id", StringType(), False),
    StructField("observed_at", TimestampType(), False), StructField("fetched_at", TimestampType(), False),
    StructField("current_price", DecimalType(38, 6), False), StructField("list_price", DecimalType(38, 6), True),
    StructField("shipping_price", DecimalType(38, 6), True), StructField("discount_amount", DecimalType(38, 6), True),
    StructField("discount_percent", DecimalType(38, 6), True), StructField("rating_value", DecimalType(38, 6), True),
    StructField("rating_scale", DecimalType(38, 6), True), StructField("rating_count", LongType(), True),
    StructField("review_count", LongType(), True), StructField("sold_count", LongType(), True),
    StructField("availability", StringType(), False), StructField("promotion_json", StringType(), True),
    StructField("ranking_position", LongType(), True), StructField("raw_sha256", StringType(), False),
    StructField("adapter_version", StringType(), False), StructField("observed_date", DateType(), False),
])

# JSON payload published by the Kafka producer (see data_ingestion/producer.py).
# `event_time` travels as an ISO-8601 string; the consumer casts to timestamp.
BEHAVIOR_EVENT_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("user_session", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
    ]
)
