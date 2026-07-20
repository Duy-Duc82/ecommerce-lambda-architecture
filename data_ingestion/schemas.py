"""Spark schema for the canonical behavioral-event contract.

Mirrors `config.schema` (the row-level contract) for use by Spark Structured
Streaming and batch. There is exactly one event shape across the platform:
view / cart / purchase behavioral events from the Kaggle store dataset.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

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
