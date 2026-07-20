"""Speed layer — real-time behavioral-event stream processing.

Reads the canonical event contract from Kafka (one topic), computes per-minute
windowed aggregates with Spark Structured Streaming, and serves them two ways:

  * Elasticsearch index `ecommerce-metrics`  -> Kibana time-series dashboards
  * Redis keys `rt:kpi:*`                     -> low-latency KPI counters / API

Raw event drill-down for Kibana is handled separately by
`data_ingestion/es_indexer.py`. This job only owns the aggregated speed view.

Run:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        speed_layer/speed_layer.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    CHECKPOINTS_DIR,
    ES_HOST,
    ES_INDEX_METRICS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_EVENTS,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PORT,
    SPARK_KAFKA_PACKAGE,
)
from data_ingestion.schemas import BEHAVIOR_EVENT_SCHEMA

APP_NAME = "EcommerceSpeedLayer"
ES_METRICS_INDEX = ES_INDEX_METRICS
WINDOW = os.getenv("SPEED_WINDOW", "1 minute")
WATERMARK = os.getenv("SPEED_WATERMARK", "2 minutes")


def build_spark() -> SparkSession:
    builder = SparkSession.builder.appName(APP_NAME)
    if SPARK_KAFKA_PACKAGE:
        builder = builder.config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
    builder = (
        builder.config("spark.sql.streaming.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_events(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_EVENTS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), BEHAVIOR_EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .filter(F.col("event_type").isNotNull())
        .withColumn("event_time", F.to_timestamp("event_time"))
        .filter(F.col("event_time").isNotNull())
    )


def windowed_metrics(events):
    return (
        events.withWatermark("event_time", WATERMARK)
        .groupBy(F.window("event_time", WINDOW), "event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("user_id").alias("unique_users"),
            F.sum(F.coalesce("price", F.lit(0.0))).alias("amount"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_type",
            "event_count",
            "unique_users",
            "amount",
        )
    )


def _es_client():
    from elasticsearch import Elasticsearch

    return Elasticsearch(ES_HOST)


def _redis_client():
    import redis

    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def write_batch(batch_df, batch_id: int) -> None:
    """Push one micro-batch of aggregates to Elasticsearch + Redis (from driver)."""
    rows = batch_df.collect()
    if not rows:
        return

    from elasticsearch import helpers

    es = _es_client()
    r = _redis_client()
    pipe = r.pipeline(transaction=False)
    actions = []

    for row in rows:
        window_start = row["window_start"].isoformat() if row["window_start"] else ""
        event_type = row["event_type"]
        revenue = float(row["amount"]) if event_type == "purchase" else 0.0
        doc = {
            "@timestamp": window_start,
            "window_start": window_start,
            "window_end": row["window_end"].isoformat() if row["window_end"] else "",
            "event_type": event_type,
            "event_count": int(row["event_count"]),
            "unique_users": int(row["unique_users"]),
            "revenue": revenue,
        }
        actions.append({"_index": ES_METRICS_INDEX, "_id": f"{window_start}:{event_type}", "_source": doc})

        # Redis KPI: latest value per event_type + rolling revenue series.
        pipe.hset(f"rt:kpi:{event_type}", mapping={
            "window_start": window_start,
            "event_count": int(row["event_count"]),
            "unique_users": int(row["unique_users"]),
            "revenue": revenue,
        })
        pipe.expire(f"rt:kpi:{event_type}", 3600)
        if event_type == "purchase":
            pipe.lpush("rt:kpi:revenue_series", json.dumps({"t": window_start, "revenue": revenue}))
            pipe.ltrim("rt:kpi:revenue_series", 0, 240)

    if actions:
        helpers.bulk(es, actions, raise_on_error=False)
    pipe.execute()
    print(f"[SpeedLayer] batch={batch_id} windows={len(rows)} -> ES({ES_METRICS_INDEX}) + Redis")


def main() -> None:
    spark = build_spark()
    print("=" * 60)
    print("SPEED LAYER — real-time aggregates")
    print(f"  Kafka:  {KAFKA_BOOTSTRAP_SERVERS} topic={KAFKA_TOPIC_EVENTS}")
    print(f"  ES:     {ES_HOST} index={ES_METRICS_INDEX}")
    print(f"  Redis:  {REDIS_HOST}:{REDIS_PORT}")
    print(f"  Window: {WINDOW}  Watermark: {WATERMARK}")
    print("=" * 60)

    events = read_events(spark)
    metrics = windowed_metrics(events)
    query = (
        metrics.writeStream.foreachBatch(write_batch)
        .outputMode("update")
        .option("checkpointLocation", str(CHECKPOINTS_DIR / "speed_metrics"))
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
