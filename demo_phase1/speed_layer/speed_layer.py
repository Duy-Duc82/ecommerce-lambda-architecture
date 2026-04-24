"""
Speed Layer (Mo rong) — Xu ly luong du lieu thoi gian thuc tu Kafka.

Chuc nang:
  - Parse tat ca event types (page_view, add_to_cart, purchase, review, price_change, search)
  - Real-time aggregations: revenue/minute, active users, conversion rate
  - Windowed computations: sliding 5min, tumbling 1 hour
  - Anomaly alerting: don hang gia tri > threshold
  - Ghi ket qua vao Redis (serving layer)

Chay:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 speed_layer/speed_layer.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterator

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    CHECKPOINTS_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_EVENTS,
    KAFKA_TOPIC_ORDERS,
    KAFKA_TOPIC_PRICES,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PORT,
    SPARK_KAFKA_PACKAGE,
)
from data_ingestion.schemas import UNIFIED_EVENT_SCHEMA


APP_NAME = "EcommerceSpeedLayer"

# Nguong canh bao
ANOMALY_ORDER_THRESHOLD = float(os.getenv("ANOMALY_ORDER_THRESHOLD", "100000000"))  # 100 trieu VND


# ============================================================
# SPARK SESSION
# ============================================================

def build_spark() -> SparkSession:
    """Khoi tao SparkSession cho speed layer voi Kafka connector."""
    builder = SparkSession.builder.appName(APP_NAME)
    if SPARK_KAFKA_PACKAGE:
        builder = builder.config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
    builder = builder.config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    return builder.getOrCreate()


# ============================================================
# REDIS WRITER
# ============================================================

def write_partition_to_redis(rows: Iterator) -> None:
    """Ghi du lieu aggregate cua tung partition vao Redis bang pipeline."""
    import redis

    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    pipe = client.pipeline(transaction=False)
    count = 0

    for row in rows:
        row_dict = row.asDict()
        for key, value in row_dict.items():
            if key.startswith("_redis_key"):
                redis_key = str(value)
            elif key.startswith("_redis_"):
                continue

        # Xay dung key va value linh hoat
        parts = []
        if "product_name" in row_dict and row_dict["product_name"]:
            parts.append(row_dict["product_name"])
        if "action" in row_dict and row_dict["action"]:
            parts.append(row_dict["action"])
        if "event_type" in row_dict and row_dict["event_type"]:
            parts.append(row_dict["event_type"])
        if "category" in row_dict and row_dict["category"]:
            parts.append(row_dict["category"])

        key_suffix = ":".join(parts) if parts else "unknown"

        # Ghi cac metrics khac nhau
        if "count" in row_dict:
            pipe.set(f"realtime:count:{key_suffix}", int(row_dict["count"]))
        if "total_revenue" in row_dict and row_dict["total_revenue"] is not None:
            pipe.set(f"realtime:revenue:{key_suffix}", float(row_dict["total_revenue"]))
        if "unique_users" in row_dict and row_dict["unique_users"] is not None:
            pipe.set(f"realtime:users:{key_suffix}", int(row_dict["unique_users"]))

        count += 1
        if count % 500 == 0:
            pipe.execute()

    if count % 500 != 0:
        pipe.execute()


def write_to_redis(batch_df, batch_id: int) -> None:
    """Xu ly tung micro-batch: bo qua batch rong va ghi ket qua sang Redis."""
    if batch_df.rdd.isEmpty():
        return

    batch_df.foreachPartition(write_partition_to_redis)
    print(f"[SpeedLayer] batch={batch_id} rows={batch_df.count()}")


def write_alerts_to_redis(batch_df, batch_id: int) -> None:
    """Ghi canh bao anomaly vao Redis."""
    import json

    import redis

    if batch_df.rdd.isEmpty():
        return

    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

    for row in batch_df.collect():
        alert = {
            "user_id": row["user_id"],
            "order_id": row.get("order_id", ""),
            "total_amount": float(row.get("total_amount", 0)),
            "event_time": str(row.get("event_time", "")),
            "alert_type": "HIGH_VALUE_ORDER",
        }
        client.lpush("alerts:fraud", json.dumps(alert, ensure_ascii=False))
        client.ltrim("alerts:fraud", 0, 999)  # Giu toi da 1000 alerts

    print(f"[ALERT] batch={batch_id} alerts={batch_df.count()}")


# ============================================================
# STREAM PROCESSING
# ============================================================

def read_kafka_stream(spark: SparkSession) -> "DataFrame":
    """Doc stream tu nhieu Kafka topics."""
    topics = ",".join([KAFKA_TOPIC_EVENTS, KAFKA_TOPIC_ORDERS, KAFKA_TOPIC_PRICES])

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topics)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON thanh struct
    events = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_str", "topic", "timestamp AS kafka_ts")
        .select(
            F.from_json(F.col("json_str"), UNIFIED_EVENT_SCHEMA).alias("data"),
            "topic",
            "kafka_ts",
        )
        .select("data.*", "topic", "kafka_ts")
        .filter(F.col("event_type").isNotNull())
        .withColumn(
            "event_time",
            F.to_timestamp(F.from_unixtime(F.col("timestamp"))).cast(TimestampType()),
        )
    )

    return events


def start_event_count_stream(events, spark: SparkSession) -> None:
    """Stream 1: Dem so luong event theo loai va san pham."""
    counts_df = (
        events
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            F.window("event_time", "1 minute"),
            "event_type",
            "product_name",
            "category",
        )
        .agg(
            F.count("*").alias("count"),
            F.countDistinct("user_id").alias("unique_users"),
        )
    )

    query = (
        counts_df.writeStream
        .foreachBatch(write_to_redis)
        .outputMode("update")
        .option("checkpointLocation", str(CHECKPOINTS_DIR / "event_counts"))
        .trigger(processingTime="10 seconds")
        .start()
    )
    return query


def start_revenue_stream(events, spark: SparkSession) -> None:
    """Stream 2: Tinh doanh thu theo thoi gian thuc."""
    revenue_df = (
        events
        .filter(F.col("event_type") == "purchase")
        .withWatermark("event_time", "30 seconds")
        .groupBy(F.window("event_time", "5 minutes"))
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("*").alias("order_count"),
            F.countDistinct("user_id").alias("unique_buyers"),
            F.avg("total_amount").alias("avg_order_value"),
        )
    )

    def write_revenue(batch_df, batch_id: int) -> None:
        import redis

        if batch_df.rdd.isEmpty():
            return
        client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        for row in batch_df.collect():
            window_start = str(row["window"]["start"])
            client.hset(f"realtime:revenue_5min:{window_start}", mapping={
                "total_revenue": str(row["total_revenue"] or 0),
                "order_count": str(row["order_count"]),
                "unique_buyers": str(row["unique_buyers"]),
                "avg_order_value": str(row["avg_order_value"] or 0),
            })
            client.expire(f"realtime:revenue_5min:{window_start}", 3600)

    query = (
        revenue_df.writeStream
        .foreachBatch(write_revenue)
        .outputMode("update")
        .option("checkpointLocation", str(CHECKPOINTS_DIR / "revenue"))
        .trigger(processingTime="10 seconds")
        .start()
    )
    return query


def start_anomaly_stream(events, spark: SparkSession) -> None:
    """Stream 3: Phat hien don hang bat thuong trong thoi gian thuc."""
    anomalies = (
        events
        .filter(
            (F.col("event_type") == "purchase")
            & (F.col("total_amount") > ANOMALY_ORDER_THRESHOLD)
        )
    )

    query = (
        anomalies.writeStream
        .foreachBatch(write_alerts_to_redis)
        .outputMode("append")
        .option("checkpointLocation", str(CHECKPOINTS_DIR / "anomalies"))
        .trigger(processingTime="5 seconds")
        .start()
    )
    return query


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    """Chay toan bo speed layer voi nhieu stream song song."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("SPEED LAYER - Real-time Processing")
    print(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Topics: {KAFKA_TOPIC_EVENTS}, {KAFKA_TOPIC_ORDERS}, {KAFKA_TOPIC_PRICES}")
    print(f"  Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"  Anomaly threshold: {ANOMALY_ORDER_THRESHOLD:,.0f} VND")
    print("=" * 60)

    events = read_kafka_stream(spark)

    # Khoi dong cac stream song song
    q1 = start_event_count_stream(events, spark)
    q2 = start_revenue_stream(events, spark)
    q3 = start_anomaly_stream(events, spark)

    print("\n[SpeedLayer] 3 streams dang chay. Nhan Ctrl+C de dung.\n")

    # Doi tat ca streams
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
