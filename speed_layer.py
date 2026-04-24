import os
from typing import Iterator

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


APP_NAME = "EcommerceSpeedLayer"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce_events")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "./checkpoints/speed_layer")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

SPARK_KAFKA_PACKAGE = os.getenv(
    "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
)


EVENT_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", LongType(), True),
    ]
)


def build_spark() -> SparkSession:
    """Khoi tao SparkSession cho speed layer va nap Kafka connector neu da cau hinh."""
    builder = SparkSession.builder.appName(APP_NAME)
    if SPARK_KAFKA_PACKAGE:
        builder = builder.config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
    return builder.getOrCreate()


def write_partition_to_redis(rows: Iterator) -> None:
    """Ghi du lieu aggregate cua tung partition vao Redis bang pipeline."""
    import redis

    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    pipe = client.pipeline(transaction=False)
    count = 0

    for row in rows:
        key = f"realtime:{row['product_name']}:{row['action']}"
        pipe.set(key, int(row["count"]))
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
    print(f"batch={batch_id} rows_written={batch_df.count()}")


def main() -> None:
    """Chay luong speed layer: doc Kafka, xu ly realtime, aggregate va ghi Redis."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .load()
    )

    events = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .filter(F.col("product_name").isNotNull() & F.col("action").isNotNull())
        .withColumn("event_time", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))
    )

    counts_df = events.groupBy("product_name", "action").count()

    query = (
        counts_df.writeStream.foreachBatch(write_to_redis)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()