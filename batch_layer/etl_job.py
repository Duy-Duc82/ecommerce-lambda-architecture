"""
Batch ETL Job — Doc du lieu tho tu MinIO, transform va load vao Postgres DW.

Pipeline:
  MinIO (raw JSON/Parquet) → PySpark (clean, enrich, deduplicate) → Postgres (fact + dim tables)

Chay:
  spark-submit --packages <kafka-pkg> batch_layer/etl_job.py
  hoac:
  python -m batch_layer.etl_job  (local mode)
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET_RAW,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    SPARK_KAFKA_PACKAGE,
)
from data_ingestion.schemas import UNIFIED_EVENT_SCHEMA


# ============================================================
# SPARK SESSION
# ============================================================

def build_spark() -> SparkSession:
    """Khoi tao SparkSession cho batch processing voi MinIO (S3) va Postgres."""
    return (
        SparkSession.builder
        .appName("EcommerceBatchETL")
        .config("spark.jars.packages", f"{SPARK_KAFKA_PACKAGE},org.postgresql:postgresql:42.7.5")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .getOrCreate()
    )


# ============================================================
# EXTRACT
# ============================================================

def extract_raw_events(spark: SparkSession, date_str: str | None = None) -> DataFrame:
    """
    Doc raw events tu MinIO.
    Cau truc luu tru: s3a://<bucket>/events/YYYY/MM/DD/*.json
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y/%m/%d")

    path = f"s3a://{MINIO_BUCKET_RAW}/events/{date_str}/"
    print(f"[EXTRACT] Doc du lieu tu {path}")

    return (
        spark.read
        .schema(UNIFIED_EVENT_SCHEMA)
        .json(path)
    )


def extract_from_local(spark: SparkSession, path: str) -> DataFrame:
    """Doc events tu file local (dung khi chua co MinIO)."""
    print(f"[EXTRACT] Doc du lieu local tu {path}")
    return spark.read.schema(UNIFIED_EVENT_SCHEMA).json(path)


# ============================================================
# TRANSFORM
# ============================================================

def transform_events(df: DataFrame) -> DataFrame:
    """Lam sach va lam giau du lieu."""
    print(f"[TRANSFORM] Xu ly {df.count()} records...")

    cleaned = (
        df
        # Loai bo records khong hop le
        .filter(F.col("event_type").isNotNull())
        .filter(F.col("timestamp").isNotNull())
        # Chuyen timestamp sang datetime
        .withColumn(
            "event_time",
            F.to_timestamp(F.from_unixtime(F.col("timestamp"))).cast(TimestampType()),
        )
        # Trich xuat time dimensions
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("event_hour", F.hour("event_time"))
        .withColumn("day_of_week", F.dayofweek("event_time"))
        # Loai bo duplicate
        .dropDuplicates(["user_id", "event_type", "product_id", "timestamp"])
    )

    print(f"[TRANSFORM] Sau khi lam sach: {cleaned.count()} records")
    return cleaned


def build_fact_events(df: DataFrame) -> DataFrame:
    """Xay dung fact table cho events."""
    return df.select(
        F.monotonically_increasing_id().alias("event_id"),
        "event_type",
        "user_id",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "price",
        "order_id",
        "total_amount",
        "payment_method",
        "rating",
        "old_price",
        "new_price",
        "query_text",
        "event_time",
        "event_date",
        "event_hour",
        "day_of_week",
    )


def build_dim_products(df: DataFrame) -> DataFrame:
    """Xay dung dimension table cho san pham (tu events)."""
    return (
        df.filter(F.col("product_id").isNotNull())
        .select("product_id", "product_name", "category")
        .dropDuplicates(["product_id"])
    )


def build_aggregation_daily_sales(df: DataFrame) -> DataFrame:
    """Tinh doanh so ban hang theo ngay va san pham."""
    purchases = df.filter(F.col("event_type") == "purchase")
    return (
        purchases
        .groupBy("event_date")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
            F.countDistinct("user_id").alias("unique_buyers"),
        )
    )


def build_aggregation_product_stats(df: DataFrame) -> DataFrame:
    """Thong ke tuong tac theo san pham."""
    return (
        df.filter(F.col("product_id").isNotNull())
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            F.sum(F.when(F.col("event_type") == "review", 1).otherwise(0)).alias("reviews"),
            F.avg(
                F.when(F.col("event_type") == "review", F.col("rating"))
            ).alias("avg_rating"),
        )
        # Ti le chuyen doi
        .withColumn(
            "conversion_rate",
            F.when(F.col("views") > 0, F.col("purchases") / F.col("views")).otherwise(0),
        )
    )


# ============================================================
# LOAD
# ============================================================

_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
_JDBC_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def load_to_postgres(df: DataFrame, table_name: str, mode: str = "append") -> None:
    """Ghi DataFrame vao Postgres."""
    print(f"[LOAD] Ghi {df.count()} rows vao {table_name} (mode={mode})")
    df.write.jdbc(url=_JDBC_URL, table=table_name, mode=mode, properties=_JDBC_PROPS)


def load_to_minio(df: DataFrame, path: str, format: str = "parquet") -> None:
    """Ghi DataFrame vao MinIO (processed zone)."""
    print(f"[LOAD] Ghi du lieu vao MinIO: {path}")
    df.write.mode("overwrite").format(format).save(path)


# ============================================================
# MAIN PIPELINE
# ============================================================

def run_etl(source_path: str | None = None, date_str: str | None = None) -> None:
    """Chay toan bo ETL pipeline."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("BATCH ETL PIPELINE - Ecommerce Data Warehouse")
    print("=" * 60)

    # --- EXTRACT ---
    if source_path:
        raw_df = extract_from_local(spark, source_path)
    else:
        raw_df = extract_raw_events(spark, date_str)

    # --- TRANSFORM ---
    cleaned_df = transform_events(raw_df)

    # --- BUILD TABLES ---
    fact_events = build_fact_events(cleaned_df)
    dim_products = build_dim_products(cleaned_df)
    daily_sales = build_aggregation_daily_sales(cleaned_df)
    product_stats = build_aggregation_product_stats(cleaned_df)

    # --- LOAD ---
    load_to_postgres(fact_events, "fact_events", mode="append")
    load_to_postgres(dim_products, "dim_products", mode="overwrite")
    load_to_postgres(daily_sales, "agg_daily_sales", mode="overwrite")
    load_to_postgres(product_stats, "agg_product_stats", mode="overwrite")

    print("=" * 60)
    print("ETL PIPELINE HOAN TAT!")
    print("=" * 60)
    spark.stop()


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Batch ETL: MinIO/Local → Postgres DW")
    parser.add_argument("--source", type=str, default=None, help="Local JSON source path")
    parser.add_argument("--date", type=str, default=None, help="Date to process (YYYY/MM/DD)")
    args = parser.parse_args()
    run_etl(source_path=args.source, date_str=args.date)


if __name__ == "__main__":
    main()
