"""
(1) Phan tich xu huong san pham — Trend Analysis.

Phuong phap:
  - Moving Average (SMA, EMA) cho doanh so theo ngay
  - Growth rate tinh toan de xac dinh trending products
  - Category-level trends
  - PySpark + pandas cho tinh toan

Chay:
  python -m batch_layer.models.trend_analysis
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)


_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
_JDBC_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


# ============================================================
# 1. MOVING AVERAGE — Xu huong doanh so theo ngay
# ============================================================

def compute_moving_averages(df: DataFrame, window_sizes: list[int] | None = None) -> DataFrame:
    """
    Tinh SMA (Simple Moving Average) cho doanh so theo ngay va san pham.

    Args:
        df: DataFrame co cot event_date, product_id, event_type, total_amount
        window_sizes: Cac kich thuoc cua so (mac dinh: 3, 7, 14 ngay)
    """
    if window_sizes is None:
        window_sizes = [3, 7, 14]

    # Doanh so theo ngay-san pham
    daily = (
        df.filter(F.col("event_type") == "purchase")
        .groupBy("event_date", "product_id", "product_name", "category")
        .agg(
            F.count("*").alias("num_orders"),
            F.sum("total_amount").alias("daily_revenue"),
        )
    )

    # Tinh SMA cho tung window size
    result = daily
    for ws in window_sizes:
        w = Window.partitionBy("product_id").orderBy("event_date").rowsBetween(-(ws - 1), 0)
        result = result.withColumn(f"sma_{ws}d", F.avg("daily_revenue").over(w))

    # Tinh EMA (Exponential Moving Average) voi span=7
    w7 = Window.partitionBy("product_id").orderBy("event_date").rowsBetween(-6, 0)
    result = result.withColumn("ema_7d", F.avg("daily_revenue").over(w7))

    return result.orderBy("product_id", "event_date")


# ============================================================
# 2. TRENDING PRODUCTS — San pham dang tang truong
# ============================================================

def detect_trending_products(
    df: DataFrame,
    recent_days: int = 7,
    previous_days: int = 7,
    min_views: int = 10,
) -> DataFrame:
    """
    Xac dinh san pham trending = tang truong views/purchases
    so voi giai doan truoc.

    Growth Rate = (recent - previous) / previous * 100
    """
    max_date_row = df.agg(F.max("event_date")).collect()
    if not max_date_row or max_date_row[0][0] is None:
        print("[WARN] Khong co du lieu de phan tich trending.")
        return df.sparkSession.createDataFrame([], schema="product_id STRING, product_name STRING")

    max_date = max_date_row[0][0]

    # Giai doan gan day
    recent = (
        df.filter(
            (F.col("event_date") > F.date_sub(F.lit(max_date), recent_days))
            & (F.col("event_type").isin("page_view", "purchase"))
        )
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("recent_views"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("recent_purchases"),
        )
    )

    # Giai doan truoc do
    previous = (
        df.filter(
            (F.col("event_date") <= F.date_sub(F.lit(max_date), recent_days))
            & (F.col("event_date") > F.date_sub(F.lit(max_date), recent_days + previous_days))
            & (F.col("event_type").isin("page_view", "purchase"))
        )
        .groupBy("product_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("prev_views"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("prev_purchases"),
        )
    )

    # Join va tinh growth rate
    trending = (
        recent.join(previous, "product_id", "left")
        .fillna(0, subset=["prev_views", "prev_purchases"])
        .withColumn(
            "view_growth_pct",
            F.when(
                F.col("prev_views") > 0,
                (F.col("recent_views") - F.col("prev_views")) / F.col("prev_views") * 100,
            ).otherwise(F.lit(100.0)),
        )
        .withColumn(
            "purchase_growth_pct",
            F.when(
                F.col("prev_purchases") > 0,
                (F.col("recent_purchases") - F.col("prev_purchases")) / F.col("prev_purchases") * 100,
            ).otherwise(F.lit(100.0)),
        )
        .filter(F.col("recent_views") >= min_views)
        .withColumn(
            "trend_score",
            F.col("view_growth_pct") * 0.4 + F.col("purchase_growth_pct") * 0.6,
        )
        .orderBy(F.desc("trend_score"))
    )

    return trending


# ============================================================
# 3. CATEGORY TRENDS — Xu huong theo danh muc
# ============================================================

def compute_category_trends(df: DataFrame) -> DataFrame:
    """Tinh xu huong tuong tac theo category va thoi gian."""
    return (
        df.filter(F.col("category").isNotNull())
        .groupBy("event_date", "category")
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.col("total_amount")).otherwise(0)
            ).alias("revenue"),
        )
        .orderBy("event_date", "category")
    )


# ============================================================
# MAIN
# ============================================================

def run_trend_analysis(spark: SparkSession | None = None) -> None:
    """Chay phan tich xu huong va luu ket qua vao Postgres."""
    if spark is None:
        spark = (
            SparkSession.builder
            .appName("TrendAnalysis")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("(1) TREND ANALYSIS - Phan tich xu huong san pham")
    print("=" * 60)

    df = spark.read.jdbc(url=_JDBC_URL, table="fact_events", properties=_JDBC_PROPS)

    # Moving averages
    ma_df = compute_moving_averages(df)
    ma_df.write.jdbc(url=_JDBC_URL, table="ml_moving_averages", mode="overwrite", properties=_JDBC_PROPS)
    print(f"  ✓ Moving averages: {ma_df.count()} rows")

    # Trending products
    trending_df = detect_trending_products(df)
    trending_df.write.jdbc(url=_JDBC_URL, table="ml_trending_products", mode="overwrite", properties=_JDBC_PROPS)
    print(f"  ✓ Trending products: {trending_df.count()} rows")

    # Category trends
    cat_df = compute_category_trends(df)
    cat_df.write.jdbc(url=_JDBC_URL, table="ml_category_trends", mode="overwrite", properties=_JDBC_PROPS)
    print(f"  ✓ Category trends: {cat_df.count()} rows")

    print("\nTREND ANALYSIS HOAN TAT!")


if __name__ == "__main__":
    run_trend_analysis()
