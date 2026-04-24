"""
Batch Views — Tao cac khung nhin (views) tong hop tu du lieu batch.

Cac views:
  1. Daily/Weekly/Monthly sales summary
  2. Product performance ranking
  3. Category comparison
  4. User segmentation (RFM)
  5. Funnel analysis (view → cart → purchase)
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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


def _read_fact_events(spark: SparkSession) -> DataFrame:
    """Doc fact_events tu Postgres."""
    return spark.read.jdbc(url=_JDBC_URL, table="fact_events", properties=_JDBC_PROPS)


# ============================================================
# VIEW 1: Doanh so theo thoi gian (ngay / tuan / thang)
# ============================================================

def build_sales_summary(df: DataFrame) -> DataFrame:
    """Tong hop doanh so theo ngay."""
    purchases = df.filter(F.col("event_type") == "purchase")
    return (
        purchases
        .groupBy("event_date")
        .agg(
            F.count("order_id").alias("num_orders"),
            F.sum("total_amount").alias("revenue"),
            F.countDistinct("user_id").alias("unique_buyers"),
            F.avg("total_amount").alias("avg_order_value"),
        )
        .orderBy("event_date")
    )


# ============================================================
# VIEW 2: Xep hang san pham (Product Ranking)
# ============================================================

def build_product_ranking(df: DataFrame) -> DataFrame:
    """Xep hang san pham theo nhieu tieu chi."""
    stats = (
        df.filter(F.col("product_id").isNotNull())
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            F.avg(
                F.when(F.col("event_type") == "review", F.col("rating"))
            ).alias("avg_rating"),
            F.count(
                F.when(F.col("event_type") == "review", F.col("rating"))
            ).alias("num_reviews"),
        )
    )

    # Tinh diem tong hop (weighted score)
    scored = stats.withColumn(
        "score",
        F.col("views") * 0.1
        + F.col("cart_adds") * 0.2
        + F.col("purchases") * 0.5
        + F.coalesce(F.col("avg_rating"), F.lit(0)) * F.col("num_reviews") * 0.2,
    )

    window = Window.orderBy(F.desc("score"))
    return scored.withColumn("rank", F.row_number().over(window))


# ============================================================
# VIEW 3: Hieu suat danh muc (Category Performance)
# ============================================================

def build_category_performance(df: DataFrame) -> DataFrame:
    """So sanh hieu suat giua cac danh muc san pham."""
    return (
        df.filter(F.col("category").isNotNull())
        .groupBy("category")
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("total_views"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.col("total_amount")).otherwise(0)
            ).alias("total_revenue"),
            F.countDistinct("product_id").alias("num_products"),
            F.avg(
                F.when(F.col("event_type") == "review", F.col("rating"))
            ).alias("avg_category_rating"),
        )
        .withColumn(
            "conversion_rate",
            F.when(F.col("total_views") > 0, F.col("total_purchases") / F.col("total_views")).otherwise(0),
        )
        .orderBy(F.desc("total_revenue"))
    )


# ============================================================
# VIEW 4: Phan nhom khach hang (RFM Segmentation)
# ============================================================

def build_user_segments(df: DataFrame) -> DataFrame:
    """Phan nhom khach hang theo mo hinh RFM (Recency, Frequency, Monetary)."""
    purchases = df.filter(F.col("event_type") == "purchase")

    max_date = purchases.agg(F.max("event_date")).collect()[0][0]

    rfm = (
        purchases
        .groupBy("user_id")
        .agg(
            F.datediff(F.lit(max_date), F.max("event_date")).alias("recency"),
            F.count("order_id").alias("frequency"),
            F.sum("total_amount").alias("monetary"),
        )
    )

    # Phan loai dua tren quantiles don gian
    return rfm.withColumn(
        "segment",
        F.when(
            (F.col("recency") <= 7) & (F.col("frequency") >= 5), "VIP"
        ).when(
            (F.col("recency") <= 14) & (F.col("frequency") >= 3), "Loyal"
        ).when(
            (F.col("recency") <= 30), "Active"
        ).when(
            (F.col("recency") <= 60), "At Risk"
        ).otherwise("Churned"),
    )


# ============================================================
# VIEW 5: Phan tich pheu chuyen doi (Funnel Analysis)
# ============================================================

def build_funnel_analysis(df: DataFrame) -> DataFrame:
    """Phan tich pheu: page_view → add_to_cart → purchase."""
    return (
        df.groupBy("event_date")
        .agg(
            F.countDistinct(
                F.when(F.col("event_type") == "page_view", F.col("user_id"))
            ).alias("viewers"),
            F.countDistinct(
                F.when(F.col("event_type") == "add_to_cart", F.col("user_id"))
            ).alias("cart_users"),
            F.countDistinct(
                F.when(F.col("event_type") == "purchase", F.col("user_id"))
            ).alias("buyers"),
        )
        .withColumn(
            "view_to_cart_rate",
            F.when(F.col("viewers") > 0, F.col("cart_users") / F.col("viewers")).otherwise(0),
        )
        .withColumn(
            "cart_to_purchase_rate",
            F.when(F.col("cart_users") > 0, F.col("buyers") / F.col("cart_users")).otherwise(0),
        )
        .withColumn(
            "overall_conversion_rate",
            F.when(F.col("viewers") > 0, F.col("buyers") / F.col("viewers")).otherwise(0),
        )
        .orderBy("event_date")
    )


# ============================================================
# MAIN
# ============================================================

def run_batch_views(spark: SparkSession | None = None) -> None:
    """Chay tao tat ca batch views va ghi vao Postgres."""
    if spark is None:
        spark = (
            SparkSession.builder
            .appName("EcommerceBatchViews")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("BUILDING BATCH VIEWS")
    print("=" * 60)

    df = _read_fact_events(spark)

    views = {
        "mv_sales_summary": build_sales_summary,
        "mv_product_ranking": build_product_ranking,
        "mv_category_performance": build_category_performance,
        "mv_user_segments": build_user_segments,
        "mv_funnel_analysis": build_funnel_analysis,
    }

    for table_name, builder_fn in views.items():
        print(f"\n>> Building {table_name}...")
        try:
            view_df = builder_fn(df)
            view_df.write.jdbc(
                url=_JDBC_URL, table=table_name, mode="overwrite", properties=_JDBC_PROPS,
            )
            print(f"   ✓ {table_name}: {view_df.count()} rows")
        except Exception as e:
            print(f"   ✗ {table_name}: {e}")

    print("\n" + "=" * 60)
    print("BATCH VIEWS HOAN TAT!")
    print("=" * 60)


if __name__ == "__main__":
    run_batch_views()
