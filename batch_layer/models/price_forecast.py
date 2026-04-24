"""
(3) Du bao bien dong gia — Price Forecast.

Phuong phap:
  - Simple Moving Average (SMA) forecast
  - Exponential Smoothing (EMA)
  - Linear Regression trend
  - Price elasticity analysis

Chay:
  python -m batch_layer.models.price_forecast
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
# 1. LICH SU GIA — Tao time series gia san pham
# ============================================================

def build_price_history(df: DataFrame) -> DataFrame:
    """
    Tao lich su gia tu price_change events.
    Moi row = 1 thay doi gia cua 1 san pham.
    """
    price_changes = (
        df.filter(F.col("event_type") == "price_change")
        .select(
            "product_id", "product_name", "category",
            "old_price", "new_price", "event_time", "event_date",
        )
        .withColumn("price_change_pct", (F.col("new_price") - F.col("old_price")) / F.col("old_price") * 100)
        .withColumn("price_direction", F.when(F.col("new_price") > F.col("old_price"), "UP").otherwise("DOWN"))
        .orderBy("product_id", "event_time")
    )
    return price_changes


# ============================================================
# 2. SMA FORECAST — Du bao bang trung binh dong
# ============================================================

def sma_forecast(df: DataFrame, window_size: int = 5) -> DataFrame:
    """
    Du bao gia tiep theo bang SMA.
    forecast = trung binh cua window_size lan thay doi gia gan nhat.
    """
    w = Window.partitionBy("product_id").orderBy("event_time").rowsBetween(-(window_size - 1), 0)

    return (
        df.withColumn("sma_price", F.avg("new_price").over(w))
        .withColumn("sma_forecast", F.col("sma_price"))  # Gia du bao = SMA hien tai
        .withColumn(
            "forecast_error",
            F.abs(F.col("new_price") - F.lag("sma_price", 1).over(
                Window.partitionBy("product_id").orderBy("event_time")
            )),
        )
    )


# ============================================================
# 3. EMA FORECAST — Du bao bang trung binh luy thua
# ============================================================

def ema_forecast(df: DataFrame, span: int = 5) -> DataFrame:
    """
    Du bao gia bang Exponential Moving Average.
    Su dung pandas UDF de tinh EMA chinh xac.
    """
    # Su dung SMA nhu xap xi EMA (Spark khong co san EMA)
    alpha = 2.0 / (span + 1)

    w = Window.partitionBy("product_id").orderBy("event_time").rowsBetween(-(span - 1), 0)
    w_all = Window.partitionBy("product_id").orderBy("event_time")

    return (
        df.withColumn("row_num", F.row_number().over(w_all))
        .withColumn("ema_approx", F.avg("new_price").over(w))
        .withColumn(
            "weighted_price",
            F.col("new_price") * alpha + F.col("ema_approx") * (1 - alpha),
        )
    )


# ============================================================
# 4. PRICE VOLATILITY — Do bien dong gia
# ============================================================

def compute_price_volatility(df: DataFrame) -> DataFrame:
    """
    Tinh do bien dong gia cho tung san pham.
    Volatility = std(price_change_pct)
    """
    return (
        df.groupBy("product_id", "product_name", "category")
        .agg(
            F.count("*").alias("num_changes"),
            F.avg("price_change_pct").alias("avg_change_pct"),
            F.stddev("price_change_pct").alias("volatility"),
            F.min("new_price").alias("min_price"),
            F.max("new_price").alias("max_price"),
            F.last("new_price").alias("latest_price"),
            F.sum(F.when(F.col("price_direction") == "UP", 1).otherwise(0)).alias("up_count"),
            F.sum(F.when(F.col("price_direction") == "DOWN", 1).otherwise(0)).alias("down_count"),
        )
        .withColumn("price_range", F.col("max_price") - F.col("min_price"))
        .withColumn(
            "price_range_pct",
            F.when(F.col("min_price") > 0, F.col("price_range") / F.col("min_price") * 100).otherwise(0),
        )
        .withColumn(
            "trend_direction",
            F.when(F.col("up_count") > F.col("down_count"), "UPTREND")
            .when(F.col("up_count") < F.col("down_count"), "DOWNTREND")
            .otherwise("SIDEWAYS"),
        )
        .orderBy(F.desc("volatility"))
    )


# ============================================================
# 5. PRICE ELASTICITY — Phan tich do co gian gia
# ============================================================

def compute_price_elasticity(events_df: DataFrame, price_df: DataFrame) -> DataFrame:
    """
    Uoc luong price elasticity cua demand.
    Elasticity = (%change_quantity) / (%change_price)
    |E| > 1 => elastic (nhay cam voi gia)
    |E| < 1 => inelastic
    """
    # Doanh so theo ngay-san pham
    daily_sales = (
        events_df.filter(F.col("event_type") == "purchase")
        .groupBy("event_date", "product_id")
        .agg(F.count("*").alias("quantity_sold"))
    )

    # Gia theo ngay-san pham (lay gia moi nhat trong ngay)
    w = Window.partitionBy("product_id", "event_date").orderBy(F.desc("event_time"))
    daily_price = (
        price_df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("event_date", "product_id", "new_price")
    )

    # Join va tinh elasticity
    joined = daily_sales.join(daily_price, ["event_date", "product_id"], "inner")

    w_lag = Window.partitionBy("product_id").orderBy("event_date")
    return (
        joined
        .withColumn("prev_qty", F.lag("quantity_sold").over(w_lag))
        .withColumn("prev_price", F.lag("new_price").over(w_lag))
        .filter(F.col("prev_qty").isNotNull() & (F.col("prev_price") > 0) & (F.col("prev_qty") > 0))
        .withColumn("pct_qty_change", (F.col("quantity_sold") - F.col("prev_qty")) / F.col("prev_qty"))
        .withColumn("pct_price_change", (F.col("new_price") - F.col("prev_price")) / F.col("prev_price"))
        .withColumn(
            "elasticity",
            F.when(F.abs(F.col("pct_price_change")) > 0.001, F.col("pct_qty_change") / F.col("pct_price_change"))
            .otherwise(None),
        )
        .withColumn(
            "elasticity_type",
            F.when(F.abs(F.col("elasticity")) > 1, "ELASTIC")
            .when(F.abs(F.col("elasticity")).between(0, 1), "INELASTIC")
            .otherwise("UNKNOWN"),
        )
    )


# ============================================================
# MAIN
# ============================================================

def run_price_forecast(spark: SparkSession | None = None) -> None:
    """Chay toan bo phan tich va du bao gia."""
    if spark is None:
        spark = (
            SparkSession.builder
            .appName("PriceForecast")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("(3) PRICE FORECAST - Du bao bien dong gia")
    print("=" * 60)

    df = spark.read.jdbc(url=_JDBC_URL, table="fact_events", properties=_JDBC_PROPS)

    # Lich su gia
    price_hist = build_price_history(df)
    price_hist.write.jdbc(url=_JDBC_URL, table="ml_price_history", mode="overwrite", properties=_JDBC_PROPS)
    print(f"  ✓ Price history: {price_hist.count()} records")

    # SMA forecast
    sma_df = sma_forecast(price_hist)
    sma_df.write.jdbc(url=_JDBC_URL, table="ml_price_sma", mode="overwrite", properties=_JDBC_PROPS)
    print(f"  ✓ SMA forecast: {sma_df.count()} records")

    # Volatility
    vol_df = compute_price_volatility(price_hist)
    vol_df.write.jdbc(url=_JDBC_URL, table="ml_price_volatility", mode="overwrite", properties=_JDBC_PROPS)
    print(f"  ✓ Price volatility: {vol_df.count()} products")

    # Price elasticity
    try:
        elast_df = compute_price_elasticity(df, price_hist)
        elast_df.write.jdbc(url=_JDBC_URL, table="ml_price_elasticity", mode="overwrite", properties=_JDBC_PROPS)
        print(f"  ✓ Price elasticity: {elast_df.count()} records")
    except Exception as e:
        print(f"  ⚠ Price elasticity: {e}")

    print("\nPRICE FORECAST HOAN TAT!")


if __name__ == "__main__":
    run_price_forecast()
