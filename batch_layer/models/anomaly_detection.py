"""
(2) Phat hien su kien bat thuong — Anomaly Detection.

Phuong phap:
  - Z-score: phat hien outlier dua tren do lech chuan
  - IQR (Interquartile Range): robust hon voi du lieu lech
  - Spike/Drop detection: phat hien tang/giam dot ngot
  - Unusual user behavior: mua qua nhieu, browse bat thuong

Chay:
  python -m batch_layer.models.anomaly_detection
"""

from __future__ import annotations

import sys
from pathlib import Path

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
# 1. Z-SCORE ANOMALY — Don hang bat thuong
# ============================================================

def detect_zscore_anomalies(
    df: DataFrame,
    metric_col: str = "total_amount",
    threshold: float = 3.0,
) -> DataFrame:
    """
    Phat hien anomaly bang Z-score.
    Z = (x - mean) / std
    |Z| > threshold => anomaly
    """
    purchases = df.filter(
        (F.col("event_type") == "purchase") & F.col(metric_col).isNotNull()
    )

    # Tinh mean va std
    stats = purchases.agg(
        F.mean(metric_col).alias("global_mean"),
        F.stddev(metric_col).alias("global_std"),
    ).collect()[0]

    mean_val = stats["global_mean"] or 0
    std_val = stats["global_std"] or 1

    if std_val == 0:
        std_val = 1

    return (
        purchases
        .withColumn("z_score", (F.col(metric_col) - F.lit(mean_val)) / F.lit(std_val))
        .withColumn("is_anomaly", F.abs(F.col("z_score")) > threshold)
        .filter(F.col("is_anomaly"))
        .withColumn("anomaly_type", F.lit("zscore_order_amount"))
        .withColumn(
            "severity",
            F.when(F.abs(F.col("z_score")) > 5, "CRITICAL")
            .when(F.abs(F.col("z_score")) > 4, "HIGH")
            .otherwise("MEDIUM"),
        )
        .select(
            "event_time", "user_id", "order_id", metric_col,
            "z_score", "anomaly_type", "severity",
        )
    )


# ============================================================
# 2. IQR ANOMALY — Robust hon voi du lieu lech
# ============================================================

def detect_iqr_anomalies(
    df: DataFrame,
    metric_col: str = "total_amount",
    multiplier: float = 1.5,
) -> DataFrame:
    """
    Phat hien anomaly bang IQR.
    Outlier: x < Q1 - 1.5*IQR hoac x > Q3 + 1.5*IQR
    """
    purchases = df.filter(
        (F.col("event_type") == "purchase") & F.col(metric_col).isNotNull()
    )

    quantiles = purchases.approxQuantile(metric_col, [0.25, 0.75], 0.01)
    if len(quantiles) < 2:
        return purchases.limit(0)

    q1, q3 = quantiles
    iqr = q3 - q1
    lower_bound = q1 - multiplier * iqr
    upper_bound = q3 + multiplier * iqr

    return (
        purchases
        .filter(
            (F.col(metric_col) < lower_bound) | (F.col(metric_col) > upper_bound)
        )
        .withColumn("anomaly_type", F.lit("iqr_order_amount"))
        .withColumn(
            "severity",
            F.when(
                (F.col(metric_col) > q3 + 3 * iqr) | (F.col(metric_col) < q1 - 3 * iqr),
                "CRITICAL",
            ).when(
                (F.col(metric_col) > q3 + 2 * iqr) | (F.col(metric_col) < q1 - 2 * iqr),
                "HIGH",
            ).otherwise("MEDIUM"),
        )
        .withColumn("lower_bound", F.lit(lower_bound))
        .withColumn("upper_bound", F.lit(upper_bound))
        .select(
            "event_time", "user_id", "order_id", metric_col,
            "anomaly_type", "severity", "lower_bound", "upper_bound",
        )
    )


# ============================================================
# 3. SPIKE / DROP DETECTION — So luong don hang
# ============================================================

def detect_spikes_drops(
    df: DataFrame,
    window_days: int = 7,
    spike_threshold: float = 2.0,
    drop_threshold: float = 0.3,
) -> DataFrame:
    """
    Phat hien tang/giam dot ngot so luong don hang theo ngay.
    Spike:  daily_count > mean_window * spike_threshold
    Drop:   daily_count < mean_window * drop_threshold
    """
    daily_counts = (
        df.filter(F.col("event_type") == "purchase")
        .groupBy("event_date")
        .agg(
            F.count("*").alias("daily_orders"),
            F.sum("total_amount").alias("daily_revenue"),
        )
    )

    w = Window.orderBy("event_date").rowsBetween(-(window_days), -1)
    with_avg = daily_counts.withColumn("avg_orders_prev", F.avg("daily_orders").over(w))

    return (
        with_avg
        .filter(F.col("avg_orders_prev").isNotNull())
        .withColumn(
            "ratio", F.col("daily_orders") / F.col("avg_orders_prev"),
        )
        .withColumn(
            "anomaly_type",
            F.when(F.col("ratio") > spike_threshold, "SPIKE")
            .when(F.col("ratio") < drop_threshold, "DROP")
            .otherwise(None),
        )
        .filter(F.col("anomaly_type").isNotNull())
        .withColumn(
            "severity",
            F.when(
                (F.col("ratio") > spike_threshold * 2) | (F.col("ratio") < drop_threshold / 2),
                "CRITICAL",
            ).otherwise("HIGH"),
        )
    )


# ============================================================
# 4. UNUSUAL USER BEHAVIOR — Hanh vi user bat thuong
# ============================================================

def detect_unusual_users(df: DataFrame, max_daily_orders: int = 20) -> DataFrame:
    """
    Phat hien user co hanh vi bat thuong:
      - Mua qua nhieu don trong 1 ngay
      - Tong gia tri don hang qua lon
    """
    user_daily = (
        df.filter(F.col("event_type") == "purchase")
        .groupBy("event_date", "user_id")
        .agg(
            F.count("*").alias("daily_order_count"),
            F.sum("total_amount").alias("daily_total_amount"),
        )
    )

    # Tinh thong ke chung
    stats = user_daily.agg(
        F.mean("daily_total_amount").alias("mean_amount"),
        F.stddev("daily_total_amount").alias("std_amount"),
    ).collect()[0]

    mean_amt = stats["mean_amount"] or 0
    std_amt = stats["std_amount"] or 1
    amount_threshold = mean_amt + 3 * std_amt

    return (
        user_daily
        .filter(
            (F.col("daily_order_count") > max_daily_orders)
            | (F.col("daily_total_amount") > amount_threshold)
        )
        .withColumn("anomaly_type", F.lit("unusual_user_behavior"))
        .withColumn(
            "reason",
            F.when(
                F.col("daily_order_count") > max_daily_orders,
                F.concat(F.lit("Qua nhieu don: "), F.col("daily_order_count")),
            ).otherwise(
                F.concat(F.lit("Tong gia tri bat thuong: "), F.col("daily_total_amount")),
            ),
        )
    )


# ============================================================
# MAIN
# ============================================================

def run_anomaly_detection(spark: SparkSession | None = None) -> None:
    """Chay tat ca cac phuong phap phat hien bat thuong."""
    if spark is None:
        spark = (
            SparkSession.builder
            .appName("AnomalyDetection")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("(2) ANOMALY DETECTION - Phat hien su kien bat thuong")
    print("=" * 60)

    df = spark.read.jdbc(url=_JDBC_URL, table="fact_events", properties=_JDBC_PROPS)

    results = {}

    # Z-score
    zscore_df = detect_zscore_anomalies(df)
    results["ml_anomaly_zscore"] = zscore_df

    # IQR
    iqr_df = detect_iqr_anomalies(df)
    results["ml_anomaly_iqr"] = iqr_df

    # Spikes & Drops
    spike_df = detect_spikes_drops(df)
    results["ml_anomaly_spikes"] = spike_df

    # Unusual users
    user_df = detect_unusual_users(df)
    results["ml_anomaly_users"] = user_df

    for table_name, result_df in results.items():
        try:
            count = result_df.count()
            result_df.write.jdbc(
                url=_JDBC_URL, table=table_name, mode="overwrite", properties=_JDBC_PROPS,
            )
            print(f"  ✓ {table_name}: {count} anomalies")
        except Exception as e:
            print(f"  ✗ {table_name}: {e}")

    print("\nANOMALY DETECTION HOAN TAT!")


if __name__ == "__main__":
    run_anomaly_detection()
