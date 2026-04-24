"""
(4) Phat hien gian lan — Fraud Detection.

Phuong phap:
  - Rule-based: nguong don gia tri cao, nhieu don tu cung user, pattern bat thuong
  - ML-based: Isolation Forest (unsupervised) de phat hien outlier
  - Feature engineering tu hanh vi nguoi dung

Chay:
  python -m batch_layer.models.fraud_detection
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
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
# 1. RULE-BASED FRAUD DETECTION
# ============================================================

def detect_high_value_fraud(df: DataFrame, threshold_vnd: float = 100_000_000) -> DataFrame:
    """
    Rule: Don hang co gia tri > threshold la dang nghi.
    Mac dinh: > 100 trieu VND.
    """
    return (
        df.filter(
            (F.col("event_type") == "purchase")
            & (F.col("total_amount") > threshold_vnd)
        )
        .withColumn("fraud_rule", F.lit("HIGH_VALUE_ORDER"))
        .withColumn("risk_score", F.col("total_amount") / F.lit(threshold_vnd))
        .select(
            "event_time", "user_id", "order_id", "total_amount",
            "payment_method", "fraud_rule", "risk_score",
        )
    )


def detect_rapid_purchases(df: DataFrame, max_orders_per_hour: int = 10) -> DataFrame:
    """
    Rule: User dat qua nhieu don trong 1 gio.
    """
    hourly_orders = (
        df.filter(F.col("event_type") == "purchase")
        .withColumn("hour_bucket", F.date_trunc("hour", "event_time"))
        .groupBy("user_id", "hour_bucket")
        .agg(
            F.count("*").alias("orders_in_hour"),
            F.sum("total_amount").alias("total_in_hour"),
            F.collect_list("order_id").alias("order_ids"),
        )
    )

    return (
        hourly_orders
        .filter(F.col("orders_in_hour") > max_orders_per_hour)
        .withColumn("fraud_rule", F.lit("RAPID_PURCHASES"))
        .withColumn("risk_score", F.col("orders_in_hour") / F.lit(max_orders_per_hour))
    )


def detect_suspicious_patterns(df: DataFrame) -> DataFrame:
    """
    Rule: Phat hien cac pattern dang nghi:
      - Mua san pham dat tien nhung khong co page_view truoc do
      - Chi dung COD cho don gia tri lon
      - User moi tao va mua ngay lap tuc
    """
    # Tim user co don purchase nhung khong co page_view
    user_events = (
        df.groupBy("user_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("total_views"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
            F.sum(
                F.when(F.col("event_type") == "purchase", F.col("total_amount")).otherwise(0)
            ).alias("total_spent"),
            F.min(F.when(F.col("event_type") == "purchase", F.col("event_time"))).alias("first_purchase"),
            F.min("event_time").alias("first_activity"),
        )
    )

    return (
        user_events
        .withColumn(
            "view_to_purchase_ratio",
            F.when(F.col("total_purchases") > 0, F.col("total_views") / F.col("total_purchases")).otherwise(999),
        )
        .withColumn(
            "time_to_first_purchase_hours",
            (F.unix_timestamp("first_purchase") - F.unix_timestamp("first_activity")) / 3600,
        )
        .withColumn(
            "is_suspicious",
            # It xem nhung mua nhieu
            (F.col("view_to_purchase_ratio") < 1)
            # Hoac mua ngay lap tuc (< 1 phut)
            | (F.col("time_to_first_purchase_hours") < 0.02)
            # Hoac chi tieu qua lon
            | (F.col("total_spent") > 500_000_000),
        )
        .filter(F.col("is_suspicious"))
        .withColumn("fraud_rule", F.lit("SUSPICIOUS_PATTERN"))
        .withColumn(
            "reason",
            F.when(F.col("view_to_purchase_ratio") < 1, "Low view-to-purchase ratio")
            .when(F.col("time_to_first_purchase_hours") < 0.02, "Instant purchase after registration")
            .otherwise("Extremely high total spending"),
        )
    )


# ============================================================
# 2. ML-BASED FRAUD DETECTION (Isolation Forest)
# ============================================================

def extract_user_features(df: DataFrame) -> DataFrame:
    """
    Trich xuat features tu hanh vi user de dua vao ML model.
    """
    return (
        df.groupBy("user_id")
        .agg(
            # Tan suat
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("view_count"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_count"),
            # Gia tri
            F.sum(
                F.when(F.col("event_type") == "purchase", F.col("total_amount")).otherwise(0)
            ).alias("total_spent"),
            F.avg(
                F.when(F.col("event_type") == "purchase", F.col("total_amount"))
            ).alias("avg_order_value"),
            F.max(
                F.when(F.col("event_type") == "purchase", F.col("total_amount"))
            ).alias("max_order_value"),
            # Thoi gian
            F.countDistinct("event_date").alias("active_days"),
            F.countDistinct(F.when(F.col("event_type") == "purchase", F.col("event_date"))).alias(
                "purchase_days"
            ),
            # Payment diversity
            F.countDistinct(
                F.when(F.col("event_type") == "purchase", F.col("payment_method"))
            ).alias("payment_methods_used"),
            # Product diversity
            F.countDistinct("product_id").alias("unique_products_interacted"),
        )
        .fillna(0)
        .withColumn(
            "purchase_rate",
            F.when(F.col("total_events") > 0, F.col("purchase_count") / F.col("total_events")).otherwise(0),
        )
        .withColumn(
            "cart_abandonment_rate",
            F.when(
                F.col("cart_count") > 0,
                1 - F.col("purchase_count") / F.col("cart_count"),
            ).otherwise(0),
        )
    )


def run_isolation_forest(features_df: DataFrame, contamination: float = 0.05) -> pd.DataFrame:
    """
    Chay Isolation Forest tren features cua user.
    Tra ve pandas DataFrame voi cot is_fraud.
    """
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler

    pdf = features_df.toPandas()

    feature_cols = [
        "total_events", "purchase_count", "view_count", "cart_count",
        "total_spent", "avg_order_value", "max_order_value",
        "active_days", "purchase_days", "payment_methods_used",
        "unique_products_interacted", "purchase_rate", "cart_abandonment_rate",
    ]

    X = pdf[feature_cols].fillna(0).values

    # Chuan hoa
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Isolation Forest
    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
        n_jobs=-1,
    )
    predictions = model.fit_predict(X_scaled)
    scores = model.decision_function(X_scaled)

    pdf["is_fraud"] = predictions == -1
    pdf["anomaly_score"] = -scores  # Diem cang cao cang dang nghi
    pdf["fraud_method"] = "isolation_forest"

    return pdf


# ============================================================
# MAIN
# ============================================================

def run_fraud_detection(spark: SparkSession | None = None) -> None:
    """Chay toan bo pipeline phat hien gian lan."""
    if spark is None:
        spark = (
            SparkSession.builder
            .appName("FraudDetection")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.5")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("(4) FRAUD DETECTION - Phat hien gian lan")
    print("=" * 60)

    df = spark.read.jdbc(url=_JDBC_URL, table="fact_events", properties=_JDBC_PROPS)

    # --- Rule-based ---
    print("\n>> Rule-based detection...")

    high_val = detect_high_value_fraud(df)
    high_val.write.jdbc(url=_JDBC_URL, table="ml_fraud_high_value", mode="overwrite", properties=_JDBC_PROPS)
    print(f"  ✓ High value orders: {high_val.count()}")

    rapid = detect_rapid_purchases(df)
    rapid.write.jdbc(url=_JDBC_URL, table="ml_fraud_rapid", mode="overwrite", properties=_JDBC_PROPS)
    print(f"  ✓ Rapid purchases: {rapid.count()}")

    suspicious = detect_suspicious_patterns(df)
    suspicious.write.jdbc(url=_JDBC_URL, table="ml_fraud_suspicious", mode="overwrite", properties=_JDBC_PROPS)
    print(f"  ✓ Suspicious patterns: {suspicious.count()}")

    # --- ML-based ---
    print("\n>> ML-based detection (Isolation Forest)...")

    features_df = extract_user_features(df)
    features_df.write.jdbc(url=_JDBC_URL, table="ml_user_features", mode="overwrite", properties=_JDBC_PROPS)
    print(f"  ✓ User features extracted: {features_df.count()}")

    try:
        fraud_pdf = run_isolation_forest(features_df)
        fraud_count = fraud_pdf["is_fraud"].sum()
        fraud_spark = spark.createDataFrame(fraud_pdf)
        fraud_spark.write.jdbc(
            url=_JDBC_URL, table="ml_fraud_isolation_forest", mode="overwrite", properties=_JDBC_PROPS,
        )
        print(f"  ✓ Isolation Forest: {fraud_count} users flagged as potential fraud")
    except Exception as e:
        print(f"  ⚠ Isolation Forest failed: {e}")

    print("\nFRAUD DETECTION HOAN TAT!")


if __name__ == "__main__":
    run_fraud_detection()
