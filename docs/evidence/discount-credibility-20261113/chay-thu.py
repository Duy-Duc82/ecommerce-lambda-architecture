"""Chạy thử mart độ tin cậy khuyến mãi (ADR-0009) trên Gold thật của backup 31/10.

Đọc `offer_price_history_daily` và `offer_current` của run mp-20261031T1700Z trong
`data/ops/backups/bk-20261031T172502Z` (Delta, đọc như Parquet vì run chỉ ghi một
lần), dựng `discount_credibility_daily` bằng đúng builder của batch, rồi ghi các
bảng tóm tắt vào thư mục này. Chạy từ gốc repo:

    python docs/evidence/discount-credibility-20261113/chay-thu.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from ecomdata.domain.discount_rules import DiscountRuleConfig  # noqa: E402
from ecomdata.processing.batch.discount import build_discount_credibility_daily  # noqa: E402

HERE = Path(__file__).resolve().parent
BACKUP = Path("data/ops/backups/bk-20261031T172502Z/lake/gold/marketplace/runs/run_id=mp-20261031T1700Z")


def load(spark: SparkSession, name: str, columns: list[str] | None = None):
    """A run-scoped Gold dataset of the backup as a Spark frame.

    Read with pyarrow (hive partitions give marketplace and observed_date;
    `_delta_log` is skipped), because Spark's own directory listing needs
    winutils on Windows. Money columns come back as Decimal objects, which
    Spark infers as decimal; the builders cast them to decimal(38,6).
    """
    import pyarrow.dataset as ds

    table = ds.dataset(str(BACKUP / name), format="parquet", partitioning="hive").to_table(columns=columns)
    frame = table.to_pandas()
    for column in list(frame.columns):
        if frame[column].isna().all():
            frame = frame.drop(columns=[column])  # Spark cannot infer a type for an all-null column
        elif str(frame[column].dtype).startswith("dictionary") or frame[column].dtype.name == "category":
            frame[column] = frame[column].astype(str)
    out = spark.createDataFrame(frame)
    if "observed_date" in out.columns:
        out = out.withColumn("observed_date", F.to_date(F.col("observed_date").cast("string")))
    return out


def main() -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("discount-credibility-trial")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    history = load(spark, "offer_price_history_daily")
    current = load(spark, "offer_current", ["offer_id", "product_title", "category_path", "source_url", "seller_id"])
    config = DiscountRuleConfig()  # 30 ngày, tối thiểu 3 ngày lịch sử, cap 50 %, gap 20 điểm, Isolation Forest bật
    mart = build_discount_credibility_daily(history, config=config).cache()

    rows = mart.count()
    by_status = mart.groupBy("marketplace", "observed_date", "credibility_status").count().toPandas()
    by_status.sort_values(["marketplace", "observed_date", "credibility_status"]).to_csv(HERE / "theo-trang-thai.csv", index=False)

    reasons = (
        mart.select("marketplace", "observed_date", F.explode(F.from_json("credibility_reasons_json", "array<string>")).alias("reason"))
        .groupBy("marketplace", "observed_date", "reason")
        .count()
        .toPandas()
    )
    reasons.sort_values(["marketplace", "observed_date", "reason"]).to_csv(HERE / "theo-ly-do.csv", index=False)

    ml = mart.groupBy("marketplace", "ml_method").count().toPandas()
    ml.to_csv(HERE / "ml-phu.csv", index=False)

    top = (
        mart.filter(F.col("credibility_status").isin("SUSPECT", "QUESTIONABLE"))
        .join(current, "offer_id", "left")
        .orderBy(F.col("rule_score").desc(), F.col("ml_rank_percent").desc_nulls_last(), F.col("displayed_discount_percent").desc())
        .select(
            "marketplace", "observed_date", "offer_id", "product_title", "category_path", "evaluated_price", "list_price",
            "displayed_discount_percent", "reference_sample_size", "reference_min_price", "effective_discount_percent",
            "claim_gap_percent", "credibility_status", "credibility_reasons_json", "rule_score", "ml_rank_percent", "source_url",
        )
        .limit(40)
        .toPandas()
    )
    top.to_csv(HERE / "top-40-de-xem-tay.csv", index=False)

    ranked = (
        mart.filter(F.col("ml_method") == "ISOLATION_FOREST")
        .join(current, "offer_id", "left")
        .orderBy(F.col("ml_rank_percent").desc(), F.col("displayed_discount_percent").desc())
        .select(
            "observed_date", "offer_id", "product_title", "evaluated_price", "list_price", "displayed_discount_percent",
            "distinct_price_count", "credibility_status", "credibility_reasons_json", "rule_score", "ml_rank_percent",
        )
        .limit(20)
        .toPandas()
    )
    ranked.to_csv(HERE / "top-20-theo-isolation-forest.csv", index=False)

    categories = (
        mart.join(current.select("offer_id", "category_path"), "offer_id", "left")
        .withColumn("category_l2", F.concat_ws("/", F.slice(F.split(F.coalesce("category_path", F.lit("__UNKNOWN__")), "/"), 1, 2)))
        .groupBy("observed_date", "category_l2")
        .agg(
            F.countDistinct("offer_id").alias("offers"),
            F.countDistinct(F.when(F.col("displayed_discount_percent") > 0, F.col("offer_id"))).alias("discounted"),
            F.countDistinct(F.when(F.col("credibility_status") == "QUESTIONABLE", F.col("offer_id"))).alias("questionable"),
            F.countDistinct(F.when(F.col("credibility_status") == "SUSPECT", F.col("offer_id"))).alias("suspect"),
            F.expr("percentile_approx(CASE WHEN displayed_discount_percent > 0 THEN displayed_discount_percent END, 0.5)").alias(
                "median_discount"
            ),
        )
        .orderBy(F.col("questionable").desc(), F.col("discounted").desc())
        .limit(25)
        .toPandas()
    )
    categories.to_csv(HERE / "theo-danh-muc-top25.csv", index=False)

    summary = {
        "rows": rows,
        "config": {k: str(v) for k, v in config.__dict__.items()},
        "status_counts": by_status.groupby("credibility_status")["count"].sum().to_dict(),
        "reason_counts": reasons.groupby("reason")["count"].sum().to_dict(),
        "ml_method_counts": ml.groupby("ml_method")["count"].sum().to_dict(),
        "days": sorted(str(d) for d in by_status["observed_date"].unique()),
    }
    (HERE / "tom-tat.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    spark.stop()


if __name__ == "__main__":
    pd.set_option("display.width", 200)
    main()
