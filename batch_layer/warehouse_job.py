"""Spark EtLT job for the E-commerce Behavioral Analytics warehouse.

Flow:
    source -> Bronze (append-only) -> Silver (validated canonical events)
           -> Gold (star schema and BI marts) -> PostgreSQL serving store

PostgreSQL is deliberately not the system of record. The lake keeps replayable
history; PostgreSQL contains only dimensions, atomic facts and compact BI marts.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import psycopg2
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, TimestampType

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (  # noqa: E402
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    data_lake_uri,
)
from config.storage import spark_hadoop_options  # noqa: E402

SUPPORTED_EVENT_TYPES = ("view", "cart", "purchase")
EVENT_TYPE_KEYS = {"view": 1, "cart": 2, "purchase": 3}
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


@dataclass(frozen=True)
class QualityResult:
    name: str
    passed: bool
    observed: float
    expectation: str


def build_spark() -> SparkSession:
    # JDBC and S3A jars are baked into the pinned warehouse-job image. Keeping
    # dependency resolution outside application code makes runs reproducible.
    builder = (
        SparkSession.builder.appName("EcommerceWarehouseV2")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "16"))
        # Raising SPARK_WORKER_MEMORY alone changes nothing: the worker only
        # advertises capacity, the executor has to be told to claim it.
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "6g"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        # Default 128MB splits turn an 8.6 GB CSV into ~67 tasks, each holding a
        # wide row set in memory. Smaller splits trade scheduling for headroom.
        .config("spark.sql.files.maxPartitionBytes", os.getenv("SPARK_MAX_PARTITION_BYTES", "67108864"))
        .config("spark.sql.adaptive.enabled", "true")
    )
    spark = builder.getOrCreate()
    # Endpoint, TLS and path-style addressing differ per object store; taking
    # them from the profile is what lets this identical job run against laptop
    # MinIO or a cloud bucket without an edit.
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    for key, value in spark_hadoop_options().items():
        hadoop.set(key, value)
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _column(df: DataFrame, *names: str, default=None):
    """Coalesce existing source columns without referencing missing columns."""
    columns = [F.col(name) for name in names if name in df.columns]
    if default is not None:
        columns.append(F.lit(default))
    if not columns:
        return F.lit(None)
    return F.coalesce(*columns)


def read_source(spark: SparkSession, source_path: str) -> DataFrame:
    lower = source_path.lower()
    if lower.endswith(".csv") or ".csv" in lower:
        df = spark.read.option("header", True).option("escape", '"').csv(source_path)
    else:
        df = spark.read.json(source_path)
    return df.withColumn("_source_file", F.input_file_name())


def write_bronze(raw: DataFrame, run_id: str, ingested_at: datetime) -> int:
    bronze = (
        raw.withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.lit(ingested_at).cast(TimestampType()))
        .withColumn("_ingest_date", F.to_date("_ingested_at"))
    )
    count = bronze.count()
    (
        bronze.write.mode("append")
        .partitionBy("_ingest_date", "_run_id")
        .parquet(data_lake_uri("bronze", "ecommerce_events"))
    )
    return count


def normalize_events(raw: DataFrame, run_id: str, ingested_at: datetime) -> DataFrame:
    raw_type = F.lower(F.trim(_column(raw, "event_type", default="")))
    event_type = (
        F.when(raw_type.isin("view", "page_view"), F.lit("view"))
        .when(raw_type.isin("cart", "add_to_cart"), F.lit("cart"))
        .when(raw_type == "purchase", F.lit("purchase"))
        .otherwise(raw_type)
    )

    if "event_time" in raw.columns:
        parsed_event_time = F.to_timestamp(F.regexp_replace(F.col("event_time"), " UTC$", ""))
    else:
        parsed_event_time = F.lit(None).cast(TimestampType())
    if "timestamp" in raw.columns:
        epoch_event_time = F.to_timestamp(F.from_unixtime(F.col("timestamp").cast("long")))
    else:
        epoch_event_time = F.lit(None).cast(TimestampType())

    if "product_ids" in raw.columns:
        array_product = F.element_at(F.col("product_ids"), 1).cast("string")
    else:
        array_product = F.lit(None).cast("string")

    source_file = _column(raw, "_source_file", default="unknown")
    normalized = raw.select(
        event_type.alias("event_type"),
        _column(raw, "user_id").cast("string").alias("source_user_id"),
        F.coalesce(_column(raw, "product_id").cast("string"), array_product).alias("source_product_id"),
        _column(raw, "category_id").cast("string").alias("category_id"),
        _column(raw, "category_code", "category", default="unknown").cast("string").alias("category_code"),
        _column(raw, "brand").cast("string").alias("brand"),
        _column(raw, "user_session", "session_id").cast("string").alias("session_id"),
        F.coalesce(parsed_event_time, epoch_event_time).alias("event_time"),
        F.coalesce(
            _column(raw, "price").cast(DecimalType(18, 4)),
            _column(raw, "total_amount").cast(DecimalType(18, 4)),
        ).alias("price"),
        source_file.alias("source_file"),
        F.lit(run_id).alias("run_id"),
        F.lit(ingested_at).cast(TimestampType()).alias("ingested_at"),
    )

    normalized = normalized.withColumn("event_date", F.to_date("event_time"))
    return normalized.withColumn(
        "event_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce("event_type", F.lit("")),
                F.coalesce("source_user_id", F.lit("")),
                F.coalesce("source_product_id", F.lit("")),
                F.coalesce("session_id", F.lit("")),
                F.coalesce(F.date_format("event_time", "yyyy-MM-dd HH:mm:ss"), F.lit("")),
            ),
            256,
        ),
    )


def split_valid_events(events: DataFrame) -> tuple[DataFrame, DataFrame]:
    invalid_reason = (
        F.when(~F.col("event_type").isin(*SUPPORTED_EVENT_TYPES), "unsupported_event_type")
        .when(F.col("event_time").isNull(), "missing_event_time")
        .when(F.col("source_user_id").isNull() | (F.trim("source_user_id") == ""), "missing_user_id")
        .when(F.col("source_product_id").isNull() | (F.trim("source_product_id") == ""), "missing_product_id")
        .when(F.col("price") < 0, "negative_price")
    )
    marked = events.withColumn("rejection_reason", invalid_reason)
    valid = marked.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
    rejected = marked.filter(F.col("rejection_reason").isNotNull())
    return valid.dropDuplicates(["event_key"]), rejected


def write_silver(valid: DataFrame, rejected: DataFrame) -> tuple[int, int]:
    valid_count = valid.count()
    rejected_count = rejected.count()
    (
        valid.write.mode("append")
        .partitionBy("event_date", "run_id")
        .parquet(data_lake_uri("silver", "behavior_events"))
    )
    if rejected_count:
        rejected.write.mode("append").partitionBy("run_id").parquet(
            data_lake_uri("silver", "quarantine/behavior_events")
        )
    return valid_count, rejected_count


def read_canonical_silver(spark: SparkSession) -> DataFrame:
    return (
        spark.read.parquet(data_lake_uri("silver", "behavior_events"))
        .withColumn(
            "latest_record",
            F.row_number().over(Window.partitionBy("event_key").orderBy(F.desc("ingested_at"))),
        )
        .filter(F.col("latest_record") == 1)
        .drop("latest_record")
    )


def build_dimensions(events: DataFrame) -> dict[str, DataFrame]:
    dim_date = events.select("event_date").distinct().select(
        F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("event_date").alias("full_date"),
        F.dayofweek("event_date").cast("short").alias("day_of_week"),
        F.date_format("event_date", "EEEE").alias("day_name"),
        F.weekofyear("event_date").cast("short").alias("week_of_year"),
        F.month("event_date").cast("short").alias("month"),
        F.date_format("event_date", "MMMM").alias("month_name"),
        F.quarter("event_date").cast("short").alias("quarter"),
        F.year("event_date").cast("short").alias("year"),
        F.dayofweek("event_date").isin(1, 7).alias("is_weekend"),
    )

    categories = (
        events.select("category_id", "category_code")
        .fillna({"category_code": "unknown"})
        .dropDuplicates(["category_code"])
        .withColumn("category_key", F.xxhash64("category_code"))
        .withColumn("category_level_1", F.split("category_code", "\\.").getItem(0))
        .withColumn("category_level_2", F.split("category_code", "\\.").getItem(1))
        .select("category_key", "category_id", "category_code", "category_level_1", "category_level_2")
    )

    latest_product = Window.partitionBy("source_product_id").orderBy(F.desc("event_time"))
    products = (
        events.withColumn("rn", F.row_number().over(latest_product))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .join(categories.select("category_key", "category_code"), "category_code", "left")
        .join(
            events.groupBy("source_product_id").agg(
                F.min("event_time").alias("first_seen_at"), F.max("event_time").alias("last_seen_at")
            ),
            "source_product_id",
        )
        .withColumn("product_key", F.xxhash64("source_product_id"))
        .select("product_key", "source_product_id", "category_key", "brand", "first_seen_at", "last_seen_at")
    )

    users = (
        events.groupBy("source_user_id")
        .agg(F.min("event_time").alias("first_seen_at"), F.max("event_time").alias("last_seen_at"))
        .withColumn("user_key", F.xxhash64("source_user_id"))
        .select("user_key", "source_user_id", "first_seen_at", "last_seen_at")
    )
    return {"dim_date": dim_date, "dim_category": categories, "dim_product": products, "dim_user": users}


def build_fact(events: DataFrame, dimensions: dict[str, DataFrame]) -> DataFrame:
    return (
        events.join(dimensions["dim_user"].select("user_key", "source_user_id"), "source_user_id")
        .join(dimensions["dim_product"].select("product_key", "source_product_id"), "source_product_id")
        .withColumn("date_key", F.date_format("event_date", "yyyyMMdd").cast("int"))
        .withColumn(
            "event_type_key",
            F.create_map(*[item for pair in EVENT_TYPE_KEYS.items() for item in (F.lit(pair[0]), F.lit(pair[1]))])[
                F.col("event_type")
            ].cast("short"),
        )
        .select(
            "event_key", "date_key", "user_key", "product_key", "event_type_key", "session_id",
            "event_time", "price", "source_file", "run_id", "ingested_at",
        )
    )


def _ratio(numerator, denominator):
    return F.when(denominator > 0, numerator.cast("double") / denominator).otherwise(F.lit(0.0))


def _revenue(condition) -> "F.Column":
    """Sum of price for rows matching condition, as double."""
    return F.sum(F.when(condition, F.coalesce("price", F.lit(0.0))).otherwise(0.0)).cast("double")


def build_marts(events: DataFrame) -> dict[str, DataFrame]:
    is_purchase = F.col("event_type") == "purchase"

    funnel = events.groupBy("event_date").agg(
        F.countDistinct(F.when(F.col("event_type") == "view", F.col("source_user_id"))).alias("viewers"),
        F.countDistinct(F.when(F.col("event_type") == "cart", F.col("source_user_id"))).alias("cart_users"),
        F.countDistinct(F.when(is_purchase, F.col("source_user_id"))).alias("buyers"),
    )
    funnel = (
        funnel.withColumn("view_to_cart_rate", _ratio(F.col("cart_users"), F.col("viewers")))
        .withColumn("cart_to_purchase_rate", _ratio(F.col("buyers"), F.col("cart_users")))
        .withColumn("conversion_rate", _ratio(F.col("buyers"), F.col("viewers")))
        .withColumn(
            "cart_abandonment_rate",
            F.when(F.col("cart_users") > 0, F.greatest(F.lit(0.0), 1 - _ratio(F.col("buyers"), F.col("cart_users"))))
            .otherwise(F.lit(0.0)),
        )
    )

    product = events.groupBy("event_date", F.col("source_product_id").alias("product_id")).agg(
        F.first("brand", ignorenulls=True).alias("brand"),
        F.first("category_code", ignorenulls=True).alias("category_code"),
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_adds"),
        F.sum(F.when(is_purchase, 1).otherwise(0)).alias("purchase_events"),
        _revenue(is_purchase).alias("revenue"),
    )
    product = (
        product.withColumn("view_to_cart_rate", _ratio(F.col("cart_adds"), F.col("views")))
        .withColumn("cart_to_purchase_rate", _ratio(F.col("purchase_events"), F.col("cart_adds")))
    )

    category = events.groupBy("event_date", "category_code").agg(
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_adds"),
        F.sum(F.when(is_purchase, 1).otherwise(0)).alias("purchase_events"),
        _revenue(is_purchase).alias("revenue"),
    ).withColumn("conversion_rate", _ratio(F.col("purchase_events"), F.col("views")))

    # Daily revenue series — the feature table consumed by the ML job.
    daily_revenue = (
        events.groupBy("event_date")
        .agg(
            _revenue(is_purchase).alias("revenue"),
            F.sum(F.when(is_purchase, 1).otherwise(0)).alias("purchase_events"),
            F.countDistinct(F.when(is_purchase, F.col("source_user_id"))).alias("buyers"),
        )
        .withColumn("avg_purchase_value", _ratio(F.col("revenue"), F.col("purchase_events")))
    )

    sessions = (
        events.filter(F.col("session_id").isNotNull() & (F.trim("session_id") != ""))
        .groupBy(F.col("session_id"))
        .agg(
            F.first("source_user_id", ignorenulls=True).alias("user_id"),
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"),
            F.count("event_key").alias("event_count"),
            F.countDistinct("source_product_id").alias("product_count"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
            F.sum(F.when(is_purchase, 1).otherwise(0)).alias("purchase_count"),
        )
        .withColumn("event_date", F.to_date("session_start"))
        .withColumn("duration_seconds", F.col("session_end").cast("long") - F.col("session_start").cast("long"))
        .withColumn(
            "session_outcome",
            F.when(F.col("purchase_count") > 0, "converted")
            .when(F.col("cart_count") > 0, "abandoned_cart")
            .otherwise("browsing"),
        )
        .select(
            "event_date", "session_id", "user_id", "session_start", "session_end",
            "duration_seconds", "event_count", "product_count", "view_count", "cart_count",
            "purchase_count", "session_outcome",
        )
    )
    return {
        "funnel_daily": funnel,
        "product_daily": product,
        "category_daily": category,
        "daily_revenue": daily_revenue,
        "session_daily": sessions,
    }


def quality_checks(events: DataFrame, fact: DataFrame, dimensions: dict[str, DataFrame]) -> list[QualityResult]:
    event_count = events.count()
    duplicate_keys = event_count - events.select("event_key").distinct().count()
    null_required = events.filter(
        F.col("event_key").isNull() | F.col("event_time").isNull() |
        F.col("source_user_id").isNull() | F.col("source_product_id").isNull()
    ).count()
    orphan_count = event_count - fact.count()
    product_count = dimensions["dim_product"].count()
    return [
        QualityResult("silver_not_empty", event_count > 0, event_count, "> 0"),
        QualityResult("event_key_unique", duplicate_keys == 0, duplicate_keys, "= 0 duplicates"),
        QualityResult("required_fields_complete", null_required == 0, null_required, "= 0 null rows"),
        QualityResult("fact_has_no_dimension_orphans", orphan_count == 0, orphan_count, "= 0 orphan rows"),
        QualityResult("product_dimension_not_empty", product_count > 0, product_count, "> 0"),
    ]


def write_gold(dimensions: dict[str, DataFrame], fact: DataFrame, marts: dict[str, DataFrame]) -> None:
    for name, frame in dimensions.items():
        frame.write.mode("overwrite").parquet(data_lake_uri("gold", f"warehouse/{name}"))
    (
        fact.write.mode("overwrite").partitionBy("date_key")
        .parquet(data_lake_uri("gold", "warehouse/fact_behavior_event"))
    )
    for name, frame in marts.items():
        (
            frame.write.mode("overwrite").partitionBy("event_date")
            .parquet(data_lake_uri("gold", f"mart/{name}"))
        )


def _connect():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD, dbname=POSTGRES_DB,
    )


def apply_warehouse_ddl() -> None:
    ddl_path = Path(__file__).resolve().parent.parent / "scripts" / "init_postgres.sql"
    with _connect() as conn, conn.cursor() as cursor:
        cursor.execute(ddl_path.read_text(encoding="utf-8"))


# Column contract for each BI mart cached in PostgreSQL. The MinIO gold zone
# remains the authoritative warehouse (dims + fact + marts); PostgreSQL only
# caches these compact marts so Superset always has data to show.
CACHE_MART_COLUMNS: dict[str, list[str]] = {
    "funnel_daily": [
        "event_date", "viewers", "cart_users", "buyers", "view_to_cart_rate",
        "cart_to_purchase_rate", "conversion_rate", "cart_abandonment_rate",
    ],
    "product_daily": [
        "event_date", "product_id", "brand", "category_code", "views", "cart_adds",
        "purchase_events", "revenue", "view_to_cart_rate", "cart_to_purchase_rate",
    ],
    "category_daily": [
        "event_date", "category_code", "views", "cart_adds", "purchase_events", "revenue",
        "conversion_rate",
    ],
    "daily_revenue": [
        "event_date", "revenue", "purchase_events", "buyers", "avg_purchase_value",
    ],
    "session_daily": [
        "event_date", "session_id", "user_id", "session_start", "session_end",
        "duration_seconds", "event_count", "product_count", "view_count", "cart_count",
        "purchase_count", "session_outcome",
    ],
}


def publish_postgres(marts: dict[str, DataFrame]) -> None:
    """Atomically refresh the PostgreSQL BI cache from Spark marts.

    Each mart lands in the `staging` schema (Spark JDBC overwrite), then a single
    transaction swaps it into `cache`. Superset therefore sees either the last
    good version or the complete new version, never a half-loaded dashboard.
    """
    apply_warehouse_ddl()
    for name in CACHE_MART_COLUMNS:
        marts[name].write.jdbc(JDBC_URL, f"staging.{name}", "overwrite", JDBC_PROPERTIES)

    with _connect() as conn, conn.cursor() as cursor:
        cache_tables = ", ".join(f"cache.{name}" for name in CACHE_MART_COLUMNS)
        cursor.execute(f"TRUNCATE {cache_tables}")
        for name, columns in CACHE_MART_COLUMNS.items():
            column_list = ", ".join(columns)
            cursor.execute(
                f"INSERT INTO cache.{name} ({column_list}) "
                f"SELECT {column_list} FROM staging.{name}"
            )


def record_run_start(run_id: str, source_path: str, started_at: datetime) -> None:
    apply_warehouse_ddl()
    with _connect() as conn, conn.cursor() as cursor:
        cursor.execute(
            """INSERT INTO audit.pipeline_run(run_id, source_path, started_at, status)
               VALUES (%s, %s, %s, 'RUNNING')
               ON CONFLICT (run_id) DO UPDATE SET status='RUNNING', error_message=NULL""",
            (run_id, source_path, started_at),
        )


def record_run_end(
    run_id: str, status: str, counts: dict[str, int], checks: list[QualityResult], error: str | None = None
) -> None:
    with _connect() as conn, conn.cursor() as cursor:
        cursor.execute(
            """UPDATE audit.pipeline_run
               SET completed_at=%s, status=%s, bronze_rows=%s, silver_rows=%s,
                   rejected_rows=%s, gold_rows=%s, error_message=%s
               WHERE run_id=%s""",
            (
                datetime.now(timezone.utc), status, counts.get("bronze", 0), counts.get("silver", 0),
                counts.get("rejected", 0), counts.get("gold", 0), error, run_id,
            ),
        )
        for check in checks:
            cursor.execute(
                """INSERT INTO audit.data_quality_result
                   (run_id, check_name, status, observed_value, expectation)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (run_id, check_name) DO UPDATE
                   SET status=EXCLUDED.status, observed_value=EXCLUDED.observed_value,
                       expectation=EXCLUDED.expectation, checked_at=CURRENT_TIMESTAMP""",
                (run_id, check.name, "PASS" if check.passed else "FAIL", check.observed, check.expectation),
            )


def run_warehouse(source_path: str, publish: bool = True) -> dict[str, object]:
    run_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc)
    counts = {"bronze": 0, "silver": 0, "rejected": 0, "gold": 0}
    checks: list[QualityResult] = []
    spark = build_spark()
    if publish:
        record_run_start(run_id, source_path, started_at)
    try:
        raw = read_source(spark, source_path).cache()
        counts["bronze"] = write_bronze(raw, run_id, started_at)
        normalized = normalize_events(raw, run_id, started_at)
        valid, rejected = split_valid_events(normalized)
        counts["silver"], counts["rejected"] = write_silver(valid, rejected)

        canonical = read_canonical_silver(spark).cache()
        dimensions = build_dimensions(canonical)
        fact = build_fact(canonical, dimensions).cache()
        marts = build_marts(canonical)
        checks = quality_checks(canonical, fact, dimensions)
        failed = [check.name for check in checks if not check.passed]
        if failed:
            raise RuntimeError(f"Data quality gate failed: {', '.join(failed)}")

        write_gold(dimensions, fact, marts)
        counts["gold"] = fact.count()
        if publish:
            publish_postgres(marts)
            record_run_end(run_id, "SUCCESS", counts, checks)
        result = {"run_id": run_id, "status": "SUCCESS", "counts": counts}
        print(json.dumps(result, indent=2))
        return result
    except Exception as exc:
        if publish:
            record_run_end(run_id, "FAILED", counts, checks, str(exc)[:4000])
        raise
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Warehouse V2 Spark EtLT pipeline")
    parser.add_argument("--source", required=True, help="Kaggle CSV or canonical JSON/JSONL path")
    parser.add_argument("--skip-postgres", action="store_true", help="Build lake zones without publishing BI tables")
    args = parser.parse_args()
    run_warehouse(args.source, publish=not args.skip_postgres)


if __name__ == "__main__":
    main()
