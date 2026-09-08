"""Read, flatten, deduplicate and compact canonical Silver observations.

Two things about the landing format drive this module.

First, the sink writes one small JSON object per observation.  Scanning the
dataset root would therefore mean listing one object per observation ever
collected, which is the predictable way to make this job unusable.  So paths are
built as explicit ``marketplace=``/``observed_date=`` globs and pruned before
Spark sees them.

Second, the schema is always supplied, never inferred.  Inference samples files,
would guess a Decimal wire string as a double, and can silently change types
between runs as the sample changes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Sequence

from config.settings import data_lake_uri

logger = logging.getLogger(__name__)

SILVER_OBSERVATIONS_DATASET = "marketplace/offer_observations"
SILVER_OBSERVATIONS_COMPACTED = "marketplace/offer_observations_compacted"
SILVER_QUARANTINE_DATASET = "quarantine/offer_observations"

# Columns whose wire value is never null; a null after casting means the cast
# silently destroyed a real value.
REQUIRED_TYPED_COLUMNS = (
    "event_id",
    "observation_id",
    "offer_id",
    "marketplace",
    "observed_at",
    "fetched_at",
    "current_price",
    "raw_uri",
    "raw_sha256",
    "adapter_version",
    "crawl_run_id",
)


class SilentCastError(ValueError):
    """Raised when a non-null wire value became null after typing."""


@dataclass(frozen=True)
class SilverReadReport:
    paths_requested: int
    paths_with_rows: int
    rows_read: int
    rows_deduplicated: int

    @property
    def duplicates_removed(self) -> int:
        return self.rows_read - self.rows_deduplicated


def date_range(start_date: date, end_date: date) -> tuple[date, ...]:
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    span = (end_date - start_date).days
    return tuple(start_date + timedelta(days=offset) for offset in range(span + 1))


def observation_partition_paths(
    *,
    marketplaces: Sequence[str],
    start_date: date,
    end_date: date,
) -> tuple[str, ...]:
    """Build one pruned glob per (marketplace, day)."""
    if not marketplaces:
        raise ValueError("at least one marketplace is required")
    base = data_lake_uri("silver", SILVER_OBSERVATIONS_DATASET)
    paths = []
    for marketplace in marketplaces:
        code = str(marketplace).strip().lower()
        if not code:
            raise ValueError("marketplace code must not be blank")
        for day in date_range(start_date, end_date):
            paths.append(f"{base}/marketplace={code}/observed_date={day.isoformat()}/*.json")
    return tuple(paths)


def quarantine_partition_paths(*, start_date: date, end_date: date) -> tuple[str, ...]:
    base = data_lake_uri("silver", SILVER_QUARANTINE_DATASET)
    return tuple(
        f"{base}/observed_date={day.isoformat()}/*/*/*.json"
        for day in date_range(start_date, end_date)
    )


def _wire_schema():
    """Phase 4's frozen wire schema, imported lazily."""
    from data_ingestion.schemas import MARKETPLACE_OBSERVATION_WIRE_SCHEMA  # type: ignore

    return MARKETPLACE_OBSERVATION_WIRE_SCHEMA


def flatten_observations(nested_df):
    """Flatten the nested envelope into the flat typed Silver shape."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType, LongType

    from config.settings import MARKETPLACE_DECIMAL_PRECISION, MARKETPLACE_DECIMAL_SCALE

    money = DecimalType(MARKETPLACE_DECIMAL_PRECISION, MARKETPLACE_DECIMAL_SCALE)
    offer = F.col("payload.offer")
    observation = F.col("payload.observation")

    def cast_money(column):
        return column.cast(money)

    flat = nested_df.select(
        F.col("event_id"),
        F.col("schema_version"),
        F.col("event_type"),
        F.col("marketplace"),
        F.col("partition_key"),
        F.col("crawl_run_id"),
        F.col("raw_uri"),
        F.to_timestamp(F.col("occurred_at")).alias("occurred_at"),
        F.to_timestamp(F.col("produced_at")).alias("produced_at"),
        offer.getField("offer_id").alias("offer_id"),
        offer.getField("marketplace_id").alias("marketplace_id"),
        offer.getField("platform_listing_id").alias("platform_listing_id"),
        offer.getField("seller_id").alias("seller_id"),
        offer.getField("product_title").alias("product_title"),
        offer.getField("brand").alias("brand"),
        offer.getField("category_path").alias("category_path"),
        offer.getField("source_url").alias("source_url"),
        offer.getField("currency").alias("currency"),
        offer.getField("active_status").alias("active_status"),
        F.to_timestamp(offer.getField("first_seen_at")).alias("first_seen_at"),
        F.to_timestamp(offer.getField("last_seen_at")).alias("last_seen_at"),
        observation.getField("observation_id").alias("observation_id"),
        F.to_timestamp(observation.getField("observed_at")).alias("observed_at"),
        F.to_timestamp(observation.getField("fetched_at")).alias("fetched_at"),
        cast_money(observation.getField("current_price")).alias("current_price"),
        cast_money(observation.getField("list_price")).alias("list_price"),
        cast_money(observation.getField("shipping_price")).alias("shipping_price"),
        cast_money(observation.getField("discount_amount")).alias("discount_amount"),
        cast_money(observation.getField("discount_percent")).alias("discount_percent"),
        cast_money(observation.getField("rating_value")).alias("rating_value"),
        cast_money(observation.getField("rating_scale")).alias("rating_scale"),
        observation.getField("rating_count").cast(LongType()).alias("rating_count"),
        observation.getField("review_count").cast(LongType()).alias("review_count"),
        observation.getField("sold_count").cast(LongType()).alias("sold_count"),
        observation.getField("availability").alias("availability"),
        observation.getField("ranking_position").cast(LongType()).alias("ranking_position"),
        observation.getField("raw_sha256").alias("raw_sha256"),
        observation.getField("adapter_version").alias("adapter_version"),
        F.to_json(observation.getField("promotion")).alias("promotion_json"),
    )
    return flat.withColumn("observed_date", F.to_date(F.col("observed_at")))


def assert_no_silent_casts(nested_df, flat_df) -> None:
    """Fail loudly when typing turned a real wire value into null.

    A cast that quietly nulls a value is worse than a crash: the row survives,
    the aggregate moves, and nothing in the output says why.
    """
    from pyspark.sql import functions as F

    wire_paths = {
        "event_id": "event_id",
        "observation_id": "payload.observation.observation_id",
        "offer_id": "payload.offer.offer_id",
        "marketplace": "marketplace",
        "observed_at": "payload.observation.observed_at",
        "fetched_at": "payload.observation.fetched_at",
        "current_price": "payload.observation.current_price",
        "raw_uri": "raw_uri",
        "raw_sha256": "payload.observation.raw_sha256",
        "adapter_version": "payload.observation.adapter_version",
        "crawl_run_id": "crawl_run_id",
    }
    wire_counts = nested_df.select(
        *[
            F.count(F.col(path)).alias(name)
            for name, path in wire_paths.items()
        ]
    ).collect()[0].asDict()
    typed_counts = flat_df.select(
        *[F.count(F.col(name)).alias(name) for name in wire_paths]
    ).collect()[0].asDict()
    for name in REQUIRED_TYPED_COLUMNS:
        if wire_counts.get(name, 0) != typed_counts.get(name, 0):
            raise SilentCastError(
                f"column {name}: {wire_counts[name]} non-null wire values became "
                f"{typed_counts[name]} after typing"
            )


def deduplicate_observations(flat_df):
    """Keep one row per observation_id, chosen deterministically.

    ``dropDuplicates()`` without an order picks a nondeterministic survivor,
    which would make two runs over the same input disagree.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy("observation_id").orderBy(
        F.col("produced_at").asc_nulls_last(), F.col("raw_uri").asc_nulls_last()
    )
    return (
        flat_df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def read_silver_observations(
    spark,
    *,
    marketplaces: Sequence[str],
    start_date: date,
    end_date: date,
    verify_casts: bool = True,
) -> tuple[object, SilverReadReport]:
    """Read, flatten and deduplicate the requested window."""
    paths = observation_partition_paths(
        marketplaces=marketplaces, start_date=start_date, end_date=end_date
    )
    nested = (
        spark.read.schema(_wire_schema())
        .option("mode", "FAILFAST")
        .json(list(paths))
    )
    rows_read = nested.count()
    if rows_read == 0:
        # A window with no objects is normal: the crawler may not have run.
        logger.info("silver read: no observations in %d requested partitions", len(paths))
    flat = flatten_observations(nested)
    if verify_casts and rows_read:
        assert_no_silent_casts(nested, flat)
    deduplicated = deduplicate_observations(flat).cache()
    rows_deduplicated = deduplicated.count()
    report = SilverReadReport(
        paths_requested=len(paths),
        paths_with_rows=0 if rows_read == 0 else len(paths),
        rows_read=rows_read,
        rows_deduplicated=rows_deduplicated,
    )
    logger.info(
        "silver read: %d rows -> %d after dedup (%d duplicates) across %d partitions",
        report.rows_read,
        report.rows_deduplicated,
        report.duplicates_removed,
        report.paths_requested,
    )
    return deduplicated, report


def compact_observations(flat_df) -> str:
    """Write the typed rows to a new compacted Parquet dataset.

    A new dataset, never in place: the JSON envelopes the sink wrote remain the
    landing truth, and this phase must be able to run again from them.
    """
    target = data_lake_uri("silver", SILVER_OBSERVATIONS_COMPACTED)
    (
        flat_df.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("marketplace", "observed_date")
        .parquet(target)
    )
    return target


def count_quarantined(spark, *, start_date: date, end_date: date) -> int:
    """Count Phase 4 quarantine objects, so rejected rows are counted not guessed."""
    paths = quarantine_partition_paths(start_date=start_date, end_date=end_date)
    try:
        return spark.read.json(list(paths)).count()
    except Exception as exc:  # missing paths are normal
        logger.info("quarantine count unavailable for the window: %s", exc)
        return 0
