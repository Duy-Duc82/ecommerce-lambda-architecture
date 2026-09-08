"""Build the marketplace Gold temporal marts from canonical Silver.

The job takes ``as_of`` as an argument and never reads a clock inside a
transform.  That is what makes it reproducible: the same observations plus the
same ``as_of`` must produce the same marts, and a test asserts no clock call
appears in this module's transform functions.

Two ordering rules run through everything:

* every per-offer window orders by ``(observed_at, observation_id)``.  Ordering
  by the timestamp alone gives a nondeterministic result whenever two
  observations share an instant, and then two runs disagree.
* nothing is imputed.  A day with no observation for an offer produces no row;
  the absence is counted in ``source_coverage_daily`` rather than hidden behind
  a forward-filled price.

``offer_change_daily`` is recomputed here from consecutive Silver observations,
never read from the speed layer's change topic.  A difference between the two
counts is a real operational signal — a sink outage, a consumer gap — and
correcting one into the other would destroy the signal that the batch layer
exists to provide.
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence

from batch_layer.marketplace_gold_contracts import (
    CATEGORY_PRICE_DAILY,
    COUNTER_DELTA_DAILY,
    CRAWL_RELIABILITY_DAILY,
    MARKETPLACE_MART_SPECS,
    OFFER_CHANGE_DAILY,
    OFFER_CURRENT,
    OFFER_FRESHNESS,
    OFFER_PRICE_HISTORY_DAILY,
    SOURCE_COVERAGE_DAILY,
    MartSpec,
    gold_dataset_path,
    make_gold_run_id,
)
from batch_layer.marketplace_rules import (
    CounterValidity,
    FreshnessStatus,
    FreshnessThresholds,
)
from config.settings import (
    COUNTER_DELTA_MAX_GAP_MINUTES,
    GOLD_DEFAULT_WINDOW_DAYS,
    GOLD_QUANTILE_ACCURACY,
    GOLD_RULE_VERSION,
    MARKETPLACE_FRESHNESS_THRESHOLD_MINUTES,
    MARKETPLACE_STALE_THRESHOLD_MINUTES,
    SPEED_LARGE_DROP_ABSOLUTE,
    SPEED_LARGE_DROP_PERCENT,
    TIKI_CATEGORIES,
    data_lake_uri,
)

logger = logging.getLogger(__name__)

SILVER_OFFERS_DATASET = "marketplace/offers"
SILVER_SELLERS_DATASET = "marketplace/sellers"
QUANTILE_METHOD = "percentile_approx"

STAGE_READ = "READ"
STAGE_COMPACT = "COMPACT"
STAGE_DIMENSIONS = "DIMENSIONS"
STAGE_MARTS = "MARTS"
STAGE_ASSERT = "ASSERT"
STAGE_PUBLISH = "PUBLISH"

STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED = "FAILED"


@dataclass(frozen=True)
class AssertionResult:
    name: str
    observed_value: str
    expectation: str
    passed: bool

    @property
    def status(self) -> str:
        return "PASSED" if self.passed else "FAILED"

    def as_row(self, rule_version: str) -> dict[str, str]:
        return {
            "assertion_name": self.name,
            "observed_value": self.observed_value,
            "expectation": self.expectation,
            "status": self.status,
            "rule_version": rule_version,
        }


class GoldAssertionFailed(RuntimeError):
    def __init__(self, failures: Sequence[AssertionResult]):
        detail = "; ".join(f"{f.name}: observed {f.observed_value}" for f in failures)
        super().__init__(f"structural assertions failed: {detail}")
        self.failures = tuple(failures)


@dataclass(frozen=True)
class GoldRunReport:
    gold_run_id: str
    as_of: datetime
    window_days: int
    rule_version: str
    started_at: datetime
    completed_at: datetime
    observations_read: int
    observations_deduplicated: int
    quarantined_rows: int
    mart_row_counts: dict[str, int] = field(default_factory=dict)
    assertions: tuple[AssertionResult, ...] = ()
    skipped_marts: dict[str, str] = field(default_factory=dict)
    published: bool = False
    status: str = STATUS_SUCCEEDED
    failure_stage: str | None = None

    def __post_init__(self) -> None:
        if self.status == STATUS_FAILED and self.failure_stage is None:
            raise ValueError("a failed run requires a failure stage")
        if self.published and self.status != STATUS_SUCCEEDED:
            raise ValueError("a failed run must not be published")
        if self.published and any(not a.passed for a in self.assertions):
            raise ValueError("a run with a failed assertion must not be published")

    def run_row(self) -> dict[str, Any]:
        return {
            "as_of": self.as_of,
            "window_days": self.window_days,
            "rule_version": self.rule_version,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "observations_read": self.observations_read,
            "quarantined_rows": self.quarantined_rows,
            "status": self.status,
            "failure_stage": self.failure_stage,
            "skipped_marts": dict(self.skipped_marts),
        }


def default_freshness_thresholds() -> FreshnessThresholds:
    return FreshnessThresholds(
        fresh_after=timedelta(minutes=MARKETPLACE_FRESHNESS_THRESHOLD_MINUTES),
        stale_after=timedelta(minutes=MARKETPLACE_STALE_THRESHOLD_MINUTES),
    )


def _offer_window():
    from pyspark.sql import Window

    return Window.partitionBy("offer_id").orderBy("observed_at", "observation_id")


def _lineage_columns(dataframe, *, gold_run_id: str, as_of: datetime, rule_version: str):
    from pyspark.sql import functions as F

    return (
        dataframe.withColumn("gold_run_id", F.lit(gold_run_id))
        .withColumn("as_of", F.lit(as_of).cast("timestamp"))
        .withColumn("rule_version", F.lit(rule_version))
    )


def _conform(dataframe, spec: MartSpec):
    """Select the declared columns in declared order.

    The contract is the authority: a transform that drifted fails here instead
    of silently publishing an extra or renamed column.
    """
    missing = [name for name in spec.column_names if name not in dataframe.columns]
    if missing:
        raise ValueError(f"mart {spec.name} is missing columns {missing}")
    return dataframe.select(*spec.column_names)


def _freshness_expression(as_of: datetime, thresholds: FreshnessThresholds):
    """Freshness as a column expression, mirroring ``classify_freshness``."""
    from pyspark.sql import functions as F

    age_seconds = F.unix_timestamp(F.lit(as_of).cast("timestamp")) - F.unix_timestamp(
        F.col("observed_at")
    )
    # A future observation reports age zero, never a negative age that would
    # otherwise leak into every average.
    age_minutes = F.greatest(F.floor(age_seconds / F.lit(60)), F.lit(0)).cast("long")
    fresh_minutes = int(thresholds.fresh_after.total_seconds() // 60)
    stale_minutes = int(thresholds.stale_after.total_seconds() // 60)
    status = (
        F.when(age_minutes <= F.lit(fresh_minutes), F.lit(FreshnessStatus.FRESH.value))
        .when(age_minutes <= F.lit(stale_minutes), F.lit(FreshnessStatus.AGING.value))
        .otherwise(F.lit(FreshnessStatus.STALE.value))
    )
    return age_minutes, status


def build_offer_dimension(observations):
    """One row per offer; descriptive attributes from the latest observation."""
    from pyspark.sql import functions as F

    # Descriptive attributes come from the newest observation: a corrected
    # title or category should win over the older value it replaced.
    newest_first = _offer_window().orderBy(
        F.col("observed_at").desc(), F.col("observation_id").desc()
    )
    latest_rows = (
        observations.withColumn("_rn", F.row_number().over(newest_first))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    bounds = observations.groupBy("offer_id").agg(
        F.min("observed_at").alias("first_seen_at"),
        F.max("observed_at").alias("last_seen_at"),
        F.count(F.lit(1)).alias("observation_count"),
    )
    return latest_rows.select(
        "offer_id",
        "marketplace",
        "marketplace_id",
        "platform_listing_id",
        "seller_id",
        "product_title",
        "brand",
        "category_path",
        "source_url",
        "currency",
        "active_status",
    ).join(bounds, on="offer_id", how="inner")


def build_seller_dimension(observations):
    """One row per known seller; observations without a seller contribute none."""
    from pyspark.sql import functions as F

    known = observations.filter(F.col("seller_id").isNotNull())
    return known.groupBy("seller_id", "marketplace", "marketplace_id").agg(
        F.min("observed_at").alias("first_seen_at"),
        F.max("observed_at").alias("last_seen_at"),
        F.countDistinct("offer_id").alias("offer_count"),
    )


def build_offer_current(observations, *, as_of, thresholds, **lineage):
    from pyspark.sql import functions as F

    window = _offer_window().orderBy(
        F.col("observed_at").desc(), F.col("observation_id").desc()
    )
    latest = (
        observations.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    age_minutes, status = _freshness_expression(as_of, thresholds)
    enriched = latest.withColumn("age_minutes", age_minutes).withColumn(
        "freshness_status", status
    )
    return _conform(_lineage_columns(enriched, as_of=as_of, **lineage), OFFER_CURRENT)


def build_offer_freshness(observations, *, as_of, thresholds, **lineage):
    from pyspark.sql import functions as F

    window = _offer_window().orderBy(
        F.col("observed_at").desc(), F.col("observation_id").desc()
    )
    latest = (
        observations.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    age_minutes, status = _freshness_expression(as_of, thresholds)
    enriched = (
        latest.withColumn("age_minutes", age_minutes)
        .withColumn("freshness_status", status)
        .withColumn("last_observation_at", F.col("observed_at"))
        .withColumn("last_observation_id", F.col("observation_id"))
        .withColumn(
            "fresh_threshold_minutes",
            F.lit(int(thresholds.fresh_after.total_seconds() // 60)).cast("long"),
        )
        .withColumn(
            "stale_threshold_minutes",
            F.lit(int(thresholds.stale_after.total_seconds() // 60)).cast("long"),
        )
    )
    return _conform(_lineage_columns(enriched, as_of=as_of, **lineage), OFFER_FRESHNESS)


def build_price_history_daily(observations, *, as_of, **lineage):
    """Daily open/close/min/max. The representative price is the close."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    day_window = Window.partitionBy("offer_id", "observed_date").orderBy(
        "observed_at", "observation_id"
    )
    ranked = observations.withColumn(
        "_first", F.row_number().over(day_window)
    ).withColumn(
        "_last",
        F.row_number().over(
            Window.partitionBy("offer_id", "observed_date").orderBy(
                F.col("observed_at").desc(), F.col("observation_id").desc()
            )
        ),
    )
    daily = ranked.groupBy("offer_id", "observed_date", "marketplace").agg(
        F.max(F.when(F.col("_first") == 1, F.col("current_price"))).alias("price_open"),
        F.max(F.when(F.col("_last") == 1, F.col("current_price"))).alias("price_close"),
        F.min("current_price").alias("price_min"),
        F.max("current_price").alias("price_max"),
        F.count(F.lit(1)).cast("long").alias("observation_count"),
    )
    return _conform(
        _lineage_columns(daily, as_of=as_of, **lineage), OFFER_PRICE_HISTORY_DAILY
    )


def build_offer_change_daily(
    observations,
    *,
    as_of,
    large_drop_absolute: Decimal = SPEED_LARGE_DROP_ABSOLUTE,
    large_drop_percent: Decimal = SPEED_LARGE_DROP_PERCENT,
    **lineage,
):
    """Recompute change counts from consecutive observations, not from Kafka."""
    from pyspark.sql import functions as F

    window = _offer_window()
    lagged = (
        observations.withColumn("prev_price", F.lag("current_price").over(window))
        .withColumn("prev_availability", F.lag("availability").over(window))
        .withColumn("prev_sold_count", F.lag("sold_count").over(window))
    )
    delta = F.col("current_price") - F.col("prev_price")
    drop = -delta
    drop_percent = F.when(
        F.col("prev_price") > 0, (drop / F.col("prev_price")) * F.lit(100)
    )
    is_price_change = F.col("prev_price").isNotNull() & (delta != 0)
    is_large_drop = (
        is_price_change
        & (drop > 0)
        & (
            (drop >= F.lit(large_drop_absolute))
            | (drop_percent >= F.lit(large_drop_percent))
        )
    )
    both_known = (
        F.col("prev_availability").isNotNull()
        & (F.col("prev_availability") != F.lit("UNKNOWN"))
        & (F.col("availability") != F.lit("UNKNOWN"))
    )
    availability_moved = F.col("prev_availability") != F.col("availability")
    # Transitions involving UNKNOWN are counted separately, not dropped: they
    # are adapter coverage changing, and hiding them would make the batch and
    # speed-layer counts look inexplicably different.
    excluded_transition = availability_moved & ~both_known

    daily = lagged.groupBy("offer_id", "observed_date", "marketplace").agg(
        F.sum(F.when(is_price_change, 1).otherwise(0)).cast("long").alias("price_changes"),
        F.sum(F.when(is_large_drop, 1).otherwise(0)).cast("long").alias("large_price_drops"),
        F.sum(F.when(both_known & availability_moved, 1).otherwise(0))
        .cast("long")
        .alias("availability_changes"),
        F.sum(F.when(excluded_transition, 1).otherwise(0))
        .cast("long")
        .alias("availability_transitions_excluded"),
        F.sum(
            F.when(
                F.col("prev_sold_count").isNotNull()
                & (F.col("sold_count") != F.col("prev_sold_count")),
                1,
            ).otherwise(0)
        )
        .cast("long")
        .alias("counter_changes"),
        F.max(F.when(is_price_change, F.abs(delta))).alias("max_abs_price_delta"),
        F.avg(F.when(is_price_change, F.abs(delta))).alias("avg_abs_price_delta"),
        F.max(F.when(is_large_drop, drop_percent)).alias("max_drop_percent"),
    )
    typed = (
        daily.withColumn("max_abs_price_delta", F.col("max_abs_price_delta").cast("decimal(38,6)"))
        .withColumn("avg_abs_price_delta", F.col("avg_abs_price_delta").cast("decimal(38,6)"))
        .withColumn("max_drop_percent", F.col("max_drop_percent").cast("decimal(38,6)"))
    )
    return _conform(_lineage_columns(typed, as_of=as_of, **lineage), OFFER_CHANGE_DAILY)


def build_category_price_daily(
    observations, *, as_of, accuracy: int = GOLD_QUANTILE_ACCURACY, **lineage
):
    """Category price distribution, with the quantile method declared."""
    from pyspark.sql import functions as F

    known_category = observations.filter(F.col("category_path").isNotNull())
    daily = known_category.groupBy("marketplace", "category_path", "observed_date").agg(
        F.countDistinct("offer_id").cast("long").alias("offer_count"),
        F.count(F.lit(1)).cast("long").alias("observation_count"),
        F.min("current_price").alias("price_min"),
        F.max("current_price").alias("price_max"),
        F.expr(f"percentile_approx(current_price, 0.25, {accuracy})").alias("price_p25"),
        F.expr(f"percentile_approx(current_price, 0.5, {accuracy})").alias("price_median"),
        F.expr(f"percentile_approx(current_price, 0.75, {accuracy})").alias("price_p75"),
    )
    declared = (
        daily.withColumn("quantile_method", F.lit(QUANTILE_METHOD))
        .withColumn("quantile_accuracy", F.lit(accuracy).cast("long"))
        .withColumn("price_p25", F.col("price_p25").cast("decimal(38,6)"))
        .withColumn("price_median", F.col("price_median").cast("decimal(38,6)"))
        .withColumn("price_p75", F.col("price_p75").cast("decimal(38,6)"))
    )
    return _conform(_lineage_columns(declared, as_of=as_of, **lineage), CATEGORY_PRICE_DAILY)


def build_counter_delta_daily(
    observations, *, as_of, max_gap_minutes: int = COUNTER_DELTA_MAX_GAP_MINUTES, **lineage
):
    """Public-counter deltas with validity, never clamped and never summed blind."""
    from pyspark.sql import functions as F

    window = _offer_window()
    lagged = observations.withColumn(
        "prev_sold_count", F.lag("sold_count").over(window)
    ).withColumn("prev_observed_at", F.lag("observed_at").over(window))
    observed_delta = F.col("sold_count") - F.col("prev_sold_count")
    elapsed_minutes = (
        F.unix_timestamp(F.col("observed_at")) - F.unix_timestamp(F.col("prev_observed_at"))
    ) / F.lit(60)
    validity = (
        F.when(
            F.col("prev_sold_count").isNull()
            | F.col("sold_count").isNull()
            | F.col("prev_observed_at").isNull(),
            F.lit(CounterValidity.NO_PREVIOUS.value),
        )
        .when(elapsed_minutes <= 0, F.lit(CounterValidity.NON_POSITIVE_ELAPSED.value))
        .when(elapsed_minutes > F.lit(max_gap_minutes), F.lit(CounterValidity.GAP_TOO_LONG.value))
        .when(observed_delta < 0, F.lit(CounterValidity.NEGATIVE_DELTA.value))
        .otherwise(F.lit(CounterValidity.VALID.value))
    )
    classified = (
        lagged.withColumn("observed_delta", observed_delta)
        .withColumn("elapsed_minutes", elapsed_minutes)
        .withColumn("validity", validity)
    )
    is_valid = F.col("validity") == F.lit(CounterValidity.VALID.value)
    velocity = F.when(
        is_valid & (F.col("elapsed_minutes") > 0),
        F.col("observed_delta") / (F.col("elapsed_minutes") / F.lit(60)),
    )
    with_velocity = classified.withColumn("velocity_per_hour", velocity)

    def reason_count(reason: CounterValidity):
        return F.sum(F.when(F.col("validity") == F.lit(reason.value), 1).otherwise(0)).cast("long")

    daily = with_velocity.groupBy("offer_id", "observed_date", "marketplace").agg(
        # Only VALID rows enter the total; the invalid ones are reported, not
        # folded in, so a counter reset cannot inflate a trend.
        F.sum(F.when(is_valid, F.col("observed_delta")).otherwise(0))
        .cast("long")
        .alias("total_valid_delta"),
        F.sum(F.when(is_valid, 1).otherwise(0)).cast("long").alias("valid_rows"),
        F.sum(F.when(is_valid, 0).otherwise(1)).cast("long").alias("invalid_rows"),
        reason_count(CounterValidity.NO_PREVIOUS).alias("no_previous_rows"),
        reason_count(CounterValidity.NEGATIVE_DELTA).alias("negative_delta_rows"),
        reason_count(CounterValidity.GAP_TOO_LONG).alias("gap_too_long_rows"),
        reason_count(CounterValidity.NON_POSITIVE_ELAPSED).alias("non_positive_elapsed_rows"),
        F.max("velocity_per_hour").cast("decimal(38,6)").alias("max_velocity_per_hour"),
    )
    return _conform(_lineage_columns(daily, as_of=as_of, **lineage), COUNTER_DELTA_DAILY)


def build_source_coverage_daily(
    observations,
    *,
    as_of,
    quarantined: int,
    thresholds: FreshnessThresholds,
    window_days: int,
    **lineage,
):
    """Coverage per source-day; absence is counted, never imputed."""
    from pyspark.sql import functions as F

    stale_minutes = int(thresholds.stale_after.total_seconds() // 60)
    per_day = observations.groupBy("marketplace", "observed_date").agg(
        F.countDistinct("offer_id").cast("long").alias("offers_observed"),
        F.count(F.lit(1)).cast("long").alias("observations"),
        F.countDistinct(F.when(F.col("seller_id").isNull(), F.col("offer_id")))
        .cast("long")
        .alias("offers_without_seller"),
    )
    # Offers known in the window but absent on a given day. A count of
    # absence, never a fabricated row for the missing day.
    known_offers = observations.select("marketplace", "offer_id").distinct()
    days = observations.select("marketplace", "observed_date").distinct()
    expected = days.join(known_offers, on="marketplace", how="inner")
    seen = observations.select("marketplace", "observed_date", "offer_id").distinct()
    missing = (
        expected.join(seen, on=["marketplace", "observed_date", "offer_id"], how="left_anti")
        .groupBy("marketplace", "observed_date")
        .agg(F.count(F.lit(1)).cast("long").alias("offers_missing"))
    )
    stale = (
        observations.groupBy("marketplace", "observed_date", "offer_id")
        .agg(F.max("observed_at").alias("last_observed_at"))
        .withColumn(
            "_age_minutes",
            F.greatest(
                F.floor(
                    (
                        F.unix_timestamp(F.lit(as_of).cast("timestamp"))
                        - F.unix_timestamp(F.col("last_observed_at"))
                    )
                    / F.lit(60)
                ),
                F.lit(0),
            ),
        )
        .groupBy("marketplace", "observed_date")
        .agg(
            F.countDistinct(
                F.when(F.col("_age_minutes") > F.lit(stale_minutes), F.col("offer_id"))
            )
            .cast("long")
            .alias("offers_stale")
        )
    )
    combined = (
        per_day.join(missing, on=["marketplace", "observed_date"], how="left")
        .join(stale, on=["marketplace", "observed_date"], how="left")
        .fillna({"offers_missing": 0, "offers_stale": 0})
        .withColumn("rows_rejected", F.lit(int(quarantined)).cast("long"))
    )
    return _conform(_lineage_columns(combined, as_of=as_of, **lineage), SOURCE_COVERAGE_DAILY)


def build_crawl_reliability_daily(audit_df, *, as_of, skipped_reason: str | None = None, **lineage):
    """Reliability from Phase 3 audit; empty with a stated reason when absent."""
    from pyspark.sql import functions as F

    if skipped_reason is not None:
        empty = audit_df.limit(0)
        stub = (
            empty.withColumn("marketplace", F.lit(None).cast("string"))
            .withColumn("observed_date", F.lit(None).cast("date"))
            .withColumn("requests", F.lit(0).cast("long"))
            .withColumn("succeeded", F.lit(0).cast("long"))
            .withColumn("failed", F.lit(0).cast("long"))
            .withColumn("success_rate", F.lit(None).cast("decimal(38,6)"))
            .withColumn("p50_latency_ms", F.lit(None).cast("long"))
            .withColumn("p95_latency_ms", F.lit(None).cast("long"))
            .withColumn("errors_json", F.lit(None).cast("string"))
            .withColumn("skipped_reason", F.lit(skipped_reason))
        )
        return _conform(_lineage_columns(stub, as_of=as_of, **lineage), CRAWL_RELIABILITY_DAILY)

    daily = audit_df.groupBy("marketplace", "observed_date").agg(
        F.count(F.lit(1)).cast("long").alias("requests"),
        F.sum(F.when(F.col("succeeded"), 1).otherwise(0)).cast("long").alias("succeeded"),
        F.sum(F.when(F.col("succeeded"), 0).otherwise(1)).cast("long").alias("failed"),
        F.expr("percentile_approx(latency_ms, 0.5, 10000)").cast("long").alias("p50_latency_ms"),
        F.expr("percentile_approx(latency_ms, 0.95, 10000)").cast("long").alias("p95_latency_ms"),
        F.to_json(
            F.map_from_entries(
                F.collect_list(F.struct(F.col("failure_kind"), F.lit(1).alias("count")))
            )
        ).alias("errors_json"),
    )
    rated = daily.withColumn(
        # Decimal, not a float ratio: this number ends up in the report.
        "success_rate",
        F.when(
            F.col("requests") > 0,
            (F.col("succeeded").cast("decimal(38,6)") / F.col("requests").cast("decimal(38,6)")),
        ).cast("decimal(38,6)"),
    ).withColumn("skipped_reason", F.lit(None).cast("string"))
    return _conform(_lineage_columns(rated, as_of=as_of, **lineage), CRAWL_RELIABILITY_DAILY)


def evaluate_assertions(
    *,
    offer_current,
    offer_dimension,
    price_history,
    observations_deduplicated: int,
) -> tuple[AssertionResult, ...]:
    """The three structural assertions that must hold before publish."""
    from pyspark.sql import functions as F

    duplicate_offers = (
        offer_current.groupBy("offer_id").count().filter(F.col("count") > 1).count()
    )
    duplicate_keys = (
        offer_dimension.groupBy("marketplace", "platform_listing_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    history_total = price_history.agg(
        F.coalesce(F.sum("observation_count"), F.lit(0)).alias("total")
    ).collect()[0]["total"]
    return (
        AssertionResult(
            name="offer_current_unique",
            observed_value=f"{duplicate_offers} offer_id(s) with more than one row",
            expectation="at most one offer_current row per offer_id",
            passed=duplicate_offers == 0,
        ),
        AssertionResult(
            name="offer_dimension_unique",
            observed_value=f"{duplicate_keys} duplicate (marketplace, platform_listing_id) key(s)",
            expectation="offer dimension has no duplicate marketplace/listing key",
            passed=duplicate_keys == 0,
        ),
        AssertionResult(
            name="daily_counts_reconcile",
            observed_value=f"{history_total} daily observations vs {observations_deduplicated} silver rows",
            expectation="sum(observation_count) equals deduplicated Silver row count",
            passed=int(history_total) == int(observations_deduplicated),
        ),
    )


def build_spark(app_name: str = "marketplace-gold-job"):
    from pyspark.sql import SparkSession

    from config.settings import SPARK_MASTER_URL
    from config.storage import spark_hadoop_options

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    for key, value in spark_hadoop_options().items():
        builder = builder.config(key, value)
    if SPARK_MASTER_URL:
        builder = builder.master(SPARK_MASTER_URL)
    return builder.getOrCreate()


def run_marketplace_gold(
    *,
    as_of: datetime,
    window_days: int = GOLD_DEFAULT_WINDOW_DAYS,
    marketplaces: Sequence[str] | None = None,
    publish: bool = False,
    spark=None,
    audit_loader: Callable[..., Any] | None = None,
    connect: Callable[[], Any] | None = None,
    clock: Callable[[], datetime] | None = None,
    rule_version: str = GOLD_RULE_VERSION,
) -> GoldRunReport:
    """Build the marketplace Gold marts for one reproducible window."""
    from batch_layer.marketplace_silver_reader import (
        compact_observations,
        count_quarantined,
        read_silver_observations,
    )

    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    if not isinstance(window_days, int) or isinstance(window_days, bool) or window_days < 1:
        raise ValueError("window_days must be a positive int")
    clock = clock or (lambda: datetime.now(timezone.utc))
    started_at = clock()
    gold_run_id = make_gold_run_id(as_of, window_days, rule_version)
    lineage = {"gold_run_id": gold_run_id, "rule_version": rule_version}
    thresholds = default_freshness_thresholds()
    end_date = as_of.date()
    start_date = end_date - timedelta(days=window_days - 1)
    codes = list(marketplaces or ["tiki"])
    owned_spark = spark is None
    spark = spark or build_spark()
    mart_row_counts: dict[str, int] = {}
    skipped: dict[str, str] = {}
    stage = STAGE_READ

    def failure(exc: Exception) -> GoldRunReport:
        return GoldRunReport(
            gold_run_id=gold_run_id,
            as_of=as_of,
            window_days=window_days,
            rule_version=rule_version,
            started_at=started_at,
            completed_at=clock(),
            observations_read=0,
            observations_deduplicated=0,
            quarantined_rows=0,
            mart_row_counts=mart_row_counts,
            skipped_marts=skipped,
            status=STATUS_FAILED,
            failure_stage=stage,
        )

    try:
        observations, read_report = read_silver_observations(
            spark, marketplaces=codes, start_date=start_date, end_date=end_date
        )
        quarantined = count_quarantined(spark, start_date=start_date, end_date=end_date)

        stage = STAGE_COMPACT
        compact_observations(observations)

        stage = STAGE_DIMENSIONS
        offer_dimension = build_offer_dimension(observations)
        seller_dimension = build_seller_dimension(observations)
        offer_dimension.write.mode("overwrite").option(
            "partitionOverwriteMode", "dynamic"
        ).partitionBy("marketplace").parquet(data_lake_uri("silver", SILVER_OFFERS_DATASET))
        seller_dimension.write.mode("overwrite").option(
            "partitionOverwriteMode", "dynamic"
        ).partitionBy("marketplace").parquet(data_lake_uri("silver", SILVER_SELLERS_DATASET))

        stage = STAGE_MARTS
        marts = {
            OFFER_CURRENT.name: build_offer_current(
                observations, as_of=as_of, thresholds=thresholds, **lineage
            ),
            OFFER_FRESHNESS.name: build_offer_freshness(
                observations, as_of=as_of, thresholds=thresholds, **lineage
            ),
            OFFER_PRICE_HISTORY_DAILY.name: build_price_history_daily(
                observations, as_of=as_of, **lineage
            ),
            OFFER_CHANGE_DAILY.name: build_offer_change_daily(
                observations, as_of=as_of, **lineage
            ),
            CATEGORY_PRICE_DAILY.name: build_category_price_daily(
                observations, as_of=as_of, **lineage
            ),
            COUNTER_DELTA_DAILY.name: build_counter_delta_daily(
                observations, as_of=as_of, **lineage
            ),
            SOURCE_COVERAGE_DAILY.name: build_source_coverage_daily(
                observations,
                as_of=as_of,
                quarantined=quarantined,
                thresholds=thresholds,
                window_days=window_days,
                **lineage,
            ),
        }

        audit_df = None
        skipped_reason = None
        if audit_loader is None:
            skipped_reason = "no Phase 3 audit loader supplied"
        else:
            try:
                audit_df = audit_loader(spark, start_date=start_date, end_date=end_date)
            except Exception as exc:
                skipped_reason = f"Phase 3 audit source unavailable: {exc}"
        if skipped_reason is not None:
            skipped[CRAWL_RELIABILITY_DAILY.name] = skipped_reason
            logger.warning("crawl reliability mart skipped: %s", skipped_reason)
        marts[CRAWL_RELIABILITY_DAILY.name] = build_crawl_reliability_daily(
            audit_df if audit_df is not None else observations,
            as_of=as_of,
            skipped_reason=skipped_reason,
            **lineage,
        )

        for spec in MARKETPLACE_MART_SPECS:
            dataframe = marts[spec.name]
            target = gold_dataset_path(spec, gold_run_id=gold_run_id)
            writer = dataframe.write.mode("overwrite")
            if spec.partition_by:
                writer = writer.partitionBy(*spec.partition_by)
            writer.parquet(target)
            mart_row_counts[spec.name] = dataframe.count()

        stage = STAGE_ASSERT
        assertions = evaluate_assertions(
            offer_current=marts[OFFER_CURRENT.name],
            offer_dimension=offer_dimension,
            price_history=marts[OFFER_PRICE_HISTORY_DAILY.name],
            observations_deduplicated=read_report.rows_deduplicated,
        )
        failures = [item for item in assertions if not item.passed]

        report = GoldRunReport(
            gold_run_id=gold_run_id,
            as_of=as_of,
            window_days=window_days,
            rule_version=rule_version,
            started_at=started_at,
            completed_at=clock(),
            observations_read=read_report.rows_read,
            observations_deduplicated=read_report.rows_deduplicated,
            quarantined_rows=quarantined,
            mart_row_counts=mart_row_counts,
            assertions=assertions,
            skipped_marts=skipped,
            published=False,
            status=STATUS_SUCCEEDED if not failures else STATUS_FAILED,
            failure_stage=None if not failures else STAGE_ASSERT,
        )
        if failures:
            raise GoldAssertionFailed(failures)

        if publish:
            stage = STAGE_PUBLISH
            if connect is None:
                raise ValueError("publish=True requires a connect callable")
            from batch_layer.marketplace_postgres_cache import publish_marts

            publish_marts(
                gold_run_id=gold_run_id,
                connect=connect,
                run_row=report.run_row(),
                assertions=[item.as_row(rule_version) for item in assertions],
            )
            report = GoldRunReport(**{**report.__dict__, "published": True})
        return report
    except GoldAssertionFailed:
        raise
    except Exception as exc:
        logger.exception("marketplace gold run failed at stage %s", stage)
        failure(exc)
        raise
    finally:
        if owned_spark:
            spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Marketplace Gold temporal warehouse")
    parser.add_argument(
        "--as-of",
        required=True,
        help="UTC instant the run is computed against, ISO-8601 (e.g. 2026-09-08T00:00:00Z)",
    )
    parser.add_argument("--window-days", type=int, default=GOLD_DEFAULT_WINDOW_DAYS)
    parser.add_argument("--marketplace", action="append", dest="marketplaces")
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Refresh the PostgreSQL BI cache after every structural assertion passes.",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    as_of = datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
    if as_of.tzinfo is None:
        raise SystemExit("--as-of must include a timezone offset")

    connect = None
    if args.publish:
        import psycopg2

        from config.settings import POSTGRES_URL

        def connect():  # type: ignore[misc]
            return psycopg2.connect(POSTGRES_URL)

    report = run_marketplace_gold(
        as_of=as_of,
        window_days=args.window_days,
        marketplaces=args.marketplaces,
        publish=args.publish,
        connect=connect,
    )
    logger.info(
        "gold run %s: %d observations -> %s (published=%s)",
        report.gold_run_id,
        report.observations_deduplicated,
        report.mart_row_counts,
        report.published,
    )


if __name__ == "__main__":
    main()
