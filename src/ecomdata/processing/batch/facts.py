"""Facts of the marketplace star, built from Silver and the dimensions.

``docs/DATA_MODEL.md`` section 4.7. Every builder is a pure function of its
input frames, like the dimension builders, so the same Silver and the same
dimensions give the same rows and keys on any run.

The atomic fact is ``fact_offer_observation``: one row per sighting, the
sponsored ones included and marked. The other facts are derived from the same
keyed sightings (``keyed_observations``) in one pass, so a key means the same
thing on every fact and no fact reads another's output back. The price facts
and the counter fact judge the **organic** sightings only, as the marts do: a
sponsored placement rotates between loads and would distort an offer's price
history. ``fact_category_page_daily`` counts every sighting a page showed and
judges prices over its organic offers.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ecomdata.domain.datasets import dataset
from ecomdata.domain.ordering import ORDER_COLUMNS
from ecomdata.processing.batch.dimensions import (
    brand_key,
    category_key,
    has_discount,
    listing_flags_key,
    marketplace_key,
    offer_key,
    page_category_id,
    page_category_key,
    seller_key,
)
from ecomdata.processing.batch.keys import UNKNOWN_KEY, date_key, key_or_unknown
from ecomdata.processing.batch.marts import build_counter_delta_daily, build_counter_transitions
from ecomdata.processing.batch.star_inputs import with_listing_columns

if TYPE_CHECKING:
    from ecomdata.processing.batch.context import MarketplaceBatchContext

__all__ = [
    "FactRules",
    "build_fact_category_page_daily",
    "build_fact_counter_daily",
    "build_fact_crawl_attempt",
    "build_fact_offer_observation",
    "build_fact_price_change",
    "build_fact_price_snapshot_daily",
    "build_marketplace_facts",
    "keyed_observations",
]

MONEY = "decimal(38,6)"
IN_STOCK = "IN_STOCK"


@dataclass(frozen=True)
class FactRules:
    """The rules a fact is judged by; ``from_context`` takes them from the batch context."""

    change_rule_version: str = "speed-rules.v1"
    large_drop_absolute: Decimal = Decimal("100000")
    large_drop_relative: Decimal = Decimal("0.20")
    counter_rule_version: str = "counter-rules.v1"
    counter_max_gap_seconds: int = 86400
    percentile_accuracy: int = 10000

    @classmethod
    def from_context(cls, context: MarketplaceBatchContext) -> FactRules:
        return cls(
            change_rule_version=context.change_rule_version,
            large_drop_absolute=context.large_drop_absolute,
            large_drop_relative=context.large_drop_relative,
            counter_rule_version=context.counter_rule_version,
            counter_max_gap_seconds=context.counter_max_gap_seconds,
            percentile_accuracy=context.percentile_accuracy,
        )


def _registry(name: str, frame: DataFrame) -> DataFrame:
    """``frame`` reduced to the registry's columns of ``name``, in the registry's order."""
    return frame.select(*dataset(name).columns)


def _organic(frame: DataFrame) -> DataFrame:
    return frame.filter(~F.coalesce(F.col("is_advertisement"), F.lit(False)))


def _first(column: str) -> F.Column:
    return F.min_by(column, F.struct(*ORDER_COLUMNS))


def _last(column: str) -> F.Column:
    return F.max_by(column, F.struct(*ORDER_COLUMNS))


def keyed_observations(observations: DataFrame, dim_listing: DataFrame) -> DataFrame:
    """The sightings with every foreign key a fact carries, and ``date_key``.

    ``dim_listing`` is ``dim_product_listing``: a sighting takes the version
    whose ``[valid_from, valid_to)`` holds its ``observed_at`` (section 4.6,
    rule 6), or ``-1`` when no version does.
    """
    sightings = with_listing_columns(observations)
    versions = dim_listing.filter(F.col("offer_key") != UNKNOWN_KEY).select(
        F.col("offer_key").alias("_v_offer"),
        F.col("listing_key").alias("_v_listing"),
        F.col("valid_from").alias("_v_from"),
        F.col("valid_to").alias("_v_to"),
    )
    keyed = (
        sightings.withColumn("offer_key", offer_key())
        .withColumn("marketplace_key", marketplace_key())
        .withColumn("seller_key", seller_key())
        .withColumn("brand_key", brand_key())
        .withColumn("category_key", category_key())
        .withColumn("page_category_key", page_category_key())
        .withColumn("listing_flags_key", listing_flags_key())
        .withColumn("date_key", date_key("observed_at"))
    )
    in_version = (
        (F.col("offer_key") == F.col("_v_offer"))
        & (F.col("observed_at") >= F.col("_v_from"))
        & (F.col("observed_at") < F.col("_v_to"))
    )
    return (
        keyed.join(versions, in_version, "left")
        .withColumn("listing_key", F.coalesce(F.col("_v_listing"), F.lit(UNKNOWN_KEY).cast("bigint")))
        .drop("_v_offer", "_v_listing", "_v_from", "_v_to")
    )


def build_fact_offer_observation(keyed: DataFrame) -> DataFrame:
    """One row per sighting (section 4.7); ``keyed`` is :func:`keyed_observations`."""
    return _registry("fact_offer_observation", keyed)


def build_fact_price_snapshot_daily(keyed: DataFrame) -> DataFrame:
    """One offer, one UTC day, one currency; the dimension keys are those of the day's last sighting."""
    return _registry(
        "fact_price_snapshot_daily",
        _organic(keyed)
        .groupBy("date_key", "offer_key", "currency")
        .agg(
            _last("listing_key").alias("listing_key"),
            _last("marketplace_key").alias("marketplace_key"),
            _last("seller_key").alias("seller_key"),
            _last("brand_key").alias("brand_key"),
            _last("category_key").alias("category_key"),
            _last("listing_flags_key").alias("listing_flags_key"),
            F.min("observed_at").alias("first_observed_at"),
            F.max("observed_at").alias("last_observed_at"),
            _last("observation_id").alias("last_observation_id"),
            _first("current_price").cast(MONEY).alias("open_price"),
            _last("current_price").cast(MONEY).alias("close_price"),
            F.min("current_price").cast(MONEY).alias("min_price"),
            F.max("current_price").cast(MONEY).alias("max_price"),
            F.avg("current_price").cast(MONEY).alias("avg_price"),
            _first("list_price").cast(MONEY).alias("first_list_price"),
            _last("list_price").cast(MONEY).alias("last_list_price"),
            F.count("observation_id").alias("observation_count"),
            F.countDistinct("current_price").alias("distinct_price_count"),
            F.avg(F.when(F.col("availability") == IN_STOCK, 1.0).otherwise(0.0)).alias("in_stock_ratio"),
        ),
    )


def build_fact_price_change(keyed: DataFrame, rules: FactRules) -> DataFrame:
    """One sighting whose price differs from the same offer's previous sighting.

    The previous sighting is looked up over the offer's whole history in the
    marts' order. A pair of sightings in two currencies is a change without a
    ratio and never a large drop.
    """
    history = Window.partitionBy("marketplace", "offer_id").orderBy(*ORDER_COLUMNS)
    sightings = _organic(keyed)
    for name, source in (
        ("previous_observation_id", "observation_id"),
        ("previous_observed_at", "observed_at"),
        ("previous_currency", "currency"),
        ("old_price", "current_price"),
    ):
        sightings = sightings.withColumn(name, F.lag(source).over(history))
    old = F.col("old_price").cast(MONEY)
    new = F.col("current_price").cast(MONEY)
    delta = new - old
    same_currency = F.col("currency").eqNullSafe(F.col("previous_currency"))
    drop = old - new
    large = (
        (new < old)
        & same_currency
        & ((drop >= F.lit(rules.large_drop_absolute).cast(MONEY))
            | ((old != 0) & (drop / old >= F.lit(rules.large_drop_relative).cast(MONEY))))
    )
    changed = sightings.filter(
        F.col("old_price").isNotNull() & F.col("current_price").isNotNull() & (F.col("old_price") != F.col("current_price"))
    )
    return _registry(
        "fact_price_change",
        changed.withColumn("changed_at", F.col("observed_at"))
        .withColumn("new_price", new)
        .withColumn("old_price", old)
        .withColumn("price_delta", delta)
        .withColumn("pct_change", F.when(same_currency & (old != 0), (delta / old).cast(MONEY)))
        .withColumn(
            "minutes_since_previous",
            (F.col("observed_at").cast("long") - F.col("previous_observed_at").cast("long")) / 60.0,
        )
        .withColumn("direction", F.when(delta > 0, "UP").otherwise("DOWN"))
        .withColumn("is_large_drop", F.coalesce(large, F.lit(False)))
        .withColumn("change_rule_version", F.lit(rules.change_rule_version)),
    )


def build_fact_counter_daily(keyed: DataFrame, rules: FactRules) -> DataFrame:
    """One offer, one UTC day, one public counter; the rules are the marts' (section 4.7, additivity)."""
    sightings = _organic(keyed)
    daily = build_counter_delta_daily(
        build_counter_transitions(
            sightings, max_gap_seconds=rules.counter_max_gap_seconds, rule_version=rules.counter_rule_version
        )
    )
    # A counter row carries the keys of the offer's last sighting that day.
    offers = (
        sightings.groupBy("marketplace", "offer_id", F.to_date("observed_at").alias("observed_date"))
        .agg(
            _last("listing_key").alias("listing_key"),
            _last("offer_key").alias("offer_key"),
            _last("marketplace_key").alias("marketplace_key"),
        )
    )
    return _registry(
        "fact_counter_daily",
        daily.join(offers, ["marketplace", "offer_id", "observed_date"]).withColumn(
            "date_key", date_key("observed_date")
        ),
    )


def build_fact_category_page_daily(
    keyed: DataFrame, listed_counts: DataFrame | None, *, percentile_accuracy: int = 10000
) -> DataFrame:
    """What a category page showed on a day, by the page's category (section 4.7).

    Counts cover every sighting the pages showed; prices and discounts are over
    organic offers, each contributing its last price of the day on the category's
    pages. ``listed_counts`` is ``StarInputs.listed_counts``; without it
    ``listed_product_count`` is NULL.
    """
    paged = keyed.filter(F.col("page_category_key") != UNKNOWN_KEY).withColumn(
        "category_key", F.col("page_category_key")
    )
    group = ["date_key", "category_key", "currency"]
    sightings = paged.groupBy(*group).agg(
        F.max("marketplace_key").alias("marketplace_key"),
        F.countDistinct("request_target", "request_page").alias("crawled_page_count"),
        F.count("observation_id").alias("observation_count"),
        F.countDistinct("offer_key").alias("observed_offer_count"),
        F.countDistinct(F.when(F.coalesce(F.col("is_advertisement"), F.lit(False)), F.col("offer_key"))).alias(
            "advertisement_count"
        ),
        F.countDistinct(F.when(F.col("seller_key") != UNKNOWN_KEY, F.col("seller_key"))).alias("seller_count"),
        F.countDistinct(F.when(F.col("brand_key") != UNKNOWN_KEY, F.col("brand_key"))).alias("brand_count"),
    )
    last_of_day = (
        _organic(paged.withColumn("_has_discount", has_discount()))
        .groupBy(*group, "offer_key")
        .agg(
            _last("current_price").alias("_price"),
            _last("discount_percent").alias("_discount_percent"),
            _last("availability").alias("_availability"),
            _last("_has_discount").alias("_discounted"),
        )
    )
    price = F.col("_price")
    prices = last_of_day.groupBy(*group).agg(
        F.min(price).cast(MONEY).alias("min_price"),
        F.percentile_approx(price, 0.25, percentile_accuracy).cast(MONEY).alias("p25_price"),
        F.percentile_approx(price, 0.5, percentile_accuracy).cast(MONEY).alias("median_price"),
        F.percentile_approx(price, 0.75, percentile_accuracy).cast(MONEY).alias("p75_price"),
        F.max(price).cast(MONEY).alias("max_price"),
        F.avg(price).cast(MONEY).alias("avg_price"),
        F.sum(F.when(F.col("_discounted"), 1).otherwise(0)).alias("discounted_offer_count"),
        F.count("offer_key").alias("_organic_offers"),
        F.percentile_approx(
            F.when(F.col("_discounted"), F.col("_discount_percent")), 0.5, percentile_accuracy
        ).cast(MONEY).alias("median_discount_percent"),
        F.avg(F.when(F.col("_availability") == IN_STOCK, 1.0).otherwise(0.0)).alias("in_stock_ratio"),
    )
    built = (
        sightings.join(prices, group, "left")
        .withColumn(
            "discounted_offer_ratio",
            F.when(F.col("_organic_offers") > 0, F.col("discounted_offer_count") / F.col("_organic_offers")),
        )
    )
    if listed_counts is None:
        built = built.withColumn("listed_product_count", F.lit(None).cast("bigint"))
    else:
        listed = listed_counts.select(
            key_or_unknown("marketplace", "category_id").alias("category_key"),
            date_key("observed_at").alias("date_key"),
            "listed_product_count",
            "observed_at",
        )
        listed = listed.groupBy("category_key", "date_key").agg(
            F.max_by("listed_product_count", "observed_at").alias("listed_product_count")
        )
        built = built.join(listed, ["category_key", "date_key"], "left")
    return _registry("fact_category_page_daily", built)


def build_fact_crawl_attempt(attempts: DataFrame) -> DataFrame:
    """One HTTP attempt to fetch one page (section 4.7).

    ``attempts`` is the audit's attempt table with the frontier's
    ``marketplace_code``, ``request_target`` and ``resource_type`` attached
    (``marts.attach_marketplace_code``); a column the audit lacks reads as
    NULL. A page that is not a category listing points at the unknown category.
    """
    present = set(attempts.columns)
    optional = [name for name in ("request_target", "resource_type", "marketplace_code") if name not in present]
    for name in optional:
        attempts = attempts.withColumn(name, F.lit(None).cast("string"))
    category = F.when(
        F.col("resource_type") == "LISTING_PAGE",
        key_or_unknown("marketplace_code", page_category_id("request_target")),
    ).otherwise(F.lit(UNKNOWN_KEY).cast("bigint"))
    return _registry(
        "fact_crawl_attempt",
        attempts.withColumn("date_key", date_key("started_at"))
        .withColumn("marketplace_key", key_or_unknown("marketplace_code"))
        .withColumn("category_key", category),
    )


def build_marketplace_facts(
    observations: DataFrame,
    dim_listing: DataFrame,
    listed_counts: DataFrame | None,
    attempts: DataFrame | None,
    rules: FactRules,
) -> dict[str, DataFrame]:
    """Every fact, in registry order.

    ``observations`` is ``StarInputs.observations`` (deduplicated, as of the
    run); ``dim_listing`` is the built ``dim_product_listing``; ``attempts`` is
    the crawl audit's attempts (``None`` leaves ``fact_crawl_attempt`` out).
    """
    keyed = keyed_observations(observations, dim_listing)
    facts = {
        "fact_offer_observation": build_fact_offer_observation(keyed),
        "fact_price_snapshot_daily": build_fact_price_snapshot_daily(keyed),
        "fact_price_change": build_fact_price_change(keyed, rules),
        "fact_counter_daily": build_fact_counter_daily(keyed, rules),
        "fact_category_page_daily": build_fact_category_page_daily(
            keyed, listed_counts, percentile_accuracy=rules.percentile_accuracy
        ),
    }
    if attempts is not None:
        facts["fact_crawl_attempt"] = build_fact_crawl_attempt(attempts)
    return facts
