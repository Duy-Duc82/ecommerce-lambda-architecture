"""Pure Spark transformations for the nine marketplace temporal marts."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from config.counter_semantics import counter_semantic
from config.settings import MARKETPLACE_PERCENTILE_ACCURACY

if TYPE_CHECKING:
    from batch_layer.marketplace_warehouse import MarketplaceBatchContext

MONEY = "decimal(38,6)"
UNKNOWN_CATEGORY = "__UNKNOWN__"


def _ordered(df: DataFrame, partition: list[str] | str) -> Window:
    return Window.partitionBy(partition).orderBy(F.col("observed_at"), F.col("fetched_at"), F.col("observation_id"))


def _latest(df: DataFrame, partition: list[str] | str) -> DataFrame:
    # row_number() = 1 takes the first row of the window, so the ordering must
    # be descending here. _ordered() stays ascending because lag() and the
    # _first markers depend on it.
    newest = Window.partitionBy(partition).orderBy(F.col("observed_at").desc(), F.col("fetched_at").desc(), F.col("observation_id").desc())
    return df.withColumn("_rn", F.row_number().over(newest)).filter("_rn = 1").drop("_rn")


def build_offer_current(observations: DataFrame) -> DataFrame:
    latest = _latest(observations, "offer_id")
    first = observations.groupBy("offer_id").agg(F.min("observed_at").alias("first_seen_at"))
    # The envelope carries its own first_seen_at; the mart's is the earliest
    # observed_at we actually hold, so drop the envelope column before the
    # join rather than leaving two columns of that name to resolve.
    return latest.drop("first_seen_at").join(first, "offer_id").select("offer_id", "marketplace", "marketplace_id", "platform_listing_id", "seller_id", "product_title", "brand", "category_path", "source_url", "currency", "active_status", "first_seen_at", F.col("observed_at").alias("last_seen_at"), F.col("observation_id").alias("current_observation_id"), "observed_at", "fetched_at", "current_price", "list_price", "shipping_price", "rating_value", "rating_scale", "rating_count", "review_count", "sold_count", "availability", "ranking_position", "raw_uri", "raw_sha256", "adapter_version", "crawl_run_id")


def build_seller_current(observations: DataFrame) -> DataFrame:
    return (observations.filter(F.col("seller_id").isNotNull())
            .groupBy("seller_id", "marketplace", "marketplace_id")
            .agg(F.min("observed_at").alias("first_seen_at"), F.max("observed_at").alias("last_seen_at"), F.countDistinct("offer_id").alias("observed_offer_count")))


def build_offer_price_history_daily(observations: DataFrame) -> DataFrame:
    ordered = observations.withColumn("observed_date", F.to_date("observed_at"))
    w = _ordered(ordered, ["marketplace", "offer_id", "observed_date", "currency"])
    marked = ordered.withColumn("_first", F.row_number().over(w)).withColumn("_last", F.row_number().over(w.orderBy(F.col("observed_at").desc(), F.col("fetched_at").desc(), F.col("observation_id").desc())))
    return marked.groupBy("marketplace", "offer_id", "observed_date", "currency").agg(
        F.min(F.when(F.col("_first") == 1, F.col("observed_at"))).alias("first_observed_at"), F.max(F.when(F.col("_last") == 1, F.col("observed_at"))).alias("last_observed_at"),
        F.min(F.when(F.col("_first") == 1, F.col("current_price"))).cast(MONEY).alias("first_price"), F.min(F.when(F.col("_last") == 1, F.col("current_price"))).cast(MONEY).alias("last_price"),
        F.min("current_price").cast(MONEY).alias("min_price"), F.max("current_price").cast(MONEY).alias("max_price"), F.avg("current_price").cast(MONEY).alias("avg_price"),
        F.min(F.when(F.col("_first") == 1, F.col("list_price"))).cast(MONEY).alias("first_list_price"), F.min(F.when(F.col("_last") == 1, F.col("list_price"))).cast(MONEY).alias("last_list_price"), F.count("observation_id").alias("observation_count"), F.countDistinct("current_price").alias("distinct_price_count"), F.min(F.when(F.col("_last") == 1, F.col("availability"))).alias("last_availability"), F.min(F.when(F.col("_last") == 1, F.col("observation_id"))).alias("last_observation_id"))


def build_offer_change_daily(observations: DataFrame) -> DataFrame:
    w = _ordered(observations, ["marketplace", "offer_id"])
    cols = ["current_price", "rating_value", "rating_count", "review_count", "sold_count", "availability"]
    prior = observations
    for col in cols: prior = prior.withColumn(f"prev_{col}", F.lag(col).over(w))
    changed_price = ~F.col("current_price").eqNullSafe(F.col("prev_current_price"))
    changed_rating = ~(F.col("rating_value").eqNullSafe(F.col("prev_rating_value")) & F.col("rating_count").eqNullSafe(F.col("prev_rating_count")))
    changed_counter = ~(F.col("review_count").eqNullSafe(F.col("prev_review_count")) & F.col("sold_count").eqNullSafe(F.col("prev_sold_count")))
    changed_avail = (~F.col("availability").eqNullSafe(F.col("prev_availability"))) & (F.col("availability") != "UNKNOWN") & (F.col("prev_availability") != "UNKNOWN")
    real = F.col("prev_current_price").isNotNull() | F.col("prev_rating_value").isNotNull() | F.col("prev_rating_count").isNotNull() | F.col("prev_review_count").isNotNull() | F.col("prev_sold_count").isNotNull() | F.col("prev_availability").isNotNull()
    day = prior.withColumn("observed_date", F.to_date("observed_at"))
    return day.groupBy("marketplace", "offer_id", "observed_date", "currency").agg(
        F.sum(F.when(real, 1).otherwise(0)).alias("transition_count"), F.sum(F.when(real & changed_price, 1).otherwise(0)).alias("price_change_count"), F.sum(F.when(real & changed_price & (F.col("current_price") < F.col("prev_current_price")), 1).otherwise(0)).alias("price_drop_count"), F.sum(F.when(real & changed_price & (F.col("current_price") > F.col("prev_current_price")), 1).otherwise(0)).alias("price_increase_count"), F.sum(F.when(real & changed_price, F.abs(F.col("current_price") - F.col("prev_current_price"))).otherwise(F.lit(0).cast(MONEY))).cast(MONEY).alias("absolute_price_change_sum"), F.sum(F.when(real & changed_price, F.col("current_price") - F.col("prev_current_price")).otherwise(F.lit(0).cast(MONEY))).cast(MONEY).alias("signed_price_change_sum"), F.max(F.when(real & (F.col("prev_current_price") > F.col("current_price")), F.col("prev_current_price") - F.col("current_price"))).cast(MONEY).alias("max_price_drop"), F.max(F.when(real & (F.col("current_price") > F.col("prev_current_price")), F.col("current_price") - F.col("prev_current_price"))).cast(MONEY).alias("max_price_increase"), F.sum(F.when(real & changed_rating, 1).otherwise(0)).alias("rating_change_count"), F.sum(F.when(real & changed_counter, 1).otherwise(0)).alias("counter_change_count"), F.sum(F.when(real & changed_avail, 1).otherwise(0)).alias("availability_change_count"))


def build_offer_freshness(observations: DataFrame, *, as_of: datetime, stale_after_seconds: int, rule_version: str) -> DataFrame:
    if as_of.tzinfo is None or as_of.utcoffset() is None: raise ValueError("as_of must be timezone-aware")
    if stale_after_seconds <= 0: raise ValueError("stale_after_seconds must be positive")
    if not rule_version.strip(): raise ValueError("rule_version is required")
    latest = _latest(observations, "offer_id")
    as_of_utc = as_of.astimezone(timezone.utc)
    age = F.col("as_of").cast("long") - F.col("last_observed_at").cast("long")
    return (latest.select(F.lit(as_of_utc).cast("timestamp").alias("as_of"), "marketplace", "offer_id", F.col("observation_id").alias("last_observation_id"), F.col("observed_at").alias("last_observed_at"))
            .withColumn("age_seconds", age).withColumn("freshness_status", F.when(F.col("age_seconds") < 0, "FUTURE").when(F.col("age_seconds") <= stale_after_seconds, "FRESH").otherwise("STALE"))
            .withColumn("stale_after_seconds", F.lit(stale_after_seconds).cast("long")).withColumn("freshness_rule_version", F.lit(rule_version)))


def build_category_price_daily(observations: DataFrame) -> DataFrame:
    return (observations.withColumn("category_path", F.coalesce("category_path", F.lit(UNKNOWN_CATEGORY))).withColumn("observed_date", F.to_date("observed_at"))
            .groupBy("marketplace", "category_path", "observed_date", "currency").agg(F.countDistinct("offer_id").alias("observed_offer_count"), F.count("observation_id").alias("observation_count"), F.min("current_price").cast(MONEY).alias("min_price"), F.percentile_approx("current_price", 0.25, MARKETPLACE_PERCENTILE_ACCURACY).cast(MONEY).alias("p25_price"), F.percentile_approx("current_price", 0.5, MARKETPLACE_PERCENTILE_ACCURACY).cast(MONEY).alias("median_price"), F.percentile_approx("current_price", 0.75, MARKETPLACE_PERCENTILE_ACCURACY).cast(MONEY).alias("p75_price"), F.max("current_price").cast(MONEY).alias("max_price"), F.avg("current_price").cast(MONEY).alias("avg_price")))


def _audit_col(df: DataFrame, name: str, default=None):
    if name in df.columns:
        return F.col(name)
    return default if isinstance(default, F.Column) else F.lit(default)


def _with_marketplace(attempts: DataFrame, runs: DataFrame) -> DataFrame:
    if "marketplace" in attempts.columns or "marketplace_code" in attempts.columns:
        return attempts.withColumn("_marketplace", F.coalesce(_audit_col(attempts, "marketplace"), _audit_col(attempts, "marketplace_code")))
    if "crawl_run_id" in attempts.columns and "crawl_run_id" in runs.columns:
        run_marketplace = "marketplace" if "marketplace" in runs.columns else "marketplace_code"
        return attempts.join(runs.select("crawl_run_id", F.col(run_marketplace).alias("_marketplace")), "crawl_run_id", "left")
    return attempts.withColumn("_marketplace", F.lit("unknown"))


def build_source_coverage_daily(observations: DataFrame, attempts: DataFrame, runs: DataFrame, *, as_of: datetime, stale_after_seconds: int, rule_version: str) -> DataFrame:
    if as_of.tzinfo is None or as_of.utcoffset() is None: raise ValueError("as_of must be timezone-aware")
    if stale_after_seconds <= 0: raise ValueError("stale_after_seconds must be positive")
    max_date = observations.select(F.max(F.to_date("observed_at")).alias("d")).collect()[0]["d"]
    end_date = min(max_date, as_of.astimezone(timezone.utc).date()) if max_date else as_of.date()
    first = observations.groupBy("marketplace", "offer_id").agg(F.min(F.to_date("observed_at")).alias("first_date"))
    calendar = first.withColumn("observed_date", F.explode(F.sequence("first_date", F.lit(end_date)))).withColumn("eligible", F.lit(1))
    # Distinct key names: this frame is joined onto a chain that already
    # carries marketplace/offer_id twice, and an unqualified name there cannot
    # be resolved.
    observed_days = observations.select(F.col("marketplace").alias("_obs_marketplace"), F.col("offer_id").alias("_obs_offer_id"), F.to_date("observed_at").alias("_obs_date")).distinct().withColumn("observed", F.lit(1))
    # Evaluate freshness at day end, except for the current partial UTC day.
    eval_time = F.least(F.to_timestamp(F.date_add(calendar.observed_date, 1)), F.lit(as_of.astimezone(timezone.utc)).cast("timestamp"))
    snapshots = calendar.join(observations, (calendar.marketplace == observations.marketplace) & (calendar.offer_id == observations.offer_id) & (F.col("observed_at") <= eval_time), "left").withColumn("_rn", F.row_number().over(Window.partitionBy(calendar.marketplace, calendar.offer_id, calendar.observed_date).orderBy(F.col("observed_at").desc(), F.col("fetched_at").desc(), F.col("observation_id").desc()))).filter("_rn = 1")
    snapshots = snapshots.withColumn("_age", eval_time.cast("long") - F.col("observed_at").cast("long"))
    # observed_days has to reach the same frame the counts are taken from, or
    # the "observed" flag is not in scope where observed_offer_count reads it.
    eligible_snapshots = (snapshots.filter(F.col("active_status").isNull() | (F.col("active_status") != "INACTIVE"))
                          .join(observed_days, (calendar.marketplace == F.col("_obs_marketplace")) & (calendar.offer_id == F.col("_obs_offer_id")) & (calendar.observed_date == F.col("_obs_date")), "left"))
    daily = eligible_snapshots.groupBy(calendar.marketplace.alias("marketplace"), calendar.observed_date.alias("observed_date")).agg(F.countDistinct(calendar.offer_id).alias("eligible_offer_count"), F.countDistinct(F.when(F.col("observed") == 1, calendar.offer_id)).alias("observed_offer_count"))
    freshness = eligible_snapshots.groupBy(calendar.marketplace.alias("marketplace"), calendar.observed_date.alias("observed_date")).agg(F.sum(F.when(F.col("_age").between(0, stale_after_seconds), 1).otherwise(0)).alias("fresh_offer_count"), F.sum(F.when(F.col("_age") > stale_after_seconds, 1).otherwise(0)).alias("stale_offer_count"))
    obs_counts = observations.withColumn("observed_date", F.to_date("observed_at")).groupBy("marketplace", "observed_date").agg(F.count("observation_id").alias("observation_count"))
    audit_input = _with_marketplace(attempts, runs)
    audit = audit_input.groupBy(F.col("_marketplace").alias("marketplace"), F.to_date(_audit_col(audit_input, "request_date", _audit_col(audit_input, "started_at"))).alias("observed_date")).agg(F.sum(_audit_col(audit_input, "parsed_count", 0)).alias("parsed_count"), F.sum(_audit_col(audit_input, "rejected_count", 0)).alias("rejected_count"))
    return (daily.join(freshness, ["marketplace", "observed_date"], "left").join(obs_counts, ["marketplace", "observed_date"], "left").join(audit, ["marketplace", "observed_date"], "left").fillna(0, subset=["observed_offer_count", "fresh_offer_count", "stale_offer_count", "observation_count", "parsed_count", "rejected_count"]).withColumn("missing_offer_count", F.col("eligible_offer_count") - F.col("observed_offer_count")).withColumn("coverage_rate", F.when(F.col("eligible_offer_count") > 0, F.col("observed_offer_count") / F.col("eligible_offer_count")).otherwise(F.lit(None).cast("double"))).withColumn("rejection_rate", F.when(F.col("parsed_count") + F.col("rejected_count") > 0, F.col("rejected_count") / (F.col("parsed_count") + F.col("rejected_count"))).otherwise(F.lit(None).cast("double"))).withColumn("freshness_rule_version", F.lit(rule_version)).select("marketplace", "observed_date", "eligible_offer_count", "observed_offer_count", "missing_offer_count", "fresh_offer_count", "stale_offer_count", "observation_count", "parsed_count", "rejected_count", "coverage_rate", "rejection_rate", "freshness_rule_version"))


def build_crawl_reliability_daily(attempts: DataFrame, runs: DataFrame) -> DataFrame:
    attempts = _with_marketplace(attempts, runs)
    marketplace = F.col("_marketplace")
    date = F.to_date(_audit_col(attempts, "request_date", _audit_col(attempts, "started_at")))
    status = F.upper(_audit_col(attempts, "status", "UNKNOWN"))
    latency = _audit_col(attempts, "latency_ms", 0)
    error_kind = F.upper(_audit_col(attempts, "error_kind", ""))
    def error_count(column: str, token: str):
        if column in attempts.columns: return F.col(column)
        return F.when(error_kind.contains(token), 1).otherwise(0)
    selected = attempts.select(marketplace.alias("marketplace"), date.alias("request_date"), status.alias("_status"), latency.alias("_latency"), _audit_col(attempts, "raw_bytes", 0).alias("raw_bytes"), _audit_col(attempts, "parsed_count", 0).alias("parsed_count"), _audit_col(attempts, "rejected_count", 0).alias("rejected_count"), error_count("rate_limited_count", "RATE").alias("rate_limited_count"), error_count("transport_error_count", "TRANSPORT").alias("transport_error_count"), error_count("server_error_count", "SERVER").alias("server_error_count"), error_count("parse_error_count", "PARSE").alias("parse_error_count"), error_count("validation_error_count", "VALIDATION").alias("validation_error_count"))
    return selected.groupBy("marketplace", "request_date").agg(F.count("_status").alias("request_count"), F.sum(F.when(F.col("_status").isin("SUCCEEDED", "PARTIAL"), 1).otherwise(0)).alias("succeeded_count"), F.sum(F.when(F.col("_status") == "FAILED", 1).otherwise(0)).alias("failed_count"), F.avg("_latency").alias("avg_latency_ms"), F.percentile_approx("_latency", 0.95, 10000).alias("p95_latency_ms"), *[F.sum(x).alias(x) for x in ("raw_bytes", "parsed_count", "rejected_count", "rate_limited_count", "transport_error_count", "server_error_count", "parse_error_count", "validation_error_count")]).withColumn("success_rate", F.when(F.col("request_count") > 0, F.col("succeeded_count") / F.col("request_count")).otherwise(F.lit(None).cast("double")))


def build_counter_transitions(observations: DataFrame, *, max_gap_seconds: int, rule_version: str) -> DataFrame:
    """Emit one evidence row per observation/counter transition."""
    order = _ordered(observations, ["marketplace", "offer_id"])
    outputs = []
    marketplaces = [r[0] for r in observations.select("marketplace").distinct().collect()]
    for field in ("rating_count", "review_count", "sold_count"):
        semantic_column = F.col("semantic_version") if "semantic_version" in observations.columns else F.col("adapter_version")
        base = (observations.select("marketplace", "offer_id", "observed_at", "fetched_at", "observation_id", "adapter_version", semantic_column.alias("semantic_version"), F.col(field).alias("current_value"))
                .withColumn("previous_value", F.lag("current_value").over(order))
                .withColumn("previous_observed_at", F.lag("observed_at").over(order))
                .withColumn("previous_adapter_version", F.lag("adapter_version").over(order))
                .withColumn("previous_semantic_version", F.lag("semantic_version").over(order))
                .withColumn("counter_name", F.lit(field))
                .withColumn("counter_rule_version", F.lit(rule_version)))
        registered = F.lit(False)
        for marketplace in marketplaces:
            registered = registered | ((F.lower(F.col("marketplace")) == marketplace.lower()) & F.lit(counter_semantic(marketplace, field) is not None))
        elapsed = F.col("observed_at").cast("long") - F.col("previous_observed_at").cast("long")
        delta = F.col("current_value") - F.col("previous_value")
        has_previous = F.col("previous_observed_at").isNotNull()
        reason = (F.when(~has_previous, F.lit(None).cast("string"))
                  .when(F.col("previous_value").isNull() | F.col("current_value").isNull(), "MISSING_VALUE")
                  .when(~registered, "SEMANTICS_UNREGISTERED")
                  .when((F.col("adapter_version") != F.col("previous_adapter_version")) | (F.col("semantic_version") != F.col("previous_semantic_version")), "SEMANTICS_VERSION_CHANGED")
                  .when(elapsed <= 0, "NON_POSITIVE_INTERVAL")
                  .when(elapsed > max_gap_seconds, "GAP_TOO_LONG")
                  .when(delta < 0, "COUNTER_DECREASED").otherwise("VALID"))
        outputs.append(base.withColumn("elapsed_seconds", elapsed).withColumn("raw_delta", F.when(has_previous, delta))
                       .withColumn("valid_delta", F.when(reason == "VALID", delta))
                       .withColumn("valid_transition", reason == "VALID")
                       .withColumn("invalid_reason", reason))
    return outputs[0].unionByName(outputs[1]).unionByName(outputs[2])


def build_counter_delta_daily(transitions: DataFrame) -> DataFrame:
    order = Window.partitionBy("marketplace", "offer_id", "observed_date", "counter_name").orderBy(F.col("observed_at"), F.col("fetched_at"), F.col("observation_id"))
    marked = transitions.withColumn("observed_date", F.to_date("observed_at")).withColumn("_first", F.row_number().over(order)).withColumn("_last", F.row_number().over(order.orderBy(F.col("observed_at").desc(), F.col("fetched_at").desc(), F.col("observation_id").desc())))
    return (marked.groupBy("marketplace", "offer_id", "observed_date", "counter_name").agg(
        F.min(F.when(F.col("_first") == 1, F.col("current_value"))).alias("first_value"), F.min(F.when(F.col("_last") == 1, F.col("current_value"))).alias("last_value"), F.sum("raw_delta").alias("raw_delta_sum"), F.coalesce(F.sum("valid_delta"), F.lit(0)).alias("valid_delta_sum"), F.sum(F.when(F.col("valid_transition"), 1).otherwise(0)).alias("valid_transition_count"), F.sum(F.when(F.col("previous_observed_at").isNotNull() & ~F.col("valid_transition"), 1).otherwise(0)).alias("invalid_transition_count"), F.sum(F.when(F.col("valid_transition"), F.col("elapsed_seconds")).otherwise(0)).alias("elapsed_seconds_valid"), F.sort_array(F.collect_set(F.when(F.col("previous_observed_at").isNotNull() & ~F.col("valid_transition"), F.col("invalid_reason")))).alias("_reasons"), F.max(F.when(F.col("previous_observed_at").isNotNull() & ~F.col("valid_transition"), F.lit(True)).otherwise(F.lit(False))).alias("counter_reset_or_invalid"), F.min("counter_rule_version").alias("counter_rule_version"))
        .withColumn("velocity_proxy_per_hour", F.when(F.col("elapsed_seconds_valid") > 0, F.col("valid_delta_sum") / F.col("elapsed_seconds_valid") * 3600).otherwise(F.lit(None).cast("double")))
        .withColumn("invalid_reasons_json", F.to_json(F.col("_reasons"))).drop("_reasons"))


def build_marketplace_marts(observations: DataFrame, attempts: DataFrame, runs: DataFrame, context: "MarketplaceBatchContext") -> dict[str, DataFrame]:
    return {"offer_current": build_offer_current(observations), "seller_current": build_seller_current(observations), "offer_price_history_daily": build_offer_price_history_daily(observations), "offer_change_daily": build_offer_change_daily(observations), "offer_freshness": build_offer_freshness(observations, as_of=context.as_of, stale_after_seconds=context.freshness_seconds, rule_version=context.freshness_rule_version), "category_price_daily": build_category_price_daily(observations), "source_coverage_daily": build_source_coverage_daily(observations, attempts, runs, as_of=context.as_of, stale_after_seconds=context.freshness_seconds, rule_version=context.freshness_rule_version), "crawl_reliability_daily": build_crawl_reliability_daily(attempts, runs), "counter_delta_daily": build_counter_delta_daily(build_counter_transitions(observations, max_gap_seconds=context.counter_max_gap_seconds, rule_version=context.counter_rule_version))}
