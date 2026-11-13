"""Discount credibility per offer-day, and the category view of discounts (ADR-0009).

``build_discount_credibility_daily`` reads ``offer_price_history_daily`` (one
offer, one UTC day, one currency), like the anomaly mart, so the price it judges
is the price the price mart shows. For each row it takes the offer's own prior
history inside a calendar window (the *reference window*), derives:

- ``displayed_discount_percent``: what the page claims, from the list price
  and the price of the day's last sighting;
- ``reference_min_price``: the lowest price of the window (the Omnibus
  "lowest prior price"), with the window's median and highest list price;
- ``effective_discount_percent``: the reduction against that reference, which
  is what a buyer who watched the offer would have seen;
- ``claim_gap_percent``: displayed minus effective;

then applies the rules of ``domain/discount_rules.py`` as column expressions,
collects the reasons that fire, scores them and grades the row. The window is
calendar days before the judged day, excluding it, so today's price can never
be its own reference, and a gap in observation injects no fabricated price.

The optional Isolation Forest (scikit-learn) ranks the rows that display a
reduction by how unusual their feature vector is; it adds a rank next to the
rules and decides nothing. It is fitted on a deterministic sample on the
driver and applied in the executors with ``mapInPandas``; without
scikit-learn, or with too few rows, the ML columns are null and ``ml_method``
says NONE, and the mart is otherwise identical.

``build_discount_daily`` aggregates the sightings by category and day: how
many offers showed a reduction, how deep, and how many of them the rules
graded QUESTIONABLE or SUSPECT.
"""
from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from datetime import date
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructField, StructType

from ecomdata.domain import discount_rules as rules
from ecomdata.domain.discount_rules import DiscountRuleConfig

MONEY = "decimal(38,6)"
UNKNOWN_CATEGORY = "__UNKNOWN__"
EPOCH = date(1970, 1, 1)

CREDIBILITY_COLUMNS = (
    "marketplace", "offer_id", "observed_date", "currency",
    "evaluated_price", "list_price", "displayed_discount_percent",
    "reference_window_days", "reference_sample_size", "reference_min_price", "reference_median_price",
    "reference_max_list_price", "effective_discount_percent", "claim_gap_percent", "distinct_price_count",
    "credibility_status", "credibility_reasons_json", "rule_score",
    "ml_method", "ml_score", "ml_rank_percent", "discount_rule_version",
)

DISCOUNT_DAILY_COLUMNS = (
    "marketplace", "category_path", "observed_date", "currency",
    "observed_offer_count", "discounted_offer_count", "discounted_offer_ratio",
    "p25_discount_percent", "median_discount_percent", "p75_discount_percent", "max_discount_percent",
    "over_cap_offer_count", "questionable_offer_count", "suspect_offer_count",
    "legal_cap_percent", "discount_rule_version",
)

# The feature vector the Isolation Forest sees, in this order. Each one is a
# column of the mart (list_raised is the LIST_PRICE_RAISED reason as a number),
# so a rank can be read back against what produced it. The price level is
# deliberately not a feature: an expensive item is not an unusual reduction.
ML_FEATURES = (
    "displayed_discount_percent",
    "effective_discount_percent",
    "claim_gap_percent",
    "list_raised",
    "distinct_price_count",
)
ML_MIN_ROWS = 50


def _percent(numerator, denominator) -> Any:
    """``numerator / denominator * 100`` as DOUBLE; null for a null or zero denominator."""
    return F.when(
        denominator.isNotNull() & (denominator != F.lit(Decimal(0))),
        (numerator.cast("double") / denominator.cast("double")) * F.lit(100.0),
    )


def _displayed_percent(price, list_price) -> Any:
    """What the page claims: the reduction from the list price, negative when the price sits above it."""
    return _percent(list_price - price, list_price)


def build_discount_credibility_daily(
    price_history: DataFrame,
    *,
    config: DiscountRuleConfig,
    model_factory: Callable[[], Any] | None = None,
) -> DataFrame:
    """Build ``discount_credibility_daily`` from ``offer_price_history_daily``.

    Grain: exactly one row per ``(marketplace, offer_id, observed_date,
    currency)``, the grain of its input. ``model_factory`` returns an unfitted
    scikit-learn estimator with ``fit`` and ``decision_function``; the default
    builds an Isolation Forest when scikit-learn is importable. ``None`` or a
    factory returning ``None`` leaves the ML columns null.
    """
    price = F.col("last_price").cast(MONEY)
    list_price = F.col("last_list_price").cast(MONEY)
    day_number = F.datediff(F.col("observed_date"), F.lit(EPOCH))

    # Calendar days before the judged day, that day excluded. Currency is in
    # the partition so an offer that switched currency restarts its history.
    reference = (
        Window.partitionBy("marketplace", "offer_id", "currency")
        .orderBy(day_number)
        .rangeBetween(-config.reference_window_days, -1)
    )
    framed = (
        price_history.withColumn("evaluated_price", price)
        .withColumn("list_price", list_price)
        .withColumn("displayed_discount_percent", _displayed_percent(price, list_price))
        .withColumn("reference_sample_size", F.count(F.col("last_price")).over(reference).cast("bigint"))
        .withColumn("reference_min_price", F.min(price).over(reference))
        .withColumn("_reference_prices", F.array_sort(F.collect_list(price).over(reference)))
        .withColumn("reference_max_list_price", F.max(list_price).over(reference))
    )
    size = F.col("reference_sample_size")
    # The median of an even count is the mean of the two middle values, as the anomaly mart defines it.
    low = F.get(F.col("_reference_prices"), F.floor((size - 1) / 2).cast("int"))
    high = F.get(F.col("_reference_prices"), F.ceil((size - 1) / 2).cast("int"))
    measured = (
        framed.withColumn("reference_median_price", F.when(size > 0, ((low + high) / F.lit(2)).cast(MONEY)))
        .drop("_reference_prices")
        .withColumn(
            "effective_discount_percent",
            _percent(F.col("reference_min_price") - F.col("evaluated_price"), F.col("reference_min_price")),
        )
        .withColumn(
            "claim_gap_percent",
            F.when(
                F.col("displayed_discount_percent").isNotNull() & F.col("effective_discount_percent").isNotNull(),
                F.col("displayed_discount_percent") - F.col("effective_discount_percent"),
            ),
        )
        .withColumn("distinct_price_count", F.col("distinct_price_count").cast("bigint"))
    )

    displayed = F.col("displayed_discount_percent")
    shows_reduction = displayed.isNotNull() & (displayed > F.lit(0.0))
    enough_history = size >= F.lit(config.min_history_days)
    fired = {
        rules.PRICE_ABOVE_LIST: displayed.isNotNull() & (displayed < F.lit(0.0)),
        rules.DISCOUNT_OVER_CAP: shows_reduction & (displayed > F.lit(float(config.legal_cap_percent))),
        rules.LIST_PRICE_RAISED: shows_reduction
        & enough_history
        & F.col("reference_max_list_price").isNotNull()
        & (F.col("list_price") > F.col("reference_max_list_price")),
        rules.CLAIM_EXCEEDS_HISTORY: shows_reduction
        & enough_history
        & (F.col("claim_gap_percent") > F.lit(float(config.claim_gap_percent))),
        rules.NOT_BELOW_REFERENCE: shows_reduction
        & enough_history
        & (F.col("effective_discount_percent") <= F.lit(0.0)),
    }
    # The reasons in the module's order, so the JSON is deterministic and
    # readable back as evidence; the score is the domain's weights and cap.
    reasons = F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, cond in fired.items()]))
    score = F.least(
        F.lit(rules.SCORE_CAP),
        sum((F.when(cond, F.lit(rules.REASON_WEIGHTS[name])).otherwise(F.lit(0)) for name, cond in fired.items()),
            F.lit(0)),
    ).cast("int")
    displayed_or_contradicted = shows_reduction | fired[rules.PRICE_ABOVE_LIST]
    status = (
        F.when(~displayed_or_contradicted & (score == 0), F.lit(rules.STATUS_NO_DISCOUNT))
        .when(score >= F.lit(rules.SUSPECT_FROM), F.lit(rules.STATUS_SUSPECT))
        .when(score >= F.lit(rules.QUESTIONABLE_FROM), F.lit(rules.STATUS_QUESTIONABLE))
        .when(enough_history, F.lit(rules.STATUS_CREDIBLE))
        .otherwise(F.lit(rules.STATUS_INSUFFICIENT_HISTORY))
    )
    judged = (
        measured.withColumn("credibility_reasons_json", F.to_json(reasons))
        .withColumn("rule_score", score)
        .withColumn("credibility_status", status)
        .withColumn("reference_window_days", F.lit(config.reference_window_days).cast("bigint"))
        .withColumn("discount_rule_version", F.lit(config.rule_version))
        # The feature the forest sees for LIST_PRICE_RAISED, kept as a number.
        .withColumn("list_raised", F.when(fired[rules.LIST_PRICE_RAISED], F.lit(1.0)).otherwise(F.lit(0.0)))
    )
    ranked = rank_with_isolation_forest(
        judged,
        config=config,
        model_factory=default_model_factory if model_factory is None else model_factory,
    )
    return ranked.select(*CREDIBILITY_COLUMNS)


def default_model_factory() -> Any:
    """An unfitted Isolation Forest, or ``None`` where scikit-learn is not installed."""
    try:
        from sklearn.ensemble import IsolationForest
    except ImportError:
        return None
    return IsolationForest(n_estimators=100, contamination="auto", random_state=0, n_jobs=1)


def _feature_matrix(frame_pd):
    """The ML_FEATURES of a pandas frame as a float matrix; a missing value is 0 (no history: no gap)."""
    import numpy as np

    matrix = frame_pd.loc[:, list(ML_FEATURES)].astype("float64").to_numpy()
    return np.nan_to_num(matrix, nan=0.0, posinf=0.0, neginf=0.0)


def rank_with_isolation_forest(
    judged: DataFrame, *, config: DiscountRuleConfig, model_factory: Callable[[], Any]
) -> DataFrame:
    """Add ``ml_method``, ``ml_score`` and ``ml_rank_percent`` to the judged rows.

    Only rows that display a reduction are ranked, each against the others of
    its marketplace and day: ``ml_score`` is minus the forest's decision
    function (higher is more unusual) and ``ml_rank_percent`` its percent rank
    within the marketplace-day, so 95 means "more unusual than 95 % of the
    reductions shown that day". The forest is fitted once, on a deterministic
    sample of at most ``ml_sample_rows`` rows, and applied per partition.
    """
    none = (
        judged.withColumn("ml_method", F.lit(rules.ML_NONE))
        .withColumn("ml_score", F.lit(None).cast("double"))
        .withColumn("ml_rank_percent", F.lit(None).cast("double"))
    )
    if not config.ml_enabled:
        return none
    model = model_factory()
    if model is None:
        return none
    eligible = judged.filter(F.col("displayed_discount_percent") > F.lit(0.0))
    total = eligible.count()
    if total < ML_MIN_ROWS:
        return none
    fraction = min(1.0, config.ml_sample_rows / total)
    sample = eligible.select(*ML_FEATURES)
    if fraction < 1.0:
        sample = sample.sample(fraction=fraction, seed=config.ml_seed)
    sample_pd = sample.limit(config.ml_sample_rows).toPandas()
    if len(sample_pd) < ML_MIN_ROWS:
        return none
    began = time.monotonic()
    model.fit(_feature_matrix(sample_pd))
    # One line per run, read back by the evaluation of the full run (Loki, docker logs):
    # how long the fit took, on how many rows, out of how many eligible.
    print(
        json.dumps(
            {
                "event": "discount_ml_fitted",
                "model": type(model).__name__,
                "sample_rows": len(sample_pd),
                "eligible_rows": int(total),
                "fit_seconds": round(time.monotonic() - began, 3),
                "rule_version": config.rule_version,
            }
        ),
        flush=True,
    )
    broadcast = judged.sparkSession.sparkContext.broadcast(model)

    key = ("marketplace", "offer_id", "observed_date", "currency")
    schema = StructType(
        [eligible.schema[name] for name in key] + [StructField("ml_score", DoubleType(), True)]
    )

    def score(batches: Iterator[Any]) -> Iterator[Any]:
        fitted = broadcast.value
        for batch in batches:
            if len(batch) == 0:
                continue
            out = batch.loc[:, list(key)].copy()
            out["ml_score"] = (-fitted.decision_function(_feature_matrix(batch))).astype("float64")
            yield out

    scored = eligible.select(*key, *ML_FEATURES).mapInPandas(score, schema=schema)
    rank = Window.partitionBy("marketplace", "observed_date").orderBy(F.col("ml_score"))
    scored = scored.withColumn("ml_rank_percent", F.percent_rank().over(rank) * F.lit(100.0))
    return judged.join(scored, list(key), "left").withColumn(
        "ml_method",
        F.when(F.col("ml_score").isNotNull(), F.lit(rules.ML_ISOLATION_FOREST)).otherwise(F.lit(rules.ML_NONE)),
    )


def build_discount_daily(
    observations: DataFrame,
    credibility: DataFrame,
    *,
    config: DiscountRuleConfig,
    percentile_accuracy: int = 10000,
) -> DataFrame:
    """Build ``discount_daily``: one category path, one UTC day, one currency.

    Counts the offers sighted that day and how many showed a reduction on
    their **last** sighting of the day (the sighting the credibility mart
    judges), the spread of the displayed reduction over those, and how many
    the rules graded QUESTIONABLE or SUSPECT.
    """
    key = ["marketplace", "offer_id", "observed_date", "currency"]
    # The last sighting of each offer-day, as the price mart takes it: the
    # category of the day's last sighting goes with it.
    ordered = observations.withColumn("observed_date", F.to_date("observed_at")).withColumn(
        "category_path", F.coalesce("category_path", F.lit(UNKNOWN_CATEGORY))
    )
    last = Window.partitionBy(*key).orderBy(F.col("observed_at").desc(), F.col("observation_id").desc())
    latest = ordered.withColumn("_rank", F.row_number().over(last)).filter(F.col("_rank") == 1)
    verdicts = credibility.select(
        *key,
        F.col("displayed_discount_percent").alias("_displayed"),
        F.col("credibility_status").alias("_status"),
    )
    joined = latest.join(verdicts, key, "left")
    shows = F.col("_displayed") > F.lit(0.0)
    cap = F.lit(float(config.legal_cap_percent))
    return (
        joined.groupBy("marketplace", "category_path", "observed_date", "currency")
        .agg(
            F.countDistinct("offer_id").alias("observed_offer_count"),
            F.countDistinct(F.when(shows, F.col("offer_id"))).alias("discounted_offer_count"),
            F.percentile_approx(F.when(shows, F.col("_displayed")), 0.25, percentile_accuracy).alias(
                "p25_discount_percent"
            ),
            F.percentile_approx(F.when(shows, F.col("_displayed")), 0.5, percentile_accuracy).alias(
                "median_discount_percent"
            ),
            F.percentile_approx(F.when(shows, F.col("_displayed")), 0.75, percentile_accuracy).alias(
                "p75_discount_percent"
            ),
            F.max(F.when(shows, F.col("_displayed"))).alias("max_discount_percent"),
            F.countDistinct(F.when(shows & (F.col("_displayed") > cap), F.col("offer_id"))).alias(
                "over_cap_offer_count"
            ),
            F.countDistinct(F.when(F.col("_status") == rules.STATUS_QUESTIONABLE, F.col("offer_id"))).alias(
                "questionable_offer_count"
            ),
            F.countDistinct(F.when(F.col("_status") == rules.STATUS_SUSPECT, F.col("offer_id"))).alias(
                "suspect_offer_count"
            ),
        )
        .withColumn(
            "discounted_offer_ratio",
            F.when(
                F.col("observed_offer_count") > 0,
                F.col("discounted_offer_count").cast("double") / F.col("observed_offer_count").cast("double"),
            ),
        )
        .withColumn("legal_cap_percent", F.lit(config.legal_cap_percent).cast(MONEY))
        .withColumn("discount_rule_version", F.lit(config.rule_version))
        .select(*DISCOUNT_DAILY_COLUMNS)
    )
