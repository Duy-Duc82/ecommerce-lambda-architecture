"""Robust price outlier detection over one offer's own observed history.

Phase 7 plan section 6. The verdict this produces is a *statistical price
outlier relative to this offer's own recent observed price history* — never a
claim that a price is wrong, fake or dishonest, and never a comparison against
another offer, another seller or another marketplace.

Input is the Phase 6 ``offer_price_history_daily`` mart rather than raw Silver.
That mart already collapses intraday observations deterministically, so reusing
it means the price mart and the outlier mart can never disagree about what an
offer's price was on a given day. The evaluated value is ``last_price``: the
last price actually observed that day. ``avg_price`` is deliberately not used,
because an average of two genuinely different observed prices is a number the
marketplace never showed.

Baseline statistics are exact order statistics over a collected window, not
``percentile_approx``. The window is bounded by the configured day count, so
collecting it is cheap, and exactness is what makes a stored verdict
reproducible from the stored parameters.
"""
from __future__ import annotations

from decimal import Decimal

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

MONEY = "decimal(38,6)"

METHOD_MAD = "ROLLING_MAD"
METHOD_IQR = "IQR_FALLBACK"
METHOD_NONE = "NONE"

STATUS_NORMAL = "NORMAL"
STATUS_HIGH = "ANOMALOUS_HIGH"
STATUS_LOW = "ANOMALOUS_LOW"
STATUS_NO_HISTORY = "INSUFFICIENT_HISTORY"
STATUS_NO_DISPERSION = "INSUFFICIENT_DISPERSION"

REASON_MIN_SAMPLE = "MIN_SAMPLE_NOT_MET"
REASON_MAD = "MAD_SCORE_EXCEEDED"
REASON_IQR = "IQR_FENCE_EXCEEDED"
REASON_WITHIN = "WITHIN_TOLERANCE"
REASON_ZERO_DISPERSION = "ZERO_DISPERSION"

# Puts a MAD-based deviation on the same scale as a z-score.
_CONSISTENCY = Decimal("0.6745")

ANOMALY_COLUMNS = (
    "marketplace", "offer_id", "observed_date", "currency", "evaluated_price",
    "baseline_sample_size", "baseline_median", "baseline_mad", "baseline_p25",
    "baseline_p75", "baseline_iqr", "deviation_amount", "deviation_percent",
    "robust_score", "lower_fence", "upper_fence", "anomaly_method",
    "anomaly_status", "anomaly_reason", "window_days", "min_samples",
    "mad_threshold", "iqr_multiplier", "anomaly_rule_version",
)


def _ordered_percentile(values: Column, size: Column, ratio: str, *, mode: str) -> Column:
    """One order-statistic definition for the median, p25 and p75.

    ``values`` must already be sorted ascending. Positions are zero-based:

        position = ratio * (size - 1)
        low      = values[floor(position)]
        high     = values[ceil(position)]

    ``mode`` picks which end to take — ``"midpoint"`` averages the two, which
    is what makes an even-count median the mean of the two middle values;
    ``"low"`` rounds down for p25 and ``"high"`` rounds up for p75.

    The ratios are 0.25, 0.5 and 0.75, each exactly representable in binary,
    and ``size - 1`` is a small integer, so the products and their floor/ceil
    are exact rather than merely close. ``F.get`` is used instead of
    ``element_at`` because it returns NULL for an out-of-range index rather
    than raising under ANSI mode, which is what an empty baseline needs.

    Keeping all three percentiles in one helper is deliberate: three separate
    index expressions would be three chances for the definitions to drift
    apart between the mart, the fences and a future reviewer's arithmetic.
    """
    position = F.lit(float(ratio)) * (size - F.lit(1))
    low = F.get(values, F.floor(position).cast("int"))
    high = F.get(values, F.ceil(position).cast("int"))
    if mode == "low":
        return low.cast(MONEY)
    if mode == "high":
        return high.cast(MONEY)
    if mode != "midpoint":
        raise ValueError(f"unsupported percentile mode: {mode}")
    return ((low + high) / F.lit(Decimal("2"))).cast(MONEY)


def build_price_anomaly_daily(
    price_history: DataFrame,
    *,
    window_days: int,
    min_samples: int,
    mad_threshold: Decimal,
    iqr_multiplier: Decimal,
    rule_version: str,
) -> DataFrame:
    """Build ``price_anomaly_daily`` from ``offer_price_history_daily``.

    Grain: exactly one row per ``(marketplace, offer_id, observed_date,
    currency)``, the same grain as the source mart, so the result joins
    one-to-one with its input. Currency is part of the key because an offer
    that switches currency within a day yields one source row per currency.
    """
    if window_days <= 0 or min_samples <= 0:
        raise ValueError("window_days and min_samples must be positive")
    if min_samples > window_days:
        raise ValueError("min_samples cannot exceed window_days")
    if mad_threshold <= 0 or iqr_multiplier <= 0:
        raise ValueError("mad_threshold and iqr_multiplier must be positive")
    if not rule_version.strip():
        raise ValueError("rule_version must be non-empty")

    # Currency is part of the partition so an offer that switches currency
    # restarts its baseline instead of comparing VND against USD. The frame
    # ends one row before the current one, so today's price can never pull the
    # median toward itself and hide inside its own baseline. Because each row
    # is one observed day, the frame counts observed days rather than calendar
    # days, and a gap in observation injects no fabricated price.
    baseline_frame = (
        Window.partitionBy("marketplace", "offer_id", "currency")
        .orderBy(F.col("observed_date"))
        .rowsBetween(-window_days, -1)
    )

    size = F.col("baseline_sample_size")
    framed = (
        price_history.withColumn("evaluated_price", F.col("last_price").cast(MONEY))
        .withColumn(
            "_baseline",
            F.array_sort(F.collect_list(F.col("last_price").cast(MONEY)).over(baseline_frame)),
        )
        .withColumn("baseline_sample_size", F.size("_baseline").cast("bigint"))
    )

    has_baseline = size > F.lit(0)
    stats = (
        framed.withColumn(
            "baseline_median",
            F.when(has_baseline, _ordered_percentile(F.col("_baseline"), size, "0.5", mode="midpoint")),
        )
        .withColumn(
            "baseline_p25",
            F.when(has_baseline, _ordered_percentile(F.col("_baseline"), size, "0.25", mode="low")),
        )
        .withColumn(
            "baseline_p75",
            F.when(has_baseline, _ordered_percentile(F.col("_baseline"), size, "0.75", mode="high")),
        )
        .withColumn("baseline_iqr", (F.col("baseline_p75") - F.col("baseline_p25")).cast(MONEY))
        .withColumn(
            "_deviations",
            F.when(
                has_baseline,
                F.array_sort(
                    F.transform("_baseline", lambda value: F.abs(value - F.col("baseline_median")))
                ),
            ),
        )
        .withColumn(
            "baseline_mad",
            F.when(has_baseline, _ordered_percentile(F.col("_deviations"), size, "0.5", mode="midpoint")),
        )
    )

    enough_samples = size >= F.lit(min_samples)
    mad_usable = enough_samples & (F.col("baseline_mad") > F.lit(Decimal(0)))
    iqr_usable = enough_samples & ~mad_usable & (F.col("baseline_iqr") > F.lit(Decimal(0)))

    deviation = (F.col("evaluated_price") - F.col("baseline_median")).cast(MONEY)
    # The score is a ratio and is DOUBLE by contract, so it is computed in
    # double. Money and the fences below stay Decimal and are compared as
    # Decimal, which is where exactness actually matters.
    score = (
        F.lit(float(_CONSISTENCY))
        * deviation.cast("double")
        / F.col("baseline_mad").cast("double")
    )
    lower_fence = (F.col("baseline_p25") - F.lit(iqr_multiplier) * F.col("baseline_iqr")).cast(MONEY)
    upper_fence = (F.col("baseline_p75") + F.lit(iqr_multiplier) * F.col("baseline_iqr")).cast(MONEY)

    scored = (
        stats.withColumn("deviation_amount", deviation)
        .withColumn(
            "deviation_percent",
            F.when(
                F.col("baseline_median").isNotNull() & (F.col("baseline_median") != F.lit(Decimal(0))),
                (deviation / F.col("baseline_median")).cast("double") * F.lit(100.0),
            ),
        )
        .withColumn("robust_score", F.when(mad_usable, score))
        .withColumn("lower_fence", F.when(iqr_usable, lower_fence))
        .withColumn("upper_fence", F.when(iqr_usable, upper_fence))
        .withColumn(
            "anomaly_method",
            F.when(mad_usable, F.lit(METHOD_MAD))
            .when(iqr_usable, F.lit(METHOD_IQR))
            .otherwise(F.lit(METHOD_NONE)),
        )
    )

    threshold = F.lit(float(mad_threshold))
    # Precedence, and the reason each branch exists:
    #   too few prior days -> no verdict, because a baseline of two prices
    #   cannot tell an outlier from normal variation;
    #   MAD is the primary rule;
    #   IQR only stands in when MAD is exactly zero;
    #   a baseline with no dispersion at all yields no verdict, because every
    #   deviation from a perfectly flat history looks infinitely significant
    #   and would flag the first price change of every stable offer.
    # A score exactly at the threshold stays NORMAL; only strictly beyond it
    # counts.
    status = (
        F.when(~enough_samples, F.lit(STATUS_NO_HISTORY))
        .when(mad_usable & (F.col("robust_score") > threshold), F.lit(STATUS_HIGH))
        .when(mad_usable & (F.col("robust_score") < -threshold), F.lit(STATUS_LOW))
        .when(mad_usable, F.lit(STATUS_NORMAL))
        .when(iqr_usable & (F.col("evaluated_price") < F.col("lower_fence")), F.lit(STATUS_LOW))
        .when(iqr_usable & (F.col("evaluated_price") > F.col("upper_fence")), F.lit(STATUS_HIGH))
        .when(iqr_usable, F.lit(STATUS_NORMAL))
        .otherwise(F.lit(STATUS_NO_DISPERSION))
    )

    labelled = scored.withColumn("anomaly_status", status).withColumn(
        "anomaly_reason",
        F.when(F.col("anomaly_status") == STATUS_NO_HISTORY, F.lit(REASON_MIN_SAMPLE))
        .when(F.col("anomaly_status") == STATUS_NO_DISPERSION, F.lit(REASON_ZERO_DISPERSION))
        .when(F.col("anomaly_status") == STATUS_NORMAL, F.lit(REASON_WITHIN))
        .when(F.col("anomaly_method") == METHOD_MAD, F.lit(REASON_MAD))
        .otherwise(F.lit(REASON_IQR)),
    )

    # The parameters travel with every row. A stored verdict without the rule
    # that produced it cannot be re-checked once the configuration moves on.
    return (
        labelled.withColumn("window_days", F.lit(window_days).cast("bigint"))
        .withColumn("min_samples", F.lit(min_samples).cast("bigint"))
        .withColumn("mad_threshold", F.lit(mad_threshold).cast(MONEY))
        .withColumn("iqr_multiplier", F.lit(iqr_multiplier).cast(MONEY))
        .withColumn("anomaly_rule_version", F.lit(rule_version))
        .select(*ANOMALY_COLUMNS)
    )
