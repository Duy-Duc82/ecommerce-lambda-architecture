"""Robust price outlier tests, Phase 7 plan section 17 items 1-16.

These need real DataFrames, so they run on a local Spark session over small
frozen fixtures. No MinIO, PostgreSQL or network is touched.

Every baseline below is hand-computed in the test body rather than derived from
the implementation, because a test that recomputes the production formula only
proves the formula is consistent with itself.
"""
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from batch_layer.marketplace_anomaly import (
    ANOMALY_COLUMNS,
    build_price_anomaly_daily,
)
from tests.spark_support import requires_spark

pytestmark = requires_spark

DAY_ONE = date(2026, 9, 1)
HISTORY_SCHEMA = (
    "marketplace string, offer_id string, observed_date date, "
    "currency string, last_price decimal(38,6)"
)
RULE = "anomaly-rules.test"


def history(spark, rows):
    """Only the five columns the transform reads are declared.

    The real input carries the whole price mart, but naming just the inputs
    here keeps each fixture readable and makes an accidental new dependency on
    another column fail loudly instead of passing on ambient data.
    """
    return spark.createDataFrame(list(rows), schema=HISTORY_SCHEMA)


def series(prices, *, offer="offer-1", currency="VND", start=DAY_ONE, step=1):
    return [
        ("tiki", offer, start + timedelta(days=index * step), currency, Decimal(price))
        for index, price in enumerate(prices)
    ]


def build(frame, *, window_days=10, min_samples=1, mad="3.5", iqr="1.5"):
    return build_price_anomaly_daily(
        frame,
        window_days=window_days,
        min_samples=min_samples,
        mad_threshold=Decimal(mad),
        iqr_multiplier=Decimal(iqr),
        rule_version=RULE,
    )


def by_date(frame):
    return {row.observed_date: row.asDict() for row in frame.collect()}


# 1
def test_median_is_exact_for_odd_and_even_sample_counts(spark):
    # Day 5 sees the four prior prices (even count, mean of the two middle
    # values); day 6 sees five (odd count, the single middle value).
    rows = by_date(build(history(spark, series(["10", "20", "30", "40", "50", "60"]))))

    assert rows[DAY_ONE + timedelta(days=4)]["baseline_sample_size"] == 4
    assert rows[DAY_ONE + timedelta(days=4)]["baseline_median"] == Decimal("25.000000")
    assert rows[DAY_ONE + timedelta(days=5)]["baseline_sample_size"] == 5
    assert rows[DAY_ONE + timedelta(days=5)]["baseline_median"] == Decimal("30.000000")


# 2
def test_mad_is_the_median_of_absolute_deviations(spark):
    # Baseline [10,20,30,40,50], median 30, deviations [20,10,0,10,20],
    # sorted [0,10,10,20,20], median 10.
    rows = by_date(build(history(spark, series(["10", "20", "30", "40", "50", "60"]))))

    assert rows[DAY_ONE + timedelta(days=5)]["baseline_mad"] == Decimal("10.000000")


# 3
def test_p25_p75_and_iqr_follow_the_documented_order_statistic(spark):
    # n=5: p25 position 0.25*4 = 1.0 rounds down to index 1 -> 20;
    #      p75 position 0.75*4 = 3.0 rounds up to index 3 -> 40.
    rows = by_date(build(history(spark, series(["10", "20", "30", "40", "50", "60"]))))
    last = rows[DAY_ONE + timedelta(days=5)]

    assert last["baseline_p25"] == Decimal("20.000000")
    assert last["baseline_p75"] == Decimal("40.000000")
    assert last["baseline_iqr"] == Decimal("20.000000")


# 4
def test_baseline_excludes_the_evaluated_day(spark):
    # The last day's price is wild. If it leaked into its own baseline the
    # median would move and the outlier would hide inside its own evidence.
    rows = by_date(build(history(spark, series(["10", "20", "30", "40", "50", "999999"]))))
    last = rows[DAY_ONE + timedelta(days=5)]

    assert last["baseline_sample_size"] == 5
    assert last["baseline_median"] == Decimal("30.000000")
    assert last["evaluated_price"] == Decimal("999999.000000")


# 5
def test_baseline_counts_observed_days_not_calendar_days(spark):
    # Three observations a week apart. A calendar-day frame would see one or
    # two rows and quietly treat the unobserved days as missing history.
    frame = history(spark, series(["10", "20", "30", "40"], step=7))
    rows = by_date(build(frame, window_days=3))

    assert rows[DAY_ONE + timedelta(days=21)]["baseline_sample_size"] == 3


# 6
def test_too_few_prior_days_yields_insufficient_history(spark):
    rows = by_date(build(history(spark, series(["10", "20", "30"])), min_samples=3))
    second = rows[DAY_ONE + timedelta(days=1)]

    assert second["anomaly_status"] == "INSUFFICIENT_HISTORY"
    assert second["anomaly_reason"] == "MIN_SAMPLE_NOT_MET"
    assert second["anomaly_method"] == "NONE"
    assert second["robust_score"] is None
    assert second["lower_fence"] is None
    assert second["upper_fence"] is None


# 7
def test_positive_mad_selects_the_rolling_mad_method_in_both_directions(spark):
    # Baseline [98,99,100,101,102]: median 100, deviations sorted
    # [0,1,1,2,2], MAD 1. Score is 0.6745 * (price - 100) / 1.
    base = ["98", "99", "100", "101", "102"]
    high = by_date(build(history(spark, series(base + ["106"]))))[DAY_ONE + timedelta(days=5)]
    low = by_date(build(history(spark, series(base + ["94"]))))[DAY_ONE + timedelta(days=5)]

    assert high["anomaly_method"] == "ROLLING_MAD"
    assert high["baseline_mad"] == Decimal("1.000000")
    assert high["robust_score"] == pytest.approx(0.6745 * 6)
    assert high["anomaly_status"] == "ANOMALOUS_HIGH"
    assert high["anomaly_reason"] == "MAD_SCORE_EXCEEDED"

    assert low["robust_score"] == pytest.approx(0.6745 * -6)
    assert low["anomaly_status"] == "ANOMALOUS_LOW"
    assert low["deviation_amount"] == Decimal("-6.000000")


# 8
def test_a_score_exactly_at_the_threshold_is_normal(spark):
    base = ["98", "99", "100", "101", "102"]
    frame = history(spark, series(base + ["106"]))
    exact = 0.6745 * 6.0

    at_threshold = by_date(build(frame, mad=str(exact)))[DAY_ONE + timedelta(days=5)]
    just_below = by_date(build(frame, mad=str(exact * 0.999)))[DAY_ONE + timedelta(days=5)]

    assert at_threshold["anomaly_status"] == "NORMAL"
    assert at_threshold["anomaly_reason"] == "WITHIN_TOLERANCE"
    assert just_below["anomaly_status"] == "ANOMALOUS_HIGH"


# 9
def test_zero_mad_with_dispersion_falls_back_to_iqr_fences(spark):
    # Baseline [100,100,100,100,120,130,140]: median 100, four zero deviations
    # out of seven so MAD is 0, but p25 100 and p75 130 leave an IQR of 30.
    base = ["100", "100", "100", "100", "120", "130", "140"]

    def verdict(price):
        return by_date(build(history(spark, series(base + [price]))))[DAY_ONE + timedelta(days=7)]

    inside = verdict("100")
    assert inside["baseline_mad"] == Decimal("0.000000")
    assert inside["baseline_p25"] == Decimal("100.000000")
    assert inside["baseline_p75"] == Decimal("130.000000")
    assert inside["anomaly_method"] == "IQR_FALLBACK"
    assert inside["lower_fence"] == Decimal("55.000000")
    assert inside["upper_fence"] == Decimal("175.000000")
    assert inside["robust_score"] is None
    assert inside["anomaly_status"] == "NORMAL"

    assert verdict("50")["anomaly_status"] == "ANOMALOUS_LOW"
    assert verdict("50")["anomaly_reason"] == "IQR_FENCE_EXCEEDED"
    assert verdict("180")["anomaly_status"] == "ANOMALOUS_HIGH"
    # The fence is exclusive: sitting exactly on it is not an outlier.
    assert verdict("55")["anomaly_status"] == "NORMAL"


# 10
def test_a_flat_baseline_never_reports_an_anomaly(spark):
    # Every deviation from a perfectly flat history looks infinitely
    # significant, which would flag the first price change of every stable
    # offer. No verdict is the honest answer.
    frame = history(spark, series(["100"] * 7 + ["999999"]))
    row = by_date(build(frame))[DAY_ONE + timedelta(days=7)]

    assert row["baseline_mad"] == Decimal("0.000000")
    assert row["baseline_iqr"] == Decimal("0.000000")
    assert row["anomaly_status"] == "INSUFFICIENT_DISPERSION"
    assert row["anomaly_reason"] == "ZERO_DISPERSION"
    assert row["anomaly_method"] == "NONE"


# 11
def test_a_currency_change_restarts_the_baseline(spark):
    rows = series(["100", "101", "102", "103"]) + series(
        ["4"], start=DAY_ONE + timedelta(days=4), currency="USD"
    )
    result = by_date(build(history(spark, rows)))
    switched = result[DAY_ONE + timedelta(days=4)]

    assert switched["currency"] == "USD"
    assert switched["baseline_sample_size"] == 0
    assert switched["baseline_median"] is None
    assert switched["anomaly_status"] == "INSUFFICIENT_HISTORY"


# 12
def test_every_row_carries_the_rule_and_its_parameters(spark):
    frame = build(history(spark, series(["10", "20"])), window_days=9, min_samples=2, mad="4.5", iqr="2.5")

    for row in frame.collect():
        assert row.window_days == 9
        assert row.min_samples == 2
        assert row.mad_threshold == Decimal("4.500000")
        assert row.iqr_multiplier == Decimal("2.500000")
        assert row.anomaly_rule_version == RULE


# 13
def test_money_columns_stay_decimal(spark):
    types = dict(build(history(spark, series(["10", "20"]))).dtypes)

    for name in (
        "evaluated_price", "baseline_median", "baseline_mad", "baseline_p25",
        "baseline_p75", "baseline_iqr", "deviation_amount", "lower_fence",
        "upper_fence", "mad_threshold", "iqr_multiplier",
    ):
        assert types[name] == "decimal(38,6)", name
    # The score and the percentage are ratios, not money.
    assert types["robust_score"] == "double"
    assert types["deviation_percent"] == "double"


# 14
def test_the_mart_has_one_row_per_offer_day_and_joins_one_to_one(spark):
    source = history(
        spark,
        series(["10", "20", "30"]) + series(["40", "50", "60"], offer="offer-2"),
    )
    result = build(source)

    assert result.count() == source.count()
    assert result.select("marketplace", "offer_id", "observed_date").distinct().count() == source.count()
    assert list(result.columns) == list(ANOMALY_COLUMNS)


# 15
def test_input_row_order_does_not_change_any_output_value(spark):
    rows = series(["98", "99", "100", "101", "102", "106"])
    forward = by_date(build(history(spark, rows)))
    shuffled = by_date(build(history(spark, [rows[3], rows[0], rows[5], rows[2], rows[4], rows[1]])))

    assert forward == shuffled


# 16
def test_the_module_makes_no_claim_about_dishonesty_or_correctness(spark):
    source = Path("batch_layer/marketplace_anomaly.py").read_text(encoding="utf-8").lower()

    for forbidden in ("scam", "fraud", "fake price", "incorrect price", "wrong price", "mispric"):
        assert forbidden not in source, forbidden
