"""Discount credibility (ADR-0009) on small frozen price histories.

Every reference value below is hand-computed in the test body, never derived
from the implementation. The fixtures declare only the columns the builder
reads, so a new dependency on another column fails loudly.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from ecomdata.domain import discount_rules as d
from ecomdata.processing.batch.discount import (
    CREDIBILITY_COLUMNS,
    DISCOUNT_DAILY_COLUMNS,
    ML_MIN_ROWS,
    build_discount_credibility_daily,
    build_discount_daily,
)
from tests.spark_support import requires_spark

pytestmark = requires_spark

DAY_ONE = date(2026, 10, 1)
HISTORY_SCHEMA = (
    "marketplace string, offer_id string, observed_date date, currency string, "
    "last_price decimal(38,6), last_list_price decimal(38,6), distinct_price_count bigint"
)
CONFIG = d.DiscountRuleConfig(reference_window_days=30, min_history_days=3, ml_enabled=False)


def history(spark, rows):
    return spark.createDataFrame(list(rows), schema=HISTORY_SCHEMA)


def series(days, *, offer="offer-1", currency="VND", start=DAY_ONE):
    """``days``: (price, list_price) per consecutive calendar day; list_price None for no list price."""
    return [
        ("tiki", offer, start + timedelta(days=index), currency, Decimal(price), None if lp is None else Decimal(lp), 1)
        for index, (price, lp) in enumerate(days)
    ]


def build(frame, config=CONFIG, **kwargs):
    return build_discount_credibility_daily(frame, config=config, **kwargs)


def by_date(frame):
    return {row.observed_date: row.asDict() for row in frame.collect()}


def reasons(row):
    return json.loads(row["credibility_reasons_json"])


def test_the_mart_has_exactly_the_registered_columns_at_the_price_mart_grain(spark):
    from ecomdata.domain.datasets import dataset

    out = build(history(spark, series([("100", "120")] * 2)))

    assert tuple(out.columns) == CREDIBILITY_COLUMNS == dataset("discount_credibility_daily").columns
    assert out.count() == 2


def test_the_displayed_reduction_is_what_the_page_claims(spark):
    rows = by_date(build(history(spark, series([("80", "100"), ("100", "100"), ("120", "100"), ("50", None)]))))

    assert rows[DAY_ONE]["displayed_discount_percent"] == pytest.approx(20.0)
    assert rows[DAY_ONE + timedelta(days=1)]["displayed_discount_percent"] == pytest.approx(0.0)
    assert rows[DAY_ONE + timedelta(days=2)]["displayed_discount_percent"] == pytest.approx(-20.0)
    assert rows[DAY_ONE + timedelta(days=3)]["displayed_discount_percent"] is None


def test_the_reference_is_the_lowest_prior_price_of_the_window_and_never_the_day_itself(spark):
    # Prices 100, 90, 95, then 60: on day 4 the reference is min(100, 90, 95) = 90,
    # the median 95, and the day's own 60 is not part of it.
    rows = by_date(build(history(spark, series([("100", "150"), ("90", "150"), ("95", "150"), ("60", "150")]))))
    day4 = rows[DAY_ONE + timedelta(days=3)]

    assert rows[DAY_ONE]["reference_sample_size"] == 0 and rows[DAY_ONE]["reference_min_price"] is None
    assert day4["reference_sample_size"] == 3
    assert day4["reference_min_price"] == Decimal("90.000000")
    assert day4["reference_median_price"] == Decimal("95.000000")
    assert day4["reference_max_list_price"] == Decimal("150.000000")
    # Displayed: (150-60)/150 = 60 %; effective against 90: 33.33 %; gap 26.67 points.
    assert day4["displayed_discount_percent"] == pytest.approx(60.0)
    assert day4["effective_discount_percent"] == pytest.approx(100 * 30 / 90)
    assert day4["claim_gap_percent"] == pytest.approx(60.0 - 100 * 30 / 90)


def test_the_window_is_calendar_days_so_an_old_price_falls_out(spark):
    config = d.DiscountRuleConfig(reference_window_days=5, min_history_days=1, ml_enabled=False)
    rows = [
        ("tiki", "o", DAY_ONE, "VND", Decimal("50"), Decimal("100"), 1),  # 10 days before: outside
        ("tiki", "o", DAY_ONE + timedelta(days=7), "VND", Decimal("80"), Decimal("100"), 1),  # 3 days before: inside
        ("tiki", "o", DAY_ONE + timedelta(days=10), "VND", Decimal("70"), Decimal("100"), 1),
    ]
    out = by_date(build(history(spark, rows), config))

    judged = out[DAY_ONE + timedelta(days=10)]
    assert judged["reference_sample_size"] == 1
    assert judged["reference_min_price"] == Decimal("80.000000")


def test_a_currency_switch_restarts_the_history(spark):
    rows = series([("100", "120")] * 3) + [("tiki", "offer-1", DAY_ONE + timedelta(days=3), "USD", Decimal("4"), Decimal("5"), 1)]
    out = by_date(build(history(spark, rows)))

    assert out[DAY_ONE + timedelta(days=3)]["reference_sample_size"] == 0


# -- the rules ---------------------------------------------------------------


def test_no_reduction_shown_and_nothing_contradicting_it_is_no_discount(spark):
    rows = by_date(build(history(spark, series([("100", "100"), ("100", None)]))))

    for row in rows.values():
        assert row["credibility_status"] == d.STATUS_NO_DISCOUNT
        assert reasons(row) == [] and row["rule_score"] == 0


def test_a_price_above_its_list_price_fires_without_any_history(spark):
    (row,) = build(history(spark, series([("120", "100")]))).collect()

    assert reasons(row.asDict()) == [d.PRICE_ABOVE_LIST]
    assert row.rule_score == 40 and row.credibility_status == d.STATUS_QUESTIONABLE


def test_a_reduction_over_the_cap_fires_and_the_cap_travels_with_the_config(spark):
    (row,) = build(history(spark, series([("40", "100")]))).collect()
    assert reasons(row.asDict()) == [d.DISCOUNT_OVER_CAP]
    assert row.credibility_status == d.STATUS_QUESTIONABLE

    lenient = d.DiscountRuleConfig(legal_cap_percent=Decimal("70"), ml_enabled=False)
    (row,) = build(history(spark, series([("40", "100")])), lenient).collect()
    assert reasons(row.asDict()) == [] and row.credibility_status == d.STATUS_INSUFFICIENT_HISTORY


def test_a_clean_reduction_is_credible_once_three_days_vouch_for_it(spark):
    # 100 for three days at list 120, then 90: a real 10 % below the reference, 25 % displayed, gap 15 < 20.
    rows = by_date(build(history(spark, series([("100", "120")] * 3 + [("90", "120")]))))

    assert rows[DAY_ONE + timedelta(days=1)]["credibility_status"] == d.STATUS_INSUFFICIENT_HISTORY
    last = rows[DAY_ONE + timedelta(days=3)]
    assert last["credibility_status"] == d.STATUS_CREDIBLE and reasons(last) == []


def test_a_claim_the_history_contradicts_is_suspect(spark):
    # The offer sold at 100 for three days; today it shows list 200 and price 100: "50 % off",
    # effectively 0 % below the reference. The claim gap (50 points) fires, and so does
    # NOT_BELOW_REFERENCE, and the raised list price.
    rows = by_date(build(history(spark, series([("100", "100")] * 3 + [("100", "200")]))))
    last = rows[DAY_ONE + timedelta(days=3)]

    assert reasons(last) == [d.LIST_PRICE_RAISED, d.CLAIM_EXCEEDS_HISTORY, d.NOT_BELOW_REFERENCE]
    assert last["rule_score"] == 100 and last["credibility_status"] == d.STATUS_SUSPECT


def test_the_history_rules_wait_for_the_minimum_history(spark):
    # Same contradiction after only two prior days: no history rule fires, the
    # displayed 50 % is at the cap, not above it, so no verdict is given.
    rows = by_date(build(history(spark, series([("100", "100")] * 2 + [("100", "200")]))))
    last = rows[DAY_ONE + timedelta(days=2)]

    assert reasons(last) == [] and last["credibility_status"] == d.STATUS_INSUFFICIENT_HISTORY


def test_the_parameters_travel_with_every_row(spark):
    (row,) = build(history(spark, series([("80", "100")]))).collect()

    assert row.reference_window_days == 30 and row.discount_rule_version == d.DISCOUNT_RULE_VERSION
    assert row.ml_method == d.ML_NONE and row.ml_score is None and row.ml_rank_percent is None


# -- the Isolation Forest rank -------------------------------------------------


def _many(spark, n=ML_MIN_ROWS + 10):
    rows = [("tiki", f"o{i}", DAY_ONE, "VND", Decimal(100 - (i % 10)), Decimal("100"), 1) for i in range(n)]
    # One reduction far outside the rest.
    rows.append(("tiki", "odd", DAY_ONE, "VND", Decimal("5"), Decimal("100"), 7))
    return history(spark, rows)


def test_without_a_model_the_ml_columns_are_null_and_the_rules_unchanged(spark):
    enabled = d.DiscountRuleConfig(ml_enabled=True)
    out = build(_many(spark), enabled, model_factory=lambda: None).collect()

    assert {row.ml_method for row in out} == {d.ML_NONE}
    assert all(row.ml_score is None for row in out)


def test_the_forest_ranks_the_odd_reduction_highest_and_leaves_the_rest_to_the_rules(spark):
    pytest.importorskip("sklearn")
    enabled = d.DiscountRuleConfig(ml_enabled=True)

    out = {row.offer_id: row for row in build(_many(spark), enabled).collect()}

    assert out["odd"].ml_method == d.ML_ISOLATION_FOREST
    assert out["odd"].ml_rank_percent == pytest.approx(100.0)
    ranked = {k: r for k, r in out.items() if r.ml_method == d.ML_ISOLATION_FOREST}
    assert all(r.ml_rank_percent < out["odd"].ml_rank_percent for k, r in ranked.items() if k != "odd")
    # Only rows that show a reduction are ranked; the rest (price at list price) carry no rank.
    unranked = {k: r for k, r in out.items() if k not in ranked}
    assert unranked and all(r.displayed_discount_percent == 0.0 and r.ml_rank_percent is None for r in unranked.values())
    # The verdict is the rules', with its reasons: the forest only ranks.
    assert out["odd"].credibility_status == d.STATUS_QUESTIONABLE and reasons(out["odd"].asDict()) == [d.DISCOUNT_OVER_CAP]


def test_a_fit_reports_its_rows_and_duration_on_one_line(spark, capsys):
    pytest.importorskip("sklearn")

    build(_many(spark), d.DiscountRuleConfig(ml_enabled=True)).collect()

    (line,) = [json.loads(x) for x in capsys.readouterr().out.splitlines() if "discount_ml_fitted" in x]
    # _many: 60 offers at 100-(i%10) against a list price of 100 (the six at 100 show no
    # reduction), plus the odd one: 55 reductions, all of them under the sample size.
    assert line["model"] == "IsolationForest" and line["sample_rows"] == line["eligible_rows"] == 55
    assert line["fit_seconds"] >= 0 and line["rule_version"] == d.DISCOUNT_RULE_VERSION


def test_too_few_reductions_leave_the_forest_out(spark):
    pytest.importorskip("sklearn")
    enabled = d.DiscountRuleConfig(ml_enabled=True)

    out = build(history(spark, series([("80", "100")] * 3)), enabled).collect()

    assert {row.ml_method for row in out} == {d.ML_NONE}


# -- the category view ---------------------------------------------------------


def _observations(spark, rows):
    schema = (
        "marketplace string, offer_id string, observation_id string, observed_at timestamp, currency string, "
        "current_price decimal(38,6), list_price decimal(38,6), category_path string"
    )
    return spark.createDataFrame(rows, schema=schema)


def test_discount_daily_counts_reductions_per_category_and_day_from_the_last_sighting(spark):
    at = datetime(2026, 10, 1, 8)
    rows = [
        # a: two sightings; the last one shows no reduction, so it does not count as discounted
        ("tiki", "a", "a1", at, "VND", Decimal("80"), Decimal("100"), "x/y"),
        ("tiki", "a", "a2", at + timedelta(hours=2), "VND", Decimal("100"), Decimal("100"), "x/y"),
        # b: 60 % off (over the cap); c: 10 % off; d: no category
        ("tiki", "b", "b1", at, "VND", Decimal("40"), Decimal("100"), "x/y"),
        ("tiki", "c", "c1", at, "VND", Decimal("90"), Decimal("100"), "x/y"),
        ("tiki", "d", "d1", at, "VND", Decimal("50"), Decimal("100"), None),
    ]
    observations = _observations(spark, rows)
    price_history = history(
        spark,
        [
            ("tiki", "a", DAY_ONE, "VND", Decimal("100"), Decimal("100"), 2),
            ("tiki", "b", DAY_ONE, "VND", Decimal("40"), Decimal("100"), 1),
            ("tiki", "c", DAY_ONE, "VND", Decimal("90"), Decimal("100"), 1),
            ("tiki", "d", DAY_ONE, "VND", Decimal("50"), Decimal("100"), 1),
        ],
    )
    credibility = build(price_history)

    out = {row.category_path: row for row in build_discount_daily(observations, credibility, config=CONFIG).collect()}

    assert tuple(build_discount_daily(observations, credibility, config=CONFIG).columns) == DISCOUNT_DAILY_COLUMNS
    xy = out["x/y"]
    assert xy.observed_offer_count == 3 and xy.discounted_offer_count == 2
    assert xy.discounted_offer_ratio == pytest.approx(2 / 3)
    assert xy.max_discount_percent == pytest.approx(60.0)
    assert xy.over_cap_offer_count == 1 and xy.questionable_offer_count == 1 and xy.suspect_offer_count == 0
    assert xy.legal_cap_percent == Decimal("50.000000")
    unknown = out["__UNKNOWN__"]
    assert unknown.observed_offer_count == 1 and unknown.discounted_offer_count == 1
