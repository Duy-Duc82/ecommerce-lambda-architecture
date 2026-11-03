"""Facts of the marketplace star, docs/DATA_MODEL.md section 4.7.

Local Spark over the frozen observation fixtures the dimension and mart tests
use: the facts are built from the same sightings and the dimensions built from
them, so a key a fact computes is checked against the row the dimension holds.
"""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from ecomdata.domain.datasets import dataset
from ecomdata.processing.batch.dimensions import build_marketplace_dimensions
from ecomdata.processing.batch.facts import (
    FactRules,
    build_fact_crawl_attempt,
    build_marketplace_facts,
    keyed_observations,
)
from ecomdata.processing.batch.keys import UNKNOWN_KEY, key_or_unknown
from ecomdata.processing.batch.marts import (
    build_counter_delta_daily,
    build_counter_transitions,
    build_offer_change_daily,
    build_offer_price_history_daily,
)
from tests.spark.test_marketplace_dimensions import frame
from tests.spark.test_marketplace_dimensions import sighting as _sighting
from tests.spark.test_marketplace_marts import DAY, _audit
from tests.spark_support import requires_spark

pytestmark = requires_spark

DAY_KEY = 20260904
NEXT_DAY_KEY = 20260905
PAGE = "dien-thoai/c1846"


def sighting(offer="offer-1", minute=0, **fields):
    """``sighting`` with the money columns the tests spell as text."""
    for name in ("list_price", "discount_percent"):
        if isinstance(fields.get(name), str):
            fields[name] = Decimal(fields[name])
    return _sighting(offer, minute, **fields)


RULES = FactRules(large_drop_absolute=Decimal("1000"), large_drop_relative=Decimal("0.5"))


def build(spark, rows, *, listed=None, rules=RULES):
    sightings = frame(spark, rows)
    dims = build_marketplace_dimensions(sightings)
    facts = build_marketplace_facts(sightings, dims["dim_product_listing"], listed, None, rules)
    return dims, facts


def rows_of(frame_, *columns):
    return sorted((tuple(r) for r in frame_.select(*columns).collect()), key=repr)


def one(frame_):
    records = frame_.collect()
    assert len(records) == 1, records
    return records[0].asDict()


# ---------- fact_offer_observation ----------


def test_the_atomic_fact_has_a_row_per_sighting_and_marks_the_sponsored_ones(spark):
    _, facts = build(
        spark,
        [
            sighting("o1", 0),
            sighting("o1", 5, price="90"),
            sighting("o2", 1, is_advertisement=True, search_rank=3),
        ],
    )

    fact = facts["fact_offer_observation"]

    assert tuple(fact.columns) == dataset("fact_offer_observation").columns
    assert fact.count() == 3
    assert one(fact.filter("is_advertisement"))["search_rank"] == 3
    assert {r.date_key for r in fact.collect()} == {DAY_KEY}


def test_every_fact_has_the_registry_columns_in_order(spark):
    _, facts = build(spark, [sighting("o1", 0, request_target=PAGE, request_page=1)])

    assert set(facts) == {
        "fact_offer_observation",
        "fact_price_snapshot_daily",
        "fact_price_change",
        "fact_counter_daily",
        "fact_category_page_daily",
    }
    for name, fact in facts.items():
        assert tuple(fact.columns) == dataset(name).columns, name


def test_every_foreign_key_exists_in_its_dimension(spark):
    dims, facts = build(
        spark,
        [
            sighting("o1", 0, seller_id="s1", brand="B", brand_id="9", request_target=PAGE, request_page=1),
            sighting("o1", 10, seller_id="s1", brand="B", brand_id="9", request_target=PAGE, request_page=1, product_title="New"),
            sighting("o2", 3, request_target=None),
            sighting("o3", 4, category_path=None, availability="IN_STOCK"),
        ],
    )
    targets = {
        "date_key": ("dim_date", "date_key"),
        "marketplace_key": ("dim_marketplace", "marketplace_key"),
        "listing_key": ("dim_product_listing", "listing_key"),
        "offer_key": ("dim_product_listing", "offer_key"),
        "seller_key": ("dim_seller", "seller_key"),
        "brand_key": ("dim_brand", "brand_key"),
        "category_key": ("dim_category", "category_key"),
        "page_category_key": ("dim_category", "category_key"),
        "listing_flags_key": ("dim_listing_flags", "listing_flags_key"),
    }
    checked = 0
    for name, fact in facts.items():
        for column in fact.columns:
            if column not in targets:
                continue
            dim, key = targets[column]
            orphans = fact.join(dims[dim].select(key).distinct(), fact[column] == dims[dim][key], "left_anti")
            assert orphans.count() == 0, f"{name}.{column}"
            checked += 1
    assert checked > 20


def test_a_sighting_takes_the_listing_version_valid_at_its_instant(spark):
    dims, facts = build(
        spark,
        [
            sighting("o1", 0, product_title="Old"),
            sighting("o1", 10, product_title="New"),
            sighting("o1", 20, product_title="New", price="95"),
        ],
    )

    versions = dims["dim_product_listing"].select("listing_key", "version", "product_title")
    joined = facts["fact_offer_observation"].join(versions, "listing_key").orderBy("observed_at")

    assert [(r.version, r.product_title) for r in joined.collect()] == [(1, "Old"), (2, "New"), (2, "New")]


def test_a_sighting_without_a_version_points_at_the_unknown_listing(spark):
    sightings = frame(spark, [sighting("o1", 0)])
    dims = build_marketplace_dimensions(sightings)
    empty = dims["dim_product_listing"].filter(f"listing_key = {UNKNOWN_KEY}")

    keyed = keyed_observations(sightings, empty)

    assert {r.listing_key for r in keyed.collect()} == {UNKNOWN_KEY}


# ---------- fact_price_snapshot_daily ----------


def test_the_price_snapshot_is_one_row_per_offer_day_and_currency_and_sparse(spark):
    _, facts = build(
        spark,
        [
            sighting("o1", 0, price="100", list_price="150", availability="IN_STOCK"),
            sighting("o1", 5, price="1", is_advertisement=True),
            sighting("o1", 10, price="80", list_price="140", availability="OUT_OF_STOCK"),
            sighting("o1", 20, price="120", list_price="160", availability="IN_STOCK"),
            sighting("o1", 24 * 60 + 5, price="50"),
        ],
    )

    snapshot = facts["fact_price_snapshot_daily"]
    first = one(snapshot.filter(f"date_key = {DAY_KEY}"))

    assert snapshot.count() == 2
    assert [first[k] for k in ("open_price", "close_price", "min_price", "max_price", "avg_price")] == [
        Decimal("100"), Decimal("120"), Decimal("80"), Decimal("120"), Decimal("100"),
    ]  # fmt: skip
    assert (first["first_list_price"], first["last_list_price"]) == (Decimal("150"), Decimal("160"))
    assert (first["observation_count"], first["distinct_price_count"]) == (3, 3)
    assert first["in_stock_ratio"] == pytest.approx(2 / 3)
    assert first["last_observation_id"] == "obs-o1-20"
    assert one(snapshot.filter(f"date_key = {NEXT_DAY_KEY}"))["observation_count"] == 1


def test_the_price_snapshot_reconciles_with_the_organic_sightings_and_the_price_mart(spark):
    rows = [
        sighting("o1", 0, price="100"),
        sighting("o1", 30, price="110"),
        sighting("o2", 1, price="70"),
        sighting("o2", 2, price="1", is_advertisement=True),
        sighting("o1", 24 * 60, price="90"),
    ]
    sightings = frame(spark, rows)
    _, facts = build(spark, rows)

    organic = facts["fact_offer_observation"].filter("NOT coalesce(is_advertisement, false)").count()
    snapshot = facts["fact_price_snapshot_daily"]

    assert snapshot.agg({"observation_count": "sum"}).first()[0] == organic == 4
    offers = {r.offer_key: r.offer_id for r in build_marketplace_dimensions(sightings)["dim_product_listing"].select("offer_key", "offer_id").collect()}
    mart = build_offer_price_history_daily(sightings.filter("NOT coalesce(is_advertisement, false)"))
    assert sorted(
        (offers[r.offer_key], r.open_price, r.close_price, r.min_price, r.max_price, r.observation_count)
        for r in snapshot.collect()
    ) == sorted(
        (r.offer_id, r.first_price, r.last_price, r.min_price, r.max_price, r.observation_count) for r in mart.collect()
    )


# ---------- fact_price_change ----------


def test_a_price_change_is_a_sighting_whose_price_differs_from_the_previous_one(spark):
    _, facts = build(
        spark,
        [
            sighting("o1", 0, price="100"),
            sighting("o1", 10, price="100"),
            sighting("o1", 20, price="80"),
            sighting("o1", 50, price="120"),
            sighting("o1", 60, price="40"),
        ],
    )

    changes = facts["fact_price_change"].orderBy("changed_at").collect()

    assert [(c.old_price, c.new_price, c.price_delta, c.direction) for c in changes] == [
        (Decimal("100"), Decimal("80"), Decimal("-20"), "DOWN"),
        (Decimal("80"), Decimal("120"), Decimal("40"), "UP"),
        (Decimal("120"), Decimal("40"), Decimal("-80"), "DOWN"),
    ]
    assert [c.pct_change for c in changes] == [Decimal("-0.2"), Decimal("0.5"), Decimal("-0.666667")]
    assert [c.minutes_since_previous for c in changes] == [10.0, 30.0, 10.0]
    assert [c.is_large_drop for c in changes] == [False, False, True]
    assert {c.change_rule_version for c in changes} == {RULES.change_rule_version}
    assert changes[0].previous_observation_id == "obs-o1-10"
    assert changes[0].observation_id == "obs-o1-20"


def test_an_absolute_drop_is_large_whatever_its_ratio(spark):
    _, facts = build(
        spark,
        [sighting("o1", 0, price="10000000"), sighting("o1", 5, price="9998000")],
    )

    assert one(facts["fact_price_change"])["is_large_drop"] is True


def test_a_change_between_currencies_has_no_ratio_and_is_never_a_large_drop(spark):
    _, facts = build(
        spark,
        [sighting("o1", 0, price="1000"), sighting("o1", 5, price="10", currency="USD")],
    )

    change = one(facts["fact_price_change"])

    assert (change["previous_currency"], change["currency"]) == ("VND", "USD")
    assert change["pct_change"] is None and change["is_large_drop"] is False


def test_sponsored_sightings_do_not_make_price_changes_and_the_count_matches_the_mart(spark):
    rows = [
        sighting("o1", 0, price="100"),
        sighting("o1", 5, price="1", is_advertisement=True),
        sighting("o1", 10, price="90"),
        sighting("o2", 1, price="10"),
        sighting("o2", 2, price="20"),
        sighting("o2", 3, price="20"),
    ]
    sightings = frame(spark, rows)
    _, facts = build(spark, rows)

    mart = build_offer_change_daily(sightings.filter("NOT coalesce(is_advertisement, false)"))

    assert facts["fact_price_change"].count() == mart.agg({"price_change_count": "sum"}).first()[0] == 2


# ---------- fact_counter_daily ----------


def test_the_counter_fact_keeps_the_mart_rules_for_valid_and_invalid_transitions(spark):
    rows = [
        sighting("o1", 0, sold_count=10, review_count=1),
        sighting("o1", 30, sold_count=14, review_count=1),
        sighting("o1", 60, sold_count=12, review_count=2),
        sighting("o1", 24 * 60, sold_count=20, review_count=2),
    ]
    sightings = frame(spark, rows)
    _, facts = build(spark, rows)

    mart = build_counter_delta_daily(
        build_counter_transitions(sightings, max_gap_seconds=RULES.counter_max_gap_seconds, rule_version=RULES.counter_rule_version)
    ).collect()
    fact = facts["fact_counter_daily"]

    assert tuple(fact.columns) == dataset("fact_counter_daily").columns
    assert fact.count() == len(mart) == 6
    sold = one(fact.filter(f"date_key = {DAY_KEY} AND counter_name = 'sold_count'"))
    assert (sold["first_value"], sold["last_value"], sold["raw_delta_sum"], sold["valid_delta_sum"]) == (10, 12, 2, 4)
    assert sold["counter_reset_or_invalid"] is True and "COUNTER_DECREASED" in sold["invalid_reasons_json"]
    by_key = {
        (r.date_key, r.counter_name): (r.first_value, r.last_value, r.valid_delta_sum, r.invalid_transition_count)
        for r in fact.collect()
    }
    from_mart = {
        (int(r.observed_date.strftime("%Y%m%d")), r.counter_name): (r.first_value, r.last_value, r.valid_delta_sum, r.invalid_transition_count)
        for r in mart
    }
    assert by_key == from_mart


# ---------- fact_category_page_daily ----------


def listed(spark, count):
    return spark.createDataFrame(
        [("tiki", "1846", count, DAY + timedelta(hours=1))],
        "marketplace string, category_id string, listed_product_count bigint, observed_at timestamp",
    )


def test_a_category_page_day_counts_sightings_and_judges_prices_over_organic_offers(spark):
    dims, facts = build(
        spark,
        [
            sighting("o1", 0, price="100", request_target=PAGE, request_page=1, seller_id="s1", brand_id="b1", availability="IN_STOCK"),
            sighting("o1", 5, price="120", request_target=PAGE, request_page=1, seller_id="s1", brand_id="b1", availability="IN_STOCK"),
            sighting(
                "o2", 1, price="200", list_price="250", discount_percent="20", request_target=PAGE, request_page=2,
                seller_id="s2", availability="UNKNOWN",
            ),
            sighting("o3", 2, price="10", request_target=PAGE, request_page=1, is_advertisement=True, seller_id="s3"),
            sighting("o4", 3, price="55"),
        ],
    )
    page = one(facts["fact_category_page_daily"])
    category = dims["dim_category"].filter("category_id = '1846'").first().category_key

    assert page["category_key"] == category and page["date_key"] == DAY_KEY and page["currency"] == "VND"
    assert (page["crawled_page_count"], page["observation_count"]) == (2, 4)
    assert (page["observed_offer_count"], page["advertisement_count"]) == (3, 1)
    assert page["seller_count"] == 3 and page["brand_count"] == 1
    assert (page["min_price"], page["max_price"], page["avg_price"]) == (Decimal("120"), Decimal("200"), Decimal("160"))
    assert (page["discounted_offer_count"], page["discounted_offer_ratio"]) == (1, 0.5)
    assert page["median_discount_percent"] == Decimal("20")
    assert page["in_stock_ratio"] == 0.5
    assert page["listed_product_count"] is None
    # The sighting on no category page (o4) is on no row of this fact.
    assert facts["fact_category_page_daily"].count() == 1


def test_the_category_size_a_page_states_is_the_days_last_value(spark):
    sightings = frame(spark, [sighting("o1", 0, request_target=PAGE, request_page=1)])
    dims = build_marketplace_dimensions(sightings)
    counts = spark.createDataFrame(
        [
            ("tiki", "1846", 4000, DAY + timedelta(hours=1)),
            ("tiki", "1846", 5000, DAY + timedelta(hours=3)),
            ("tiki", "1846", 4800, DAY + timedelta(days=1, hours=1)),
            ("tiki", "999", 7, DAY + timedelta(hours=1)),
        ],
        "marketplace string, category_id string, listed_product_count bigint, observed_at timestamp",
    )

    facts = build_marketplace_facts(sightings, dims["dim_product_listing"], counts, None, RULES)

    assert one(facts["fact_category_page_daily"])["listed_product_count"] == 5000


def test_a_sighting_whose_page_is_not_a_category_is_on_no_category_page_row(spark):
    _, facts = build(spark, [sighting("o1", 0), sighting("o2", 1, request_target="search?q=x")])

    assert facts["fact_category_page_daily"].count() == 0


# ---------- fact_crawl_attempt ----------


def test_a_crawl_attempt_takes_its_marketplace_and_category_from_the_frontier(spark):
    attempts, _runs = _audit(spark, [("run-1", "tiki", date(2026, 10, 25), "SUCCEEDED", 120, 4096, 30, 1, None)])

    fact = build_fact_crawl_attempt(attempts)

    assert tuple(fact.columns) == dataset("fact_crawl_attempt").columns
    attempt = one(fact)
    assert attempt["marketplace_key"] == one(spark.createDataFrame([("tiki",)], "m string").select(key_or_unknown("m").alias("k")))["k"]
    assert attempt["category_key"] == one(spark.createDataFrame([("tiki", "1846")], "m string, c string").select(key_or_unknown("m", "c").alias("k")))["k"]
    assert attempt["date_key"] == 20261025
    assert (attempt["status"], attempt["parsed_count"], attempt["rejected_count"], attempt["latency_ms"]) == ("SUCCEEDED", 30, 1, 120)


def test_attempts_are_a_fact_only_when_the_audit_is_given(spark):
    sightings = frame(spark, [sighting("o1", 0)])
    dims = build_marketplace_dimensions(sightings)
    attempts, _runs = _audit(spark, [("run-1", "tiki", date(2026, 10, 25), "FAILED", 5, 0, 0, 0, "UNKNOWN")])

    facts = build_marketplace_facts(sightings, dims["dim_product_listing"], None, attempts, RULES)

    assert "fact_crawl_attempt" in facts and facts["fact_crawl_attempt"].count() == 1


# ---------- every fact ----------

MIXED = [
    sighting(f"o{i}", i * 7, price=str(100 - i), seller_id=f"s{i % 2}", request_target=PAGE, request_page=1 + i % 2, sold_count=i)
    for i in range(6)
] + [sighting("o0", 90, price="70", sold_count=9, request_target=PAGE, request_page=1)]


def snapshot(facts):
    return {name: rows_of(fact, *fact.columns) for name, fact in facts.items()}


def test_keys_are_unique_in_every_fact(spark):
    _, facts = build(spark, MIXED)

    for name, fact in facts.items():
        key = list(dataset(name).key)
        assert fact.groupBy(*key).count().filter("count > 1").count() == 0, name


def test_building_again_from_reordered_input_gives_the_same_rows(spark):
    _, facts = build(spark, MIXED)

    _, again = build(spark, list(reversed(MIXED)))

    assert snapshot(again) == snapshot(facts)


def test_the_rules_come_from_the_batch_context():
    from ecomdata.processing.batch.context import MarketplaceBatchContext

    context = MarketplaceBatchContext(
        "run-1", datetime(2026, 9, 4, tzinfo=UTC), "memory://silver", "memory://gold",
        change_rule_version="speed-rules.v9", large_drop_absolute=Decimal("5"), large_drop_relative=Decimal("0.1"),
    )  # fmt: skip

    rules = FactRules.from_context(context)

    assert (rules.change_rule_version, rules.large_drop_absolute, rules.large_drop_relative) == (
        "speed-rules.v9", Decimal("5"), Decimal("0.1"),
    )
