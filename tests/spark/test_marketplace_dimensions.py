"""Dimensions of the marketplace star, docs/DATA_MODEL.md sections 4.4-4.6.

Local Spark over small frozen observation fixtures, the same ones the mart
tests use. No MinIO, PostgreSQL or network.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pyspark.sql import functions as F

from ecomdata.processing.batch.dim_date import build_dim_date, vn_holidays
from ecomdata.processing.batch.dimensions import (
    build_bridge_category_ancestry,
    build_dim_category,
    build_dim_marketplace,
    build_dim_offer_status,
    build_dim_product_listing,
    build_dim_seller,
    build_marketplace_dimensions,
    offer_status_key,
)
from ecomdata.processing.batch.keys import OPEN_VALID_TO, UNKNOWN_KEY, UNKNOWN_NATURAL_KEY
from ecomdata.domain.datasets import dataset
from tests.spark_support import requires_spark
from tests.spark.test_marketplace_marts import DAY, observation, observations

pytestmark = requires_spark

PATH_4 = "1/2/1846/8095"
PATH_8 = "1/2/1846/8095/2458/3001/3002/3003"


def known(frame, key):
    return frame.filter(f"{key} != {UNKNOWN_KEY}")


def collectable(frame):
    """Timestamps as UTC strings.

    PySpark turns a collected timestamp into a local datetime, and on Windows
    that fails for the open valid_to in year 9999.
    """
    return frame.select(*[F.date_format(f.name, "yyyy-MM-dd HH:mm:ss").alias(f.name)
                          if f.dataType.typeName() == "timestamp" else F.col(f.name) for f in frame.schema.fields])


def versions(spark, rows, offer="offer-1"):
    dim = collectable(build_dim_product_listing(observations(spark, rows)))
    return sorted((r.asDict() for r in dim.filter(f"offer_id = '{offer}'").collect()), key=lambda r: r["version"])


def utc(moment):
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def xxhash64(spark, *values):
    placeholders = ", ".join(f"v{i}" for i in range(len(values)))
    schema = ", ".join(f"v{i} {'timestamp' if isinstance(v, datetime) else 'string'}" for i, v in enumerate(values))
    return spark.createDataFrame([values], schema).selectExpr(f"xxhash64({placeholders}) AS k").first().k


# ---------- every dimension ----------

MIXED = [observation(offer=f"o{i}", minute=i, seller_id=f"s{i % 2}", category_path=PATH_8 if i % 2 else PATH_4,
                     product_title="T" if i < 4 else "Renamed") for i in range(6)]


@pytest.fixture(scope="module")
def mixed_dimensions(spark):
    return build_marketplace_dimensions(observations(spark, MIXED))


def test_every_dimension_has_the_registry_columns_and_one_unknown_row(mixed_dimensions):
    dims = mixed_dimensions
    assert set(dims) == {"dim_date", "dim_marketplace", "dim_seller", "dim_category",
                         "bridge_category_ancestry", "dim_product_listing", "dim_offer_status"}
    for name, frame in dims.items():
        entry = dataset(name)
        assert tuple(frame.columns) == entry.columns, name
        if entry.kind == "dim":
            assert frame.filter(f"{entry.key[0]} = {UNKNOWN_KEY}").count() == 1, name


def test_keys_are_unique_in_every_dimension(mixed_dimensions):
    for name, frame in mixed_dimensions.items():
        key = list(dataset(name).key)
        assert frame.groupBy(*key).count().filter("count > 1").count() == 0, name


def test_building_again_from_reordered_input_gives_the_same_rows(spark, mixed_dimensions):
    def snapshot(dims):
        return {name: sorted((tuple(r) for r in collectable(frame).collect()), key=repr)
                for name, frame in dims.items() if name != "dim_date"}

    again = build_marketplace_dimensions(observations(spark, list(reversed(MIXED))))
    assert snapshot(again) == snapshot(mixed_dimensions)


# ---------- dim_date ----------

def test_dim_date_is_a_fixed_calendar_with_vietnamese_retail_flags(spark):
    dim = build_dim_date(spark, date(2026, 1, 1), date(2026, 12, 31))
    assert dim.count() == 365 + 1
    rows = {r.full_date: r for r in dim.filter("date_key != -1").collect()}
    assert rows[date(2026, 10, 24)].date_key == 20261024
    assert rows[date(2026, 10, 24)].is_weekend  # a Saturday
    assert rows[date(2026, 10, 10)].is_double_day_sale
    assert not rows[date(2026, 10, 11)].is_double_day_sale
    assert rows[date(2026, 2, 17)].is_vn_holiday  # first day of Tet
    assert rows[date(2026, 9, 2)].is_vn_holiday
    assert not rows[date(2026, 9, 3)].is_vn_holiday
    assert rows[date(2026, 10, 24)].week_of_year == 43


def test_dim_date_default_range_covers_both_stars(spark):
    dim = build_dim_date(spark)
    bounds = dim.filter("date_key != -1").agg({"full_date": "min"}).first()[0], dim.agg({"full_date": "max"}).first()[0]
    assert bounds == (date(2019, 1, 1), date(2030, 12, 31))


def test_holidays_refuse_a_year_without_recorded_lunar_dates():
    assert date(2025, 1, 29) in vn_holidays(date(2025, 1, 1), date(2025, 12, 31))
    with pytest.raises(ValueError, match="2031"):
        vn_holidays(date(2031, 1, 1), date(2031, 12, 31))


# ---------- dim_marketplace, dim_seller ----------

def test_dim_marketplace_takes_attributes_from_configuration(spark):
    rows = [observation(minute=3), observation(minute=1)]
    (tiki,) = collectable(known(build_dim_marketplace(observations(spark, rows)), "marketplace_key")).collect()
    assert (tiki.marketplace_code, tiki.marketplace_name, tiki.country, tiki.default_currency) == ("tiki", "Tiki", "VN", "VND")
    assert tiki.first_crawled_at == utc(DAY + timedelta(minutes=1))
    assert tiki.marketplace_key == xxhash64(spark, "tiki")


def test_dim_seller_skips_missing_sellers_and_counts_distinct_listings(spark):
    rows = [observation(offer="a", minute=0, seller_id="s1"), observation(offer="a", minute=5, seller_id="s1"),
            observation(offer="b", minute=9, seller_id="s1"), observation(offer="c", minute=1, seller_id=None)]
    sellers = collectable(known(build_dim_seller(observations(spark, rows)), "seller_key")).collect()
    assert [(s.seller_id, s.observed_listing_count) for s in sellers] == [("s1", 2)]
    assert sellers[0].last_seen_at == utc(DAY + timedelta(minutes=9))
    assert sellers[0].seller_key == xxhash64(spark, "s1")


# ---------- dim_category and the closure ----------

def test_category_tree_four_and_eight_levels(spark):
    rows = [observation(offer="a", minute=0, category_path=PATH_4), observation(offer="b", minute=1, category_path=PATH_8)]
    dim = known(build_dim_category(observations(spark, rows)), "category_key")
    nodes = {r.category_id: r for r in dim.collect()}
    assert len(nodes) == 8  # the 4-level path is a prefix of the 8-level one
    assert nodes["1"].level == 1 and nodes["1"].parent_key is None
    assert nodes["3003"].level == 8 and nodes["3003"].full_path == PATH_8
    assert nodes["3003"].parent_key == nodes["3002"].category_key
    assert nodes["8095"].full_path == PATH_4
    assert not nodes["8095"].is_leaf  # 2458 hangs under it
    assert [k for k, n in nodes.items() if n.is_leaf] == ["3003"]
    assert nodes["8095"].category_key == xxhash64(spark, "tiki", "8095")


def test_closure_is_complete(spark):
    rows = [observation(offer="a", minute=0, category_path="1/2/10/11"),
            observation(offer="b", minute=1, category_path=PATH_8)]
    dim = build_dim_category(observations(spark, rows))
    bridge = build_bridge_category_ancestry(dim)
    ids = {r.category_key: r.category_id for r in dim.collect()}
    pairs = {(ids[r.ancestor_key], ids[r.descendant_key], r.depth) for r in bridge.collect()}
    # every node pairs with itself and each ancestor on its path
    expected = {(UNKNOWN_NATURAL_KEY, UNKNOWN_NATURAL_KEY, 0)}
    for path in ("1/2/10/11", PATH_8):
        parts = path.split("/")
        for i, descendant in enumerate(parts):
            for j in range(i + 1):
                expected.add((parts[j], descendant, i - j))
    assert pairs == expected
    leaf_ancestors = bridge.filter(bridge.descendant_key == next(k for k, v in ids.items() if v == "3003"))
    assert sorted(r.depth for r in leaf_ancestors.collect()) == list(range(8))


def test_a_node_under_two_parents_takes_the_latest_path(spark):
    rows = [observation(offer="a", minute=0, category_path="1/5/9"), observation(offer="b", minute=30, category_path="1/6/9")]
    nodes = {r.category_id: r for r in known(build_dim_category(observations(spark, rows)), "category_key").collect()}
    assert nodes["9"].full_path == "1/6/9"
    assert nodes["9"].parent_key == nodes["6"].category_key
    assert nodes["5"].is_leaf  # no chosen child any more


def test_slug_comes_from_the_frontier_target(spark):
    rows = [observation(minute=0, category_path="1/2/1846")]
    frontier = spark.createDataFrame([("tiki", "laptop-may-vi-tinh-linh-kien/c1846"), ("tiki", "1789")],
                                     "marketplace_code string, target string")
    nodes = {r.category_id: r for r in build_dim_category(observations(spark, rows), frontier).collect()}
    assert nodes["1846"].category_slug == "laptop-may-vi-tinh-linh-kien"
    assert nodes["2"].category_slug is None
    assert nodes["1846"].category_name is None  # names are not on the path (risk R1)


def test_an_observation_without_a_path_adds_no_node(spark):
    rows = [observation(minute=0, category_path=None), observation(minute=1, category_path="")]
    assert known(build_dim_category(observations(spark, rows)), "category_key").count() == 0


# ---------- dim_offer_status ----------

def test_offer_status_is_36_combinations_and_the_expression_agrees(spark):
    dim = build_dim_offer_status(spark)
    real = known(dim, "offer_status_key")
    assert real.count() == 36
    assert sorted(r.offer_status_key for r in real.collect()) == list(range(1, 37))
    computed = real.select("offer_status_key", offer_status_key(
        "availability", "active_status", discount=real.has_discount, rating=real.has_rating).alias("k"))
    assert computed.filter("offer_status_key != k").count() == 0


def test_offer_status_flags_come_from_the_observation(spark):
    rows = [observation(minute=0, availability="IN_STOCK", list_price=Decimal("120"), rating_count=0),
            observation(minute=1, availability="OUT_OF_STOCK", rating_count=5, active_status="INACTIVE"),
            observation(minute=2, availability=None)]
    keys = [r.k for r in observations(spark, rows).orderBy("observed_at").select(offer_status_key().alias("k")).collect()]
    dim = {r.offer_status_key: r for r in build_dim_offer_status(spark).collect()}
    assert (dim[keys[0]].availability, dim[keys[0]].has_discount, dim[keys[0]].has_rating) == ("IN_STOCK", True, False)
    assert (dim[keys[1]].active_status, dim[keys[1]].has_discount, dim[keys[1]].has_rating) == ("INACTIVE", False, True)
    assert keys[2] == UNKNOWN_KEY


# ---------- dim_product_listing: SCD2 ----------

def test_a_single_sighting_is_one_open_version(spark):
    (v,) = versions(spark, [observation(minute=0)])
    assert (v["version"], v["is_current"], v["valid_from"]) == (1, True, utc(DAY))
    assert v["valid_to"] == utc(OPEN_VALID_TO)
    assert v["listing_key"] == xxhash64(spark, "offer-1", DAY)
    assert v["offer_key"] == xxhash64(spark, "offer-1")


def test_a_price_change_alone_opens_no_version(spark):
    rows = [observation(minute=0, price="100"), observation(minute=5, price="90")]
    assert len(versions(spark, rows)) == 1


def test_a_renamed_title_opens_a_version_that_closes_the_last(spark):
    rows = [observation(minute=0), observation(minute=5), observation(minute=10, product_title="New title")]
    v1, v2 = versions(spark, rows)
    assert v1["valid_to"] == v2["valid_from"] == utc(DAY + timedelta(minutes=10))
    assert (v1["is_current"], v2["is_current"]) == (False, True)
    assert v2["product_title"] == "New title"
    assert v1["offer_key"] == v2["offer_key"] and v1["listing_key"] != v2["listing_key"]


def test_a_brand_appearing_opens_a_version(spark):
    rows = [observation(minute=0, brand=None), observation(minute=5, brand="Acme")]
    assert [v["brand"] for v in versions(spark, rows)] == [None, "Acme"]


def test_a_new_seller_opens_a_version(spark):
    rows = [observation(minute=0, seller_id="s1"), observation(minute=5, seller_id="s2")]
    v1, v2 = versions(spark, rows)
    assert (v1["seller_key"], v2["seller_key"]) == (xxhash64(spark, "s1"), xxhash64(spark, "s2"))


def test_a_lost_seller_points_to_the_unknown_member(spark):
    rows = [observation(minute=0, seller_id="s1"), observation(minute=5, seller_id=None)]
    assert [v["seller_key"] for v in versions(spark, rows)] == [xxhash64(spark, "s1"), UNKNOWN_KEY]


def test_a_new_category_opens_a_version_keyed_by_the_leaf(spark):
    rows = [observation(minute=0, category_path="1/2/3"), observation(minute=5, category_path="1/2/4")]
    v1, v2 = versions(spark, rows)
    assert (v1["category_key"], v2["category_key"]) == (xxhash64(spark, "tiki", "3"), xxhash64(spark, "tiki", "4"))


def test_a_value_that_returns_is_a_third_version(spark):
    rows = [observation(minute=0, product_title="A"), observation(minute=5, product_title="B"),
            observation(minute=10, product_title="A")]
    assert [(v["version"], v["product_title"]) for v in versions(spark, rows)] == [(1, "A"), (2, "B"), (3, "A")]


def test_two_sightings_at_one_instant_keep_the_last_by_tie_break(spark):
    rows = [observation(minute=0, product_title="A"),
            observation(minute=5, product_title="B", observation_id="obs-x"),
            observation(minute=5, product_title="A", observation_id="obs-y")]
    # obs-y sorts after obs-x, so the instant reads "A" and nothing changed.
    (v,) = versions(spark, rows)
    assert v["product_title"] == "A"

    rows[2] = observation(minute=5, product_title="A", observation_id="obs-a")
    v1, v2 = versions(spark, rows)  # obs-x now sorts last: "B" from minute 5
    assert (v2["product_title"], v2["valid_from"]) == ("B", utc(DAY + timedelta(minutes=5)))


def test_versions_neither_overlap_nor_leave_gaps(spark):
    titles = ["A", "A", "B", "C", "C", "B"]
    rows = [observation(minute=i * 7, product_title=t) for i, t in enumerate(titles)]
    vs = versions(spark, rows)
    assert [v["version"] for v in vs] == [1, 2, 3, 4]
    for earlier, later in zip(vs, vs[1:]):
        assert earlier["valid_to"] == later["valid_from"]
    assert sum(v["is_current"] for v in vs) == 1


def test_source_url_is_overwritten_on_every_version(spark):
    rows = [observation(minute=0, product_title="A", source_url="https://tiki.vn/old.html"),
            observation(minute=5, product_title="B", source_url="https://tiki.vn/new.html")]
    assert {v["source_url"] for v in versions(spark, rows)} == {"https://tiki.vn/new.html"}


def test_offers_are_versioned_independently(spark):
    rows = [observation(offer="a", minute=0, product_title="A"), observation(offer="b", minute=1, product_title="X"),
            observation(offer="a", minute=5, product_title="A2")]
    dim = known(build_dim_product_listing(observations(spark, rows)), "listing_key")
    counts = {r.offer_id: r["count"] for r in dim.groupBy("offer_id").count().collect()}
    assert counts == {"a": 2, "b": 1}
