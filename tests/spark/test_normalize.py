"""Silver normalisation of Tiki and Lazada records: canonical columns, ids, quarantine, drift."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from ecomdata.domain.acquisition import FetchBundle, FetchPart, ListingPageRequest
from ecomdata.domain.identity import make_observation_id, make_offer_id, make_seller_id
from ecomdata.domain.observations import ResourceType, create_raw_artifact
from ecomdata.domain.records import build_records, record_json
from ecomdata.domain.sources import LAZADA, SOURCES, TIKI
from ecomdata.ingestion.sources.lazada.extractor import LazadaExtractor
from ecomdata.ingestion.sources.tiki.extractor import TikiExtractor
from ecomdata.processing.normalize import common
from ecomdata.processing.normalize.sources import lazada as lazada_norm
from ecomdata.processing.normalize.sources import tiki as tiki_norm
from tests.paths import FIXTURES
from tests.spark_support import requires_spark

pytestmark = requires_spark

FETCHED = datetime(2026, 10, 25, 8, 30, 15, 123456, tzinfo=UTC)
TIKI_PAGE = (FIXTURES / "sources" / "tiki" / "category_page.html").read_bytes()
LAZADA_LISTING = (FIXTURES / "sources" / "lazada" / "listing_xhr.json").read_bytes()
BODY_SHA = "ab" * 32


def utc(value):
    """Spark hands timestamps back as naive local time; the instant is what matters."""
    return value.astimezone(UTC)


def artifact(spec, url, fetched=FETCHED):
    return create_raw_artifact(
        marketplace_code=spec.code, crawl_run_id="run-1", marketplace_id=spec.marketplace_id, request_url=url,
        resource_type=ResourceType.LISTING_PAGE, fetched_at=fetched, http_status=200, content_type="text/html",
        body_sha256=BODY_SHA, raw_uri=f"file:///lake/{spec.code}/part-0.bin.gz", adapter_version=spec.adapter_version,
        raw_bytes=10,
    )


def tiki_records(extra_rows=(), page=TIKI_PAGE, fetched=FETCHED):
    url = "https://tiki.vn/dien-thoai-may-tinh-bang/c1789"
    bundle = FetchBundle(url, fetched, (FetchPart(url, 200, "text/html", page),))
    extracted = TikiExtractor().extract(bundle)
    request = ListingPageRequest("tiki", TIKI.marketplace_id, "dien-thoai-may-tinh-bang/c1789", 1)
    return build_records(
        TIKI, request=request, raw_artifact=artifact(TIKI, url, fetched), rows=(*extracted.rows, *extra_rows),
        page_context=extracted.page_context,
    )


def lazada_records():
    url = "https://www.lazada.vn/dien-thoai-di-dong/?page=1"
    bundle = FetchBundle(
        url, FETCHED,
        (FetchPart(url, 200, "text/html", b"<html>"), FetchPart(url + "&ajax=true", 200, "application/json", LAZADA_LISTING, role="xhr")),
    )
    extracted = LazadaExtractor().extract(bundle)
    request = ListingPageRequest("lazada", LAZADA.marketplace_id, "dien-thoai-di-dong", 1)
    return build_records(
        LAZADA, request=request, raw_artifact=artifact(LAZADA, url), rows=extracted.rows, page_context=extracted.page_context
    )


def records_frame(spark, records):
    """Bronze/records lines as the pipeline writes them, decoded by the Spark reader.

    The host's Spark has no filesystem (no winutils), so the lines are handed
    over in memory; reading real files is exercised in the Docker stack.
    """
    return common.decode_records(spark.createDataFrame([(record_json(r),) for r in records], "value string"))


@pytest.fixture
def tiki_frame(spark):
    return records_frame(spark, tiki_records())


def test_the_reader_splits_the_envelope_from_the_untouched_payload(spark):
    frame = records_frame(spark, tiki_records())

    rows = {row.record_kind: row for row in frame.collect() if row.record_kind == "page_context"}
    listing = [row for row in frame.collect() if row.record_kind == "listing_row"]
    assert len(listing) == 5 and len(rows) == 1
    assert {row.source_code for row in listing} == {"tiki"} and {utc(row.fetched_at) for row in listing} == {FETCHED}
    assert sorted(json.loads(row.payload_json)["id"] for row in listing)[0] == 278890829
    assert json.loads(rows["page_context"].payload_json)["categoryData"]["id"] == 1789
    assert common.listing_rows(frame).count() == 5 and common.page_contexts(frame).count() == 1


def test_a_row_with_another_envelope_version_never_reaches_a_normaliser(spark):
    good = record_json(tiki_records()[1])
    stale = json.dumps({**json.loads(good), "schema_version": "marketplace-record.v0"})
    frame = common.decode_records(spark.createDataFrame([(good,), (stale,)], "value string"))

    assert common.unsupported_envelopes(frame).count() == 1
    assert common.listing_rows(frame).count() == 1


def test_the_canonical_columns_are_exactly_the_declared_ones_with_their_types(spark, tiki_frame):
    valid, _ = common.split_quarantine(tiki_norm.to_offer_observation(tiki_frame))

    assert [(f.name, f.dataType.simpleString()) for f in valid.schema.fields] == [
        (name, spark.sql(f"select cast(null as {dtype}) c").schema[0].dataType.simpleString())
        for name, dtype in common.CANONICAL_COLUMNS
    ]


def test_ids_equal_the_python_identity_functions(spark, tiki_frame):
    valid, _ = common.split_quarantine(tiki_norm.to_offer_observation(tiki_frame))

    for row in valid.collect():
        assert row.offer_id == make_offer_id("tiki", row.platform_listing_id)
        organic_form = make_observation_id("tiki", row.platform_listing_id, FETCHED, BODY_SHA)
        # A sponsored row also carries its display position, so it never collides with an organic one.
        assert (row.observation_id != organic_form) == bool(row.is_advertisement)
        assert row.event_id == row.observation_id
        assert row.partition_key == f"tiki:{row.platform_listing_id}"


def test_a_tiki_row_maps_to_the_pre_elt_observation_fields(spark, tiki_frame):
    valid, _ = common.split_quarantine(tiki_norm.to_offer_observation(tiki_frame))
    row = {r.platform_listing_id: r for r in valid.collect()}["279411794"]

    assert row.product_title.startswith("Máy tính bảng Samsung Galaxy Tab A11")
    assert row.current_price == Decimal("4190000") and row.list_price == Decimal("6990000")
    assert row.discount_amount == Decimal("2800000") and row.discount_percent == Decimal("40")
    assert row.brand == "Samsung" and row.seller_name == "Tiki Trading" and row.brand_id == "18802"
    assert row.currency == "VND" and row.marketplace == "tiki" and row.marketplace_id == "marketplace-tiki"
    assert row.source_url == "https://tiki.vn/may-tinh-bang-samsung-galaxy-tab-a11-4g-4gb-64gb-p279411794.html"
    assert row.availability == "IN_STOCK" and row.active_status == "UNKNOWN"
    assert utc(row.observed_at) == utc(row.fetched_at) == utc(row.first_seen_at) == FETCHED
    assert row.seller_id == make_seller_id("tiki", "1")
    assert row.observed_date.isoformat() == "2026-10-25"
    assert row.rating_scale == Decimal("5") and row.category_ids == ["1789", "1794"]
    assert row.raw_sha256 == BODY_SHA and row.raw_uri.endswith("part-0.bin.gz") and row.crawl_run_id == "run-1"
    assert row.category_path == "1/2/1789/1794"  # Tiki's own path, not the crawl target
    assert row.shipping_price is None and row.promotion_json is None


def test_the_advertisement_is_kept_and_marked_and_ranks_are_numbered_among_organic_rows(spark, tiki_frame):
    valid, _ = common.split_quarantine(tiki_norm.to_offer_observation(tiki_frame))
    rows = sorted(valid.collect(), key=lambda r: r.search_rank)

    assert [r.is_advertisement for r in rows] == [True, False, False, False, False]
    assert [r.search_rank for r in rows] == [1, 2, 3, 4, 5]
    assert [r.ranking_position for r in rows] == [None, 1, 2, 3, 4]


def test_a_row_without_an_id_or_a_price_is_quarantined_with_its_reason(spark):
    broken = [{"name": "no id", "price": 10}, {"id": 5, "name": "no price"}, {"id": 6, "price": 7}, {"id": 7, "name": "neg", "price": -1}]
    frame = records_frame(spark, tiki_records(extra_rows=broken))

    valid, quarantined = common.split_quarantine(tiki_norm.to_offer_observation(frame))

    assert valid.count() == 5
    reasons = sorted((r.reason, json.loads(r.payload_json).get("name") or r.payload_json) for r in quarantined.collect())
    assert [reason for reason, _ in reasons] == [
        "missing_listing_id", "missing_or_invalid_price", "missing_or_invalid_price", "missing_title"
    ]
    row = quarantined.filter("reason = 'missing_listing_id'").collect()[0]
    assert (row.source_code, row.crawl_run_id, row.raw_artifact_id) == ("tiki", "run-1", tiki_records()[0].raw_artifact_id)
    assert row.record_id and utc(row.fetched_at) == FETCHED and row.request_target == "dien-thoai-may-tinh-bang/c1789"


def test_a_field_of_an_unexpected_type_costs_only_that_field(spark):
    odd = {"id": 8, "name": "odd types", "price": 100, "rating_average": "not a number", "seller_id": 3, "discount": "x"}
    frame = records_frame(spark, tiki_records(extra_rows=[odd]))

    valid, quarantined = common.split_quarantine(tiki_norm.to_offer_observation(frame))
    row = {r.platform_listing_id: r for r in valid.collect()}["8"]

    assert quarantined.count() == 0
    assert row.current_price == Decimal("100") and row.rating_value is None and row.discount_amount is None


def test_the_wide_table_keeps_every_declared_field_typed_and_the_raw_payload(spark, tiki_frame):
    wide = tiki_norm.to_wide(tiki_frame)
    row = {r.p.id: r for r in wide.collect()}[279411794]

    assert row.p.name.startswith("Máy tính bảng") and row.p.price == 4190000
    assert row.p.category_ids == [1789, 1794] and row.p.seller_id == 1
    assert json.loads(row.payload_json)["id"] == 279411794
    assert len(tiki_norm.PAYLOAD_SCHEMA.fields) >= 40


def test_drift_names_the_keys_the_schema_does_not_know_and_counts_them(spark):
    new = [{"id": 11, "name": "n", "price": 1, "brand_new_field": 1}, {"id": 12, "name": "m", "price": 2, "brand_new_field": 2, "other": {"x": 1}}]
    frame = records_frame(spark, tiki_records(extra_rows=new))

    drift = common.payload_drift(common.listing_rows(frame), tiki_norm.PAYLOAD_SCHEMA)

    assert [(r.key, r.occurrences) for r in drift.collect()] == [("brand_new_field", 2), ("other", 1)]


def test_the_fixture_has_no_drift_against_its_declared_schema(spark, tiki_frame):
    assert common.payload_drift(common.listing_rows(tiki_frame), tiki_norm.PAYLOAD_SCHEMA).count() == 0


def test_a_lazada_row_maps_to_the_same_canonical_columns(spark):
    frame = records_frame(spark, lazada_records())

    valid, quarantined = common.split_quarantine(lazada_norm.to_offer_observation(frame))
    rows = sorted(valid.collect(), key=lambda r: r.search_rank)

    assert quarantined.count() == 0 and len(rows) == 5
    first = rows[0]
    assert first.marketplace == "lazada" and first.currency == "VND" and first.source_code == "lazada"
    assert first.platform_listing_id == first.sku.split("_")[0] and first.current_price > 0
    assert first.offer_id == make_offer_id("lazada", first.platform_listing_id)
    assert first.observation_id == make_observation_id("lazada", first.platform_listing_id, FETCHED, BODY_SHA)
    assert first.seller_id == make_seller_id("lazada", "200194944463")
    assert first.source_url.startswith("https://www.lazada.vn/products/pdp-i")
    assert first.category_ids == ["4402", "4518"] and first.category_path == "dien-thoai-di-dong"
    assert first.availability == "IN_STOCK" and first.location == "Hong Kong"
    assert [r.is_advertisement for r in rows] == [False, False, False, False, True]
    assert [r.ranking_position for r in rows] == [1, 2, 3, 4, None]


def test_lazada_discount_and_original_price_are_read_from_their_text_forms(spark):
    rows = list(lazada_records())
    priced = {**dict(rows[1].payload), "discount": "6% Off", "originalPrice": "4690000", "price": "4390000", "itemId": "999"}
    changed = rows[1].__class__(**{**{n: getattr(rows[1], n) for n in rows[1].__dataclass_fields__}, "payload": priced})
    frame = records_frame(spark, [rows[0], changed])

    valid, _ = common.split_quarantine(lazada_norm.to_offer_observation(frame))
    row = {r.platform_listing_id: r for r in valid.collect()}["999"]

    assert row.list_price == Decimal("4690000") and row.discount_percent == Decimal("6")


def test_the_registry_resolves_each_normaliser_module(spark):
    assert SOURCES.normalizer("tiki") is tiki_norm
    assert SOURCES.normalizer("lazada") is lazada_norm
    for module in (tiki_norm, lazada_norm):
        assert callable(module.to_wide) and callable(module.to_offer_observation) and module.PAYLOAD_SCHEMA.fields
        # What the star reads from each source (processing/batch/star_inputs.py).
        assert callable(module.listing_attributes) and callable(module.category_names)
        assert callable(module.category_paths)
