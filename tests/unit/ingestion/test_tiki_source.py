"""Tiki: extractor, discovery and the lossless ELT claim (AC6) on the category-page fixture."""
from __future__ import annotations

import json
import re
from datetime import UTC, datetime

import pytest

from ecomdata.domain.acquisition import (
    ExtractedPage,
    FetchBlockedError,
    FetchBundle,
    FetchPart,
    ListingPageParseError,
    ListingPageRequest,
)
from ecomdata.domain.observations import ResourceType, create_raw_artifact
from ecomdata.domain.records import RecordKind, build_records
from ecomdata.domain.sources import SOURCES, TIKI
from ecomdata.ingestion.sources.tiki.discovery import TikiSitemapDiscovery, sitemap_locations
from ecomdata.ingestion.sources.tiki.extractor import PAGE_CONTEXT_KEYS, TikiExtractor
from tests.paths import FIXTURES

URL = "https://tiki.vn/dien-thoai-may-tinh-bang/c1789"
FETCHED = datetime(2026, 10, 25, 8, 30, tzinfo=UTC)
PAGE = (FIXTURES / "sources" / "tiki" / "category_page.html").read_bytes()


def source_catalog():
    """The fixture's catalog read without the extractor, as the oracle."""
    match = re.search(rb'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', PAGE, re.S)
    return json.loads(match.group(1))["props"]["initialState"]["catalog"]


def bundle(body=PAGE, status=200):
    return FetchBundle(URL, FETCHED, (FetchPart(URL, status, "text/html", body),))


def html_with(catalog):
    data = {"props": {"initialState": {"catalog": catalog}}}
    return b'<script id="__NEXT_DATA__" type="application/json">' + json.dumps(data).encode() + b"</script>"


def test_the_extractor_is_the_one_the_registry_names():
    assert isinstance(SOURCES.extractor("tiki")(), TikiExtractor)


def test_the_request_url_is_the_category_page_and_only_page_one_exists():
    extractor = TikiExtractor()

    assert extractor.request_url("https://tiki.vn", "laptop/c8095", 1) == "https://tiki.vn/laptop/c8095"
    assert extractor.request_url("https://tiki.vn/", "laptop/c8095", 1) == "https://tiki.vn/laptop/c8095"
    with pytest.raises(ValueError, match="only page 1"):
        extractor.request_url("https://tiki.vn", "laptop/c8095", 2)
    for target in ("c8095", "laptop", "../etc/passwd", "Laptop/c8095", "laptop/c8095?x=1"):
        with pytest.raises(ValueError, match="<slug>/c<id>"):
            extractor.request_url("https://tiki.vn", target, 1)


def test_every_source_row_comes_out_advertisements_included():
    page = TikiExtractor().extract(bundle())

    assert isinstance(page, ExtractedPage)
    assert len(page.rows) == len(source_catalog()["data"]) == 5
    assert sum(1 for row in page.rows if row.get("advertisement")) == 1


def test_rows_are_the_source_objects_not_a_selection_of_their_fields():
    page = TikiExtractor().extract(bundle())

    assert list(page.rows) == source_catalog()["data"]
    assert len(page.rows[1].keys()) > 30


def test_the_page_context_is_what_surrounds_the_rows():
    page = TikiExtractor().extract(bundle())
    catalog = source_catalog()

    assert set(page.page_context) == set(PAGE_CONTEXT_KEYS) & set(catalog)
    assert page.page_context["categoryData"]["id"] == 1789
    assert page.page_context["paging"] == catalog["paging"]
    assert "data" not in page.page_context
    assert page.last_page == 1


def test_records_hold_exactly_the_source_rows_deeply_equal():
    """AC6: listing_row count equals the source rows, payload equals the source object."""
    request = ListingPageRequest("tiki", "marketplace-tiki", "dien-thoai-may-tinh-bang/c1789", 1)
    raw = create_raw_artifact(
        marketplace_code="tiki", crawl_run_id="run-1", marketplace_id="marketplace-tiki", request_url=URL,
        resource_type=ResourceType.LISTING_PAGE, fetched_at=FETCHED, http_status=200, content_type="text/html",
        body_sha256="d" * 64, raw_uri="file:///lake/part-0.bin.gz", adapter_version=TIKI.adapter_version, raw_bytes=len(PAGE),
    )
    page = TikiExtractor().extract(bundle())

    records = build_records(TIKI, request=request, raw_artifact=raw, rows=page.rows, page_context=page.page_context)

    rows = [r for r in records if r.record_kind is RecordKind.LISTING_ROW]
    contexts = [r for r in records if r.record_kind is RecordKind.PAGE_CONTEXT]
    assert len(rows) == len(source_catalog()["data"])
    assert [dict(r.payload) for r in rows] == source_catalog()["data"]
    assert [r.record_index for r in rows] == list(range(len(rows)))
    assert len(contexts) == 1 and dict(contexts[0].payload) == page.page_context
    assert all(r.dropped_fields == () for r in records)


def test_a_page_without_next_data_is_a_block_not_a_parse_failure():
    # Tiki answers a rate-limited request with HTTP 200 and an obfuscated script.
    with pytest.raises(FetchBlockedError, match="__NEXT_DATA__") as blocked:
        TikiExtractor().extract(bundle(b"<html><script>var _0x1f=[];</script></html>"))

    assert "_0x1f" in blocked.value.evidence


@pytest.mark.parametrize(
    "body, message",
    [
        (b'<script id="__NEXT_DATA__">{not json</script>', "not valid"),
        (b'<script id="__NEXT_DATA__">{"props":{}}</script>', "no props.initialState.catalog"),
        (html_with({"data": "x"}), "must be a list"),
        (html_with({"data": []}), "no products"),
        (html_with({"data": [1]}), "not an object"),
        (html_with({}), "must be a list"),
    ],
)
def test_a_changed_page_shape_fails_loudly(body, message):
    with pytest.raises(ListingPageParseError, match=message):
        TikiExtractor().extract(bundle(body))


# --- discovery ----------------------------------------------------------------

INDEX = """<sitemapindex><sitemap><loc>https://tiki.vn/clover/sitemap_categories_index.xml</loc></sitemap>
<sitemap><loc>https://tiki.vn/clover/sitemap_products_index.xml</loc></sitemap></sitemapindex>"""
CATEGORIES_INDEX = """<sitemapindex><sitemap><loc>https://tiki.vn/clover/sitemap_categories_1.xml</loc></sitemap>
<sitemap><loc>https://tiki.vn/clover/sitemap_categories_2.xml</loc></sitemap>
<sitemap><loc>https://elsewhere.test/sitemap_categories_3.xml</loc></sitemap></sitemapindex>"""
FIRST = """<urlset><url><loc>https://tiki.vn/xe-dap/c8431</loc></url><url><loc>https://tiki.vn/laptop/c8095</loc></url>
<url><loc>https://tiki.vn/not-a-category</loc></url></urlset>"""
SECOND = """<urlset><url><loc>https://tiki.vn/dien-thoai/c1789</loc></url><url><loc>https://tiki.vn/laptop/c8095</loc></url></urlset>"""


def fake_fetch(requested):
    pages = {
        "https://tiki.vn/clover/sitemap_index.xml": INDEX,
        "https://tiki.vn/clover/sitemap_categories_index.xml": CATEGORIES_INDEX,
        "https://tiki.vn/clover/sitemap_categories_1.xml": FIRST,
        "https://tiki.vn/clover/sitemap_categories_2.xml": SECOND,
    }

    def fetch(url):
        requested.append(url)
        return pages[url]

    return fetch


def test_discovery_walks_the_sitemaps_and_orders_categories_by_id():
    requested = []

    targets = TikiSitemapDiscovery(fake_fetch(requested)).discover()

    assert targets == ["dien-thoai/c1789", "laptop/c8095", "xe-dap/c8431"]
    assert "https://elsewhere.test/sitemap_categories_3.xml" not in requested
    assert all(url.startswith("https://tiki.vn/") for url in requested)


def test_discovery_honours_a_limit_by_keeping_the_lowest_ids():
    assert TikiSitemapDiscovery(fake_fetch([])).discover(limit=2) == ["dien-thoai/c1789", "laptop/c8095"]


def test_discovery_refuses_an_index_without_a_categories_sitemap():
    with pytest.raises(ValueError, match="no categories sitemap"):
        TikiSitemapDiscovery(lambda url: "<sitemapindex></sitemapindex>").discover()


def test_sitemap_locations_reads_loc_elements():
    assert sitemap_locations("<a><loc> x </loc><loc>y</loc></a>") == ["x", "y"]


def test_every_discovered_target_is_a_valid_request_target():
    for target in TikiSitemapDiscovery(fake_fetch([])).discover():
        assert TikiExtractor().request_url("https://tiki.vn", target, 1).endswith(target)
