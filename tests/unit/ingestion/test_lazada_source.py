"""Lazada: extractor, discovery and the lossless ELT claim (AC6, AC10) on the XHR fixture."""
from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from ecomdata.domain.acquisition import (
    ROLE_XHR,
    FetchBundle,
    FetchPart,
    ListingPageParseError,
    ListingPageRequest,
)
from ecomdata.domain.observations import ResourceType, create_raw_artifact
from ecomdata.domain.records import RecordKind, build_records
from ecomdata.domain.sources import LAZADA, SOURCES
from ecomdata.ingestion.sources.lazada.discovery import DEFAULT_PATH, LazadaConfiguredDiscovery
from ecomdata.ingestion.sources.lazada.extractor import MAX_PAGES, LazadaExtractor
from tests.paths import FIXTURES

URL = "https://www.lazada.vn/dien-thoai-di-dong/?page=1"
AJAX = URL + "&ajax=true"
FETCHED = datetime(2026, 10, 25, 9, 0, tzinfo=UTC)
LISTING = (FIXTURES / "sources" / "lazada" / "listing_xhr.json").read_bytes()
SHELL = (FIXTURES / "sources" / "lazada" / "category_page.html").read_bytes()


def source_listing():
    """The fixture read without the extractor, as the oracle."""
    return json.loads(LISTING)


def bundle(*xhr_bodies, document=SHELL):
    parts = [FetchPart(URL, 200, "text/html", document)]
    parts += [FetchPart(AJAX, 200, "application/json", body, role=ROLE_XHR) for body in xhr_bodies]
    return FetchBundle(URL, FETCHED, tuple(parts))


def test_the_extractor_is_the_one_the_registry_names():
    assert isinstance(SOURCES.extractor("lazada")(), LazadaExtractor)


def test_the_request_url_is_the_category_with_its_page_number():
    extractor = LazadaExtractor()

    assert extractor.request_url("https://www.lazada.vn", "dien-thoai-di-dong", 3) == (
        "https://www.lazada.vn/dien-thoai-di-dong/?page=3"
    )
    assert extractor.request_url("https://www.lazada.vn/", "a/b-c", 1) == "https://www.lazada.vn/a/b-c/?page=1"
    for target in ("", "/x", "x/", "X", "a b", "a?b=1", "../x"):
        with pytest.raises(ValueError, match="category target"):
            extractor.request_url("https://www.lazada.vn", target, 1)
    for page in (0, MAX_PAGES + 1):
        with pytest.raises(ValueError, match="serves pages"):
            extractor.request_url("https://www.lazada.vn", "laptop", page)


def test_every_listed_product_comes_out_sponsored_rows_included():
    page = LazadaExtractor().extract(bundle(LISTING))

    assert len(page.rows) == len(source_listing()["mods"]["listItems"]) == 5
    assert sum(1 for row in page.rows if row.get("isSponsored")) == 1


def test_rows_are_the_source_objects_not_a_selection_of_their_fields():
    page = LazadaExtractor().extract(bundle(LISTING))

    assert list(page.rows) == source_listing()["mods"]["listItems"]
    assert len(page.rows[0]) >= 35


def test_the_page_context_is_what_surrounds_the_rows():
    page = LazadaExtractor().extract(bundle(LISTING))
    source = source_listing()

    assert set(page.page_context) == {"mods", "mainInfo", "seoInfo"}
    assert set(page.page_context["mods"]) == {"filter", "breadcrumb", "sortBar"}
    assert page.page_context["mainInfo"] == source["mainInfo"]
    assert "listItems" not in page.page_context["mods"]


def test_the_last_page_follows_from_the_total_and_the_page_size():
    assert LazadaExtractor().extract(bundle(LISTING)).last_page == 60  # 2400 results, 40 a page


def test_the_last_page_is_capped_and_unknown_when_the_page_does_not_say():
    capped = source_listing()
    capped["mainInfo"].update(totalResults="99999", pageSize="40")
    unknown = source_listing()
    del unknown["mainInfo"]["totalResults"]

    assert LazadaExtractor().extract(bundle(json.dumps(capped).encode())).last_page == MAX_PAGES
    assert LazadaExtractor().extract(bundle(json.dumps(unknown).encode())).last_page is None


def test_the_listing_is_found_among_other_captured_responses():
    other = json.dumps({"mods": {"unrelated": 1}}).encode()

    page = LazadaExtractor().extract(bundle(b"/* a script */", other, LISTING))

    assert len(page.rows) == 5


def test_a_page_without_a_listing_response_fails_loudly():
    with pytest.raises(ListingPageParseError, match="none of the 0 captured"):
        LazadaExtractor().extract(bundle())
    with pytest.raises(ListingPageParseError, match="none of the 1 captured"):
        LazadaExtractor().extract(bundle(b'{"mods": {}}'))


@pytest.mark.parametrize(
    "document, message",
    [
        ({"mods": {"listItems": {"a": 1}}}, "must be a list"),
        ({"mods": {"listItems": [1]}}, "not an object"),
    ],
)
def test_a_changed_listing_shape_fails_loudly(document, message):
    with pytest.raises(ListingPageParseError, match=message):
        LazadaExtractor().extract(bundle(json.dumps(document).encode()))


def test_an_empty_listing_is_a_page_without_rows_not_an_error():
    # The page past the last one answers with no products; that ends the walk.
    document = {"mods": {"listItems": []}, "mainInfo": {"totalResults": "0", "pageSize": "40"}}

    page = LazadaExtractor().extract(bundle(json.dumps(document).encode()))

    assert page.rows == () and page.last_page == 1


def test_records_hold_the_source_rows_deeply_equal_and_drop_only_the_visitor_id():
    """AC6: listing_row count equals the source rows, payload equals the source object; PII is named."""
    request = ListingPageRequest("lazada", "marketplace-lazada", "dien-thoai-di-dong", 1)
    raw = create_raw_artifact(
        marketplace_code="lazada", crawl_run_id="run-1", marketplace_id="marketplace-lazada", request_url=URL,
        resource_type=ResourceType.LISTING_PAGE, fetched_at=FETCHED, http_status=200, content_type="text/html",
        body_sha256="9" * 64, raw_uri="file:///lake/part-0.bin.gz", adapter_version=LAZADA.adapter_version,
        raw_bytes=len(LISTING),
    )
    page = LazadaExtractor().extract(bundle(LISTING))

    records = build_records(LAZADA, request=request, raw_artifact=raw, rows=page.rows, page_context=page.page_context)

    rows = [r for r in records if r.record_kind is RecordKind.LISTING_ROW]
    (context,) = [r for r in records if r.record_kind is RecordKind.PAGE_CONTEXT]
    assert [dict(r.payload) for r in rows] == source_listing()["mods"]["listItems"]
    assert all(r.dropped_fields == () for r in rows)
    assert context.dropped_fields == ("mainInfo.anonUid", "mainInfo.userId")
    assert "anonUid" not in context.payload["mainInfo"] and "userId" not in context.payload["mainInfo"]
    assert context.payload["mainInfo"]["totalResults"] == "2400"
    assert "FIXTURE-ANON-UID" in page.page_context["mainInfo"]["anonUid"]  # the extracted page is untouched


def test_the_spec_names_what_the_browser_captures_and_what_a_block_looks_like():
    assert LAZADA.fetcher == "browser" and LAZADA.paginated is True and LAZADA.min_delay_seconds >= 5
    assert any("ajax=true" in pattern for pattern in LAZADA.capture_patterns)
    assert LAZADA.blocked_patterns


# --- discovery ------------------------------------------------------------------------


def test_discovery_lists_the_configured_categories_in_order():
    targets = LazadaConfiguredDiscovery().discover()

    assert 15 <= len(targets) <= 25
    assert targets[0] == "dien-thoai-di-dong" and len(set(targets)) == len(targets)


def test_discovery_honours_a_limit():
    assert LazadaConfiguredDiscovery().discover(limit=3) == LazadaConfiguredDiscovery().discover()[:3]


def test_every_configured_category_is_a_valid_request_target():
    for target in LazadaConfiguredDiscovery().discover():
        assert LazadaExtractor().request_url("https://www.lazada.vn", target, 1).endswith(f"/{target}/?page=1")


def test_a_list_naming_a_category_twice_is_refused(tmp_path):
    path = tmp_path / "categories.json"
    path.write_text(json.dumps({"categories": [{"target": "a"}, {"target": "a"}]}), encoding="utf-8")

    with pytest.raises(ValueError, match="twice"):
        LazadaConfiguredDiscovery(path=path).discover()


def test_the_default_list_is_the_one_in_env_sources():
    assert DEFAULT_PATH.name == "lazada_categories.json" and DEFAULT_PATH.parent.name == "sources"
    assert DEFAULT_PATH.is_file()
