import hashlib
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from ecomdata.domain.acquisition import FetchResult, ListingPageParseError, ListingPageRequest
from ecomdata.domain.identity import make_offer_id
from ecomdata.domain.observations import ResourceType, create_raw_artifact
from ecomdata.ingestion.cli import SITE_CRAWLERS
from ecomdata.ingestion.sources.tiki.api import TikiCrawler
from ecomdata.ingestion.sources.tiki.html import TikiCategoryHtmlCrawler
from tests.paths import FIXTURES

FIXTURE = FIXTURES / "tiki_category_page_sample.html"
FETCHED = datetime(2026, 10, 12, 4, 30, tzinfo=UTC)
TARGET = "nha-sach-tiki/c8322"


def _adapter():
    adapter = TikiCategoryHtmlCrawler.__new__(TikiCategoryHtmlCrawler)
    adapter.user_agent = "test-agent"
    adapter._clock = lambda: FETCHED
    adapter._monotonic = lambda: 1.0
    return adapter


def _parse(adapter, body=None):
    body = FIXTURE.read_bytes() if body is None else body
    fetch = FetchResult(adapter.request_url(TARGET, 1), FETCHED, 200, "text/html", body, elapsed_ms=10)
    artifact = create_raw_artifact(
        marketplace_code="tiki",
        crawl_run_id="run-1",
        marketplace_id="marketplace-tiki",
        request_url=fetch.request_url,
        resource_type=ResourceType.LISTING_PAGE,
        fetched_at=fetch.fetched_at,
        http_status=fetch.http_status,
        content_type=fetch.content_type,
        body_sha256=hashlib.sha256(fetch.body).hexdigest(),
        raw_uri="file:///lake/body.bin",
        adapter_version=adapter.adapter_version,
        raw_bytes=len(fetch.body),
    )
    request = ListingPageRequest(marketplace_code="tiki", marketplace_id="marketplace-tiki", target=TARGET, page=1)
    return adapter.parse_listing_page(
        request=request, fetch_result=fetch, raw_artifact=artifact, crawl_run_id="run-1", produced_at=FETCHED,
    )


def test_registered_as_its_own_site_key_under_marketplace_tiki():
    assert SITE_CRAWLERS["tiki-html"] is TikiCategoryHtmlCrawler
    assert TikiCategoryHtmlCrawler.site_name == "tiki"
    assert TikiCategoryHtmlCrawler.adapter_version != TikiCrawler.adapter_version


def test_request_url_is_the_public_category_page():
    adapter = _adapter()
    assert adapter.request_url(TARGET, 1) == "https://tiki.vn/nha-sach-tiki/c8322"
    assert adapter.robots_url == "https://tiki.vn/robots.txt"


@pytest.mark.parametrize("target", ["8322", "/nha-sach-tiki/c8322", "nha-sach-tiki", "../api/c1"])
def test_request_url_rejects_targets_that_are_not_slug_and_category_id(target):
    with pytest.raises(ValueError):
        _adapter().request_url(target, 1)


def test_request_url_refuses_page_two_because_the_site_repeats_page_one():
    with pytest.raises(ValueError):
        _adapter().request_url(TARGET, 2)


def test_parses_organic_rows_and_drops_sponsored_ones():
    page = _parse(_adapter())
    ids = [event.payload.offer.platform_listing_id for event in page.observations]
    assert ids == ["279325148", "279218500", "279157961"]  # 271518926 is the ad row
    assert page.source_record_count == 3
    assert page.rejections == ()
    assert page.last_page == 1


def test_row_fields_map_like_the_listing_api_adapter():
    first = _parse(_adapter()).observations[0].payload
    assert first.offer.offer_id == make_offer_id("tiki", "279325148")
    assert first.offer.source_url == "https://tiki.vn/but-bi-claro-arrow-0-5mm-muc-xanh-p279325148.html"
    assert first.observation.current_price == Decimal("3750")
    assert first.observation.list_price == Decimal("5000")
    assert first.observation.sold_count == 612
    assert first.observation.adapter_version == "tiki-category-html-v1"
    assert first.observation.ranking_position == 1


def test_challenge_page_without_next_data_fails_the_whole_page():
    challenge = b"<html><head><script>var _0x1a2b=['\\x61'];(function(){})();</script></head><body></body></html>"
    with pytest.raises(ListingPageParseError, match="__NEXT_DATA__"):
        _parse(_adapter(), challenge)


def test_page_with_only_sponsored_rows_fails_rather_than_reporting_zero_products():
    body = FIXTURE.read_bytes()
    only_ad = body.replace(b'"advertisement": null', b'"advertisement": {"ad": [1]}')
    with pytest.raises(ListingPageParseError, match="organic"):
        _parse(_adapter(), only_ad)
