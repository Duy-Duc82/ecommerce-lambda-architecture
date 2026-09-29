import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest

from common.identity import make_observation_id, make_offer_id, make_seller_id
from common.serialization import serialize_for_wire
from config.marketplace_schema import ResourceType, create_raw_artifact
from crawler.contracts import (
    AcquisitionStage,
    FetchResult,
    ListingPageRequest,
    ListingPageParseError,
)
from crawler.sites.tiki import LISTING_URL, PAGE_SIZE, TikiCrawler


FIXTURE = Path(__file__).parent / "fixtures" / "tiki_listing_sample.json"
FETCHED = datetime(2026, 9, 4, 8, 30, tzinfo=timezone.utc)


def _adapter():
    adapter = TikiCrawler.__new__(TikiCrawler)
    adapter.user_agent = "test-agent"
    adapter._clock = lambda: FETCHED
    adapter._monotonic = lambda: 1.0
    return adapter


def _fetch(adapter, body=None, *, status=200, headers=None):
    body = FIXTURE.read_bytes() if body is None else body
    return FetchResult(
        adapter.request_url("1846", 1),
        FETCHED,
        status,
        (headers or {}).get("Content-Type", "application/json"),
        body,
        retry_after=(headers or {}).get("Retry-After"),
        elapsed_ms=10,
    )


def _artifact(fetch, *, raw_uri="file:///lake/body.bin"):
    import hashlib

    return create_raw_artifact(
        marketplace_code="tiki",
        crawl_run_id="run-1",
        marketplace_id="marketplace-tiki",
        request_url=fetch.request_url,
        resource_type=ResourceType.LISTING_PAGE,
        fetched_at=fetch.fetched_at,
        http_status=fetch.http_status,
        content_type=fetch.content_type,
        body_sha256=hashlib.sha256(fetch.body).hexdigest(),
        raw_uri=raw_uri,
        adapter_version="tiki-listing-v1",
        raw_bytes=len(fetch.body),
    )


def test_request_url_contains_encoded_category_page_and_limit():
    adapter = _adapter()
    url = adapter.request_url("Điện thoại/1846", 2)
    parsed = urlparse(url)
    assert f"{parsed.scheme}://{parsed.netloc}{parsed.path}" == LISTING_URL
    assert parse_qs(parsed.query) == {
        "category": ["Điện thoại/1846"],
        "page": ["2"],
        "limit": [str(PAGE_SIZE)],
    }


@pytest.mark.parametrize("status", [404, 429, 500])
def test_fetch_returns_http_body_without_json_or_raise_for_status(monkeypatch, status):
    adapter = _adapter()
    calls = []

    class Response:
        status_code = status
        url = adapter.request_url("1846", 1)
        content = b"not-json-but-preserved"
        headers = {"Content-Type": "text/plain", "Retry-After": "7"}
        elapsed = SimpleNamespace(total_seconds=lambda: 0.01)

        def json(self):
            raise AssertionError("fetch must not decode JSON")

        def raise_for_status(self):
            raise AssertionError("fetch must not raise for HTTP status")

    def fake_get(url, headers=None, timeout=None):
        calls.append((url, headers, timeout))
        return Response()

    adapter._http_get = fake_get
    result = adapter.fetch_listing_page(ListingPageRequest("tiki", "marketplace-tiki", "1846", 1))
    assert calls[0][0] == adapter.request_url("1846", 1)
    assert calls[0][1] == {"User-Agent": "test-agent"}
    assert calls[0][2] == 10
    assert result.http_status == status
    assert result.body == b"not-json-but-preserved"
    assert result.retry_after == "7"
    assert result.content_type == "text/plain"


def test_fetch_uses_injected_clock_and_returns_final_url():
    adapter = _adapter()
    final_url = "https://tiki.vn/api/listings?redirected=1"

    class Response:
        status_code = 200
        url = final_url
        content = b"{}"
        headers = {}

    adapter._http_get = lambda *args, **kwargs: Response()
    result = adapter.fetch_listing_page(ListingPageRequest("tiki", "marketplace-tiki", "1846", 1))
    assert result.request_url == final_url
    assert result.fetched_at == FETCHED


def test_fixture_maps_to_phase1_canonical_events_and_lineage():
    adapter = _adapter()
    request = ListingPageRequest("tiki", "marketplace-tiki", "1846", 1)
    fetch = _fetch(adapter)
    artifact = _artifact(fetch)
    parsed = adapter.parse_listing_page(
        request=request,
        fetch_result=fetch,
        raw_artifact=artifact,
        crawl_run_id="run-1",
        produced_at=FETCHED,
    )

    assert len(parsed.observations) == 2
    assert len(parsed.rejections) == 1
    assert parsed.rejections[0].record_index == 2
    assert parsed.rejections[0].stage is AcquisitionStage.VALIDATION
    first = parsed.observations[0]
    offer = first.payload.offer
    observation = first.payload.observation
    assert offer.offer_id == make_offer_id("tiki", "279212151")
    assert offer.seller_id == make_seller_id("tiki", "1")
    assert observation.observation_id == make_observation_id(
        "tiki", "279212151", FETCHED, artifact.body_sha256
    )
    assert observation.current_price == Decimal("16990000")
    assert observation.list_price == Decimal("18990000")
    assert observation.discount_percent == Decimal("11")
    assert observation.sold_count == 14
    assert observation.availability.value == "IN_STOCK"
    assert observation.ranking_position == 1
    assert observation.raw_uri == artifact.raw_uri
    assert observation.raw_sha256 == artifact.body_sha256
    assert first.raw_uri == artifact.raw_uri
    assert first.crawl_run_id == "run-1"
    encoded = json.dumps(serialize_for_wire(first), ensure_ascii=False)
    assert '"current_price": "16990000"' in encoded


def test_missing_optional_fields_remain_null_and_availability_unknown():
    adapter = _adapter()
    request = ListingPageRequest("tiki", "marketplace-tiki", "1846", 1)
    body = json.dumps(
        {"data": [{"id": 1, "name": "x", "price": 10, "url_key": "x-p1"}]},
        ensure_ascii=False,
    ).encode()
    fetch = _fetch(adapter, body)
    parsed = adapter.parse_listing_page(
        request=request,
        fetch_result=fetch,
        raw_artifact=_artifact(fetch),
        crawl_run_id="run-1",
        produced_at=FETCHED,
    )
    event = parsed.observations[0]
    assert event.payload.offer.brand is None
    assert event.payload.offer.seller_id is None
    assert event.payload.observation.availability.value == "UNKNOWN"
    assert event.payload.observation.sold_count is None


@pytest.mark.parametrize(
    ("availability", "shippable", "expected"),
    [(1, True, "IN_STOCK"), (0, True, "OUT_OF_STOCK"), (1, False, "OUT_OF_STOCK")],
)
def test_explicit_availability_mapping(availability, shippable, expected):
    adapter = _adapter()
    request = ListingPageRequest("tiki", "marketplace-tiki", "1846", 1)
    body = json.dumps(
        {
            "data": [
                {
                    "id": 1,
                    "name": "x",
                    "price": 10,
                    "url_key": "x-p1",
                    "availability": availability,
                    "shippable": shippable,
                }
            ]
        }
    ).encode()
    fetch = _fetch(adapter, body)
    parsed = adapter.parse_listing_page(
        request=request,
        fetch_result=fetch,
        raw_artifact=_artifact(fetch),
        crawl_run_id="run-1",
        produced_at=FETCHED,
    )
    assert parsed.observations[0].payload.observation.availability.value == expected


def test_duplicate_listing_id_is_suppressed_and_counted():
    adapter = _adapter()
    request = ListingPageRequest("tiki", "marketplace-tiki", "1846", 1)
    row = {"id": 1, "name": "x", "price": 10, "url_key": "x-p1"}
    fetch = _fetch(adapter, json.dumps({"data": [row, row]}).encode())
    parsed = adapter.parse_listing_page(
        request=request,
        fetch_result=fetch,
        raw_artifact=_artifact(fetch),
        crawl_run_id="run-1",
        produced_at=FETCHED,
    )
    assert len(parsed.observations) == 1
    assert parsed.duplicate_count == 1
    assert parsed.source_record_count == 2


@pytest.mark.parametrize(
    "body",
    [b"\xff\xfe", b"not-json", json.dumps({"data": {}}).encode(), json.dumps([]).encode()],
)
def test_invalid_page_encoding_json_or_shape_fails_whole_page(body):
    adapter = _adapter()
    request = ListingPageRequest("tiki", "marketplace-tiki", "1846", 1)
    fetch = _fetch(adapter, body)
    with pytest.raises(ListingPageParseError):
        adapter.parse_listing_page(
            request=request,
            fetch_result=fetch,
            raw_artifact=_artifact(fetch),
            crawl_run_id="run-1",
            produced_at=FETCHED,
        )


def test_malformed_optional_numeric_rejects_only_that_row():
    adapter = _adapter()
    request = ListingPageRequest("tiki", "marketplace-tiki", "1846", 1)
    good = {"id": 1, "name": "good", "price": 10, "url_key": "good-p1"}
    bad = {"id": 2, "name": "bad", "price": 10, "url_key": "bad-p2", "review_count": "many"}
    fetch = _fetch(adapter, json.dumps({"data": [good, bad]}).encode())
    parsed = adapter.parse_listing_page(
        request=request,
        fetch_result=fetch,
        raw_artifact=_artifact(fetch),
        crawl_run_id="run-1",
        produced_at=FETCHED,
    )
    assert [event.payload.offer.platform_listing_id for event in parsed.observations] == ["1"]
    assert len(parsed.rejections) == 1
    assert parsed.rejections[0].platform_listing_id == "2"


def test_reparsing_same_saved_bytes_is_equal():
    adapter = _adapter()
    request = ListingPageRequest("tiki", "marketplace-tiki", "1846", 1)
    fetch = _fetch(adapter)
    artifact = _artifact(fetch)
    first = adapter.parse_listing_page(
        request=request,
        fetch_result=fetch,
        raw_artifact=artifact,
        crawl_run_id="run-1",
        produced_at=FETCHED,
    )
    second = adapter.parse_listing_page(
        request=request,
        fetch_result=fetch,
        raw_artifact=artifact,
        crawl_run_id="run-1",
        produced_at=FETCHED,
    )
    assert first.observations == second.observations
    assert [(item.record_index, item.stage, item.platform_listing_id, item.error_message)
            for item in first.rejections] == [
                (item.record_index, item.stage, item.platform_listing_id, item.error_message)
                for item in second.rejections
            ]
