from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from config.marketplace_schema import ResourceType, create_marketplace_offer, create_observation_event, create_offer_observation
from crawler.acquisition import acquire_listing_page
from crawler.contracts import (
    AcquisitionStage,
    AcquisitionStatus,
    FetchResult,
    ListingPageRequest,
    ListingPageParseError,
    ParsedListingPage,
    RecordRejection,
    RawPersistenceError,
)
from crawler.raw_store import persist_fetch_result


UTC = timezone.utc
START = datetime(2026, 9, 4, 8, 30, tzinfo=UTC)
FETCHED = START + timedelta(seconds=1)
PRODUCED = START + timedelta(seconds=2)
DONE = START + timedelta(seconds=3)


class FakeAdapter:
    site_name = "fake"
    marketplace_id = "marketplace-fake"
    adapter_version = "fake-v1"

    def __init__(self, *, status=200, body=b"page", allowed=True, parsed=None, fetch_error=None):
        self.status = status
        self.body = body
        self.allowed_result = allowed
        self.parsed = parsed
        self.fetch_error = fetch_error
        self.calls = []

    def request_url(self, target, page):
        self.calls.append(("url", target, page))
        return f"https://example.test/listings?target={target}&page={page}"

    def allowed(self, url):
        self.calls.append(("robots", url))
        return self.allowed_result

    def throttle(self):
        self.calls.append(("throttle",))

    def fetch_listing_page(self, request):
        self.calls.append(("fetch", request))
        if self.fetch_error is not None:
            raise self.fetch_error
        return FetchResult(
            self.request_url_without_recording(request.target, request.page),
            FETCHED,
            self.status,
            "application/json",
            self.body,
            retry_after="9" if self.status == 429 else None,
        )

    def request_url_without_recording(self, target, page):
        return f"https://example.test/listings?target={target}&page={page}"

    def parse_listing_page(self, **kwargs):
        self.calls.append(("parse", kwargs))
        if isinstance(self.parsed, Exception):
            raise self.parsed
        if callable(self.parsed):
            return self.parsed(**kwargs)
        return self.parsed


def _request():
    return ListingPageRequest("fake", "marketplace-fake", "cat-1", 1)


def _event(raw_uri="file:///lake/body.bin", raw_sha="a" * 64, run_id="run-1"):
    offer = create_marketplace_offer(
        marketplace_code="fake",
        marketplace_id="marketplace-fake",
        platform_listing_id="p1",
        seller_id=None,
        product_title="Product",
        brand=None,
        category_path="cat-1",
        source_url="https://example.test/p1.html",
        currency="VND",
        first_seen_at=FETCHED,
        last_seen_at=FETCHED,
    )
    observation = create_offer_observation(
        marketplace_code="fake",
        platform_listing_id="p1",
        offer_id=offer.offer_id,
        observed_at=FETCHED,
        fetched_at=FETCHED,
        current_price=Decimal("10.00"),
        raw_uri=raw_uri,
        raw_sha256=raw_sha,
        adapter_version="fake-v1",
        crawl_run_id=run_id,
    )
    return create_observation_event(
        marketplace_code="fake",
        offer=offer,
        observation=observation,
        platform_listing_id="p1",
        produced_at=PRODUCED,
    )


def _writer(calls, *, fail_at=None):
    def write(zone, path, data):
        calls.append(("write", zone, path, data))
        if fail_at == len(calls):
            raise OSError("writer failure")
        return f"file:///lake/{path}"

    return write


def _clock():
    values = iter([START, PRODUCED, DONE])
    return lambda: next(values)


def _success_parsed(**kwargs):
    artifact = kwargs["raw_artifact"]
    return ParsedListingPage(
        observations=(_event(raw_uri=artifact.raw_uri, raw_sha=artifact.body_sha256),),
        rejections=(),
        source_record_count=1,
        duplicate_count=0,
        last_page=1,
    )


def test_success_enforces_body_metadata_parse_order_and_lineage():
    adapter = FakeAdapter(parsed=_success_parsed)
    calls = []
    report = acquire_listing_page(
        adapter,
        _request(),
        crawl_run_id="run-1",
        writer=_writer(calls),
        clock=_clock(),
    )
    assert report.status is AcquisitionStatus.SUCCEEDED
    assert report.is_task_success is True
    assert report.raw_artifact is not None
    assert report.raw_metadata_uri is not None
    assert report.canonical_observation_count == 1
    assert [call[0] for call in adapter.calls] == ["url", "robots", "throttle", "fetch", "parse"]
    assert [call[2].split("/")[-1] for call in calls] == ["body.bin", "metadata.json"]
    parse_call = next(call for call in adapter.calls if call[0] == "parse")
    assert parse_call[1]["raw_artifact"] == report.raw_artifact


def test_robots_denial_has_no_fetch_or_write():
    adapter = FakeAdapter(allowed=False)
    calls = []
    report = acquire_listing_page(
        adapter, _request(), crawl_run_id="run-1", writer=_writer(calls), clock=_clock()
    )
    assert report.status is AcquisitionStatus.FAILED
    assert report.failure.stage is AcquisitionStage.ROBOTS
    assert [call[0] for call in adapter.calls] == ["url", "robots"]
    assert calls == []


def test_transport_failure_has_no_raw_artifact():
    adapter = FakeAdapter(fetch_error=ConnectionError("network down"))
    calls = []
    report = acquire_listing_page(
        adapter, _request(), crawl_run_id="run-1", writer=_writer(calls), clock=_clock()
    )
    assert report.failure.stage is AcquisitionStage.FETCH
    assert report.raw_artifact is None
    assert calls == []


def test_body_failure_stops_before_metadata_and_parse():
    adapter = FakeAdapter(parsed=_success_parsed)
    calls = []
    report = acquire_listing_page(
        adapter, _request(), crawl_run_id="run-1", writer=_writer(calls, fail_at=1), clock=_clock()
    )
    assert report.failure.stage is AcquisitionStage.STORAGE
    assert isinstance(report.failure.error, RawPersistenceError)
    assert report.raw_artifact is None
    assert len(calls) == 1
    assert all(call[0] != "parse" for call in adapter.calls)


def test_metadata_failure_retains_body_artifact_and_stops_parse():
    adapter = FakeAdapter(parsed=_success_parsed)
    calls = []
    report = acquire_listing_page(
        adapter, _request(), crawl_run_id="run-1", writer=_writer(calls, fail_at=2), clock=_clock()
    )
    assert report.failure.stage is AcquisitionStage.STORAGE
    assert report.raw_artifact is not None
    assert report.raw_metadata_uri is None
    assert len(calls) == 2
    assert all(call[0] != "parse" for call in adapter.calls)


@pytest.mark.parametrize("status", [404, 429, 500])
def test_http_failure_persists_body_before_status_handling(status):
    adapter = FakeAdapter(status=status, parsed=_success_parsed)
    calls = []
    report = acquire_listing_page(
        adapter, _request(), crawl_run_id="run-1", writer=_writer(calls), clock=_clock()
    )
    assert report.failure.stage is AcquisitionStage.HTTP
    assert report.http_status == status
    assert report.raw_artifact is not None
    assert report.retry_after == ("9" if status == 429 else None)
    assert len(calls) == 2
    assert all(call[0] != "parse" for call in adapter.calls)


def test_parse_failure_retains_complete_raw_lineage():
    adapter = FakeAdapter(parsed=ListingPageParseError("malformed page"))
    calls = []
    report = acquire_listing_page(
        adapter, _request(), crawl_run_id="run-1", writer=_writer(calls), clock=_clock()
    )
    assert report.failure.stage is AcquisitionStage.PARSE
    assert report.raw_artifact is not None
    assert report.raw_metadata_uri is not None
    assert report.observations == ()


def test_mixed_rows_are_partial_and_not_retried_by_phase2():
    def parsed(**kwargs):
        artifact = kwargs["raw_artifact"]
        rejection = RecordRejection(1, AcquisitionStage.VALIDATION, ValueError("bad row"))
        return ParsedListingPage(
            (_event(raw_uri=artifact.raw_uri, raw_sha=artifact.body_sha256),),
            (rejection,),
            2,
            0,
            1,
        )

    adapter = FakeAdapter(parsed=parsed)
    report = acquire_listing_page(
        adapter, _request(), crawl_run_id="run-1", writer=_writer([]), clock=_clock()
    )
    assert report.status is AcquisitionStatus.PARTIAL
    assert report.is_task_success is True
    assert report.rejected_count == 1


def test_all_invalid_rows_are_terminal_validation_failure():
    rejection = RecordRejection(0, AcquisitionStage.VALIDATION, ValueError("bad row"))
    parsed = ParsedListingPage((), (rejection,), 1, 0, 1)
    adapter = FakeAdapter(parsed=parsed)
    report = acquire_listing_page(
        adapter, _request(), crawl_run_id="run-1", writer=_writer([]), clock=_clock()
    )
    assert report.status is AcquisitionStatus.FAILED
    assert report.failure.stage is AcquisitionStage.VALIDATION
    assert report.is_task_success is False


def test_empty_page_is_successful_and_exhausted():
    adapter = FakeAdapter(parsed=ParsedListingPage((), (), 0, 0, None))
    report = acquire_listing_page(
        adapter, _request(), crawl_run_id="run-1", writer=_writer([]), clock=_clock()
    )
    assert report.status is AcquisitionStatus.SUCCEEDED
    assert report.page_exhausted is True
    assert report.canonical_observation_count == 0


def test_programmer_contract_errors_are_not_converted_to_source_failure():
    adapter = FakeAdapter(parsed=None)
    with pytest.raises(TypeError, match="ParsedListingPage"):
        acquire_listing_page(
            adapter, _request(), crawl_run_id="run-1", writer=_writer([]), clock=_clock()
        )
