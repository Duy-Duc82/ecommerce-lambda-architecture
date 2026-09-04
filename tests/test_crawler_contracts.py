from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from config.marketplace_schema import (
    Availability,
    create_marketplace_offer,
    create_observation_event,
    create_offer_observation,
    create_raw_artifact,
    ResourceType,
)
from crawler.contracts import (
    AcquisitionFailure,
    AcquisitionStage,
    AcquisitionStatus,
    FetchResult,
    ListingPageRequest,
    ParsedListingPage,
    Phase2AcquisitionReport,
    RawPersistenceError,
    RecordRejection,
    decode_listing_page_task_target,
    encode_listing_page_task_target,
)


UTC = timezone.utc
STARTED = datetime(2026, 9, 4, 8, 30, tzinfo=UTC)
FETCHED = STARTED + timedelta(seconds=1)
RAW_SHA = "a" * 64


def _request(page=1):
    return ListingPageRequest("TIKI", "marketplace-tiki", "1846", page)


def _artifact():
    return create_raw_artifact(
        marketplace_code="tiki",
        crawl_run_id="run-1",
        marketplace_id="marketplace-tiki",
        request_url="https://tiki.vn/api/listings?category=1846&page=1&limit=40",
        resource_type=ResourceType.LISTING_PAGE,
        fetched_at=FETCHED,
        http_status=200,
        content_type="application/json",
        body_sha256=RAW_SHA,
        raw_uri="file:///tmp/raw/body.bin",
        adapter_version="tiki-listing-v1",
        raw_bytes=10,
    )


def _event():
    offer = create_marketplace_offer(
        marketplace_code="tiki",
        marketplace_id="marketplace-tiki",
        platform_listing_id="p1",
        seller_id=None,
        product_title="Product",
        brand=None,
        category_path="1846",
        source_url="https://tiki.vn/p1.html",
        currency="VND",
        first_seen_at=FETCHED,
        last_seen_at=FETCHED,
    )
    observation = create_offer_observation(
        marketplace_code="tiki",
        platform_listing_id="p1",
        offer_id=offer.offer_id,
        observed_at=FETCHED,
        fetched_at=FETCHED,
        current_price=Decimal("100.00"),
        raw_uri="file:///tmp/raw/body.bin",
        raw_sha256=RAW_SHA,
        adapter_version="tiki-listing-v1",
        crawl_run_id="run-1",
        availability=Availability.UNKNOWN,
    )
    return create_observation_event(
        marketplace_code="tiki",
        offer=offer,
        observation=observation,
        platform_listing_id="p1",
        produced_at=FETCHED,
    )


def _report(status, *, observations=(), rejections=(), failure=None, artifact=None, metadata="file:///tmp/raw/metadata.json", source_count=None):
    if source_count is None:
        source_count = len(observations) + len(rejections)
    return Phase2AcquisitionReport(
        status=status,
        marketplace_code="tiki",
        marketplace_id="marketplace-tiki",
        target="1846",
        page=1,
        resource_type=ResourceType.LISTING_PAGE,
        crawl_run_id="run-1",
        request_url="https://tiki.vn/api/listings?category=1846&page=1&limit=40",
        started_at=STARTED,
        completed_at=FETCHED,
        http_status=200 if artifact else None,
        retry_after=None,
        raw_artifact=artifact,
        raw_metadata_uri=metadata if artifact and metadata else None,
        observations=tuple(observations),
        rejections=tuple(rejections),
        source_record_count=source_count,
        duplicate_count=0,
        last_page=1,
        failure=failure,
    )


def test_listing_request_normalizes_and_restricts_page_resource():
    request = _request()
    assert request.marketplace_code == "tiki"
    assert request.resource_type is ResourceType.LISTING_PAGE
    with pytest.raises(ValueError, match="page"):
        _request(page=0)
    with pytest.raises(ValueError, match="bool"):
        ListingPageRequest("tiki", "marketplace-tiki", "1846", True)


def test_fetch_result_preserves_bytes_and_normalizes_time():
    result = FetchResult(
        "https://example.test/listings",
        datetime(2026, 9, 4, 15, 30, tzinfo=timezone(timedelta(hours=7))),
        200,
        " application/json; charset=utf-8 ",
        b"\xff\x00raw",
        elapsed_ms=0,
    )
    assert result.fetched_at == datetime(2026, 9, 4, 8, 30, tzinfo=UTC)
    assert result.content_type == "application/json; charset=utf-8"
    assert result.body == b"\xff\x00raw"
    with pytest.raises(ValueError, match="bool"):
        FetchResult("https://example.test", FETCHED, True, None, b"")


def test_task_target_encoding_is_canonical_and_round_trips_unicode():
    encoded = encode_listing_page_task_target("Điện thoại/1846", 2)
    assert encoded == '{"page":2,"target":"Điện thoại/1846"}'
    assert decode_listing_page_task_target(encoded) == ("Điện thoại/1846", 2)


@pytest.mark.parametrize("value", ["", "not-json", '{"target":"x"}', '{"page":true,"target":"x"}', '{"page":1,"target":"x","extra":1}'])
def test_task_target_decoder_rejects_invalid_shape(value):
    with pytest.raises(ValueError):
        decode_listing_page_task_target(value)


def test_parsed_page_counts_reconcile():
    rejection = RecordRejection(1, AcquisitionStage.VALIDATION, ValueError("bad"), "p2")
    page = ParsedListingPage((_event(),), (rejection,), 3, 1, 2)
    assert page.source_record_count == 3
    with pytest.raises(ValueError, match="source_record_count"):
        ParsedListingPage((_event(),), (rejection,), 2, 1, 2)


def test_report_properties_and_success_invariants():
    report = _report(AcquisitionStatus.SUCCEEDED, observations=(_event(),), artifact=_artifact())
    assert report.canonical_observation_count == 1
    assert report.rejected_count == 0
    assert report.raw_bytes == 10
    assert report.page_exhausted is True
    assert report.is_task_success is True


def test_partial_requires_both_valid_and_rejected_rows():
    rejection = RecordRejection(1, AcquisitionStage.VALIDATION, ValueError("bad"))
    report = _report(
        AcquisitionStatus.PARTIAL,
        observations=(_event(),),
        rejections=(rejection,),
        artifact=_artifact(),
        source_count=2,
    )
    assert report.is_task_success is True
    with pytest.raises(ValueError, match="PARTIAL"):
        _report(AcquisitionStatus.PARTIAL, artifact=_artifact(), source_count=0)


def test_failed_report_can_retain_body_when_metadata_write_failed():
    failure = AcquisitionFailure(
        AcquisitionStage.STORAGE,
        RawPersistenceError("metadata failed", write_stage="METADATA", raw_artifact=_artifact()),
    )
    report = _report(
        AcquisitionStatus.FAILED,
        failure=failure,
        artifact=_artifact(),
        metadata=None,
        source_count=0,
    )
    assert report.raw_artifact is not None
    assert report.raw_metadata_uri is None
    assert report.is_task_success is False


def test_report_rejects_lineage_mismatch():
    artifact = _artifact()
    event = _event()
    bad = event.payload.observation
    # The event points to the fixture artifact, so changing the report run is
    # detected by the report invariant before any downstream publish.
    with pytest.raises(ValueError, match="crawl_run_id"):
        Phase2AcquisitionReport(
            status=AcquisitionStatus.SUCCEEDED,
            marketplace_code="tiki",
            marketplace_id="marketplace-tiki",
            target="1846",
            page=1,
            resource_type=ResourceType.LISTING_PAGE,
            crawl_run_id="different-run",
            request_url="https://tiki.vn/api/listings",
            started_at=STARTED,
            completed_at=FETCHED,
            http_status=200,
            retry_after=None,
            raw_artifact=artifact,
            raw_metadata_uri="file:///tmp/raw/metadata.json",
            observations=(event,),
            rejections=(),
            source_record_count=1,
            duplicate_count=0,
            last_page=1,
            failure=None,
        )


def test_fetch_result_rejects_naive_timestamp():
    with pytest.raises(ValueError, match="timezone-aware"):
        FetchResult("https://example.test", datetime(2026, 9, 4, 8, 30), 200, None, b"")
