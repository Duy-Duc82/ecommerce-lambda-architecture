from datetime import UTC, datetime, timedelta, timezone

import pytest

from ecomdata.domain.acquisition import (
    AcquisitionFailure,
    AcquisitionReport,
    AcquisitionStage,
    AcquisitionStatus,
    ExtractedPage,
    FetchBlockedError,
    FetchBundle,
    FetchPart,
    HttpResponseError,
    ListingPageRequest,
    RawPersistenceError,
    RecordPublishError,
    decode_listing_page_task_target,
    encode_listing_page_task_target,
)
from ecomdata.domain.observations import ResourceType, create_raw_artifact
from ecomdata.domain.records import build_records
from ecomdata.domain.sources import TIKI

STARTED = datetime(2026, 9, 4, 8, 30, tzinfo=UTC)
FETCHED = STARTED + timedelta(seconds=1)
RAW_SHA = "a" * 64
URL = "https://tiki.vn/dien-thoai/c1789"


def _request(page=1):
    return ListingPageRequest("TIKI", "marketplace-tiki", "dien-thoai/c1789", page)


def _artifact(fetched_at=FETCHED):
    return create_raw_artifact(
        marketplace_code="tiki",
        crawl_run_id="run-1",
        marketplace_id="marketplace-tiki",
        request_url=URL,
        resource_type=ResourceType.LISTING_PAGE,
        fetched_at=fetched_at,
        http_status=200,
        content_type="text/html",
        body_sha256=RAW_SHA,
        raw_uri="file:///tmp/raw/part-0.bin.gz",
        adapter_version=TIKI.adapter_version,
        raw_bytes=10,
    )


def _records(artifact=None, rows=({"id": 1},)):
    return build_records(
        TIKI, request=_request(), raw_artifact=artifact or _artifact(), rows=rows, page_context={"h1": "x"}
    )


def _report(status, *, records=(), failure=None, artifact=None, metadata="file:///tmp/raw/metadata.json", **extra):
    fields = {
        "status": status,
        "marketplace_code": "tiki",
        "marketplace_id": "marketplace-tiki",
        "target": "dien-thoai/c1789",
        "page": 1,
        "resource_type": ResourceType.LISTING_PAGE,
        "crawl_run_id": "run-1",
        "request_url": URL,
        "started_at": STARTED,
        "completed_at": FETCHED,
        "http_status": 200 if artifact else None,
        "retry_after": None,
        "raw_artifact": artifact,
        "raw_metadata_uri": metadata if artifact and metadata else None,
        "records": tuple(records),
        "records_uri": "file:///tmp/records.jsonl.gz" if records else None,
        "last_page": 1,
        "failure": failure,
        "row_count": max(0, len(records) - 1),
    }
    fields.update(extra)
    return AcquisitionReport(**fields)


def test_listing_request_normalizes_and_restricts_page_resource():
    request = _request()
    assert request.marketplace_code == "tiki"
    assert request.resource_type is ResourceType.LISTING_PAGE
    with pytest.raises(ValueError, match="page"):
        _request(page=0)
    with pytest.raises(ValueError, match="bool"):
        ListingPageRequest("tiki", "marketplace-tiki", "1846", True)


def test_a_fetch_part_preserves_bytes_and_checks_its_role():
    part = FetchPart("https://example.test/listings", 200, " application/json; charset=utf-8 ", b"\xff\x00raw")
    assert part.content_type == "application/json; charset=utf-8"
    assert part.body == b"\xff\x00raw"
    with pytest.raises(ValueError, match="bool"):
        FetchPart("https://example.test", True, None, b"")
    with pytest.raises(ValueError, match="role"):
        FetchPart("https://example.test", 200, None, b"", role="other")


def test_a_bundle_holds_exactly_one_document_and_normalizes_time():
    document = FetchPart(URL, 200, "text/html", b"<html>")
    xhr = FetchPart(URL + "?ajax=true", 200, "application/json", b"{}", role="xhr")
    bundle = FetchBundle(URL, datetime(2026, 9, 4, 15, 30, tzinfo=timezone(timedelta(hours=7))), (document, xhr))

    assert bundle.fetched_at == datetime(2026, 9, 4, 8, 30, tzinfo=UTC)
    assert bundle.document is document and bundle.captured == (xhr,)
    assert bundle.http_status == 200 and bundle.body == b"<html>" and bundle.total_bytes == 8
    with pytest.raises(ValueError, match="exactly one document"):
        FetchBundle(URL, FETCHED, (document, document))
    with pytest.raises(ValueError, match="exactly one document"):
        FetchBundle(URL, FETCHED, (xhr,))
    with pytest.raises(ValueError, match="timezone-aware"):
        FetchBundle(URL, datetime(2026, 9, 4, 8, 30), (document,))


def test_an_extracted_page_holds_objects_only():
    page = ExtractedPage(rows=({"id": 1},), page_context={"a": 1}, last_page=3)
    assert page.last_page == 3
    with pytest.raises(ValueError, match="rows"):
        ExtractedPage(rows=([1],))
    with pytest.raises(ValueError, match="rows"):
        ExtractedPage(rows=[{"id": 1}])


def test_task_target_encoding_is_canonical_and_round_trips_unicode():
    encoded = encode_listing_page_task_target("Điện thoại/1846", 2)
    assert encoded == '{"page":2,"target":"Điện thoại/1846"}'
    assert decode_listing_page_task_target(encoded) == ("Điện thoại/1846", 2)


@pytest.mark.parametrize(
    "value", ["", "not-json", '{"target":"x"}', '{"page":true,"target":"x"}', '{"page":1,"target":"x","extra":1}']
)
def test_task_target_decoder_rejects_invalid_shape(value):
    with pytest.raises(ValueError):
        decode_listing_page_task_target(value)


def test_report_properties_and_success_invariants():
    report = _report(AcquisitionStatus.SUCCEEDED, records=_records(), artifact=_artifact())
    assert report.record_count == 1
    assert report.rejected_count == 0
    assert report.raw_bytes == 10
    assert report.page_exhausted is True
    assert report.is_task_success is True


def test_a_success_needs_its_raw_artifact_manifest_and_records_file():
    with pytest.raises(ValueError, match="successful report requires"):
        _report(AcquisitionStatus.SUCCEEDED, records=_records(), artifact=_artifact(), records_uri=None)
    with pytest.raises(ValueError, match="successful report requires"):
        _report(AcquisitionStatus.SUCCEEDED, records=_records(), artifact=None)


def test_a_failed_report_has_a_failure_and_no_records():
    failure = AcquisitionFailure(AcquisitionStage.HTTP, HttpResponseError(500, None))
    report = _report(AcquisitionStatus.FAILED, failure=failure, artifact=_artifact())
    assert report.is_task_success is False and report.record_count == 0
    with pytest.raises(ValueError, match="requires a failure"):
        _report(AcquisitionStatus.FAILED)
    with pytest.raises(ValueError, match="cannot contain records"):
        _report(AcquisitionStatus.FAILED, failure=failure, artifact=_artifact(), records=_records())


def test_failed_report_can_retain_the_parts_when_the_manifest_write_failed():
    failure = AcquisitionFailure(
        AcquisitionStage.STORAGE,
        RawPersistenceError("manifest failed", write_stage="MANIFEST", raw_artifact=_artifact()),
    )
    report = _report(AcquisitionStatus.FAILED, failure=failure, artifact=_artifact(), metadata=None)
    assert report.raw_artifact is not None
    assert report.raw_metadata_uri is None


def test_a_persistence_error_names_a_known_write_stage():
    with pytest.raises(ValueError, match="write_stage"):
        RawPersistenceError("x", write_stage="BODY")


def test_report_rejects_lineage_mismatch():
    with pytest.raises(ValueError, match="crawl_run_id"):
        _report(AcquisitionStatus.SUCCEEDED, records=_records(), artifact=_artifact(), crawl_run_id="different-run")


def test_report_rejects_a_record_of_another_artifact():
    other = create_raw_artifact(
        marketplace_code="tiki",
        crawl_run_id="run-1",
        marketplace_id="marketplace-tiki",
        request_url=URL,
        resource_type=ResourceType.LISTING_PAGE,
        fetched_at=FETCHED,
        http_status=200,
        content_type="text/html",
        body_sha256="b" * 64,
        raw_uri="file:///tmp/raw/other.bin.gz",
        adapter_version=TIKI.adapter_version,
        raw_bytes=10,
    )
    with pytest.raises(ValueError, match="raw artifact"):
        _report(AcquisitionStatus.SUCCEEDED, records=_records(other), artifact=_artifact())


# --- fetch instant must sit inside the acquisition window -------------------
# This is the invariant that would have caught a fetcher reading a different
# clock from the pipeline: the report's own timestamps came from the injected
# clock while fetched_at came from wall time, so the report claimed a fetch
# that happened after the acquisition had already completed.


def _failed_report_fetched_at(fetched_at, *, started_at=STARTED, completed_at=FETCHED):
    return _report(
        AcquisitionStatus.FAILED,
        artifact=_artifact(fetched_at),
        started_at=started_at,
        completed_at=completed_at,
        http_status=500,
        failure=AcquisitionFailure(AcquisitionStage.HTTP, HttpResponseError(500, None)),
    )


def test_report_accepts_a_fetch_inside_its_own_window():
    assert _failed_report_fetched_at(STARTED + timedelta(milliseconds=500)).raw_artifact is not None


def test_report_accepts_a_fetch_on_either_boundary():
    assert _failed_report_fetched_at(STARTED) is not None
    assert _failed_report_fetched_at(FETCHED) is not None


def test_report_rejects_a_fetch_after_completion():
    with pytest.raises(ValueError, match="fetched_at must lie between"):
        _failed_report_fetched_at(FETCHED + timedelta(seconds=1))


def test_report_rejects_a_fetch_before_the_acquisition_started():
    with pytest.raises(ValueError, match="fetched_at must lie between"):
        _failed_report_fetched_at(STARTED - timedelta(seconds=1))


def test_a_publish_error_counts_what_kafka_acknowledged():
    report = _report(AcquisitionStatus.SUCCEEDED, records=_records(rows=({"id": 1}, {"id": 2})), artifact=_artifact())
    error = RecordPublishError(report=report, acknowledged=1, cause=TimeoutError("kafka"))
    assert error.acknowledged == 1 and error.report is report
    with pytest.raises(ValueError, match="acknowledged"):
        RecordPublishError(report=report, acknowledged=3, cause=TimeoutError("kafka"))


def test_a_block_carries_its_evidence():
    error = FetchBlockedError("challenge page", evidence="<script>")
    assert error.evidence == "<script>"
