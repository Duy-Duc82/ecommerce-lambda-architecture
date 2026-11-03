"""The acquisition pipeline: raw first, records second, every failure a report."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from ecomdata.domain.acquisition import (
    AcquisitionStage,
    AcquisitionStatus,
    ExtractedPage,
    FetchBlockedError,
    FetchBundle,
    FetchPart,
    ListingPageParseError,
    ListingPageRequest,
)
from ecomdata.domain.sources import TIKI
from ecomdata.ingestion.pipeline import acquire_listing_page
from ecomdata.ingestion.record_store import decode_records

START = datetime(2026, 9, 4, 8, 30, tzinfo=UTC)
FETCHED = START + timedelta(seconds=1)
DONE = START + timedelta(seconds=3)
URL = "https://tiki.vn/laptop/c8095"


class FakeFetcher:
    def __init__(self, *, status=200, allowed=True, fetch_error=None, retry_after=None, url_error=None):
        self.status, self.allowed_result, self.fetch_error = status, allowed, fetch_error
        self.retry_after, self.url_error = retry_after, url_error
        self.calls = []

    def request_url(self, request):
        self.calls.append("url")
        if self.url_error:
            raise self.url_error
        return URL

    def allowed(self, url):
        self.calls.append("robots")
        if isinstance(self.allowed_result, Exception):
            raise self.allowed_result
        return self.allowed_result

    def throttle(self, url):
        self.calls.append("throttle")

    def fetch(self, request, url):
        self.calls.append("fetch")
        if self.fetch_error is not None:
            raise self.fetch_error
        part = FetchPart(url, self.status, "text/html", b"<page>")
        return FetchBundle(url, FETCHED, (part,), retry_after=self.retry_after)


class FakeExtractor:
    def __init__(self, result=None, error=None, log=None):
        self.result = result or ExtractedPage(rows=({"id": 1}, {"id": 2}), page_context={"h1": "x"}, last_page=1)
        self.error, self.log = error, log if log is not None else []

    def extract(self, bundle):
        self.log.append("extract")
        if self.error:
            raise self.error
        return self.result


def request():
    return ListingPageRequest("tiki", "marketplace-tiki", "laptop/c8095", 1)


class Lake:
    def __init__(self, fail_on=None):
        self.writes, self.fail_on = [], fail_on

    def write(self, zone, path, data):
        self.writes.append((zone, path, data))
        if self.fail_on and self.fail_on in path:
            raise OSError("writer failure")
        return f"file:///lake/{path}"

    @property
    def names(self):
        return [path.split("/")[-1] for _, path, _ in self.writes]


def clock():
    values = iter([START, DONE, DONE, DONE, DONE])
    return lambda: next(values)


def acquire(fetcher=None, extractor=None, lake=None, req=None):
    fetcher = fetcher or FakeFetcher()
    lake = lake or Lake()
    report = acquire_listing_page(
        TIKI, fetcher, extractor or FakeExtractor(), req or request(), crawl_run_id="run-1", writer=lake.write, clock=clock()
    )
    return report, fetcher, lake


def test_a_success_stores_the_raw_parts_then_the_manifest_then_the_records():
    log = []
    fetcher, extractor = FakeFetcher(), FakeExtractor(log=log)

    report, fetcher, lake = acquire(fetcher, extractor)

    assert report.status is AcquisitionStatus.SUCCEEDED and report.is_task_success
    assert fetcher.calls == ["url", "robots", "throttle", "fetch"]
    assert lake.names[:2] == ["part-0.bin.gz", "metadata.json"]
    assert lake.names[2].endswith(".jsonl.gz")
    assert report.row_count == 2 and report.record_count == 2 and report.page_exhausted
    assert len(report.records) == 3  # the page context and two rows
    assert report.raw_artifact.adapter_version == TIKI.adapter_version
    assert report.records_uri == f"file:///lake/{lake.writes[2][1]}"
    assert [r.record_id for r in decode_records(lake.writes[2][2])] == [r.record_id for r in report.records]


def test_robots_denial_has_no_fetch_and_no_write():
    report, fetcher, lake = acquire(FakeFetcher(allowed=False))

    assert report.failure.stage is AcquisitionStage.ROBOTS and report.status is AcquisitionStatus.FAILED
    assert fetcher.calls == ["url", "robots"] and lake.writes == []


def test_a_failing_robots_check_denies():
    report, _, lake = acquire(FakeFetcher(allowed=RuntimeError("dns")))

    assert report.failure.stage is AcquisitionStage.ROBOTS and "dns" in str(report.failure.error)
    assert lake.writes == []


def test_a_transport_failure_has_no_raw_artifact():
    report, _, lake = acquire(FakeFetcher(fetch_error=ConnectionError("network down")))

    assert report.failure.stage is AcquisitionStage.FETCH and report.raw_artifact is None
    assert type(report.failure.error).__name__ == "FetchTransportError"
    assert lake.writes == []


def test_a_block_is_reported_as_itself_so_it_can_open_the_circuit():
    report, _, _ = acquire(FakeFetcher(fetch_error=FetchBlockedError("captcha", evidence="slide to verify")))

    assert isinstance(report.failure.error, FetchBlockedError)
    assert report.failure.stage is AcquisitionStage.FETCH


def test_a_part_write_failure_stops_before_the_manifest():
    report, _, lake = acquire(lake=Lake(fail_on="part-0"))

    assert report.failure.stage is AcquisitionStage.STORAGE and report.raw_artifact is None
    assert lake.names == ["part-0.bin.gz"]


def test_a_manifest_write_failure_keeps_the_artifact_and_never_extracts():
    log = []
    report, _, lake = acquire(extractor=FakeExtractor(log=log), lake=Lake(fail_on="metadata.json"))

    assert report.failure.stage is AcquisitionStage.STORAGE
    assert report.raw_artifact is not None and report.raw_metadata_uri is None
    assert log == [] and lake.names == ["part-0.bin.gz", "metadata.json"]


@pytest.mark.parametrize("status", [404, 429, 500, 503])
def test_an_http_failure_persists_the_page_before_judging_the_status(status):
    report, _, lake = acquire(FakeFetcher(status=status, retry_after="9"))

    assert report.failure.stage is AcquisitionStage.HTTP and report.http_status == status
    assert report.retry_after == "9" and report.raw_artifact is not None
    assert lake.names == ["part-0.bin.gz", "metadata.json"]


def test_a_parse_failure_keeps_the_complete_raw_lineage():
    report, _, lake = acquire(extractor=FakeExtractor(error=ListingPageParseError("layout changed")))

    assert report.failure.stage is AcquisitionStage.PARSE
    assert report.raw_artifact is not None and report.raw_metadata_uri is not None
    assert lake.names == ["part-0.bin.gz", "metadata.json"]


def test_an_unexpected_extractor_error_becomes_a_parse_failure():
    report, _, _ = acquire(extractor=FakeExtractor(error=KeyError("props")))

    assert report.failure.stage is AcquisitionStage.PARSE
    assert isinstance(report.failure.error, ListingPageParseError)
    assert isinstance(report.failure.error.__cause__, KeyError)


def test_a_block_found_while_extracting_stays_a_block():
    report, _, _ = acquire(extractor=FakeExtractor(error=FetchBlockedError("no __NEXT_DATA__")))

    assert isinstance(report.failure.error, FetchBlockedError)
    assert report.failure.stage is AcquisitionStage.PARSE and report.raw_artifact is not None


def test_a_records_write_failure_keeps_the_raw_artifact():
    report, _, lake = acquire(lake=Lake(fail_on=".jsonl.gz"))

    assert report.failure.stage is AcquisitionStage.STORAGE
    assert report.raw_artifact is not None and report.raw_metadata_uri is not None and report.records == ()


def test_a_page_with_no_rows_is_a_success_and_exhausted():
    empty = ExtractedPage(rows=(), page_context={"h1": "x"}, last_page=None)

    report, _, _ = acquire(extractor=FakeExtractor(empty))

    assert report.is_task_success and report.row_count == 0 and report.page_exhausted
    assert len(report.records) == 1  # the page context alone


def test_the_last_page_ends_a_paginated_walk():
    page = ExtractedPage(rows=({"id": 1},), last_page=3)

    report, _, _ = acquire(extractor=FakeExtractor(page), req=ListingPageRequest("tiki", "marketplace-tiki", "laptop/c8095", 2))

    assert report.page == 2 and not report.page_exhausted
    assert report.last_page == 3


def test_a_target_the_source_cannot_turn_into_a_url_is_a_failed_report():
    # Regression, bug (c): request_url() ran outside the try, so the ValueError
    # left the pipeline instead of becoming a FAILED report.
    fetcher = FakeFetcher(url_error=ValueError("category id must be digits"))

    report, fetcher, lake = acquire(fetcher)

    assert report.status is AcquisitionStatus.FAILED and report.failure.stage is AcquisitionStage.FETCH
    assert "category id must be digits" in str(report.failure.error)
    assert fetcher.calls == ["url"] and lake.writes == []


def test_a_request_of_another_source_is_a_programming_error():
    other = ListingPageRequest("lazada", "marketplace-lazada", "laptop", 1)

    with pytest.raises(ValueError, match="does not belong"):
        acquire(req=other)


def test_a_fetcher_returning_the_wrong_type_is_a_programming_error():
    class Broken(FakeFetcher):
        def fetch(self, request, url):
            return b"raw"

    with pytest.raises(TypeError, match="FetchBundle"):
        acquire(Broken())
