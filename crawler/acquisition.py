"""Raw-first orchestration for one marketplace listing-page request."""

from __future__ import annotations

from datetime import datetime
from typing import Callable

from common.object_store import put_bytes
from crawler.contracts import (
    AcquisitionFailure,
    AcquisitionStage,
    AcquisitionStatus,
    FetchResult,
    FetchTransportError,
    HttpResponseError,
    ListingPageAdapter,
    ListingPageParseError,
    ListingPageRequest,
    Phase2AcquisitionReport,
    ParsedListingPage,
    RawPersistenceError,
    RobotsDeniedError,
)
from crawler.raw_store import persist_fetch_result


def _completion(clock: Callable[[], datetime]) -> datetime:
    return clock()


def _failed_report(
    *,
    request: ListingPageRequest,
    crawl_run_id: str,
    request_url: str,
    started_at: datetime,
    http_status: int | None,
    retry_after: str | None,
    raw_artifact,
    raw_metadata_uri: str | None,
    failure: AcquisitionFailure,
    clock: Callable[[], datetime],
    rejections=(),
    source_record_count: int = 0,
    duplicate_count: int = 0,
    last_page: int | None = None,
) -> Phase2AcquisitionReport:
    return Phase2AcquisitionReport(
        status=AcquisitionStatus.FAILED,
        marketplace_code=request.marketplace_code,
        marketplace_id=request.marketplace_id,
        target=request.target,
        page=request.page,
        resource_type=request.resource_type,
        crawl_run_id=crawl_run_id,
        request_url=request_url,
        started_at=started_at,
        completed_at=_completion(clock),
        http_status=http_status,
        retry_after=retry_after,
        raw_artifact=raw_artifact,
        raw_metadata_uri=raw_metadata_uri,
        observations=(),
        rejections=tuple(rejections),
        source_record_count=source_record_count,
        duplicate_count=duplicate_count,
        last_page=last_page,
        failure=failure,
    )


def _report_from_parsed(
    *,
    request: ListingPageRequest,
    crawl_run_id: str,
    fetch_result: FetchResult,
    persisted,
    parsed: ParsedListingPage,
    started_at: datetime,
    clock: Callable[[], datetime],
) -> Phase2AcquisitionReport:
    if parsed.rejections and parsed.observations:
        status = AcquisitionStatus.PARTIAL
        failure = None
    elif parsed.rejections:
        status = AcquisitionStatus.FAILED
        failure = AcquisitionFailure(
            AcquisitionStage.VALIDATION,
            ValueError("all non-duplicate listing rows were rejected"),
        )
    else:
        status = AcquisitionStatus.SUCCEEDED
        failure = None

    return Phase2AcquisitionReport(
        status=status,
        marketplace_code=request.marketplace_code,
        marketplace_id=request.marketplace_id,
        target=request.target,
        page=request.page,
        resource_type=request.resource_type,
        crawl_run_id=crawl_run_id,
        request_url=fetch_result.request_url,
        started_at=started_at,
        completed_at=_completion(clock),
        http_status=fetch_result.http_status,
        retry_after=fetch_result.retry_after,
        raw_artifact=persisted.artifact,
        raw_metadata_uri=persisted.metadata_uri,
        observations=parsed.observations,
        rejections=parsed.rejections,
        source_record_count=parsed.source_record_count,
        duplicate_count=parsed.duplicate_count,
        last_page=parsed.last_page,
        failure=failure,
    )


def acquire_listing_page(
    adapter: ListingPageAdapter,
    request: ListingPageRequest,
    *,
    crawl_run_id: str,
    writer: Callable[[str, str, bytes], str] = put_bytes,
    clock: Callable[[], datetime],
) -> Phase2AcquisitionReport:
    """Fetch, persist and parse one page in the mandatory raw-first order."""
    if not isinstance(request, ListingPageRequest):
        raise TypeError("request must be a ListingPageRequest")
    if not isinstance(crawl_run_id, str) or not crawl_run_id.strip():
        raise ValueError("crawl_run_id is required")
    if not callable(clock):
        raise TypeError("clock must be callable")
    if request.marketplace_code != adapter.site_name:
        raise ValueError("request marketplace_code does not match adapter")
    if request.marketplace_id != adapter.marketplace_id:
        raise ValueError("request marketplace_id does not match adapter")
    if not isinstance(adapter.adapter_version, str) or not adapter.adapter_version.strip():
        raise ValueError("adapter must expose a non-empty adapter_version")

    started_at = clock()
    requested_url = adapter.request_url(request.target, request.page)
    try:
        allowed = adapter.allowed(requested_url)
    except Exception as exc:
        error = RobotsDeniedError(f"robots check failed for {requested_url}: {exc}")
        error.__cause__ = exc
        return _failed_report(
            request=request,
            crawl_run_id=crawl_run_id,
            request_url=requested_url,
            started_at=started_at,
            http_status=None,
            retry_after=None,
            raw_artifact=None,
            raw_metadata_uri=None,
            failure=AcquisitionFailure(AcquisitionStage.ROBOTS, error),
            clock=clock,
        )
    if not allowed:
        return _failed_report(
            request=request,
            crawl_run_id=crawl_run_id,
            request_url=requested_url,
            started_at=started_at,
            http_status=None,
            retry_after=None,
            raw_artifact=None,
            raw_metadata_uri=None,
            failure=AcquisitionFailure(
                AcquisitionStage.ROBOTS,
                RobotsDeniedError(f"robots.txt disallows {requested_url}"),
            ),
            clock=clock,
        )

    try:
        adapter.throttle()
        fetch_result = adapter.fetch_listing_page(request)
    except Exception as exc:
        wrapped = FetchTransportError(f"fetch failed for {requested_url}: {exc}")
        wrapped.__cause__ = exc
        return _failed_report(
            request=request,
            crawl_run_id=crawl_run_id,
            request_url=requested_url,
            started_at=started_at,
            http_status=None,
            retry_after=None,
            raw_artifact=None,
            raw_metadata_uri=None,
            failure=AcquisitionFailure(AcquisitionStage.FETCH, wrapped),
            clock=clock,
        )
    if not isinstance(fetch_result, FetchResult):
        raise TypeError("adapter.fetch_listing_page() must return FetchResult")

    try:
        persisted = persist_fetch_result(
            request=request,
            fetch_result=fetch_result,
            crawl_run_id=crawl_run_id,
            adapter_version=adapter.adapter_version,
            writer=writer,
        )
    except RawPersistenceError as exc:
        return _failed_report(
            request=request,
            crawl_run_id=crawl_run_id,
            request_url=fetch_result.request_url,
            started_at=started_at,
            http_status=fetch_result.http_status,
            retry_after=fetch_result.retry_after,
            raw_artifact=exc.raw_artifact,
            raw_metadata_uri=None,
            failure=AcquisitionFailure(AcquisitionStage.STORAGE, exc),
            clock=clock,
        )

    if not 200 <= fetch_result.http_status <= 299:
        error = HttpResponseError(fetch_result.http_status, fetch_result.retry_after)
        return _failed_report(
            request=request,
            crawl_run_id=crawl_run_id,
            request_url=fetch_result.request_url,
            started_at=started_at,
            http_status=fetch_result.http_status,
            retry_after=fetch_result.retry_after,
            raw_artifact=persisted.artifact,
            raw_metadata_uri=persisted.metadata_uri,
            failure=AcquisitionFailure(AcquisitionStage.HTTP, error),
            clock=clock,
        )

    produced_at = clock()
    try:
        parsed = adapter.parse_listing_page(
            request=request,
            fetch_result=fetch_result,
            raw_artifact=persisted.artifact,
            crawl_run_id=crawl_run_id,
            produced_at=produced_at,
        )
    except Exception as exc:
        if isinstance(exc, ListingPageParseError):
            error = exc
        else:
            error = ListingPageParseError(f"listing page parse failed: {exc}")
            error.__cause__ = exc
        return _failed_report(
            request=request,
            crawl_run_id=crawl_run_id,
            request_url=fetch_result.request_url,
            started_at=started_at,
            http_status=fetch_result.http_status,
            retry_after=fetch_result.retry_after,
            raw_artifact=persisted.artifact,
            raw_metadata_uri=persisted.metadata_uri,
            failure=AcquisitionFailure(AcquisitionStage.PARSE, error),
            clock=clock,
        )
    if not isinstance(parsed, ParsedListingPage):
        raise TypeError("adapter.parse_listing_page() must return ParsedListingPage")
    return _report_from_parsed(
        request=request,
        crawl_run_id=crawl_run_id,
        fetch_result=fetch_result,
        persisted=persisted,
        parsed=parsed,
        started_at=started_at,
        clock=clock,
    )
