"""Acquire one listing page, raw first.

The steps and their order are fixed; what varies by source is injected:

    url -> robots -> throttle -> fetch -> persist raw -> HTTP status ->
    extract -> build records -> persist records -> report

A failure at any step ends in a FAILED report that says which step and keeps
whatever was already stored. The raw responses are written before anything
parses them, and the records file before anything publishes them.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from ecomdata.domain.acquisition import (
    AcquisitionFailure,
    AcquisitionReport,
    AcquisitionStage,
    AcquisitionStatus,
    ExtractedPage,
    FetchBlockedError,
    FetchBundle,
    FetchTransportError,
    HttpResponseError,
    ListingPageParseError,
    ListingPageRequest,
    RawPersistenceError,
    RecordBuildError,
    RobotsDeniedError,
)
from ecomdata.domain.records import build_records
from ecomdata.ingestion.raw_store import persist_fetch_bundle
from ecomdata.ingestion.record_store import persist_records

# A report needs a URL even when none could be built.
_UNKNOWN_URL = "https://invalid.invalid/"


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
) -> AcquisitionReport:
    return AcquisitionReport(
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
        records=(),
        records_uri=None,
        last_page=None,
        failure=failure,
    )


def acquire_listing_page(
    spec: Any,
    fetcher: Any,
    extractor: Any,
    request: ListingPageRequest,
    *,
    crawl_run_id: str,
    writer: Callable[[str, str, bytes], str],
    clock: Callable[[], datetime],
) -> AcquisitionReport:
    """Fetch, store and extract one page in the mandatory raw-first order."""
    if request.marketplace_code != spec.code or request.marketplace_id != spec.marketplace_id:
        raise ValueError("request does not belong to the source spec")

    started_at = clock()

    def fail(stage, error, *, url, bundle=None, persisted=None, raw_artifact=None) -> AcquisitionReport:
        return _failed_report(
            request=request,
            crawl_run_id=crawl_run_id,
            request_url=url,
            started_at=started_at,
            http_status=bundle.http_status if bundle else None,
            retry_after=bundle.retry_after if bundle else None,
            raw_artifact=persisted.artifact if persisted else raw_artifact,
            raw_metadata_uri=persisted.metadata_uri if persisted else None,
            failure=AcquisitionFailure(stage, error),
            clock=clock,
        )

    try:
        url = fetcher.request_url(request)
    except ValueError as exc:
        # A target the source cannot turn into a URL is a failed task, not a
        # crash of the worker: the report carries the reason.
        error = FetchTransportError(f"no request URL for target {request.target!r} page {request.page}: {exc}")
        error.__cause__ = exc
        return fail(AcquisitionStage.FETCH, error, url=getattr(spec, "base_url", None) or _UNKNOWN_URL)

    try:
        allowed = fetcher.allowed(url)
    except Exception as exc:
        error = RobotsDeniedError(f"robots check failed for {url}: {exc}")
        error.__cause__ = exc
        return fail(AcquisitionStage.ROBOTS, error, url=url)
    if not allowed:
        return fail(AcquisitionStage.ROBOTS, RobotsDeniedError(f"robots.txt disallows {url}"), url=url)

    try:
        fetcher.throttle(url)
        bundle = fetcher.fetch(request, url)
    except FetchBlockedError as exc:
        return fail(AcquisitionStage.FETCH, exc, url=url)
    except Exception as exc:
        wrapped = FetchTransportError(f"fetch failed for {url}: {exc}")
        wrapped.__cause__ = exc
        return fail(AcquisitionStage.FETCH, wrapped, url=url)
    if not isinstance(bundle, FetchBundle):
        raise TypeError("fetcher.fetch() must return a FetchBundle")

    try:
        persisted = persist_fetch_bundle(
            request=request,
            bundle=bundle,
            crawl_run_id=crawl_run_id,
            adapter_version=spec.adapter_version,
            writer=writer,
        )
    except RawPersistenceError as exc:
        return fail(AcquisitionStage.STORAGE, exc, url=bundle.request_url, bundle=bundle, raw_artifact=exc.raw_artifact)

    def fail_after_store(stage, error) -> AcquisitionReport:
        return fail(stage, error, url=bundle.request_url, bundle=bundle, persisted=persisted)

    if not 200 <= bundle.http_status <= 299:
        return fail_after_store(AcquisitionStage.HTTP, HttpResponseError(bundle.http_status, bundle.retry_after))

    try:
        extracted = extractor.extract(bundle)
    except (ListingPageParseError, FetchBlockedError) as exc:
        return fail_after_store(AcquisitionStage.PARSE, exc)
    except Exception as exc:
        error = ListingPageParseError(f"listing page extraction failed: {exc}")
        error.__cause__ = exc
        return fail_after_store(AcquisitionStage.PARSE, error)
    if not isinstance(extracted, ExtractedPage):
        raise TypeError("extractor.extract() must return an ExtractedPage")

    try:
        records = build_records(
            spec,
            request=request,
            raw_artifact=persisted.artifact,
            rows=extracted.rows,
            page_context=extracted.page_context,
        )
    except Exception as exc:
        error = RecordBuildError(f"records could not be built: {exc}")
        error.__cause__ = exc
        return fail_after_store(AcquisitionStage.VALIDATION, error)

    try:
        records_uri = persist_records(records, writer=writer)
    except RawPersistenceError as exc:
        return fail_after_store(AcquisitionStage.STORAGE, exc)

    return AcquisitionReport(
        status=AcquisitionStatus.SUCCEEDED,
        marketplace_code=request.marketplace_code,
        marketplace_id=request.marketplace_id,
        target=request.target,
        page=request.page,
        resource_type=request.resource_type,
        crawl_run_id=crawl_run_id,
        request_url=bundle.request_url,
        started_at=started_at,
        completed_at=_completion(clock),
        http_status=bundle.http_status,
        retry_after=bundle.retry_after,
        raw_artifact=persisted.artifact,
        raw_metadata_uri=persisted.metadata_uri,
        records=records,
        records_uri=records_uri,
        last_page=extracted.last_page,
        failure=None,
        row_count=len(extracted.rows),
    )
