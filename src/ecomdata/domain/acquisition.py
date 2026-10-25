"""Contracts of acquiring one listing page, shared by the pipeline and its callers.

Acquisition is ELT: a :class:`Fetcher` returns a :class:`FetchBundle` (the
document and any responses the browser captured), the raw store keeps it, an
:class:`Extractor` finds the rows in it, and the rows travel on as
:class:`~ecomdata.domain.records.SourceRecord` envelopes with every field the
source sent. Nothing here performs I/O; the objects only describe one bounded
request and what came of it.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Protocol

from ecomdata.domain import validation as v
from ecomdata.domain.observations import RawArtifact, ResourceType
from ecomdata.domain.records import SourceRecord

ROLE_DOCUMENT = "document"
ROLE_XHR = "xhr"
PART_ROLES = (ROLE_DOCUMENT, ROLE_XHR)


class AcquisitionStatus(str, Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class AcquisitionStage(str, Enum):
    ROBOTS = "ROBOTS"
    FETCH = "FETCH"
    HTTP = "HTTP"
    STORAGE = "STORAGE"
    PARSE = "PARSE"
    VALIDATION = "VALIDATION"


class AcquisitionError(Exception):
    """Base class for expected acquisition-boundary failures."""


class RobotsDeniedError(AcquisitionError):
    pass


class FetchTransportError(AcquisitionError):
    pass


class FetchBlockedError(AcquisitionError):
    """The source answered with a block, challenge or captcha page.

    Never retried quickly and never worked around: it opens the source circuit
    so the crawl backs off (plan risk K2).
    """

    def __init__(self, message: str, *, evidence: str | None = None):
        self.evidence = evidence
        super().__init__(message)


class HttpResponseError(AcquisitionError):
    def __init__(self, status_code: int, retry_after: str | None = None):
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(f"HTTP response status {status_code}")


WRITE_STAGES = frozenset({"PART", "MANIFEST", "RECORDS"})


class RawPersistenceError(AcquisitionError):
    def __init__(self, message: str, *, write_stage: str, raw_artifact: RawArtifact | None = None):
        if write_stage not in WRITE_STAGES:
            raise ValueError(f"write_stage must be one of {sorted(WRITE_STAGES)}")
        self.write_stage = write_stage
        self.raw_artifact = raw_artifact
        super().__init__(message)


class ListingPageParseError(AcquisitionError):
    pass


class RecordBuildError(AcquisitionError):
    """A row cannot be wrapped in a record envelope."""


class RecordPublishError(Exception):
    """An attempt stored its page but could not publish every record.

    Not an acquisition failure: the fetch, the raw write and the extraction all
    succeeded. ``acknowledged`` counts the records Kafka accepted before the
    failure, in publish order; the attempt audit records that as parsed, since
    only those can reach Silver.
    """

    def __init__(self, *, report: Any, acknowledged: int, cause: BaseException):
        records = len(getattr(report, "records", ()) or ())
        if isinstance(acknowledged, bool) or not isinstance(acknowledged, int) or not 0 <= acknowledged < max(records, 1):
            raise ValueError(f"acknowledged must be between 0 and {max(records - 1, 0)}, got {acknowledged!r}")
        self.report = report
        self.acknowledged = acknowledged
        super().__init__(f"published {acknowledged} of {records} records: {cause}")
        self.__cause__ = cause


def _error_message(error: Exception) -> str:
    return str(error)[:2000]


@dataclass(frozen=True)
class ListingPageRequest:
    marketplace_code: str
    marketplace_id: str
    target: str
    page: int
    resource_type: ResourceType = ResourceType.LISTING_PAGE

    def __post_init__(self) -> None:
        object.__setattr__(self, "marketplace_code", v.require_text(self.marketplace_code, "marketplace_code").lower())
        object.__setattr__(self, "marketplace_id", v.require_text(self.marketplace_id, "marketplace_id"))
        object.__setattr__(self, "target", v.require_text(self.target, "target"))
        v.non_bool_int(self.page, "page", minimum=1)
        if not isinstance(self.resource_type, ResourceType):
            raise ValueError("resource_type must be a ResourceType")
        if self.resource_type is not ResourceType.LISTING_PAGE:
            raise ValueError("only LISTING_PAGE requests are acquired")


@dataclass(frozen=True)
class FetchPart:
    """One HTTP response: the page document, or a response the browser captured."""

    url: str
    http_status: int
    content_type: str | None
    body: bytes
    role: str = ROLE_DOCUMENT
    elapsed_ms: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "url", v.http_url(self.url, "url"))
        v.http_status(self.http_status)
        if not isinstance(self.body, bytes):
            raise ValueError("body must be bytes")
        object.__setattr__(self, "content_type", v.optional_text(self.content_type, "content_type"))
        if self.role not in PART_ROLES:
            raise ValueError(f"role must be one of {PART_ROLES}")
        if self.elapsed_ms is not None:
            v.non_bool_int(self.elapsed_ms, "elapsed_ms")


@dataclass(frozen=True)
class FetchBundle:
    """Everything one fetch returned: exactly one document part, plus captured responses."""

    request_url: str
    fetched_at: datetime
    parts: tuple[FetchPart, ...]
    retry_after: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "request_url", v.http_url(self.request_url, "request_url"))
        object.__setattr__(self, "fetched_at", v.aware_utc(self.fetched_at, "fetched_at"))
        if not isinstance(self.parts, tuple) or not all(isinstance(part, FetchPart) for part in self.parts):
            raise ValueError("parts must be a tuple of FetchPart")
        if sum(1 for part in self.parts if part.role == ROLE_DOCUMENT) != 1:
            raise ValueError("a bundle holds exactly one document part")
        object.__setattr__(self, "retry_after", v.optional_text(self.retry_after, "retry_after"))

    @property
    def document(self) -> FetchPart:
        return next(part for part in self.parts if part.role == ROLE_DOCUMENT)

    @property
    def captured(self) -> tuple[FetchPart, ...]:
        return tuple(part for part in self.parts if part.role != ROLE_DOCUMENT)

    @property
    def http_status(self) -> int:
        return self.document.http_status

    @property
    def content_type(self) -> str | None:
        return self.document.content_type

    @property
    def body(self) -> bytes:
        return self.document.body

    @property
    def total_bytes(self) -> int:
        return sum(len(part.body) for part in self.parts)


@dataclass(frozen=True)
class ExtractedPage:
    """What an extractor found: the listing rows as sent, and the page around them."""

    rows: tuple[Mapping[str, Any], ...]
    page_context: Mapping[str, Any] = field(default_factory=dict)
    last_page: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.rows, tuple) or not all(isinstance(row, Mapping) for row in self.rows):
            raise ValueError("rows must be a tuple of objects")
        if not isinstance(self.page_context, Mapping):
            raise ValueError("page_context must be an object")
        if self.last_page is not None:
            v.non_bool_int(self.last_page, "last_page", minimum=1)


def encode_listing_page_task_target(target: str, page: int) -> str:
    """Encode a generic target/page pair for the frontier's string task field."""
    target = v.require_text(target, "target")
    v.non_bool_int(page, "page", minimum=1)
    return json.dumps({"page": page, "target": target}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decode_listing_page_task_target(value: str) -> tuple[str, int]:
    """Decode and strictly validate the canonical task-target JSON string."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("task target must be non-empty text")
    try:
        decoded = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("task target must be valid JSON") from exc
    if not isinstance(decoded, dict) or set(decoded) != {"page", "target"}:
        raise ValueError("task target must contain exactly page and target")
    target = v.require_text(decoded["target"], "target")
    page = v.non_bool_int(decoded["page"], "page", minimum=1)
    return target, page


@dataclass(frozen=True)
class AcquisitionFailure:
    stage: AcquisitionStage
    error: Exception

    def __post_init__(self) -> None:
        if not isinstance(self.stage, AcquisitionStage):
            raise ValueError("stage must be an AcquisitionStage")
        v.exception(self.error)

    @property
    def error_type(self) -> str:
        return type(self.error).__name__

    @property
    def error_message(self) -> str:
        return _error_message(self.error)


@dataclass(frozen=True)
class AcquisitionReport:
    status: AcquisitionStatus
    marketplace_code: str
    marketplace_id: str
    target: str
    page: int
    resource_type: ResourceType
    crawl_run_id: str
    request_url: str
    started_at: datetime
    completed_at: datetime
    http_status: int | None
    retry_after: str | None
    raw_artifact: RawArtifact | None
    raw_metadata_uri: str | None
    records: tuple[SourceRecord, ...]
    records_uri: str | None
    last_page: int | None
    failure: AcquisitionFailure | None
    # Rows and page-context records the page held; ``records`` is what is published.
    row_count: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.status, AcquisitionStatus):
            raise ValueError("status must be an AcquisitionStatus")
        object.__setattr__(self, "marketplace_code", v.require_text(self.marketplace_code, "marketplace_code").lower())
        object.__setattr__(self, "marketplace_id", v.require_text(self.marketplace_id, "marketplace_id"))
        object.__setattr__(self, "target", v.require_text(self.target, "target"))
        v.non_bool_int(self.page, "page", minimum=1)
        if not isinstance(self.resource_type, ResourceType):
            raise ValueError("resource_type must be a ResourceType")
        object.__setattr__(self, "crawl_run_id", v.require_text(self.crawl_run_id, "crawl_run_id"))
        object.__setattr__(self, "request_url", v.http_url(self.request_url, "request_url"))
        started_at = v.aware_utc(self.started_at, "started_at")
        completed_at = v.aware_utc(self.completed_at, "completed_at")
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "completed_at", completed_at)
        if completed_at < started_at:
            raise ValueError("completed_at must not be before started_at")
        if self.http_status is not None:
            v.http_status(self.http_status)
        object.__setattr__(self, "retry_after", v.optional_text(self.retry_after, "retry_after"))
        if not isinstance(self.records, tuple) or not all(isinstance(item, SourceRecord) for item in self.records):
            raise ValueError("records must be a tuple of SourceRecord")
        v.non_bool_int(self.row_count, "row_count")
        if self.last_page is not None:
            v.non_bool_int(self.last_page, "last_page", minimum=1)
        if self.raw_metadata_uri is not None:
            object.__setattr__(self, "raw_metadata_uri", v.require_text(self.raw_metadata_uri, "raw_metadata_uri"))
        if self.records_uri is not None:
            object.__setattr__(self, "records_uri", v.require_text(self.records_uri, "records_uri"))
        if self.raw_artifact is not None and not isinstance(self.raw_artifact, RawArtifact):
            raise ValueError("raw_artifact must be a RawArtifact")
        if self.failure is not None and not isinstance(self.failure, AcquisitionFailure):
            raise ValueError("failure must be an AcquisitionFailure")

        if self.status is AcquisitionStatus.SUCCEEDED:
            if self.failure is not None:
                raise ValueError("SUCCEEDED report cannot contain a failure")
            if self.raw_artifact is None or self.raw_metadata_uri is None or self.records_uri is None:
                raise ValueError("successful report requires the raw artifact, its manifest URI and the records URI")
        else:
            if self.failure is None:
                raise ValueError("FAILED report requires a failure")
            if self.records:
                raise ValueError("FAILED report cannot contain records")

        if self.raw_artifact is None:
            if self.raw_metadata_uri is not None:
                raise ValueError("manifest URI requires a raw artifact")
            return
        if self.raw_artifact.marketplace_id != self.marketplace_id:
            raise ValueError("raw artifact marketplace_id does not match report")
        if self.raw_artifact.crawl_run_id != self.crawl_run_id:
            raise ValueError("raw artifact crawl_run_id does not match report")
        if self.raw_artifact.resource_type is not self.resource_type:
            raise ValueError("raw artifact resource_type does not match report")
        # The fetch happened inside this acquisition, so its instant must lie
        # inside the report's own window. Outside it, the fetcher and the
        # pipeline read different clocks, and every partition keyed on
        # fetched_at would be stamped by whichever clock the machine had.
        if not started_at <= self.raw_artifact.fetched_at <= completed_at:
            raise ValueError("raw artifact fetched_at must lie between started_at and completed_at")
        if self.raw_metadata_uri is None and not (
            self.failure is not None
            and self.failure.stage is AcquisitionStage.STORAGE
            and isinstance(self.failure.error, RawPersistenceError)
        ):
            raise ValueError("raw artifact requires a manifest URI unless the manifest write failed")
        for record in self.records:
            if record.source_code != self.marketplace_code:
                raise ValueError("record source does not match report")
            if record.crawl_run_id != self.crawl_run_id:
                raise ValueError("record crawl_run_id does not match report")
            if record.raw_artifact_id != self.raw_artifact.raw_artifact_id:
                raise ValueError("record raw artifact does not match the report's artifact")
            if record.body_sha256 != self.raw_artifact.body_sha256:
                raise ValueError("record checksum does not match artifact")
            if record.adapter_version != self.raw_artifact.adapter_version:
                raise ValueError("record adapter version does not match artifact")

    @property
    def record_count(self) -> int:
        """Listing rows kept; ``audit.crawl_request_attempt.parsed_count``."""
        return self.row_count

    @property
    def rejected_count(self) -> int:
        return 0

    @property
    def raw_bytes(self) -> int:
        return self.raw_artifact.raw_bytes if self.raw_artifact is not None else 0

    @property
    def page_exhausted(self) -> bool:
        return self.row_count == 0 or (self.last_page is not None and self.page >= self.last_page)

    @property
    def is_task_success(self) -> bool:
        return self.status is AcquisitionStatus.SUCCEEDED


class Fetcher(Protocol):
    """How a source's pages are downloaded (strategy: HTTP, browser, API)."""

    def request_url(self, request: ListingPageRequest) -> str: ...

    def allowed(self, url: str) -> bool: ...

    def throttle(self, url: str) -> None: ...

    def fetch(self, request: ListingPageRequest, url: str) -> FetchBundle: ...

    def close(self) -> None: ...


class Extractor(Protocol):
    """Finds the listing rows and the page context in a fetched bundle; changes nothing."""

    def request_url(self, base_url: str, target: str, page: int) -> str: ...

    def extract(self, bundle: FetchBundle) -> ExtractedPage: ...
