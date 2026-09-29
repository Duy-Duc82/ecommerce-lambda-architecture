"""Contracts shared by raw-first marketplace acquisition and its callers.

The objects in this module describe one bounded listing-page request.  They do
not perform HTTP, storage, scheduling or publication.  Keeping the boundary
small lets Phase 3 wrap acquisition with leases/retries and lets Phase 4
publish only validated observations.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Protocol
from urllib.parse import urlparse

from config.marketplace_schema import (
    MarketplaceObservationV1,
    RawArtifact,
    ResourceType,
)


class AcquisitionStatus(str, Enum):
    SUCCEEDED = "SUCCEEDED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class AcquisitionStage(str, Enum):
    ROBOTS = "ROBOTS"
    FETCH = "FETCH"
    HTTP = "HTTP"
    STORAGE = "STORAGE"
    PARSE = "PARSE"
    VALIDATION = "VALIDATION"


class Phase2AcquisitionError(Exception):
    """Base class for expected acquisition-boundary failures."""


class RobotsDeniedError(Phase2AcquisitionError):
    pass


class FetchTransportError(Phase2AcquisitionError):
    pass


class HttpResponseError(Phase2AcquisitionError):
    def __init__(self, status_code: int, retry_after: str | None = None):
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(f"HTTP response status {status_code}")


class RawPersistenceError(Phase2AcquisitionError):
    def __init__(
        self,
        message: str,
        *,
        write_stage: str,
        raw_artifact: RawArtifact | None = None,
    ):
        if write_stage not in {"BODY", "METADATA"}:
            raise ValueError("write_stage must be BODY or METADATA")
        self.write_stage = write_stage
        self.raw_artifact = raw_artifact
        super().__init__(message)


class ListingPageParseError(Phase2AcquisitionError):
    pass


class CanonicalRecordError(Phase2AcquisitionError):
    pass


def _require_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be text")
    value = value.strip()
    if not value:
        raise ValueError(f"{field_name} is required")
    return value


def _optional_text(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be text")
    value = value.strip()
    return value or None


def _aware_utc(value: Any, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _non_bool_int(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer, not bool")
    if value < minimum:
        raise ValueError(f"{field_name} must be at least {minimum}")
    return value


def _http_url(value: Any, field_name: str) -> str:
    value = _require_text(value, field_name)
    try:
        parsed = urlparse(value)
        hostname = parsed.hostname
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid HTTP(S) URL") from exc
    if parsed.scheme.lower() not in {"http", "https"} or not hostname:
        raise ValueError(f"{field_name} must be a valid HTTP(S) URL")
    return value


def _error(value: Any, field_name: str = "error") -> Exception:
    if not isinstance(value, Exception):
        raise ValueError(f"{field_name} must be an Exception")
    return value


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
        object.__setattr__(self, "marketplace_code", _require_text(self.marketplace_code, "marketplace_code").lower())
        object.__setattr__(self, "marketplace_id", _require_text(self.marketplace_id, "marketplace_id"))
        object.__setattr__(self, "target", _require_text(self.target, "target"))
        _non_bool_int(self.page, "page", minimum=1)
        if not isinstance(self.resource_type, ResourceType):
            raise ValueError("resource_type must be a ResourceType")
        if self.resource_type is not ResourceType.LISTING_PAGE:
            raise ValueError("Phase 2 accepts only LISTING_PAGE requests")


@dataclass(frozen=True)
class FetchResult:
    request_url: str
    fetched_at: datetime
    http_status: int
    content_type: str | None
    body: bytes
    retry_after: str | None = None
    elapsed_ms: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "request_url", _http_url(self.request_url, "request_url"))
        object.__setattr__(self, "fetched_at", _aware_utc(self.fetched_at, "fetched_at"))
        _non_bool_int(self.http_status, "http_status", minimum=100)
        if self.http_status > 599:
            raise ValueError("http_status must be between 100 and 599")
        if not isinstance(self.body, bytes):
            raise ValueError("body must be bytes")
        object.__setattr__(self, "content_type", _optional_text(self.content_type, "content_type"))
        object.__setattr__(self, "retry_after", _optional_text(self.retry_after, "retry_after"))
        if self.elapsed_ms is not None:
            _non_bool_int(self.elapsed_ms, "elapsed_ms")


def encode_listing_page_task_target(target: str, page: int) -> str:
    """Encode a generic target/page pair for Phase 3's string task field."""
    target = _require_text(target, "target")
    _non_bool_int(page, "page", minimum=1)
    return json.dumps(
        {"page": page, "target": target},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


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
    target = _require_text(decoded["target"], "target")
    page = _non_bool_int(decoded["page"], "page", minimum=1)
    return target, page


@dataclass(frozen=True)
class RecordRejection:
    record_index: int
    stage: AcquisitionStage
    error: Exception
    platform_listing_id: str | None = None

    def __post_init__(self) -> None:
        _non_bool_int(self.record_index, "record_index")
        if self.stage not in {AcquisitionStage.PARSE, AcquisitionStage.VALIDATION}:
            raise ValueError("record rejection stage must be PARSE or VALIDATION")
        _error(self.error)
        object.__setattr__(
            self,
            "platform_listing_id",
            _optional_text(self.platform_listing_id, "platform_listing_id"),
        )

    @property
    def error_type(self) -> str:
        return type(self.error).__name__

    @property
    def error_message(self) -> str:
        return _error_message(self.error)


@dataclass(frozen=True)
class ParsedListingPage:
    observations: tuple[MarketplaceObservationV1, ...]
    rejections: tuple[RecordRejection, ...]
    source_record_count: int
    duplicate_count: int
    last_page: int | None

    def __post_init__(self) -> None:
        if not isinstance(self.observations, tuple):
            raise ValueError("observations must be a tuple")
        if not all(isinstance(item, MarketplaceObservationV1) for item in self.observations):
            raise ValueError("observations must contain MarketplaceObservationV1 values")
        if not isinstance(self.rejections, tuple):
            raise ValueError("rejections must be a tuple")
        if not all(isinstance(item, RecordRejection) for item in self.rejections):
            raise ValueError("rejections must contain RecordRejection values")
        _non_bool_int(self.source_record_count, "source_record_count")
        _non_bool_int(self.duplicate_count, "duplicate_count")
        if len({item.event_id for item in self.observations}) != len(self.observations):
            raise ValueError("observations must not contain duplicate event IDs")
        if len(self.observations) + len(self.rejections) + self.duplicate_count != self.source_record_count:
            raise ValueError("accepted + rejected + duplicate rows must equal source_record_count")
        if self.last_page is not None:
            _non_bool_int(self.last_page, "last_page", minimum=1)


@dataclass(frozen=True)
class AcquisitionFailure:
    stage: AcquisitionStage
    error: Exception

    def __post_init__(self) -> None:
        if not isinstance(self.stage, AcquisitionStage):
            raise ValueError("stage must be an AcquisitionStage")
        _error(self.error)

    @property
    def error_type(self) -> str:
        return type(self.error).__name__

    @property
    def error_message(self) -> str:
        return _error_message(self.error)


@dataclass(frozen=True)
class Phase2AcquisitionReport:
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
    observations: tuple[MarketplaceObservationV1, ...]
    rejections: tuple[RecordRejection, ...]
    source_record_count: int
    duplicate_count: int
    last_page: int | None
    failure: AcquisitionFailure | None

    def __post_init__(self) -> None:
        if not isinstance(self.status, AcquisitionStatus):
            raise ValueError("status must be an AcquisitionStatus")
        object.__setattr__(self, "marketplace_code", _require_text(self.marketplace_code, "marketplace_code").lower())
        object.__setattr__(self, "marketplace_id", _require_text(self.marketplace_id, "marketplace_id"))
        object.__setattr__(self, "target", _require_text(self.target, "target"))
        _non_bool_int(self.page, "page", minimum=1)
        if not isinstance(self.resource_type, ResourceType):
            raise ValueError("resource_type must be a ResourceType")
        object.__setattr__(self, "crawl_run_id", _require_text(self.crawl_run_id, "crawl_run_id"))
        object.__setattr__(self, "request_url", _http_url(self.request_url, "request_url"))
        started_at = _aware_utc(self.started_at, "started_at")
        completed_at = _aware_utc(self.completed_at, "completed_at")
        object.__setattr__(self, "started_at", started_at)
        object.__setattr__(self, "completed_at", completed_at)
        if completed_at < started_at:
            raise ValueError("completed_at must not be before started_at")
        if self.http_status is not None:
            _non_bool_int(self.http_status, "http_status", minimum=100)
            if self.http_status > 599:
                raise ValueError("http_status must be between 100 and 599")
        object.__setattr__(self, "retry_after", _optional_text(self.retry_after, "retry_after"))
        if not isinstance(self.observations, tuple):
            raise ValueError("observations must be a tuple")
        if not all(isinstance(item, MarketplaceObservationV1) for item in self.observations):
            raise ValueError("observations must contain MarketplaceObservationV1 values")
        if not isinstance(self.rejections, tuple):
            raise ValueError("rejections must be a tuple")
        if not all(isinstance(item, RecordRejection) for item in self.rejections):
            raise ValueError("rejections must contain RecordRejection values")
        _non_bool_int(self.source_record_count, "source_record_count")
        _non_bool_int(self.duplicate_count, "duplicate_count")
        if len(self.observations) + len(self.rejections) + self.duplicate_count != self.source_record_count:
            raise ValueError("accepted + rejected + duplicate rows must equal source_record_count")
        if self.last_page is not None:
            _non_bool_int(self.last_page, "last_page", minimum=1)
        if self.raw_metadata_uri is not None:
            object.__setattr__(self, "raw_metadata_uri", _require_text(self.raw_metadata_uri, "raw_metadata_uri"))
        if self.raw_artifact is not None and not isinstance(self.raw_artifact, RawArtifact):
            raise ValueError("raw_artifact must be a RawArtifact")
        if self.failure is not None and not isinstance(self.failure, AcquisitionFailure):
            raise ValueError("failure must be an AcquisitionFailure")

        if self.status is AcquisitionStatus.SUCCEEDED:
            if self.failure is not None or self.rejections:
                raise ValueError("SUCCEEDED report cannot contain failure or rejections")
            if self.raw_artifact is None or self.raw_metadata_uri is None:
                raise ValueError("successful report requires raw artifact and metadata URI")
        elif self.status is AcquisitionStatus.PARTIAL:
            if self.failure is not None or not self.observations or not self.rejections:
                raise ValueError("PARTIAL report requires observations and rejections only")
            if self.raw_artifact is None or self.raw_metadata_uri is None:
                raise ValueError("partial report requires raw artifact and metadata URI")
        else:
            if self.failure is None:
                raise ValueError("FAILED report requires a failure")
            if self.observations:
                raise ValueError("FAILED report cannot contain accepted observations")

        if self.raw_artifact is None:
            if self.raw_metadata_uri is not None:
                raise ValueError("metadata URI requires a raw artifact")
        else:
            if self.raw_artifact.marketplace_id != self.marketplace_id:
                raise ValueError("raw artifact marketplace_id does not match report")
            if self.raw_artifact.crawl_run_id != self.crawl_run_id:
                raise ValueError("raw artifact crawl_run_id does not match report")
            if self.raw_artifact.resource_type is not self.resource_type:
                raise ValueError("raw artifact resource_type does not match report")
            if self.raw_metadata_uri is None and not (
                self.failure is not None
                and self.failure.stage is AcquisitionStage.STORAGE
                and isinstance(self.failure.error, RawPersistenceError)
            ):
                raise ValueError("raw artifact requires metadata URI unless metadata storage failed")
            for event in self.observations:
                observation = event.payload.observation
                if event.marketplace != self.marketplace_code:
                    raise ValueError("observation marketplace does not match report")
                if event.crawl_run_id != self.crawl_run_id:
                    raise ValueError("observation crawl_run_id does not match report")
                if event.raw_uri != observation.raw_uri:
                    raise ValueError("event raw URI does not match observation")
                if observation.raw_uri != self.raw_artifact.raw_uri:
                    raise ValueError("observation raw URI does not match artifact")
                if observation.raw_sha256 != self.raw_artifact.body_sha256:
                    raise ValueError("observation checksum does not match artifact")
                if observation.adapter_version != self.raw_artifact.adapter_version:
                    raise ValueError("observation adapter version does not match artifact")

    @property
    def canonical_observation_count(self) -> int:
        return len(self.observations)

    @property
    def rejected_count(self) -> int:
        return len(self.rejections)

    @property
    def raw_bytes(self) -> int:
        return self.raw_artifact.raw_bytes if self.raw_artifact is not None else 0

    @property
    def page_exhausted(self) -> bool:
        return self.source_record_count == 0 or (
            self.last_page is not None and self.page >= self.last_page
        )

    @property
    def is_task_success(self) -> bool:
        return self.status in {AcquisitionStatus.SUCCEEDED, AcquisitionStatus.PARTIAL}


class ListingPageAdapter(Protocol):
    site_name: str
    marketplace_id: str
    adapter_version: str

    def request_url(self, target: str, page: int) -> str: ...
    def allowed(self, request_url: str) -> bool: ...
    def throttle(self) -> None: ...
    def fetch_listing_page(self, request: ListingPageRequest) -> FetchResult: ...
    def parse_listing_page(
        self,
        *,
        request: ListingPageRequest,
        fetch_result: FetchResult,
        raw_artifact: RawArtifact,
        crawl_run_id: str,
        produced_at: datetime,
    ) -> ParsedListingPage: ...
