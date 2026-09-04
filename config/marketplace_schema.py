"""Versioned marketplace contracts and factories."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import re
from typing import Any, TypeVar
from urllib.parse import urlparse

from common.identity import (
    make_observation_id,
    make_offer_id,
    make_raw_artifact_id,
)


OBSERVATION_SCHEMA_VERSION = "marketplace-observation.v1"
CHANGE_SCHEMA_VERSION = "marketplace-change.v1"
OBSERVATION_EVENT_TYPE = "OFFER_OBSERVED"


class Availability(str, Enum):
    IN_STOCK = "IN_STOCK"
    OUT_OF_STOCK = "OUT_OF_STOCK"
    UNKNOWN = "UNKNOWN"


class OfficialStatus(str, Enum):
    OFFICIAL = "OFFICIAL"
    NOT_OFFICIAL = "NOT_OFFICIAL"
    UNKNOWN = "UNKNOWN"


class OfferActiveStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    UNKNOWN = "UNKNOWN"


class CrawlRunStatus(str, Enum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class ResourceType(str, Enum):
    LISTING_PAGE = "LISTING_PAGE"
    PRODUCT_DETAIL = "PRODUCT_DETAIL"


class MarketplaceChangeType(str, Enum):
    NEW_OFFER = "NEW_OFFER"
    PRICE_CHANGED = "PRICE_CHANGED"
    LARGE_PRICE_DROP = "LARGE_PRICE_DROP"
    RATING_CHANGED = "RATING_CHANGED"
    COUNTER_CHANGED = "COUNTER_CHANGED"
    AVAILABILITY_CHANGED = "AVAILABILITY_CHANGED"
    OFFER_STALE = "OFFER_STALE"


_CURRENCY_PATTERN = re.compile(r"[A-Za-z]{3}\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-fA-F]{64}\Z")
_RAW_URI_SCHEMES = {"s3a", "s3", "file"}

EnumType = TypeVar("EnumType", bound=Enum)


def _require_text(value: str, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be text")
    value = value.strip()
    if not value:
        raise ValueError(f"{field_name} is required")
    return value


def _optional_text(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be text")
    value = value.strip()
    return value or None


def _require_aware_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _require_non_negative_decimal(value: Decimal | None, field_name: str) -> None:
    if value is None:
        return
    if not isinstance(value, Decimal):
        raise ValueError(f"{field_name} must be a Decimal")
    if not value.is_finite() or value < 0:
        raise ValueError(f"{field_name} must be non-negative and finite")


def _require_non_negative_int(value: int | None, field_name: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer, not bool")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


def _require_sha256(value: str, field_name: str) -> str:
    value = _require_text(value, field_name)
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be exactly 64 hexadecimal characters")
    return value.lower()


def _require_http_url(value: str, field_name: str) -> str:
    value = _require_text(value, field_name)
    try:
        parsed = urlparse(value)
        hostname = parsed.hostname
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid HTTP(S) URL") from exc
    if parsed.scheme.lower() not in {"http", "https"} or not hostname:
        raise ValueError(f"{field_name} must be a valid HTTP(S) URL")
    return value


def _require_currency(value: str) -> str:
    value = _require_text(value, "currency")
    if not _CURRENCY_PATTERN.fullmatch(value):
        raise ValueError("currency must be exactly three ASCII letters")
    return value.upper()


def _require_enum(value: Any, enum_type: type[EnumType], field_name: str) -> EnumType:
    if not isinstance(value, enum_type):
        raise ValueError(f"{field_name} must be a {enum_type.__name__}")
    return value


def _require_raw_uri(value: str, field_name: str = "raw_uri") -> str:
    value = _require_text(value, field_name)
    parsed = urlparse(value)
    scheme = parsed.scheme.lower()
    if scheme not in _RAW_URI_SCHEMES:
        raise ValueError(f"{field_name} must use s3a, s3, or file scheme")
    if scheme in {"s3", "s3a"} and not parsed.hostname:
        raise ValueError(f"{field_name} must include a bucket hostname")
    if scheme == "file" and not parsed.path:
        raise ValueError(f"{field_name} must include a file path")
    return value


@dataclass(frozen=True)
class Marketplace:
    marketplace_id: str
    code: str
    name: str
    base_url: str
    default_currency: str
    locale: str
    semantics_version: str
    active: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "marketplace_id", _require_text(self.marketplace_id, "marketplace_id"))
        object.__setattr__(self, "code", _require_text(self.code, "code").lower())
        object.__setattr__(self, "name", _require_text(self.name, "name"))
        object.__setattr__(self, "base_url", _require_http_url(self.base_url, "base_url"))
        object.__setattr__(self, "default_currency", _require_currency(self.default_currency))
        object.__setattr__(self, "locale", _require_text(self.locale, "locale"))
        object.__setattr__(self, "semantics_version", _require_text(self.semantics_version, "semantics_version"))
        if type(self.active) is not bool:
            raise ValueError("active must be a boolean")


@dataclass(frozen=True)
class CrawlRun:
    crawl_run_id: str
    marketplace_id: str
    started_at: datetime
    completed_at: datetime | None
    status: CrawlRunStatus
    requested: int = 0
    succeeded: int = 0
    failed: int = 0
    raw_bytes: int = 0
    parsed: int = 0
    rejected: int = 0
    adapter_version: str = ""
    error_summary: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "crawl_run_id", _require_text(self.crawl_run_id, "crawl_run_id"))
        object.__setattr__(self, "marketplace_id", _require_text(self.marketplace_id, "marketplace_id"))
        started_at = _require_aware_datetime(self.started_at, "started_at")
        object.__setattr__(self, "started_at", started_at)
        if self.completed_at is not None:
            completed_at = _require_aware_datetime(self.completed_at, "completed_at")
            object.__setattr__(self, "completed_at", completed_at)
            if completed_at < started_at:
                raise ValueError("completed_at must not be before started_at")
        _require_enum(self.status, CrawlRunStatus, "status")
        for field_name in ("requested", "succeeded", "failed", "raw_bytes", "parsed", "rejected"):
            _require_non_negative_int(getattr(self, field_name), field_name)
        object.__setattr__(self, "adapter_version", _require_text(self.adapter_version, "adapter_version"))
        if self.error_summary is not None and not isinstance(self.error_summary, dict):
            raise ValueError("error_summary must be a dictionary")
        if self.status is CrawlRunStatus.RUNNING and self.completed_at is not None:
            raise ValueError("RUNNING crawl run cannot have completed_at")
        if self.status is not CrawlRunStatus.RUNNING and self.completed_at is None:
            raise ValueError("terminal crawl run status requires completed_at")
        if self.succeeded + self.failed > self.requested:
            raise ValueError("succeeded + failed cannot exceed requested")


@dataclass(frozen=True)
class RawArtifact:
    raw_artifact_id: str
    crawl_run_id: str
    marketplace_id: str
    request_url: str
    resource_type: ResourceType
    fetched_at: datetime
    http_status: int | None
    content_type: str | None
    body_sha256: str
    raw_uri: str
    adapter_version: str
    raw_bytes: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "raw_artifact_id", _require_text(self.raw_artifact_id, "raw_artifact_id"))
        object.__setattr__(self, "crawl_run_id", _require_text(self.crawl_run_id, "crawl_run_id"))
        object.__setattr__(self, "marketplace_id", _require_text(self.marketplace_id, "marketplace_id"))
        object.__setattr__(self, "request_url", _require_http_url(self.request_url, "request_url"))
        _require_enum(self.resource_type, ResourceType, "resource_type")
        object.__setattr__(self, "fetched_at", _require_aware_datetime(self.fetched_at, "fetched_at"))
        if self.http_status is not None:
            if isinstance(self.http_status, bool) or not isinstance(self.http_status, int):
                raise ValueError("http_status must be an integer")
            if not 100 <= self.http_status <= 599:
                raise ValueError("http_status must be between 100 and 599")
        object.__setattr__(self, "content_type", _optional_text(self.content_type, "content_type"))
        object.__setattr__(self, "body_sha256", _require_sha256(self.body_sha256, "body_sha256"))
        object.__setattr__(self, "raw_uri", _require_raw_uri(self.raw_uri))
        object.__setattr__(self, "adapter_version", _require_text(self.adapter_version, "adapter_version"))
        _require_non_negative_int(self.raw_bytes, "raw_bytes")


@dataclass(frozen=True)
class Seller:
    seller_id: str
    marketplace_id: str
    platform_seller_id: str
    first_seen_at: datetime
    last_seen_at: datetime
    seller_name: str | None = None
    seller_url: str | None = None
    official_status: OfficialStatus = OfficialStatus.UNKNOWN

    def __post_init__(self) -> None:
        object.__setattr__(self, "seller_id", _require_text(self.seller_id, "seller_id"))
        object.__setattr__(self, "marketplace_id", _require_text(self.marketplace_id, "marketplace_id"))
        object.__setattr__(self, "platform_seller_id", _require_text(self.platform_seller_id, "platform_seller_id"))
        first_seen_at = _require_aware_datetime(self.first_seen_at, "first_seen_at")
        last_seen_at = _require_aware_datetime(self.last_seen_at, "last_seen_at")
        object.__setattr__(self, "first_seen_at", first_seen_at)
        object.__setattr__(self, "last_seen_at", last_seen_at)
        if last_seen_at < first_seen_at:
            raise ValueError("last_seen_at must be greater than or equal to first_seen_at")
        object.__setattr__(self, "seller_name", _optional_text(self.seller_name, "seller_name"))
        if self.seller_url is not None:
            object.__setattr__(self, "seller_url", _require_http_url(self.seller_url, "seller_url"))
        _require_enum(self.official_status, OfficialStatus, "official_status")


@dataclass(frozen=True)
class MarketplaceOffer:
    offer_id: str
    marketplace_id: str
    platform_listing_id: str
    seller_id: str | None
    product_title: str
    brand: str | None
    category_path: str | None
    source_url: str
    currency: str
    first_seen_at: datetime
    last_seen_at: datetime
    active_status: OfferActiveStatus = OfferActiveStatus.UNKNOWN

    def __post_init__(self) -> None:
        object.__setattr__(self, "offer_id", _require_text(self.offer_id, "offer_id"))
        object.__setattr__(self, "marketplace_id", _require_text(self.marketplace_id, "marketplace_id"))
        object.__setattr__(self, "platform_listing_id", _require_text(self.platform_listing_id, "platform_listing_id"))
        object.__setattr__(self, "seller_id", _optional_text(self.seller_id, "seller_id"))
        object.__setattr__(self, "product_title", _require_text(self.product_title, "product_title"))
        object.__setattr__(self, "brand", _optional_text(self.brand, "brand"))
        object.__setattr__(self, "category_path", _optional_text(self.category_path, "category_path"))
        object.__setattr__(self, "source_url", _require_http_url(self.source_url, "source_url"))
        object.__setattr__(self, "currency", _require_currency(self.currency))
        first_seen_at = _require_aware_datetime(self.first_seen_at, "first_seen_at")
        last_seen_at = _require_aware_datetime(self.last_seen_at, "last_seen_at")
        object.__setattr__(self, "first_seen_at", first_seen_at)
        object.__setattr__(self, "last_seen_at", last_seen_at)
        if last_seen_at < first_seen_at:
            raise ValueError("last_seen_at must be greater than or equal to first_seen_at")
        _require_enum(self.active_status, OfferActiveStatus, "active_status")


@dataclass(frozen=True)
class OfferObservation:
    observation_id: str
    offer_id: str
    observed_at: datetime
    fetched_at: datetime
    current_price: Decimal
    list_price: Decimal | None = None
    shipping_price: Decimal | None = None
    discount_amount: Decimal | None = None
    discount_percent: Decimal | None = None
    rating_value: Decimal | None = None
    rating_scale: Decimal | None = None
    rating_count: int | None = None
    review_count: int | None = None
    sold_count: int | None = None
    availability: Availability = Availability.UNKNOWN
    promotion: dict[str, Any] | None = None
    ranking_position: int | None = None
    raw_uri: str = ""
    raw_sha256: str = ""
    adapter_version: str = ""
    crawl_run_id: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "observation_id", _require_text(self.observation_id, "observation_id"))
        object.__setattr__(self, "offer_id", _require_text(self.offer_id, "offer_id"))
        object.__setattr__(self, "observed_at", _require_aware_datetime(self.observed_at, "observed_at"))
        object.__setattr__(self, "fetched_at", _require_aware_datetime(self.fetched_at, "fetched_at"))
        _require_non_negative_decimal(self.current_price, "current_price")
        for field_name in (
            "list_price",
            "shipping_price",
            "discount_amount",
            "discount_percent",
            "rating_value",
            "rating_scale",
        ):
            _require_non_negative_decimal(getattr(self, field_name), field_name)
        if self.discount_percent is not None and self.discount_percent > 100:
            raise ValueError("discount_percent must be between 0 and 100")
        for field_name in ("rating_count", "review_count", "sold_count"):
            _require_non_negative_int(getattr(self, field_name), field_name)
        _require_enum(self.availability, Availability, "availability")
        if self.promotion is not None and not isinstance(self.promotion, dict):
            raise ValueError("promotion must be a dictionary")
        if self.ranking_position is not None:
            _require_non_negative_int(self.ranking_position, "ranking_position")
            if self.ranking_position == 0:
                raise ValueError("ranking_position must be greater than zero")
        if self.rating_value is not None:
            if self.rating_scale is None:
                raise ValueError("rating_scale is required when rating_value is present")
            if self.rating_scale <= 0:
                raise ValueError("rating_scale must be greater than zero")
            if self.rating_value > self.rating_scale:
                raise ValueError("rating_value cannot exceed rating_scale")
        object.__setattr__(self, "raw_uri", _require_raw_uri(self.raw_uri))
        object.__setattr__(self, "raw_sha256", _require_sha256(self.raw_sha256, "raw_sha256"))
        object.__setattr__(self, "adapter_version", _require_text(self.adapter_version, "adapter_version"))
        object.__setattr__(self, "crawl_run_id", _require_text(self.crawl_run_id, "crawl_run_id"))


@dataclass(frozen=True)
class ObservationPayload:
    offer: MarketplaceOffer
    observation: OfferObservation

    def __post_init__(self) -> None:
        if not isinstance(self.offer, MarketplaceOffer):
            raise ValueError("offer must be a MarketplaceOffer")
        if not isinstance(self.observation, OfferObservation):
            raise ValueError("observation must be an OfferObservation")
        if self.observation.offer_id != self.offer.offer_id:
            raise ValueError("observation.offer_id must equal offer.offer_id")


@dataclass(frozen=True)
class MarketplaceObservationV1:
    event_id: str
    schema_version: str
    event_type: str
    occurred_at: datetime
    produced_at: datetime
    marketplace: str
    partition_key: str
    crawl_run_id: str
    raw_uri: str
    payload: ObservationPayload

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _require_text(self.event_id, "event_id"))
        object.__setattr__(self, "schema_version", _require_text(self.schema_version, "schema_version"))
        object.__setattr__(self, "event_type", _require_text(self.event_type, "event_type"))
        occurred_at = _require_aware_datetime(self.occurred_at, "occurred_at")
        produced_at = _require_aware_datetime(self.produced_at, "produced_at")
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "produced_at", produced_at)
        object.__setattr__(self, "marketplace", _require_text(self.marketplace, "marketplace"))
        object.__setattr__(self, "partition_key", _require_text(self.partition_key, "partition_key"))
        object.__setattr__(self, "crawl_run_id", _require_text(self.crawl_run_id, "crawl_run_id"))
        object.__setattr__(self, "raw_uri", _require_raw_uri(self.raw_uri))
        if not isinstance(self.payload, ObservationPayload):
            raise ValueError("payload must be an ObservationPayload")
        if self.schema_version != OBSERVATION_SCHEMA_VERSION:
            raise ValueError("schema_version must equal OBSERVATION_SCHEMA_VERSION")
        if self.event_type != OBSERVATION_EVENT_TYPE:
            raise ValueError("event_type must equal OBSERVATION_EVENT_TYPE")
        observation = self.payload.observation
        if self.event_id != observation.observation_id:
            raise ValueError("event_id must equal payload.observation.observation_id")
        if occurred_at != observation.observed_at:
            raise ValueError("occurred_at must equal observation.observed_at")
        if self.crawl_run_id != observation.crawl_run_id:
            raise ValueError("crawl_run_id must equal observation.crawl_run_id")
        if self.raw_uri != observation.raw_uri:
            raise ValueError("raw_uri must equal observation.raw_uri")


@dataclass(frozen=True)
class MarketplaceChangeV1:
    event_id: str
    schema_version: str
    change_type: MarketplaceChangeType
    detected_at: datetime
    marketplace: str
    offer_id: str
    previous_observation_id: str | None
    current_observation_id: str
    field_name: str | None
    previous_value: Any
    current_value: Any
    rule_version: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_id", _require_text(self.event_id, "event_id"))
        object.__setattr__(self, "schema_version", _require_text(self.schema_version, "schema_version"))
        _require_enum(self.change_type, MarketplaceChangeType, "change_type")
        object.__setattr__(self, "detected_at", _require_aware_datetime(self.detected_at, "detected_at"))
        object.__setattr__(self, "marketplace", _require_text(self.marketplace, "marketplace"))
        object.__setattr__(self, "offer_id", _require_text(self.offer_id, "offer_id"))
        object.__setattr__(
            self,
            "previous_observation_id",
            _optional_text(self.previous_observation_id, "previous_observation_id"),
        )
        object.__setattr__(
            self,
            "current_observation_id",
            _require_text(self.current_observation_id, "current_observation_id"),
        )
        object.__setattr__(self, "field_name", _optional_text(self.field_name, "field_name"))
        object.__setattr__(self, "rule_version", _require_text(self.rule_version, "rule_version"))
        if self.schema_version != CHANGE_SCHEMA_VERSION:
            raise ValueError("schema_version must equal CHANGE_SCHEMA_VERSION")
        if self.change_type is not MarketplaceChangeType.NEW_OFFER and self.previous_observation_id is None:
            raise ValueError("previous_observation_id is required for this change_type")
        if (
            self.change_type not in {MarketplaceChangeType.NEW_OFFER, MarketplaceChangeType.OFFER_STALE}
            and self.field_name is None
        ):
            raise ValueError("field_name is required for this change_type")


def create_raw_artifact(
    *,
    marketplace_code: str,
    crawl_run_id: str,
    marketplace_id: str,
    request_url: str,
    resource_type: ResourceType,
    fetched_at: datetime,
    http_status: int | None,
    content_type: str | None,
    body_sha256: str,
    raw_uri: str,
    adapter_version: str,
    raw_bytes: int,
) -> RawArtifact:
    raw_artifact_id = make_raw_artifact_id(marketplace_code, request_url, fetched_at, body_sha256)
    return RawArtifact(
        raw_artifact_id=raw_artifact_id,
        crawl_run_id=crawl_run_id,
        marketplace_id=marketplace_id,
        request_url=request_url,
        resource_type=resource_type,
        fetched_at=fetched_at,
        http_status=http_status,
        content_type=content_type,
        body_sha256=body_sha256,
        raw_uri=raw_uri,
        adapter_version=adapter_version,
        raw_bytes=raw_bytes,
    )


def create_marketplace_offer(
    *,
    marketplace_code: str,
    marketplace_id: str,
    platform_listing_id: str,
    seller_id: str | None,
    product_title: str,
    brand: str | None,
    category_path: str | None,
    source_url: str,
    currency: str,
    first_seen_at: datetime,
    last_seen_at: datetime,
    active_status: OfferActiveStatus = OfferActiveStatus.UNKNOWN,
) -> MarketplaceOffer:
    offer_id = make_offer_id(marketplace_code, platform_listing_id)
    return MarketplaceOffer(
        offer_id=offer_id,
        marketplace_id=marketplace_id,
        platform_listing_id=platform_listing_id,
        seller_id=seller_id,
        product_title=product_title,
        brand=brand,
        category_path=category_path,
        source_url=source_url,
        currency=currency,
        first_seen_at=first_seen_at,
        last_seen_at=last_seen_at,
        active_status=active_status,
    )


def create_offer_observation(
    *,
    marketplace_code: str,
    platform_listing_id: str,
    offer_id: str,
    observed_at: datetime,
    fetched_at: datetime,
    current_price: Decimal,
    raw_uri: str,
    raw_sha256: str,
    adapter_version: str,
    crawl_run_id: str,
    **optional_fields: Any,
) -> OfferObservation:
    allowed_optional_fields = {
        "list_price",
        "shipping_price",
        "discount_amount",
        "discount_percent",
        "rating_value",
        "rating_scale",
        "rating_count",
        "review_count",
        "sold_count",
        "availability",
        "promotion",
        "ranking_position",
    }
    unknown_fields = set(optional_fields) - allowed_optional_fields
    if unknown_fields:
        unknown = ", ".join(sorted(unknown_fields))
        raise TypeError(f"unknown OfferObservation field(s): {unknown}")
    observation_id = make_observation_id(
        marketplace_code,
        platform_listing_id,
        observed_at,
        raw_sha256,
    )
    return OfferObservation(
        observation_id=observation_id,
        offer_id=offer_id,
        observed_at=observed_at,
        fetched_at=fetched_at,
        current_price=current_price,
        raw_uri=raw_uri,
        raw_sha256=raw_sha256,
        adapter_version=adapter_version,
        crawl_run_id=crawl_run_id,
        **optional_fields,
    )


def create_observation_event(
    *,
    marketplace_code: str,
    offer: MarketplaceOffer,
    observation: OfferObservation,
    platform_listing_id: str,
    produced_at: datetime,
) -> MarketplaceObservationV1:
    marketplace_code = _require_text(marketplace_code, "marketplace_code").lower()
    platform_listing_id = _require_text(platform_listing_id, "platform_listing_id")
    return MarketplaceObservationV1(
        event_id=observation.observation_id,
        schema_version=OBSERVATION_SCHEMA_VERSION,
        event_type=OBSERVATION_EVENT_TYPE,
        occurred_at=observation.observed_at,
        produced_at=produced_at,
        marketplace=marketplace_code,
        partition_key=f"{marketplace_code}:{platform_listing_id}",
        crawl_run_id=observation.crawl_run_id,
        raw_uri=observation.raw_uri,
        payload=ObservationPayload(offer=offer, observation=observation),
    )
