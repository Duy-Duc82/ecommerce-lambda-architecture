"""Versioned marketplace contracts and factories."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from ecomdata.domain import validation as v
from ecomdata.domain.identity import (
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
        object.__setattr__(self, "marketplace_id", v.require_text(self.marketplace_id, "marketplace_id"))
        object.__setattr__(self, "code", v.require_text(self.code, "code").lower())
        object.__setattr__(self, "name", v.require_text(self.name, "name"))
        object.__setattr__(self, "base_url", v.http_url(self.base_url, "base_url"))
        object.__setattr__(self, "default_currency", v.currency_code(self.default_currency))
        object.__setattr__(self, "locale", v.require_text(self.locale, "locale"))
        object.__setattr__(self, "semantics_version", v.require_text(self.semantics_version, "semantics_version"))
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
        object.__setattr__(self, "crawl_run_id", v.require_text(self.crawl_run_id, "crawl_run_id"))
        object.__setattr__(self, "marketplace_id", v.require_text(self.marketplace_id, "marketplace_id"))
        started_at = v.aware_utc(self.started_at, "started_at")
        object.__setattr__(self, "started_at", started_at)
        if self.completed_at is not None:
            completed_at = v.aware_utc(self.completed_at, "completed_at")
            object.__setattr__(self, "completed_at", completed_at)
            if completed_at < started_at:
                raise ValueError("completed_at must not be before started_at")
        v.enum_member(self.status, CrawlRunStatus, "status")
        for field_name in ("requested", "succeeded", "failed", "raw_bytes", "parsed", "rejected"):
            v.optional_non_negative_int(getattr(self, field_name), field_name)
        object.__setattr__(self, "adapter_version", v.require_text(self.adapter_version, "adapter_version"))
        if self.error_summary is not None and not isinstance(self.error_summary, dict):
            raise ValueError("error_summary must be a dictionary")
        if self.status is CrawlRunStatus.RUNNING and self.completed_at is not None:
            raise ValueError("RUNNING crawl run cannot have completed_at")
        if self.status is not CrawlRunStatus.RUNNING and self.completed_at is None:
            raise ValueError("terminal crawl run status requires completed_at")
        if self.succeeded + self.failed > self.requested:
            raise ValueError("succeeded + failed cannot exceed requested")


@dataclass(frozen=True)
class RawPart:
    """One stored response of a raw artifact (the document, or a captured XHR)."""

    index: int
    url: str
    http_status: int
    content_type: str | None
    role: str
    sha256: str
    bytes: int
    encoding: str = "gzip"

    def __post_init__(self) -> None:
        v.non_bool_int(self.index, "index")
        object.__setattr__(self, "url", v.http_url(self.url, "url"))
        v.http_status(self.http_status)
        object.__setattr__(self, "content_type", v.optional_text(self.content_type, "content_type"))
        object.__setattr__(self, "role", v.require_text(self.role, "role"))
        object.__setattr__(self, "sha256", v.sha256_hex(self.sha256, "sha256"))
        v.non_bool_int(self.bytes, "bytes")
        if self.encoding not in ("gzip", "identity"):
            raise ValueError("encoding must be gzip or identity")


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
    # Empty for a v1 artifact (one uncompressed ``body.bin``); the stored
    # responses of a v2 artifact, whose ``body_sha256`` is the manifest's.
    parts: tuple[RawPart, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "raw_artifact_id", v.require_text(self.raw_artifact_id, "raw_artifact_id"))
        object.__setattr__(self, "crawl_run_id", v.require_text(self.crawl_run_id, "crawl_run_id"))
        object.__setattr__(self, "marketplace_id", v.require_text(self.marketplace_id, "marketplace_id"))
        object.__setattr__(self, "request_url", v.http_url(self.request_url, "request_url"))
        v.enum_member(self.resource_type, ResourceType, "resource_type")
        object.__setattr__(self, "fetched_at", v.aware_utc(self.fetched_at, "fetched_at"))
        if self.http_status is not None:
            if isinstance(self.http_status, bool) or not isinstance(self.http_status, int):
                raise ValueError("http_status must be an integer")
            if not 100 <= self.http_status <= 599:
                raise ValueError("http_status must be between 100 and 599")
        object.__setattr__(self, "content_type", v.optional_text(self.content_type, "content_type"))
        object.__setattr__(self, "body_sha256", v.sha256_hex(self.body_sha256, "body_sha256"))
        object.__setattr__(self, "raw_uri", v.raw_uri(self.raw_uri))
        object.__setattr__(self, "adapter_version", v.require_text(self.adapter_version, "adapter_version"))
        v.optional_non_negative_int(self.raw_bytes, "raw_bytes")


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
        object.__setattr__(self, "seller_id", v.require_text(self.seller_id, "seller_id"))
        object.__setattr__(self, "marketplace_id", v.require_text(self.marketplace_id, "marketplace_id"))
        object.__setattr__(self, "platform_seller_id", v.require_text(self.platform_seller_id, "platform_seller_id"))
        first_seen_at = v.aware_utc(self.first_seen_at, "first_seen_at")
        last_seen_at = v.aware_utc(self.last_seen_at, "last_seen_at")
        object.__setattr__(self, "first_seen_at", first_seen_at)
        object.__setattr__(self, "last_seen_at", last_seen_at)
        if last_seen_at < first_seen_at:
            raise ValueError("last_seen_at must be greater than or equal to first_seen_at")
        object.__setattr__(self, "seller_name", v.optional_text(self.seller_name, "seller_name"))
        if self.seller_url is not None:
            object.__setattr__(self, "seller_url", v.http_url(self.seller_url, "seller_url"))
        v.enum_member(self.official_status, OfficialStatus, "official_status")


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
        object.__setattr__(self, "offer_id", v.require_text(self.offer_id, "offer_id"))
        object.__setattr__(self, "marketplace_id", v.require_text(self.marketplace_id, "marketplace_id"))
        object.__setattr__(self, "platform_listing_id", v.require_text(self.platform_listing_id, "platform_listing_id"))
        object.__setattr__(self, "seller_id", v.optional_text(self.seller_id, "seller_id"))
        object.__setattr__(self, "product_title", v.require_text(self.product_title, "product_title"))
        object.__setattr__(self, "brand", v.optional_text(self.brand, "brand"))
        object.__setattr__(self, "category_path", v.optional_text(self.category_path, "category_path"))
        object.__setattr__(self, "source_url", v.http_url(self.source_url, "source_url"))
        object.__setattr__(self, "currency", v.currency_code(self.currency))
        first_seen_at = v.aware_utc(self.first_seen_at, "first_seen_at")
        last_seen_at = v.aware_utc(self.last_seen_at, "last_seen_at")
        object.__setattr__(self, "first_seen_at", first_seen_at)
        object.__setattr__(self, "last_seen_at", last_seen_at)
        if last_seen_at < first_seen_at:
            raise ValueError("last_seen_at must be greater than or equal to first_seen_at")
        v.enum_member(self.active_status, OfferActiveStatus, "active_status")


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
        object.__setattr__(self, "observation_id", v.require_text(self.observation_id, "observation_id"))
        object.__setattr__(self, "offer_id", v.require_text(self.offer_id, "offer_id"))
        object.__setattr__(self, "observed_at", v.aware_utc(self.observed_at, "observed_at"))
        object.__setattr__(self, "fetched_at", v.aware_utc(self.fetched_at, "fetched_at"))
        v.non_negative_decimal(self.current_price, "current_price")
        for field_name in (
            "list_price",
            "shipping_price",
            "discount_amount",
            "discount_percent",
            "rating_value",
            "rating_scale",
        ):
            v.non_negative_decimal(getattr(self, field_name), field_name)
        if self.discount_percent is not None and self.discount_percent > 100:
            raise ValueError("discount_percent must be between 0 and 100")
        for field_name in ("rating_count", "review_count", "sold_count"):
            v.optional_non_negative_int(getattr(self, field_name), field_name)
        v.enum_member(self.availability, Availability, "availability")
        if self.promotion is not None and not isinstance(self.promotion, dict):
            raise ValueError("promotion must be a dictionary")
        if self.ranking_position is not None:
            v.optional_non_negative_int(self.ranking_position, "ranking_position")
            if self.ranking_position == 0:
                raise ValueError("ranking_position must be greater than zero")
        if self.rating_value is not None:
            if self.rating_scale is None:
                raise ValueError("rating_scale is required when rating_value is present")
            if self.rating_scale <= 0:
                raise ValueError("rating_scale must be greater than zero")
            if self.rating_value > self.rating_scale:
                raise ValueError("rating_value cannot exceed rating_scale")
        object.__setattr__(self, "raw_uri", v.raw_uri(self.raw_uri))
        object.__setattr__(self, "raw_sha256", v.sha256_hex(self.raw_sha256, "raw_sha256"))
        object.__setattr__(self, "adapter_version", v.require_text(self.adapter_version, "adapter_version"))
        object.__setattr__(self, "crawl_run_id", v.require_text(self.crawl_run_id, "crawl_run_id"))


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
        object.__setattr__(self, "event_id", v.require_text(self.event_id, "event_id"))
        object.__setattr__(self, "schema_version", v.require_text(self.schema_version, "schema_version"))
        object.__setattr__(self, "event_type", v.require_text(self.event_type, "event_type"))
        occurred_at = v.aware_utc(self.occurred_at, "occurred_at")
        produced_at = v.aware_utc(self.produced_at, "produced_at")
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "produced_at", produced_at)
        object.__setattr__(self, "marketplace", v.require_text(self.marketplace, "marketplace"))
        object.__setattr__(self, "partition_key", v.require_text(self.partition_key, "partition_key"))
        object.__setattr__(self, "crawl_run_id", v.require_text(self.crawl_run_id, "crawl_run_id"))
        object.__setattr__(self, "raw_uri", v.raw_uri(self.raw_uri))
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
        object.__setattr__(self, "event_id", v.require_text(self.event_id, "event_id"))
        object.__setattr__(self, "schema_version", v.require_text(self.schema_version, "schema_version"))
        v.enum_member(self.change_type, MarketplaceChangeType, "change_type")
        object.__setattr__(self, "detected_at", v.aware_utc(self.detected_at, "detected_at"))
        object.__setattr__(self, "marketplace", v.require_text(self.marketplace, "marketplace"))
        object.__setattr__(self, "offer_id", v.require_text(self.offer_id, "offer_id"))
        object.__setattr__(
            self,
            "previous_observation_id",
            v.optional_text(self.previous_observation_id, "previous_observation_id"),
        )
        object.__setattr__(
            self,
            "current_observation_id",
            v.require_text(self.current_observation_id, "current_observation_id"),
        )
        object.__setattr__(self, "field_name", v.optional_text(self.field_name, "field_name"))
        object.__setattr__(self, "rule_version", v.require_text(self.rule_version, "rule_version"))
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
    parts: tuple[RawPart, ...] = (),
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
        parts=tuple(parts),
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
    marketplace_code = v.require_text(marketplace_code, "marketplace_code").lower()
    platform_listing_id = v.require_text(platform_listing_id, "platform_listing_id")
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
