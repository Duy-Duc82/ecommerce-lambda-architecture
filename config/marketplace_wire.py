"""Strict JSON wire codecs for the marketplace v1 contracts."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from common.identity import make_change_id, make_observation_id, make_offer_id
from common.serialization import serialize_for_wire
from config.marketplace_schema import (
    Availability, CHANGE_SCHEMA_VERSION, MarketplaceChangeType, MarketplaceChangeV1,
    MarketplaceObservationV1, OBSERVATION_EVENT_TYPE, OBSERVATION_SCHEMA_VERSION,
    ObservationPayload, OfferActiveStatus, MarketplaceOffer, OfferObservation,
)


class WireContractError(ValueError):
    """A decoded object does not match its versioned contract."""


def _object(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WireContractError(f"{path} must be an object")
    return value


def _exact(value: Mapping[str, Any], expected: set[str], path: str) -> None:
    missing = sorted(expected - set(value))
    unknown = sorted(set(value) - expected)
    if missing or unknown:
        detail = []
        if missing: detail.append(f"missing={missing}")
        if unknown: detail.append(f"unknown={unknown}")
        raise WireContractError(f"{path}: " + ", ".join(detail))


def _text(value: Any, path: str, *, optional: bool = False) -> str | None:
    if value is None and optional: return None
    if not isinstance(value, str) or not value.strip():
        raise WireContractError(f"{path} must be a non-empty string")
    return value


def _decimal(value: Any, path: str, *, optional: bool = False) -> Decimal | None:
    if value is None and optional: return None
    if not isinstance(value, str) or not value.strip():
        raise WireContractError(f"{path} must be a decimal string or null")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise WireContractError(f"{path} must be a decimal string") from exc
    if not result.is_finite():
        raise WireContractError(f"{path} must be finite")
    return result


def _integer(value: Any, path: str, *, optional: bool = False) -> int | None:
    if value is None and optional: return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise WireContractError(f"{path} must be an integer or null")
    return value


def _datetime(value: Any, path: str) -> datetime:
    if not isinstance(value, str):
        raise WireContractError(f"{path} must be an ISO-8601 string")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise WireContractError(f"{path} must be an ISO-8601 string") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise WireContractError(f"{path} must include a timezone")
    return result.astimezone(timezone.utc)


def marketplace_observation_from_wire(value: Mapping[str, Any]) -> MarketplaceObservationV1:
    top = _object(value, "event")
    top_fields = {"event_id", "schema_version", "event_type", "occurred_at", "produced_at", "marketplace", "partition_key", "crawl_run_id", "raw_uri", "payload"}
    _exact(top, top_fields, "event")
    if top["schema_version"] != OBSERVATION_SCHEMA_VERSION:
        raise WireContractError("event.schema_version must equal marketplace-observation.v1")
    if top["event_type"] != OBSERVATION_EVENT_TYPE:
        raise WireContractError("event.event_type must equal OFFER_OBSERVED")
    payload = _object(top["payload"], "event.payload")
    _exact(payload, {"offer", "observation"}, "event.payload")
    offer_raw = _object(payload["offer"], "event.payload.offer")
    offer_fields = {"offer_id", "marketplace_id", "platform_listing_id", "seller_id", "product_title", "brand", "category_path", "source_url", "currency", "first_seen_at", "last_seen_at", "active_status"}
    _exact(offer_raw, offer_fields, "event.payload.offer")
    obs_raw = _object(payload["observation"], "event.payload.observation")
    obs_fields = {"observation_id", "offer_id", "observed_at", "fetched_at", "current_price", "list_price", "shipping_price", "discount_amount", "discount_percent", "rating_value", "rating_scale", "rating_count", "review_count", "sold_count", "availability", "promotion", "ranking_position", "raw_uri", "raw_sha256", "adapter_version", "crawl_run_id"}
    _exact(obs_raw, obs_fields, "event.payload.observation")
    try:
        marketplace = _text(top["marketplace"], "event.marketplace")
        offer = MarketplaceOffer(
            offer_id=_text(offer_raw["offer_id"], "offer.offer_id"), marketplace_id=_text(offer_raw["marketplace_id"], "offer.marketplace_id"),
            platform_listing_id=_text(offer_raw["platform_listing_id"], "offer.platform_listing_id"), seller_id=_text(offer_raw["seller_id"], "offer.seller_id", optional=True),
            product_title=_text(offer_raw["product_title"], "offer.product_title"), brand=_text(offer_raw["brand"], "offer.brand", optional=True), category_path=_text(offer_raw["category_path"], "offer.category_path", optional=True),
            source_url=_text(offer_raw["source_url"], "offer.source_url"), currency=_text(offer_raw["currency"], "offer.currency"), first_seen_at=_datetime(offer_raw["first_seen_at"], "offer.first_seen_at"), last_seen_at=_datetime(offer_raw["last_seen_at"], "offer.last_seen_at"), active_status=OfferActiveStatus(_text(offer_raw["active_status"], "offer.active_status")),
        )
        observation = OfferObservation(
            observation_id=_text(obs_raw["observation_id"], "observation.observation_id"), offer_id=_text(obs_raw["offer_id"], "observation.offer_id"), observed_at=_datetime(obs_raw["observed_at"], "observation.observed_at"), fetched_at=_datetime(obs_raw["fetched_at"], "observation.fetched_at"),
            current_price=_decimal(obs_raw["current_price"], "observation.current_price"), list_price=_decimal(obs_raw["list_price"], "observation.list_price", optional=True), shipping_price=_decimal(obs_raw["shipping_price"], "observation.shipping_price", optional=True), discount_amount=_decimal(obs_raw["discount_amount"], "observation.discount_amount", optional=True), discount_percent=_decimal(obs_raw["discount_percent"], "observation.discount_percent", optional=True), rating_value=_decimal(obs_raw["rating_value"], "observation.rating_value", optional=True), rating_scale=_decimal(obs_raw["rating_scale"], "observation.rating_scale", optional=True), rating_count=_integer(obs_raw["rating_count"], "observation.rating_count", optional=True), review_count=_integer(obs_raw["review_count"], "observation.review_count", optional=True), sold_count=_integer(obs_raw["sold_count"], "observation.sold_count", optional=True), availability=Availability(_text(obs_raw["availability"], "observation.availability")), promotion=obs_raw["promotion"], ranking_position=_integer(obs_raw["ranking_position"], "observation.ranking_position", optional=True), raw_uri=_text(obs_raw["raw_uri"], "observation.raw_uri"), raw_sha256=_text(obs_raw["raw_sha256"], "observation.raw_sha256"), adapter_version=_text(obs_raw["adapter_version"], "observation.adapter_version"), crawl_run_id=_text(obs_raw["crawl_run_id"], "observation.crawl_run_id"),
        )
        event = MarketplaceObservationV1(event_id=_text(top["event_id"], "event.event_id"), schema_version=top["schema_version"], event_type=top["event_type"], occurred_at=_datetime(top["occurred_at"], "event.occurred_at"), produced_at=_datetime(top["produced_at"], "event.produced_at"), marketplace=marketplace, partition_key=_text(top["partition_key"], "event.partition_key"), crawl_run_id=_text(top["crawl_run_id"], "event.crawl_run_id"), raw_uri=_text(top["raw_uri"], "event.raw_uri"), payload=ObservationPayload(offer=offer, observation=observation))
    except (ValueError, TypeError) as exc:
        raise WireContractError(f"event contract validation failed: {exc}") from exc
    expected_offer_ids = {
        make_offer_id(event.marketplace, event.payload.offer.platform_listing_id),
        # Phase 1's historical factory accepts an uppercase marketplace code
        # for offer identity while the event envelope normalizes it to lower.
        make_offer_id(event.marketplace.upper(), event.payload.offer.platform_listing_id),
    }
    if event.marketplace.lower() != event.marketplace or event.payload.offer.offer_id not in expected_offer_ids:
        raise WireContractError("event.payload.offer.offer_id does not match marketplace/listing identity")
    if event.payload.observation.observation_id != make_observation_id(event.marketplace, event.payload.offer.platform_listing_id, event.payload.observation.observed_at, event.payload.observation.raw_sha256):
        raise WireContractError("event.payload.observation.observation_id does not match identity")
    if event.partition_key != f"{event.marketplace.lower()}:{event.payload.offer.platform_listing_id}":
        raise WireContractError("event.partition_key does not match canonical partition key")
    return event


def _json_value(value: Any, path: str) -> Any:
    if value is None or isinstance(value, (str, bool, int)) and not isinstance(value, float):
        return value
    if isinstance(value, float):
        raise WireContractError(f"{path} contains forbidden JSON float")
    if isinstance(value, list): return [_json_value(item, f"{path}[]") for item in value]
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value): raise WireContractError(f"{path} has non-string mapping key")
        return {key: _json_value(item, f"{path}.{key}") for key, item in value.items()}
    raise WireContractError(f"{path} contains unsupported value {type(value).__name__}")


def marketplace_change_from_wire(value: Mapping[str, Any]) -> MarketplaceChangeV1:
    raw = _object(value, "change")
    fields = {"event_id", "schema_version", "change_type", "detected_at", "marketplace", "offer_id", "previous_observation_id", "current_observation_id", "field_name", "previous_value", "current_value", "rule_version"}
    _exact(raw, fields, "change")
    if raw["schema_version"] != CHANGE_SCHEMA_VERSION: raise WireContractError("change.schema_version must equal marketplace-change.v1")
    try:
        change_type = MarketplaceChangeType(_text(raw["change_type"], "change.change_type"))
        event = MarketplaceChangeV1(event_id=_text(raw["event_id"], "change.event_id"), schema_version=raw["schema_version"], change_type=change_type, detected_at=_datetime(raw["detected_at"], "change.detected_at"), marketplace=_text(raw["marketplace"], "change.marketplace"), offer_id=_text(raw["offer_id"], "change.offer_id"), previous_observation_id=_text(raw["previous_observation_id"], "change.previous_observation_id", optional=True), current_observation_id=_text(raw["current_observation_id"], "change.current_observation_id"), field_name=_text(raw["field_name"], "change.field_name", optional=True), previous_value=_json_value(raw["previous_value"], "change.previous_value"), current_value=_json_value(raw["current_value"], "change.current_value"), rule_version=_text(raw["rule_version"], "change.rule_version"))
    except (ValueError, TypeError) as exc:
        raise WireContractError(f"change contract validation failed: {exc}") from exc
    if event.event_id != make_change_id(event.offer_id, event.current_observation_id, event.change_type, event.rule_version):
        raise WireContractError("change.event_id does not match deterministic identity")
    if event.change_type is MarketplaceChangeType.NEW_OFFER and event.previous_observation_id is not None: raise WireContractError("NEW_OFFER previous_observation_id must be null")
    if event.change_type in {MarketplaceChangeType.NEW_OFFER, MarketplaceChangeType.OFFER_STALE} and event.field_name is not None: raise WireContractError(f"{event.change_type.value} field_name must be null")
    if event.change_type not in {MarketplaceChangeType.NEW_OFFER, MarketplaceChangeType.OFFER_STALE} and event.field_name is None: raise WireContractError(f"{event.change_type.value} field_name is required")
    if event.change_type is MarketplaceChangeType.OFFER_STALE and event.previous_observation_id is None: raise WireContractError("OFFER_STALE previous_observation_id is required")
    return event


def to_wire(value: Any) -> Any:
    """Canonical JSON-compatible value for either marketplace event type."""
    return serialize_for_wire(value)


def canonical_json(value: Any) -> str:
    return json.dumps(to_wire(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
