"""Pure, deterministic marketplace offer comparison rules."""
from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from common.identity import make_change_id
from common.serialization import serialize_for_wire
from config.marketplace_schema import Availability, MarketplaceChangeType, MarketplaceObservationV1


class ObservationDisposition(str, Enum):
    APPLIED = "APPLIED"
    DUPLICATE = "DUPLICATE"
    LATE = "LATE"


@dataclass(frozen=True)
class ChangeRuleConfig:
    rule_version: str
    large_drop_absolute: Decimal
    large_drop_relative: Decimal
    stale_after_seconds: int

    def __post_init__(self) -> None:
        if not self.rule_version.strip(): raise ValueError("rule_version is required")
        if self.large_drop_absolute < 0: raise ValueError("large_drop_absolute must be non-negative")
        if not Decimal("0") <= self.large_drop_relative <= Decimal("1"): raise ValueError("large_drop_relative must be between 0 and 1")
        if self.stale_after_seconds <= 0: raise ValueError("stale_after_seconds must be positive")


@dataclass(frozen=True)
class OfferState:
    marketplace: str
    marketplace_id: str
    offer_id: str
    platform_listing_id: str
    seller_id: str | None
    product_title: str
    brand: str | None
    category_path: str | None
    source_url: str
    currency: str
    active_status: str
    observation_id: str
    observed_at: datetime
    produced_at: datetime
    current_price: Decimal
    list_price: Decimal | None
    rating_value: Decimal | None
    rating_count: int | None
    review_count: int | None
    sold_count: int | None
    availability: str
    stale_emitted: bool = False


@dataclass(frozen=True)
class ChangeDetectionResult:
    disposition: ObservationDisposition
    next_state: OfferState
    changes: tuple[Any, ...]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None: raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


def state_from_observation(event: MarketplaceObservationV1) -> OfferState:
    offer, obs = event.payload.offer, event.payload.observation
    return OfferState(event.marketplace, offer.marketplace_id, offer.offer_id, offer.platform_listing_id, offer.seller_id, offer.product_title, offer.brand, offer.category_path, offer.source_url, offer.currency, offer.active_status.value, obs.observation_id, _utc(obs.observed_at), _utc(event.produced_at), obs.current_price, obs.list_price, obs.rating_value, obs.rating_count, obs.review_count, obs.sold_count, obs.availability.value, False)


def _state_mapping(state: OfferState) -> dict[str, Any]:
    return {"offer_id": state.offer_id, "marketplace": state.marketplace, "platform_listing_id": state.platform_listing_id, "seller_id": state.seller_id, "product_title": state.product_title, "brand": state.brand, "category_path": state.category_path, "source_url": state.source_url, "currency": state.currency, "active_status": state.active_status, "observation_id": state.observation_id, "observed_at": state.observed_at, "produced_at": state.produced_at, "current_price": state.current_price, "list_price": state.list_price, "rating_value": state.rating_value, "rating_count": state.rating_count, "review_count": state.review_count, "sold_count": state.sold_count, "availability": state.availability}


def _change(state: OfferState, current_id: str, kind: MarketplaceChangeType, field: str | None, previous: Any, current: Any, detected_at: datetime, rule_version: str, previous_id: str | None = None):
    from config.marketplace_schema import MarketplaceChangeV1
    return MarketplaceChangeV1(make_change_id(state.offer_id, current_id, kind, rule_version), "marketplace-change.v1", kind, _utc(detected_at), state.marketplace, state.offer_id, previous_id, current_id, field, serialize_for_wire(previous), serialize_for_wire(current), rule_version)


def _next_state(event: MarketplaceObservationV1, stale: bool = False) -> OfferState:
    state = state_from_observation(event)
    return replace(state, stale_emitted=stale)


def detect_observation_changes(previous: OfferState | None, event: MarketplaceObservationV1, config: ChangeRuleConfig) -> ChangeDetectionResult:
    incoming = state_from_observation(event)
    if previous is not None:
        ordering = (incoming.observed_at, incoming.observation_id)
        current_ordering = (_utc(previous.observed_at), previous.observation_id)
        if incoming.observation_id == previous.observation_id:
            return ChangeDetectionResult(ObservationDisposition.DUPLICATE, previous, ())
        if ordering < current_ordering:
            return ChangeDetectionResult(ObservationDisposition.LATE, previous, ())
    if previous is None:
        return ChangeDetectionResult(ObservationDisposition.APPLIED, incoming, (_change(incoming, incoming.observation_id, MarketplaceChangeType.NEW_OFFER, None, None, _state_mapping(incoming), incoming.produced_at, config.rule_version),))
    changes = []
    current_id = incoming.observation_id
    prev_id = previous.observation_id
    if previous.current_price != incoming.current_price:
        changes.append(_change(incoming, current_id, MarketplaceChangeType.PRICE_CHANGED, "current_price", previous.current_price, incoming.current_price, incoming.produced_at, config.rule_version, prev_id))
        if incoming.current_price < previous.current_price:
            drop = previous.current_price - incoming.current_price
            ratio = (drop / previous.current_price) if previous.current_price != 0 else None
            if drop >= config.large_drop_absolute or (ratio is not None and ratio >= config.large_drop_relative):
                value = {"previous_price": previous.current_price, "current_price": incoming.current_price, "drop_amount": drop, "drop_ratio": ratio}
                changes.append(_change(incoming, current_id, MarketplaceChangeType.LARGE_PRICE_DROP, "current_price", previous.current_price, value, incoming.produced_at, config.rule_version, prev_id))
    if (previous.rating_value, previous.rating_count) != (incoming.rating_value, incoming.rating_count):
        changes.append(_change(incoming, current_id, MarketplaceChangeType.RATING_CHANGED, "rating", {"value": previous.rating_value, "count": previous.rating_count}, {"value": incoming.rating_value, "count": incoming.rating_count}, incoming.produced_at, config.rule_version, prev_id))
    old_counters = {k: getattr(previous, k) for k in ("review_count", "sold_count")}
    new_counters = {k: getattr(incoming, k) for k in ("review_count", "sold_count")}
    changed_old = {k: v for k, v in old_counters.items() if v != new_counters[k]}
    changed_new = {k: new_counters[k] for k in changed_old}
    if changed_old:
        changes.append(_change(incoming, current_id, MarketplaceChangeType.COUNTER_CHANGED, "public_counters", changed_old, changed_new, incoming.produced_at, config.rule_version, prev_id))
    if previous.availability != incoming.availability and previous.availability != Availability.UNKNOWN.value and incoming.availability != Availability.UNKNOWN.value:
        changes.append(_change(incoming, current_id, MarketplaceChangeType.AVAILABILITY_CHANGED, "availability", previous.availability, incoming.availability, incoming.produced_at, config.rule_version, prev_id))
    return ChangeDetectionResult(ObservationDisposition.APPLIED, incoming, tuple(changes))


def detect_stale_change(state: OfferState, config: ChangeRuleConfig) -> tuple[OfferState, Any | None]:
    if state.stale_emitted:
        return state, None
    boundary = _utc(state.observed_at) + timedelta(seconds=config.stale_after_seconds)
    stale_state = replace(state, stale_emitted=True)
    change = _change(stale_state, state.observation_id, MarketplaceChangeType.OFFER_STALE, None, state.observed_at, boundary, boundary, config.rule_version, state.observation_id)
    return stale_state, change


def offer_state_to_json(state: OfferState) -> str:
    return json.dumps(serialize_for_wire(state), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def offer_state_from_json(value: str) -> OfferState:
    raw = json.loads(value)
    if not isinstance(raw, dict): raise ValueError("offer state must be a JSON object")
    def dt(name): return _utc(datetime.fromisoformat(raw[name].replace("Z", "+00:00")))
    return OfferState(raw["marketplace"], raw.get("marketplace_id", ""), raw["offer_id"], raw["platform_listing_id"], raw.get("seller_id"), raw["product_title"], raw.get("brand"), raw.get("category_path"), raw["source_url"], raw["currency"], raw["active_status"], raw["observation_id"], dt("observed_at"), dt("produced_at"), Decimal(raw["current_price"]), Decimal(raw["list_price"]) if raw.get("list_price") is not None else None, Decimal(raw["rating_value"]) if raw.get("rating_value") is not None else None, raw.get("rating_count"), raw.get("review_count"), raw.get("sold_count"), raw["availability"], bool(raw.get("stale_emitted", False)))
