"""Pure previous/current comparison producing bounded marketplace changes.

This module is the testable core of the speed layer.  It performs no I/O, reads
no clock and imports no engine or client library, so the change semantics can be
verified without Kafka, Spark, Redis or Elasticsearch running.

The seven change types in ``MarketplaceChangeType`` are the whole vocabulary.
Discount, shipping, promotion and title movement are deliberately not change
types: they belong to the batch temporal marts, where a full history is
available to interpret them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Iterable

from config.marketplace_schema import (
    Availability,
    MarketplaceChangeType,
    MarketplaceChangeV1,
    MarketplaceObservationV1,
    create_change_event,
)


class ObservationOutcome(str, Enum):
    """What one observation was to the state it was compared against."""

    DETECTED = "DETECTED"
    DUPLICATE = "DUPLICATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class OfferStateSnapshot:
    """The last known state of one offer — the speed layer's whole memory."""

    offer_id: str
    marketplace: str
    platform_listing_id: str
    observation_id: str
    observed_at: datetime
    current_price: Decimal
    list_price: Decimal | None
    availability: Availability
    rating_value: Decimal | None
    review_count: int | None
    sold_count: int | None
    raw_uri: str


@dataclass(frozen=True)
class ChangeThresholds:
    large_drop_absolute: Decimal
    large_drop_percent: Decimal
    stale_after: timedelta

    def __post_init__(self) -> None:
        for name in ("large_drop_absolute", "large_drop_percent"):
            value = getattr(self, name)
            if not isinstance(value, Decimal):
                raise TypeError(f"{name} must be a Decimal, not {type(value).__name__}")
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.large_drop_percent > 100:
            raise ValueError("large_drop_percent must not exceed 100")
        if not isinstance(self.stale_after, timedelta) or self.stale_after <= timedelta(0):
            raise ValueError("stale_after must be a positive timedelta")


@dataclass(frozen=True)
class ChangeDetectionResult:
    outcome: ObservationOutcome
    changes: tuple[MarketplaceChangeV1, ...]
    next_state: OfferStateSnapshot | None
    counter_reset_or_invalid: bool
    secondary_rating_field: str | None

    def __post_init__(self) -> None:
        # A guard hit must never advance state: returning a next_state here
        # would let a caller quietly overwrite good state with a duplicate or
        # an out-of-order observation.
        if self.outcome is not ObservationOutcome.DETECTED:
            if self.changes:
                raise ValueError(f"{self.outcome.value} outcome cannot carry changes")
            if self.next_state is not None:
                raise ValueError(f"{self.outcome.value} outcome cannot carry a next state")


def _guard_result(outcome: ObservationOutcome) -> ChangeDetectionResult:
    return ChangeDetectionResult(
        outcome=outcome,
        changes=(),
        next_state=None,
        counter_reset_or_invalid=False,
        secondary_rating_field=None,
    )


def state_from_observation(event: MarketplaceObservationV1) -> OfferStateSnapshot:
    """Project one canonical observation onto the state the rules compare."""
    if not isinstance(event, MarketplaceObservationV1):
        raise TypeError("event must be a MarketplaceObservationV1")
    offer = event.payload.offer
    observation = event.payload.observation
    return OfferStateSnapshot(
        offer_id=offer.offer_id,
        marketplace=event.marketplace,
        platform_listing_id=offer.platform_listing_id,
        observation_id=observation.observation_id,
        observed_at=observation.observed_at,
        current_price=observation.current_price,
        list_price=observation.list_price,
        availability=observation.availability,
        rating_value=observation.rating_value,
        review_count=observation.review_count,
        sold_count=observation.sold_count,
        raw_uri=observation.raw_uri,
    )


def _is_large_drop(
    previous: Decimal,
    current: Decimal,
    thresholds: ChangeThresholds,
) -> bool:
    drop = previous - current
    if drop <= 0:
        return False
    if drop >= thresholds.large_drop_absolute:
        return True
    if previous == 0:
        # No percentage exists relative to zero; the absolute branch already
        # had its say, so stop rather than dividing.
        return False
    return (drop / previous) * Decimal(100) >= thresholds.large_drop_percent


def detect_changes(
    *,
    previous: OfferStateSnapshot | None,
    event: MarketplaceObservationV1,
    detected_at: datetime,
    thresholds: ChangeThresholds,
    rule_version: str,
) -> ChangeDetectionResult:
    """Compare one observation against stored state, purely.

    Change order in the result is fixed so two runs over the same input produce
    identical output, including identical event identities.
    """
    if not isinstance(event, MarketplaceObservationV1):
        raise TypeError("event must be a MarketplaceObservationV1")
    if not isinstance(thresholds, ChangeThresholds):
        raise TypeError("thresholds must be a ChangeThresholds")
    current = state_from_observation(event)

    if previous is not None:
        if not isinstance(previous, OfferStateSnapshot):
            raise TypeError("previous must be an OfferStateSnapshot")
        if previous.offer_id != current.offer_id:
            raise ValueError("previous state belongs to a different offer")
        if previous.observation_id == current.observation_id:
            return _guard_result(ObservationOutcome.DUPLICATE)
        if previous.observed_at > current.observed_at:
            return _guard_result(ObservationOutcome.OUT_OF_ORDER)
        if previous.observed_at == current.observed_at:
            # Two distinct raw bodies at one observed instant is a source or
            # clock problem, not a change worth publishing.
            return _guard_result(ObservationOutcome.CONFLICT)

    def build(
        change_type: MarketplaceChangeType,
        *,
        field_name: str | None = None,
        previous_value: object = None,
        current_value: object = None,
    ) -> MarketplaceChangeV1:
        return create_change_event(
            marketplace_code=current.marketplace,
            offer_id=current.offer_id,
            change_type=change_type,
            current_observation_id=current.observation_id,
            detected_at=detected_at,
            rule_version=rule_version,
            previous_observation_id=None if previous is None else previous.observation_id,
            field_name=field_name,
            previous_value=previous_value,
            current_value=current_value,
        )

    changes: list[MarketplaceChangeV1] = []
    counter_reset_or_invalid = False
    secondary_rating_field: str | None = None

    if previous is None:
        changes.append(build(MarketplaceChangeType.NEW_OFFER))
        return ChangeDetectionResult(
            outcome=ObservationOutcome.DETECTED,
            changes=tuple(changes),
            next_state=current,
            counter_reset_or_invalid=False,
            secondary_rating_field=None,
        )

    if previous.current_price != current.current_price:
        changes.append(
            build(
                MarketplaceChangeType.PRICE_CHANGED,
                field_name="current_price",
                previous_value=previous.current_price,
                current_value=current.current_price,
            )
        )
        # Additive, never a replacement: a consumer counting price changes must
        # not have to know the drop rule exists.
        if _is_large_drop(previous.current_price, current.current_price, thresholds):
            changes.append(
                build(
                    MarketplaceChangeType.LARGE_PRICE_DROP,
                    field_name="current_price",
                    previous_value=previous.current_price,
                    current_value=current.current_price,
                )
            )

    rating_moved = previous.rating_value != current.rating_value
    reviews_moved = previous.review_count != current.review_count
    if rating_moved or reviews_moved:
        # make_change_id() does not take field_name, so two RATING_CHANGED
        # events for one observation would share an identity.  Emit one and
        # report the other field for the sink-side document instead.
        if rating_moved:
            primary_field, previous_value, current_value = (
                "rating_value",
                previous.rating_value,
                current.rating_value,
            )
            secondary_rating_field = "review_count" if reviews_moved else None
        else:
            primary_field, previous_value, current_value = (
                "review_count",
                previous.review_count,
                current.review_count,
            )
        changes.append(
            build(
                MarketplaceChangeType.RATING_CHANGED,
                field_name=primary_field,
                previous_value=previous_value,
                current_value=current_value,
            )
        )

    if previous.sold_count != current.sold_count:
        # A public counter movement, not a sale: the marketplace does not
        # document what it counts or when it resets.
        if (
            previous.sold_count is None
            or current.sold_count is None
            or current.sold_count < previous.sold_count
        ):
            counter_reset_or_invalid = True
        changes.append(
            build(
                MarketplaceChangeType.COUNTER_CHANGED,
                field_name="sold_count",
                previous_value=previous.sold_count,
                current_value=current.sold_count,
            )
        )

    known = (previous.availability, current.availability)
    if (
        previous.availability is not current.availability
        and Availability.UNKNOWN not in known
    ):
        # UNKNOWN -> IN_STOCK is adapter coverage improving, not a stock event.
        changes.append(
            build(
                MarketplaceChangeType.AVAILABILITY_CHANGED,
                field_name="availability",
                previous_value=previous.availability,
                current_value=current.availability,
            )
        )

    return ChangeDetectionResult(
        outcome=ObservationOutcome.DETECTED,
        changes=tuple(changes),
        next_state=current,
        counter_reset_or_invalid=counter_reset_or_invalid,
        secondary_rating_field=secondary_rating_field,
    )


def detect_stale_offers(
    *,
    states: Iterable[OfferStateSnapshot],
    now: datetime,
    thresholds: ChangeThresholds,
    rule_version: str,
    limit: int,
) -> tuple[MarketplaceChangeV1, ...]:
    """Emit one OFFER_STALE per offer whose last observation is too old.

    A missing observation produces no record, so staleness cannot be a
    per-record rule.  Identity ties to the last observation, which makes an
    offer go stale exactly once per last-seen observation no matter how often
    this sweep runs.
    """
    if not isinstance(thresholds, ChangeThresholds):
        raise TypeError("thresholds must be a ChangeThresholds")
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
        raise ValueError("limit must be a non-negative int")

    candidates = [
        state
        for state in states
        if now - state.observed_at > thresholds.stale_after
    ]
    candidates.sort(key=lambda state: (state.observed_at, state.offer_id))
    return tuple(
        create_change_event(
            marketplace_code=state.marketplace,
            offer_id=state.offer_id,
            change_type=MarketplaceChangeType.OFFER_STALE,
            current_observation_id=state.observation_id,
            detected_at=now,
            rule_version=rule_version,
            previous_observation_id=state.observation_id,
            previous_value=state.observed_at,
            current_value=now,
        )
        for state in candidates[:limit]
    )
