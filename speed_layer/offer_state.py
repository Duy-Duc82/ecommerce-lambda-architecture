"""Previous-offer-state storage for the speed layer.

The state store is also the serving store: Redis holds the latest observed
state both because detection needs a previous value and because the dashboards
need a current one.  Keeping one store avoids the classic drift where the
detector's memory and the served "latest" disagree.

Every write is an overwrite.  ``INCR``, ``HINCRBY`` and ``ZINCRBY`` are
deliberately absent: the stream is at-least-once, so an arithmetic update would
double-count on replay with no way to detect it afterwards.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable, Mapping, Protocol, Sequence

from config.marketplace_schema import Availability
from speed_layer.change_rules import OfferStateSnapshot

STATE_SCHEMA_VERSION = "marketplace-offer-state.v1"

_STATE_FIELDS = (
    "offer_id",
    "marketplace",
    "platform_listing_id",
    "observation_id",
    "observed_at",
    "current_price",
    "list_price",
    "availability",
    "rating_value",
    "review_count",
    "sold_count",
    "raw_uri",
)


class BackwardsStateWrite(ValueError):
    """Raised when a write would move an offer's state back in time."""


class OfferStateStore(Protocol):
    def get_many(self, offer_ids: Sequence[str]) -> dict[str, OfferStateSnapshot]: ...

    def put_many(self, states: Sequence[OfferStateSnapshot]) -> None: ...

    def scan_states(self, limit: int) -> tuple[OfferStateSnapshot, ...]: ...


def _require_limit(limit: int) -> int:
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 0:
        raise ValueError("limit must be a non-negative int")
    return limit


def _check_forward(
    previous: OfferStateSnapshot | None,
    candidate: OfferStateSnapshot,
) -> None:
    if previous is None:
        return
    if candidate.observed_at < previous.observed_at:
        raise BackwardsStateWrite(
            f"refusing to move {candidate.offer_id} back from "
            f"{previous.observed_at.isoformat()} to {candidate.observed_at.isoformat()}"
        )


def state_to_mapping(state: OfferStateSnapshot) -> dict[str, str]:
    """Flatten one state to Redis hash fields.

    Money and rating are stored as canonical Decimal strings, never as floats:
    the served "current price" must be the same value the change event carries.
    """
    values = {
        "schema_version": STATE_SCHEMA_VERSION,
        "offer_id": state.offer_id,
        "marketplace": state.marketplace,
        "platform_listing_id": state.platform_listing_id,
        "observation_id": state.observation_id,
        "observed_at": state.observed_at.isoformat(),
        "current_price": str(state.current_price),
        "availability": state.availability.value,
        "raw_uri": state.raw_uri,
    }
    for name in ("list_price", "rating_value"):
        value = getattr(state, name)
        if value is not None:
            values[name] = str(value)
    for name in ("review_count", "sold_count"):
        value = getattr(state, name)
        if value is not None:
            values[name] = str(value)
    return values


def state_from_mapping(mapping: Mapping[str, str]) -> OfferStateSnapshot:
    """Rebuild one state from Redis hash fields."""

    def text(key: str) -> str:
        value = mapping.get(key)
        if value is None:
            raise ValueError(f"stored state is missing {key}")
        return value if isinstance(value, str) else value.decode("utf-8")

    def optional(key: str) -> str | None:
        value = mapping.get(key)
        if value is None:
            return None
        return value if isinstance(value, str) else value.decode("utf-8")

    def decimal(key: str) -> Decimal | None:
        raw = optional(key)
        return None if raw is None else Decimal(raw)

    def integer(key: str) -> int | None:
        raw = optional(key)
        return None if raw is None else int(raw)

    observed_at = datetime.fromisoformat(text("observed_at"))
    if observed_at.tzinfo is None:
        raise ValueError("stored observed_at must be timezone-aware")
    return OfferStateSnapshot(
        offer_id=text("offer_id"),
        marketplace=text("marketplace"),
        platform_listing_id=text("platform_listing_id"),
        observation_id=text("observation_id"),
        observed_at=observed_at.astimezone(timezone.utc),
        current_price=Decimal(text("current_price")),
        list_price=decimal("list_price"),
        availability=Availability(text("availability")),
        rating_value=decimal("rating_value"),
        review_count=integer("review_count"),
        sold_count=integer("sold_count"),
        raw_uri=text("raw_uri"),
    )


class InMemoryOfferStateStore:
    """Reference implementation for tests and dry runs."""

    def __init__(self, initial: Iterable[OfferStateSnapshot] = ()):
        self._states: dict[str, OfferStateSnapshot] = {}
        for state in initial:
            self._states[state.offer_id] = state

    def get_many(self, offer_ids: Sequence[str]) -> dict[str, OfferStateSnapshot]:
        return {
            offer_id: self._states[offer_id]
            for offer_id in offer_ids
            if offer_id in self._states
        }

    def put_many(self, states: Sequence[OfferStateSnapshot]) -> None:
        for state in states:
            _check_forward(self._states.get(state.offer_id), state)
        for state in states:
            self._states[state.offer_id] = state

    def scan_states(self, limit: int) -> tuple[OfferStateSnapshot, ...]:
        _require_limit(limit)
        ordered = sorted(self._states.values(), key=lambda s: (s.observed_at, s.offer_id))
        return tuple(ordered[:limit])


class RedisOfferStateStore:
    """Redis-backed state store.

    ``put_many()`` guards against a backwards write by reading the stored
    ``observed_at`` first.  That is a read-then-write check, safe because one
    consumer owns a Kafka partition and all observations of one offer share a
    partition key.  It is explicitly not a distributed lock, and this module
    does not pretend otherwise.
    """

    def __init__(self, client, *, namespace: str):
        if not isinstance(namespace, str) or not namespace.strip():
            raise ValueError("namespace is required")
        self._client = client
        self._namespace = namespace.strip()

    def _state_key(self, offer_id: str) -> str:
        return f"{self._namespace}:offer:{offer_id}"

    @property
    def _index_key(self) -> str:
        return f"{self._namespace}:offers:index"

    def get_many(self, offer_ids: Sequence[str]) -> dict[str, OfferStateSnapshot]:
        if not offer_ids:
            return {}
        # One pipeline, not one round trip per offer: a 5000-observation
        # micro-batch must not become 5000 Redis calls.
        pipe = self._client.pipeline()
        for offer_id in offer_ids:
            pipe.hgetall(self._state_key(offer_id))
        raw_states = pipe.execute()
        states: dict[str, OfferStateSnapshot] = {}
        for offer_id, mapping in zip(offer_ids, raw_states):
            if mapping:
                states[offer_id] = state_from_mapping(mapping)
        return states

    def put_many(self, states: Sequence[OfferStateSnapshot]) -> None:
        if not states:
            return
        stored = self.get_many([state.offer_id for state in states])
        for state in states:
            _check_forward(stored.get(state.offer_id), state)
        pipe = self._client.pipeline()
        for state in states:
            pipe.hset(self._state_key(state.offer_id), mapping=state_to_mapping(state))
            pipe.sadd(self._index_key, state.offer_id)
        pipe.execute()

    def scan_states(self, limit: int) -> tuple[OfferStateSnapshot, ...]:
        _require_limit(limit)
        if limit == 0:
            return ()
        offer_ids = sorted(
            member if isinstance(member, str) else member.decode("utf-8")
            for member in self._client.smembers(self._index_key)
        )
        states = self.get_many(offer_ids)
        ordered = sorted(states.values(), key=lambda s: (s.observed_at, s.offer_id))
        return tuple(ordered[:limit])
