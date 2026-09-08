"""Idempotent Redis serving writes for marketplace changes and freshness.

Separate from ``serving_layer/redis_cache.py``, which serves the legacy
behavioral KPIs and is untouched by this layer.

Every command here is an overwrite (``HSET``, ``ZADD``, ``SADD``) or a bounded
trim.  Nothing increments: the observation stream is at-least-once, so an
``INCR``-based counter would inflate on every replay and there would be no way
to tell an inflated number from a real one.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence

from config.marketplace_schema import MarketplaceChangeV1

FORBIDDEN_COMMANDS = ("incr", "incrby", "hincrby", "hincrbyfloat", "zincrby", "decr")


def _epoch_millis(moment: datetime) -> int:
    if moment.tzinfo is None:
        raise ValueError("moment must be timezone-aware")
    return int(moment.timestamp() * 1000)


class MarketplaceRedisWriter:
    """Writes latest change entries and per-source freshness."""

    def __init__(
        self,
        client,
        *,
        namespace: str,
        recent_changes_max: int,
        change_doc_ttl_seconds: int,
        freshness_threshold_minutes: int,
    ):
        if not isinstance(namespace, str) or not namespace.strip():
            raise ValueError("namespace is required")
        for name, value in (
            ("recent_changes_max", recent_changes_max),
            ("change_doc_ttl_seconds", change_doc_ttl_seconds),
            ("freshness_threshold_minutes", freshness_threshold_minutes),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive int")
        self._client = client
        self._namespace = namespace.strip()
        self._recent_changes_max = recent_changes_max
        self._change_doc_ttl_seconds = change_doc_ttl_seconds
        self._freshness_threshold_minutes = freshness_threshold_minutes

    def _key(self, *parts: str) -> str:
        return ":".join((self._namespace,) + parts)

    def write_changes(
        self,
        changes: Sequence[MarketplaceChangeV1],
        documents: Mapping[str, Mapping[str, Any]],
    ) -> None:
        """Store change documents and index them per marketplace.

        Members are deterministic change IDs, so replaying a change overwrites
        its entry instead of appending a second one.
        """
        if not changes:
            return
        pipe = self._client.pipeline()
        touched_marketplaces: set[str] = set()
        for change in changes:
            if not isinstance(change, MarketplaceChangeV1):
                raise TypeError("changes must contain MarketplaceChangeV1 values")
            document = documents.get(change.event_id)
            if document is None:
                raise KeyError(f"no document supplied for change {change.event_id}")
            doc_key = self._key("change", change.event_id)
            pipe.hset(doc_key, mapping={k: str(v) for k, v in document.items() if v is not None})
            pipe.expire(doc_key, self._change_doc_ttl_seconds)
            pipe.zadd(
                self._key("changes", change.marketplace),
                {change.event_id: _epoch_millis(change.detected_at)},
            )
            touched_marketplaces.add(change.marketplace)
        for marketplace in sorted(touched_marketplaces):
            # Keep the newest N by rank; a bounded serving view, not history.
            pipe.zremrangebyrank(
                self._key("changes", marketplace),
                0,
                -(self._recent_changes_max + 1),
            )
        pipe.execute()

    def write_source_freshness(
        self,
        *,
        marketplace: str,
        last_observation_at: datetime | None = None,
        last_change_at: datetime | None = None,
        last_stale_sweep_at: datetime | None = None,
    ) -> None:
        """Overwrite the per-source health hash. Timestamps only, no counters."""
        if not isinstance(marketplace, str) or not marketplace.strip():
            raise ValueError("marketplace is required")
        mapping: dict[str, str] = {
            "freshness_threshold_minutes": str(self._freshness_threshold_minutes)
        }
        for name, moment in (
            ("last_observation_at", last_observation_at),
            ("last_change_at", last_change_at),
            ("last_stale_sweep_at", last_stale_sweep_at),
        ):
            if moment is not None:
                if moment.tzinfo is None:
                    raise ValueError(f"{name} must be timezone-aware")
                mapping[name] = moment.isoformat()
        self._client.hset(self._key("source", marketplace.strip()), mapping=mapping)
