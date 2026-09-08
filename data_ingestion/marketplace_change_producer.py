"""Kafka producer for derived MarketplaceChangeV1 events.

The key is ``offer_id``, per the parent plan's topic table: every change for one
offer must stay ordered inside one partition, so a consumer replaying a
partition sees NEW_OFFER before the price changes that followed it.

A change that cannot be published is an operational sink failure, not a bad
record.  It is never routed to the observation DLQ — that queue exists for
records which failed to decode, and mixing the two would make the DLQ useless
as a bad-data signal.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Sequence

from common.serialization import serialize_for_wire
from config.marketplace_schema import MarketplaceChangeV1

logger = logging.getLogger(__name__)

DEFAULT_ACK_TIMEOUT_SECONDS = 30.0


class ChangePublishError(RuntimeError):
    """Raised when a change could not be acknowledged by the broker."""

    def __init__(self, message: str, *, acked: int, attempted: int):
        super().__init__(message)
        self.acked = acked
        self.attempted = attempted


def change_topic() -> str:
    """Resolve the change topic from the Phase 4 registry, else from settings.

    Imported lazily so this module works before ``config/topics.py`` exists and
    picks the registry up automatically once Phase 4 lands, instead of holding
    a second copy of the topic name.
    """
    try:
        from config.topics import MARKETPLACE_CHANGES  # type: ignore

        return MARKETPLACE_CHANGES.name
    except Exception:
        from config.settings import (  # type: ignore
            KAFKA_TOPIC_MARKETPLACE_CHANGES,
        )

        return KAFKA_TOPIC_MARKETPLACE_CHANGES


def encode_change(change: MarketplaceChangeV1) -> bytes:
    """Canonical wire bytes — the same form the observation path uses."""
    if not isinstance(change, MarketplaceChangeV1):
        raise TypeError("change must be a MarketplaceChangeV1")
    return json.dumps(
        serialize_for_wire(change),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def change_key(change: MarketplaceChangeV1) -> bytes:
    if not isinstance(change, MarketplaceChangeV1):
        raise TypeError("change must be a MarketplaceChangeV1")
    return change.offer_id.encode("utf-8")


def create_change_producer(*, bootstrap_servers: str, **overrides: Any):
    """Build a KafkaProducer. Imported lazily so tests need no kafka package."""
    from kafka import KafkaProducer  # type: ignore

    options: dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers,
        "acks": "all",
        "enable_idempotence": True,
        "retries": 5,
        "linger_ms": 20,
    }
    options.update(overrides)
    return KafkaProducer(**options)


def publish_change(
    producer,
    change: MarketplaceChangeV1,
    *,
    topic: str | None = None,
    ack_timeout_seconds: float = DEFAULT_ACK_TIMEOUT_SECONDS,
) -> None:
    """Publish one change and wait for the broker acknowledgement."""
    resolved_topic = topic or change_topic()
    payload = encode_change(change)
    future = producer.send(resolved_topic, key=change_key(change), value=payload)
    future.get(timeout=ack_timeout_seconds)


def publish_changes(
    producer,
    changes: Sequence[MarketplaceChangeV1],
    *,
    topic: str | None = None,
    ack_timeout_seconds: float = DEFAULT_ACK_TIMEOUT_SECONDS,
) -> int:
    """Publish changes in order, failing fast and reporting acked progress.

    The acked count is part of the exception so the caller's audit row can say
    how far the batch got instead of recording an all-or-nothing guess.
    """
    resolved_topic = topic or change_topic()
    for change in changes:
        if not isinstance(change, MarketplaceChangeV1):
            raise TypeError("changes must contain MarketplaceChangeV1 values")
    acked = 0
    for change in changes:
        try:
            publish_change(
                producer,
                change,
                topic=resolved_topic,
                ack_timeout_seconds=ack_timeout_seconds,
            )
        except Exception as exc:
            raise ChangePublishError(
                f"change {change.event_id} was not acknowledged: {exc}",
                acked=acked,
                attempted=len(changes),
            ) from exc
        acked += 1
    return acked
