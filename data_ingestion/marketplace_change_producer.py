"""Acknowledged producer for deterministic marketplace change events."""
from __future__ import annotations
import json
from typing import Any, Iterable

from config.marketplace_wire import canonical_json, marketplace_change_from_wire
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_CHANGE_ACK_TIMEOUT_SECONDS
from config.topics import MARKETPLACE_CHANGES


def create_change_producer(bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS) -> Any:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:  # pragma: no cover
        raise ImportError("kafka-python-ng is required") from exc
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8") if key is not None else None,
        value_serializer=lambda value: canonical_json(value).encode("utf-8"),
        # At-least-once, not idempotent: kafka-python-ng has no idempotent
        # producer. Deterministic event IDs absorb a duplicate; one request in
        # flight keeps a retry from reordering an offer's changes.
        acks="all", retries=5, max_in_flight_requests_per_connection=1,
    )


def _send_change(producer: Any, event: Any) -> Any:
    wire = json.loads(canonical_json(event))
    checked = marketplace_change_from_wire(wire)
    return producer.send(MARKETPLACE_CHANGES.name, key=checked.offer_id, value=wire)


def publish_change(producer: Any, event: Any, *, ack_timeout_seconds: int = KAFKA_CHANGE_ACK_TIMEOUT_SECONDS) -> Any:
    if ack_timeout_seconds <= 0: raise ValueError("ack_timeout_seconds must be positive")
    return _send_change(producer, event).get(timeout=ack_timeout_seconds)


def publish_changes(producer: Any, events: Iterable[Any], *,
                    ack_timeout_seconds: int = KAFKA_CHANGE_ACK_TIMEOUT_SECONDS) -> list[Any]:
    """Send every change, flush once, then wait for each ack.

    The speed sink once waited for each change's ack before sending the
    next: a broker round trip per change, most of a large batch's time. Each
    ack is still checked before this returns, so a batch is audited as
    written only once Kafka holds all of it. Order per offer is kept by the
    producer's single request in flight, not by waiting here."""
    if ack_timeout_seconds <= 0: raise ValueError("ack_timeout_seconds must be positive")
    futures = [_send_change(producer, event) for event in events]
    if futures and hasattr(producer, "flush"): producer.flush()
    return [future.get(timeout=ack_timeout_seconds) for future in futures]
