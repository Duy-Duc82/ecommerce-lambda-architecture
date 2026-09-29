"""Acknowledged producer for deterministic marketplace change events."""
from __future__ import annotations
import json
from typing import Any

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
        acks="all", enable_idempotence=True, retries=5, max_in_flight_requests_per_connection=5,
    )


def publish_change(producer: Any, event: Any, *, ack_timeout_seconds: int = KAFKA_CHANGE_ACK_TIMEOUT_SECONDS) -> Any:
    if ack_timeout_seconds <= 0: raise ValueError("ack_timeout_seconds must be positive")
    wire = json.loads(canonical_json(event))
    checked = marketplace_change_from_wire(wire)
    future = producer.send(MARKETPLACE_CHANGES.name, key=checked.offer_id, value=wire)
    return future.get(timeout=ack_timeout_seconds)
