"""The one Kafka producer factory, and acknowledged sends.

kafka-python-ng has no idempotent producer, so delivery is at-least-once: a
retry may duplicate a message, which the deterministic ids downstream absorb.
One request in flight keeps a retry from reordering one key's messages, which
the speed layer's per-offer state relies on.
"""
from __future__ import annotations

from typing import Any

from ecomdata.domain.wire import canonical_json


def create_producer(bootstrap_servers: str, *, client_id: str | None = None) -> Any:
    """String keys, canonical-JSON values, ``acks=all``."""
    try:
        from kafka import KafkaProducer
    except ImportError as exc:  # pragma: no cover - environment, not logic
        raise ImportError("kafka-python-ng is required") from exc
    options: dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers,
        "key_serializer": lambda key: key.encode("utf-8") if key is not None else None,
        "value_serializer": lambda value: canonical_json(value).encode("utf-8"),
        "acks": "all",
        "retries": 5,
        "max_in_flight_requests_per_connection": 1,
    }
    if client_id:
        options["client_id"] = client_id
    return KafkaProducer(**options)


def send_acknowledged(producer: Any, topic: str, *, key: str | None, value: Any, ack_timeout_seconds: float) -> Any:
    """Send one message and wait for the broker's acknowledgement."""
    if ack_timeout_seconds <= 0:
        raise ValueError("ack_timeout_seconds must be positive")
    return producer.send(topic, key=key, value=value).get(timeout=ack_timeout_seconds)
