"""Acknowledged Kafka producer for canonical marketplace observations."""
from __future__ import annotations
import json
from typing import Any
from config.marketplace_wire import canonical_json, marketplace_observation_from_wire
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_PRODUCER_ACK_TIMEOUT_SECONDS
from config.topics import MARKETPLACE_OBSERVATIONS

def create_marketplace_producer(bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS) -> Any:
    try:
        from kafka import KafkaProducer
    except ImportError as exc: raise ImportError("kafka-python-ng is required") from exc
    # kafka-python-ng has no idempotent producer, so delivery is at-least-once:
    # a retry may duplicate a record, which the deterministic observation_id
    # absorbs downstream. One request in flight keeps a retry from reordering
    # an offer's observations, which the speed layer's change state relies on.
    return KafkaProducer(bootstrap_servers=bootstrap_servers, key_serializer=lambda k: k.encode("utf-8") if k else None, value_serializer=lambda v: canonical_json(v).encode("utf-8"), acks="all", retries=5, max_in_flight_requests_per_connection=1)

def publish_observation(producer: Any, event: Any, *, ack_timeout_seconds: int = KAFKA_PRODUCER_ACK_TIMEOUT_SECONDS) -> Any:
    if ack_timeout_seconds <= 0: raise ValueError("ack_timeout_seconds must be positive")
    if not isinstance(event, __import__("config.marketplace_schema", fromlist=["MarketplaceObservationV1"]).MarketplaceObservationV1): raise TypeError("event must be MarketplaceObservationV1")
    wire = json.loads(canonical_json(event)); checked = marketplace_observation_from_wire(wire)
    return producer.send(MARKETPLACE_OBSERVATIONS.name, key=checked.partition_key, value=wire).get(timeout=ack_timeout_seconds)
