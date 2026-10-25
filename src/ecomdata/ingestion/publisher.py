"""Acknowledged Kafka publication of source records.

The producer itself comes from ``ecomdata.infra.kafka.create_producer``. Only
listing rows are published: the page context is large, nothing downstream of
Kafka reads it, and it is complete in Bronze/records.
"""
from __future__ import annotations

import json
from typing import Any

from ecomdata.domain.observations import MarketplaceObservationV1
from ecomdata.domain.records import RecordKind, SourceRecord, record_from_wire, record_to_wire
from ecomdata.domain.topics import MARKETPLACE_OBSERVATIONS, MARKETPLACE_RECORDS
from ecomdata.domain.wire import canonical_json, marketplace_observation_from_wire
from ecomdata.infra.kafka import send_acknowledged


def publish_record(producer: Any, record: SourceRecord, *, ack_timeout_seconds: int = 30) -> Any:
    if ack_timeout_seconds <= 0:
        raise ValueError("ack_timeout_seconds must be positive")
    if not isinstance(record, SourceRecord):
        raise TypeError("record must be a SourceRecord")
    if record.record_kind is not RecordKind.LISTING_ROW:
        raise ValueError("only listing rows are published to Kafka")
    wire = record_to_wire(record)
    checked = record_from_wire(wire)
    return send_acknowledged(
        producer,
        MARKETPLACE_RECORDS.name,
        key=checked.partition_key,
        value=wire,
        ack_timeout_seconds=ack_timeout_seconds,
    )


def publish_observation(producer: Any, event: Any, *, ack_timeout_seconds: int = 30) -> Any:
    """Publish a canonical observation to the pre-ELT topic.

    No crawler calls this any more: the topic has no producer since records are
    published instead. It stays only for the Silver sink and the speed layer
    until they read ``marketplace.records.v1`` (R7), and for their tests.
    """
    if ack_timeout_seconds <= 0:
        raise ValueError("ack_timeout_seconds must be positive")
    if not isinstance(event, MarketplaceObservationV1):
        raise TypeError("event must be MarketplaceObservationV1")
    wire = json.loads(canonical_json(event))
    checked = marketplace_observation_from_wire(wire)
    return send_acknowledged(
        producer,
        MARKETPLACE_OBSERVATIONS.name,
        key=checked.partition_key,
        value=wire,
        ack_timeout_seconds=ack_timeout_seconds,
    )
