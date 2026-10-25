"""Acknowledged Kafka publication of source records.

The producer itself comes from ``ecomdata.infra.kafka.create_producer``. Only
listing rows are published: the page context is large, nothing downstream of
Kafka reads it, and it is complete in Bronze/records.
"""
from __future__ import annotations

from typing import Any

from ecomdata.domain.records import RecordKind, SourceRecord, record_from_wire, record_to_wire
from ecomdata.domain.topics import MARKETPLACE_RECORDS
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
