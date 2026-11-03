"""Acknowledged producer for deterministic marketplace change events."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from ecomdata.domain.topics import MARKETPLACE_CHANGES
from ecomdata.domain.wire import canonical_json, marketplace_change_from_wire
from ecomdata.infra.kafka import send_acknowledged


def _checked(event: Any) -> tuple[str, Any]:
    """Validate a change against the wire contract; return its key and wire form."""
    wire = json.loads(canonical_json(event))
    return marketplace_change_from_wire(wire).offer_id, wire


def publish_change(producer: Any, event: Any, *, ack_timeout_seconds: int = 30) -> Any:
    """Validate a change against the wire contract, send it keyed by offer, wait for the ack."""
    if ack_timeout_seconds <= 0:
        raise ValueError("ack_timeout_seconds must be positive")
    key, wire = _checked(event)
    return send_acknowledged(
        producer, MARKETPLACE_CHANGES.name, key=key, value=wire, ack_timeout_seconds=ack_timeout_seconds
    )


def publish_changes(producer: Any, events: Iterable[Any], *, ack_timeout_seconds: int = 30) -> list[Any]:
    """Send every change, flush once, then wait for each ack.

    The speed sink once waited for each change's ack before sending the
    next: a broker round trip per change, most of a large batch's time. Each
    ack is still checked before this returns, so a batch is audited as
    written only once Kafka holds all of it. Order per offer is kept by the
    producer's single request in flight, not by waiting here."""
    if ack_timeout_seconds <= 0:
        raise ValueError("ack_timeout_seconds must be positive")
    futures = []
    for event in events:
        key, wire = _checked(event)
        futures.append(producer.send(MARKETPLACE_CHANGES.name, key=key, value=wire))
    if futures and hasattr(producer, "flush"):
        producer.flush()
    return [future.get(timeout=ack_timeout_seconds) for future in futures]
