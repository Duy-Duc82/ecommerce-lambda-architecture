"""Acknowledged producer for deterministic marketplace change events."""

from __future__ import annotations

import json
from typing import Any

from ecomdata.domain.topics import MARKETPLACE_CHANGES
from ecomdata.domain.wire import canonical_json, marketplace_change_from_wire
from ecomdata.infra.kafka import send_acknowledged


def publish_change(producer: Any, event: Any, *, ack_timeout_seconds: int = 30) -> Any:
    """Validate a change against the wire contract, send it keyed by offer, wait for the ack."""
    if ack_timeout_seconds <= 0:
        raise ValueError("ack_timeout_seconds must be positive")
    wire = json.loads(canonical_json(event))
    checked = marketplace_change_from_wire(wire)
    return send_acknowledged(
        producer, MARKETPLACE_CHANGES.name, key=checked.offer_id, value=wire, ack_timeout_seconds=ack_timeout_seconds
    )
