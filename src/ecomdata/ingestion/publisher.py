"""Acknowledged Kafka publication of canonical marketplace observations.

The producer itself comes from ``ecomdata.infra.kafka.create_producer``.
"""

from __future__ import annotations

import json
from typing import Any

from ecomdata.domain.observations import MarketplaceObservationV1
from ecomdata.domain.topics import MARKETPLACE_OBSERVATIONS
from ecomdata.domain.wire import canonical_json, marketplace_observation_from_wire
from ecomdata.infra.kafka import send_acknowledged


def publish_observation(producer: Any, event: Any, *, ack_timeout_seconds: int = 30) -> Any:
    if ack_timeout_seconds <= 0:
        raise ValueError("ack_timeout_seconds must be positive")
    if not isinstance(event, MarketplaceObservationV1):
        raise TypeError("event must be MarketplaceObservationV1")
    wire = json.loads(canonical_json(event))
    checked = marketplace_observation_from_wire(wire)
    return send_acknowledged(
        producer, MARKETPLACE_OBSERVATIONS.name, key=checked.partition_key, value=wire,
        ack_timeout_seconds=ack_timeout_seconds,
    )
