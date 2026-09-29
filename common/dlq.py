"""Dead-letter-queue helper shared by every Kafka-publishing ingestion source.

Any producer that rejects a malformed record should route it here instead of
a bare ``except: continue`` — the record and the reason it failed become
observable on ``<topic>.dlq`` instead of silently vanishing.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def dlq_topic_for(topic: str) -> str:
    return f"{topic}.dlq"


def publish_to_dlq(producer: Any, topic: str, raw_payload: Any, error: Exception, source: str) -> None:
    """Best-effort publish of one rejected record to ``<topic>.dlq``.

    Never raises: a DLQ outage must not take down the ingestion path it is
    protecting. Logs loudly instead so the failure stays observable.
    """
    dlq_record = {
        "source": source,
        "error": str(error),
        "error_type": type(error).__name__,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "raw_payload": raw_payload,
    }
    try:
        producer.send(dlq_topic_for(topic), value=dlq_record)
    except Exception:
        logger.exception(
            "Failed to publish to DLQ for topic=%s source=%s; record dropped: %r",
            topic, source, raw_payload,
        )
def publish_marketplace_dlq(producer, record, *, ack_timeout_seconds: int):
    """Publish a marketplace DLQ record and wait for broker acknowledgement."""
    import json
    from common.serialization import serialize_for_wire
    from config.topics import MARKETPLACE_OBSERVATIONS_DLQ
    if ack_timeout_seconds <= 0:
        raise ValueError("ack_timeout_seconds must be positive")
    value = serialize_for_wire(record)
    future = producer.send(MARKETPLACE_OBSERVATIONS_DLQ.name, key=record.dlq_id, value=value)
    return future.get(timeout=ack_timeout_seconds)
