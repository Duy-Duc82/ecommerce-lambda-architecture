"""Finite Kafka-record processor for immutable marketplace Silver landing."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from urllib.parse import quote

from ecomdata.domain.dlq import DlqStage, create_observation_dlq
from ecomdata.domain.serialization import serialize_for_wire
from ecomdata.domain.topics import MARKETPLACE_OBSERVATIONS
from ecomdata.domain.wire import marketplace_observation_from_wire
from ecomdata.infra.dlq import publish_marketplace_dlq


@dataclass(frozen=True)
class SourceRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes


@dataclass(frozen=True)
class SilverSinkResult:
    status: str
    object_uri: str
    event_id: str | None
    dlq_id: str | None


class SilverWriteError(RuntimeError):
    """A valid observation could not be written to Silver.

    Not a bad record: the caller must retry it at the same offset. Sending it
    to the DLQ would lose a good observation to a storage outage.
    """


DEFAULT_SILVER_DATASET = "marketplace/offer_observations"


def silver_observation_path(event, dataset: str = DEFAULT_SILVER_DATASET) -> str:
    """Where an observation lands in Silver; the raw reparse reads it back here."""
    observation = event.payload.observation
    return f"{dataset.strip('/')}/marketplace={quote(event.marketplace, safe='')}/observed_date={observation.observed_at.date().isoformat()}/observation_id={quote(observation.observation_id, safe='')}.json"


def process_record(
    record: SourceRecord, *, writer, dlq_producer, clock, dataset: str = DEFAULT_SILVER_DATASET, ack_timeout_seconds: int = 30
):
    """Write one Kafka record to Silver under ``dataset``, or quarantine it.

    ``dataset`` is the configured Silver dataset (``MARKETPLACE_SILVER_DATASET``);
    it used to be hard-coded here, so a stack configured with another dataset
    landed observations where neither the batch nor the reparse looked.
    """
    if record.topic != MARKETPLACE_OBSERVATIONS.name:
        raise ValueError("source topic is not marketplace observations")
    text = None
    stage = DlqStage.DECODE
    try:
        text = record.value.decode("utf-8")
        raw = json.loads(text)
        stage = DlqStage.CONTRACT_VALIDATION
        event = marketplace_observation_from_wire(raw)
        key = record.key.decode("utf-8") if record.key is not None else None
        if key != event.partition_key:
            raise ValueError("Kafka key does not equal event partition_key")
        path = silver_observation_path(event, dataset)
        payload = json.dumps(
            serialize_for_wire(event), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    except Exception as error:
        if text is None:
            text = record.value.decode("utf-8", "replace")
        key = None
        if record.key is not None:
            try:
                key = record.key.decode("utf-8")
            except UnicodeDecodeError:
                key = "base64:" + base64.b64encode(record.key).decode("ascii")
        failed_at = clock()
        dlq = create_observation_dlq(
            failed_at=failed_at,
            stage=stage,
            source_topic=record.topic,
            source_partition=record.partition,
            source_offset=record.offset,
            source_key=key,
            payload_text=text,
            error=error,
        )
        path = f"quarantine/offer_observations/observed_date={failed_at.astimezone(__import__('datetime').timezone.utc).date().isoformat()}/source_topic={quote(record.topic, safe='')}/partition={record.partition}/offset={record.offset}.json"
        uri = writer(
            "silver",
            path,
            json.dumps(serialize_for_wire(dlq), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            ),
        )
        publish_marketplace_dlq(dlq_producer, dlq, ack_timeout_seconds=ack_timeout_seconds)
        return SilverSinkResult("QUARANTINE", uri, None, dlq.dlq_id)
    # Outside the except above on purpose: only decoding and validation decide
    # that a record is bad. A write that fails here is the store's fault.
    try:
        uri = writer("silver", path, payload)
    except Exception as error:
        raise SilverWriteError(f"Silver write failed for {event.event_id}: {error}") from error
    return SilverSinkResult("SILVER", uri, event.event_id, None)
