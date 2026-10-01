"""Finite Kafka-record processor for immutable marketplace Silver landing."""
from __future__ import annotations
import base64, json
from dataclasses import dataclass
from urllib.parse import quote
from config.marketplace_dlq import DlqStage, create_observation_dlq
from config.marketplace_wire import marketplace_observation_from_wire
from config.topics import MARKETPLACE_OBSERVATIONS
from common.dlq import publish_marketplace_dlq
from common.serialization import serialize_for_wire

@dataclass(frozen=True)
class SourceRecord:
    topic: str; partition: int; offset: int; key: bytes | None; value: bytes
@dataclass(frozen=True)
class SilverSinkResult:
    status: str; object_uri: str; event_id: str | None; dlq_id: str | None

def silver_observation_path(event, dataset: str = "marketplace/offer_observations") -> str:
    """Where an observation lands in Silver; the raw reparse reads it back here."""
    observation = event.payload.observation
    return f"{dataset.strip('/')}/marketplace={quote(event.marketplace, safe='')}/observed_date={observation.observed_at.date().isoformat()}/observation_id={quote(observation.observation_id, safe='')}.json"

def process_record(record: SourceRecord, *, writer, dlq_producer, clock):
    if record.topic != MARKETPLACE_OBSERVATIONS.name: raise ValueError("source topic is not marketplace observations")
    text = None; stage = DlqStage.DECODE
    try:
        text = record.value.decode("utf-8")
        raw = json.loads(text)
        stage = DlqStage.CONTRACT_VALIDATION
        event = marketplace_observation_from_wire(raw)
        key = record.key.decode("utf-8") if record.key is not None else None
        if key != event.partition_key: raise ValueError("Kafka key does not equal event partition_key")
        path = silver_observation_path(event)
        payload = json.dumps(serialize_for_wire(event), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return SilverSinkResult("SILVER", writer("silver", path, payload), event.event_id, None)
    except Exception as error:
        if text is None: text = record.value.decode("utf-8", "replace")
        key = None
        if record.key is not None:
            try: key = record.key.decode("utf-8")
            except UnicodeDecodeError: key = "base64:" + base64.b64encode(record.key).decode("ascii")
        failed_at = clock()
        dlq = create_observation_dlq(failed_at=failed_at, stage=stage, source_topic=record.topic, source_partition=record.partition, source_offset=record.offset, source_key=key, payload_text=text, error=error)
        path = f"quarantine/offer_observations/observed_date={failed_at.astimezone(__import__('datetime').timezone.utc).date().isoformat()}/source_topic={quote(record.topic, safe='')}/partition={record.partition}/offset={record.offset}.json"
        uri = writer("silver", path, json.dumps(serialize_for_wire(dlq), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))
        publish_marketplace_dlq(dlq_producer, dlq, ack_timeout_seconds=30)
        return SilverSinkResult("QUARANTINE", uri, None, dlq.dlq_id)
