"""Silver landing and quarantine tests for the Phase 4 sink.

Everything runs against an in-memory writer and a fake DLQ producer.
"""
import json
from datetime import datetime, timezone

import pytest

from common.serialization import serialize_for_wire
from config.marketplace_dlq import (
    MARKETPLACE_DLQ_SCHEMA_VERSION,
    DlqStage,
    create_observation_dlq,
)
from config.marketplace_wire import canonical_json
from config.topics import MARKETPLACE_OBSERVATIONS, MARKETPLACE_OBSERVATIONS_DLQ
from data_ingestion.marketplace_silver_sink import SourceRecord, process_record
from tests.test_marketplace_producer import FakeProducer
from tests.test_marketplace_schema import make_event

FAILED_AT = datetime(2026, 9, 29, 6, 0, tzinfo=timezone.utc)


class MemoryWriter:
    def __init__(self):
        self.objects = {}

    def __call__(self, bucket, path, payload):
        self.objects[(bucket, path)] = payload
        return f"s3a://{bucket}/{path}"


def _record(event=None, *, key=None, value=None, offset=3):
    event = event or make_event()
    if value is None:
        value = canonical_json(event).encode("utf-8")
    if key is None:
        key = event.partition_key.encode("utf-8")
    return SourceRecord(MARKETPLACE_OBSERVATIONS.name, 0, offset, key, value)


def _process(record):
    writer, producer = MemoryWriter(), FakeProducer()
    result = process_record(
        record, writer=writer, dlq_producer=producer, clock=lambda: FAILED_AT
    )
    return result, writer, producer


def test_a_valid_record_lands_in_silver_under_its_observation_identity():
    event = make_event()

    result, writer, producer = _process(_record(event))

    observation = event.payload.observation
    assert result.status == "SILVER"
    assert result.event_id == event.event_id
    assert result.dlq_id is None
    assert result.object_uri == (
        "s3a://silver/marketplace/offer_observations/"
        "marketplace=tiki/"
        f"observed_date={observation.observed_at.date().isoformat()}/"
        f"observation_id={observation.observation_id}.json"
    )
    assert producer.sent == []


def test_the_landed_object_is_the_canonical_event():
    event = make_event()

    _, writer, _ = _process(_record(event))

    payload = json.loads(next(iter(writer.objects.values())).decode("utf-8"))
    assert payload == serialize_for_wire(event)


def test_landing_the_same_record_twice_writes_the_same_object():
    first_result, first_writer, _ = _process(_record())
    second_result, second_writer, _ = _process(_record(offset=99))

    assert first_result.object_uri == second_result.object_uri
    assert first_writer.objects == second_writer.objects


def test_a_record_from_another_topic_is_rejected_outright():
    record = SourceRecord("other.topic", 0, 1, b"k", b"{}")

    with pytest.raises(ValueError, match="source topic"):
        process_record(
            record,
            writer=MemoryWriter(),
            dlq_producer=FakeProducer(),
            clock=lambda: FAILED_AT,
        )


def test_undecodable_bytes_are_quarantined_at_the_decode_stage():
    result, writer, producer = _process(_record(value=b"\xff\xfe not json"))

    assert result.status == "QUARANTINE"
    assert result.event_id is None
    stored = json.loads(next(iter(writer.objects.values())).decode("utf-8"))
    assert stored["stage"] == DlqStage.DECODE.value
    assert stored["schema_version"] == MARKETPLACE_DLQ_SCHEMA_VERSION


def test_a_contract_violation_is_quarantined_at_the_validation_stage():
    wire = json.loads(canonical_json(make_event()))
    wire["partition_key"] = "tiki/p1"

    result, writer, _ = _process(_record(value=json.dumps(wire).encode("utf-8")))

    stored = json.loads(next(iter(writer.objects.values())).decode("utf-8"))
    assert result.status == "QUARANTINE"
    assert stored["stage"] == DlqStage.CONTRACT_VALIDATION.value
    assert "partition_key" in stored["error_message"]


def test_a_key_that_disagrees_with_the_envelope_is_quarantined():
    result, writer, _ = _process(_record(key=b"tiki:other"))

    stored = json.loads(next(iter(writer.objects.values())).decode("utf-8"))
    assert result.status == "QUARANTINE"
    assert stored["stage"] == DlqStage.CONTRACT_VALIDATION.value
    assert stored["source_key"] == "tiki:other"


def test_a_non_utf8_key_is_preserved_as_base64_evidence():
    result, writer, _ = _process(_record(key=b"\xff\xfe"))

    stored = json.loads(next(iter(writer.objects.values())).decode("utf-8"))
    assert result.status == "QUARANTINE"
    assert stored["source_key"].startswith("base64:")


def test_quarantine_keeps_the_raw_payload_and_source_coordinates():
    result, writer, _ = _process(_record(value=b"not json", offset=17))

    stored = json.loads(next(iter(writer.objects.values())).decode("utf-8"))
    assert stored["payload_text"] == "not json"
    assert stored["source_topic"] == MARKETPLACE_OBSERVATIONS.name
    assert stored["source_partition"] == 0
    assert stored["source_offset"] == 17
    assert result.dlq_id == stored["dlq_id"]


def test_the_quarantine_path_is_partitioned_by_failure_date_and_coordinates():
    result, _, _ = _process(_record(value=b"not json", offset=17))

    assert result.object_uri == (
        "s3a://silver/quarantine/offer_observations/"
        "observed_date=2026-09-29/"
        f"source_topic={MARKETPLACE_OBSERVATIONS.name}/partition=0/offset=17.json"
    )


def test_a_quarantined_record_is_acknowledged_on_the_dlq_topic():
    result, _, producer = _process(_record(value=b"not json"))

    sent = producer.sent[0]
    assert sent["topic"] == MARKETPLACE_OBSERVATIONS_DLQ.name
    assert sent["key"] == result.dlq_id
    assert sent["future"].timeouts == [30]


def test_the_dlq_id_is_deterministic_for_the_same_coordinates_and_stage():
    first, _, _ = _process(_record(value=b"not json", offset=5))
    second, _, _ = _process(_record(value=b"other garbage", offset=5))

    assert first.dlq_id == second.dlq_id


def test_dlq_record_truncates_an_oversized_error_message():
    record = create_observation_dlq(
        failed_at=FAILED_AT,
        stage=DlqStage.DECODE,
        source_topic=MARKETPLACE_OBSERVATIONS.name,
        source_partition=0,
        source_offset=1,
        source_key=None,
        payload_text="x",
        error=RuntimeError("e" * 5000),
    )

    assert len(record.error_message) == 2000


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"source_partition": -1}, "source coordinates"),
        ({"source_offset": -1}, "source coordinates"),
        ({"payload_text": ""}, "payload is required"),
        ({"raw_uri": "https://example/raw.json"}, "raw_uri"),
    ],
)
def test_dlq_record_rejects_invalid_evidence(overrides, message):
    values = {
        "failed_at": FAILED_AT,
        "stage": DlqStage.DECODE,
        "source_topic": MARKETPLACE_OBSERVATIONS.name,
        "source_partition": 0,
        "source_offset": 1,
        "source_key": None,
        "payload_text": "x",
        "error": RuntimeError("boom"),
    }
    values.update(overrides)

    with pytest.raises(ValueError, match=message):
        create_observation_dlq(**values)


def test_dlq_record_rejects_a_naive_failed_at():
    with pytest.raises(ValueError, match="failed_at"):
        create_observation_dlq(
            failed_at=datetime(2026, 9, 29, 6, 0),
            stage=DlqStage.DECODE,
            source_topic=MARKETPLACE_OBSERVATIONS.name,
            source_partition=0,
            source_offset=1,
            source_key=None,
            payload_text="x",
            error=RuntimeError("boom"),
        )


# ----------------------------------------------------------------------------
# Phase 8 plan section 6.2. A valid observation whose Silver write fails is
# not a bad record: it must be retried, never quarantined. The sink used to
# wrap the write in the same except as decoding and validation, so a MinIO
# outage sent perfectly good observations to the DLQ as CONTRACT_VALIDATION,
# and they never reached Silver.
# ----------------------------------------------------------------------------
from data_ingestion.marketplace_silver_sink import SilverWriteError


class SilverOutageWriter(MemoryWriter):
    """Refuses Silver observation writes; quarantine writes still work."""

    def __call__(self, bucket, path, payload):
        if path.startswith("marketplace/offer_observations/"):
            raise ConnectionError("MinIO unavailable")
        return super().__call__(bucket, path, payload)


def test_a_silver_write_failure_on_a_valid_event_raises_and_produces_no_dlq_record():
    writer, producer = SilverOutageWriter(), FakeProducer()

    with pytest.raises(SilverWriteError) as caught:
        process_record(_record(), writer=writer, dlq_producer=producer, clock=lambda: FAILED_AT)

    assert isinstance(caught.value.__cause__, ConnectionError)
    assert producer.sent == []
    assert writer.objects == {}


def test_bad_records_are_still_quarantined_while_silver_is_down():
    # The outage must not change what counts as a bad record.
    writer, producer = SilverOutageWriter(), FakeProducer()

    result = process_record(_record(value=b"\xff\xfe not utf-8"), writer=writer, dlq_producer=producer, clock=lambda: FAILED_AT)

    assert result.status == "QUARANTINE"
    assert len(producer.sent) == 1
