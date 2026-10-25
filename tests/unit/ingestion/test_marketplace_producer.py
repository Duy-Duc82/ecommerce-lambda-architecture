"""Acknowledged producer tests: exact topic, key and ack timeout."""

import json
from decimal import Decimal

import pytest

from ecomdata.domain.change_rules import detect_observation_changes
from ecomdata.domain.topics import MARKETPLACE_CHANGES
from ecomdata.domain.wire import WireContractError, canonical_json
from ecomdata.processing.speed.change_producer import publish_change
from tests.unit.domain.test_marketplace_change_rules import CONFIG, _event


class FakeFuture:
    def __init__(self, record, error=None):
        self.record = record
        self.error = error
        self.timeouts = []

    def get(self, timeout=None):
        self.timeouts.append(timeout)
        if self.error is not None:
            raise self.error
        return self.record


class FakeProducer:
    """Records sends the way KafkaProducer would accept them."""

    def __init__(self, error=None):
        self.sent = []
        self.error = error
        self.flushed = 0

    def send(self, topic, key=None, value=None):
        future = FakeFuture({"topic": topic, "offset": len(self.sent)}, self.error)
        self.sent.append({"topic": topic, "key": key, "value": value, "future": future})
        return future

    def flush(self):
        self.flushed += 1


def _new_offer_change():
    return detect_observation_changes(None, _event(), CONFIG).changes[0]


def test_change_publish_uses_topic_offer_key_and_ack_timeout():
    producer = FakeProducer()
    change = _new_offer_change()

    publish_change(producer, change, ack_timeout_seconds=11)

    sent = producer.sent[0]
    assert sent["topic"] == MARKETPLACE_CHANGES.name
    assert sent["key"] == change.offer_id
    assert sent["future"].timeouts == [11]


def test_change_publish_revalidates_the_wire_before_sending():
    producer = FakeProducer()
    change = _new_offer_change()

    publish_change(producer, change)

    assert producer.sent[0]["value"] == json.loads(canonical_json(change))


def test_publish_rejects_non_positive_ack_timeout():
    with pytest.raises(ValueError, match="ack_timeout_seconds"):
        publish_change(FakeProducer(), _new_offer_change(), ack_timeout_seconds=0)


def test_change_publish_failure_propagates():
    producer = FakeProducer(error=RuntimeError("broker down"))

    with pytest.raises(RuntimeError, match="broker down"):
        publish_change(producer, _new_offer_change())


def test_change_publish_is_replay_identical():
    first, second = FakeProducer(), FakeProducer()
    change = _new_offer_change()

    publish_change(first, change)
    publish_change(second, change)

    assert first.sent[0]["key"] == second.sent[0]["key"]
    assert first.sent[0]["value"] == second.sent[0]["value"]


def test_change_publish_rejects_a_price_change_with_a_drifted_rule_version():
    producer = FakeProducer()
    first = detect_observation_changes(None, _event(), CONFIG)
    price_change = detect_observation_changes(
        first.next_state,
        _event(
            observed_at=first.next_state.observed_at.replace(microsecond=1),
            current_price=Decimal("150.00"),
        ),
        CONFIG,
    ).changes[0]
    forged = type(price_change)(**{**price_change.__dict__, "rule_version": "speed-rules.v2"})

    with pytest.raises(WireContractError):
        publish_change(producer, forged)


# ----------------------------------------------------------------------------
# The factories must build a producer the installed client accepts. Every
# test above uses a fake, which is how enable_idempotence — an option
# kafka-python-ng does not have — survived until the first real start
# (Phase 8 WP1, 2026-10-01). Checked against the client's own config table,
# so no broker is needed.
# ----------------------------------------------------------------------------
from ecomdata.infra.kafka import create_producer


def _captured_config(monkeypatch, factory):
    kafka = pytest.importorskip("kafka")
    captured = {}

    class RecordingProducer:
        def __init__(self, **configs):
            captured.update(configs)

    monkeypatch.setattr(kafka, "KafkaProducer", RecordingProducer)
    factory("localhost:9092")
    return captured


def _known_options():
    from kafka.producer.kafka import KafkaProducer

    return set(KafkaProducer.DEFAULT_CONFIG)


@pytest.mark.parametrize("factory", [create_producer])
def test_the_producer_factory_passes_only_options_the_client_knows(monkeypatch, factory):
    captured = _captured_config(monkeypatch, factory)

    unknown = set(captured) - _known_options()

    assert unknown == set(), f"KafkaProducer would refuse {sorted(unknown)}"


@pytest.mark.parametrize("factory", [create_producer])
def test_the_producer_keeps_per_key_order_under_retries(monkeypatch, factory):
    # Without idempotence, a retried batch can overtake the next one when more
    # than one request is in flight. One in flight is what keeps an offer's
    # observations, and its changes, in the order they were sent.
    captured = _captured_config(monkeypatch, factory)

    assert captured["acks"] == "all"
    assert captured["retries"] > 0
    assert captured["max_in_flight_requests_per_connection"] == 1


# --- source records (marketplace.records.v1) ---
from datetime import UTC, datetime  # noqa: E402

from ecomdata.domain.acquisition import ListingPageRequest  # noqa: E402
from ecomdata.domain.observations import ResourceType, create_raw_artifact  # noqa: E402
from ecomdata.domain.records import RecordKind, build_records, record_to_wire  # noqa: E402
from ecomdata.domain.sources import TIKI  # noqa: E402
from ecomdata.domain.topics import MARKETPLACE_RECORDS  # noqa: E402
from ecomdata.ingestion.publisher import publish_record  # noqa: E402


def _records():
    raw = create_raw_artifact(
        marketplace_code="tiki", crawl_run_id="run-1", marketplace_id="marketplace-tiki",
        request_url="https://tiki.vn/laptop/c8095", resource_type=ResourceType.LISTING_PAGE,
        fetched_at=datetime(2026, 10, 25, 8, 30, tzinfo=UTC), http_status=200, content_type="text/html",
        body_sha256="a" * 64, raw_uri="file:///lake/part-0.bin.gz", adapter_version=TIKI.adapter_version, raw_bytes=5,
    )
    request = ListingPageRequest("tiki", "marketplace-tiki", "laptop/c8095", 1)
    return build_records(TIKI, request=request, raw_artifact=raw, rows=({"id": 1, "name": "A"},), page_context={"h1": "x"})


def test_a_record_is_published_to_the_records_topic_keyed_by_its_category_page():
    producer = FakeProducer()
    context, row = _records()

    publish_record(producer, row, ack_timeout_seconds=9)

    sent = producer.sent[0]
    assert sent["topic"] == MARKETPLACE_RECORDS.name == "marketplace.records.v1"
    assert sent["key"] == "tiki|laptop/c8095" == row.partition_key
    assert sent["value"] == record_to_wire(row)
    assert sent["value"]["payload"] == {"id": 1, "name": "A"}
    assert sent["future"].timeouts == [9]
    assert context.record_kind is RecordKind.PAGE_CONTEXT


def test_only_listing_rows_go_to_kafka():
    context, _ = _records()

    with pytest.raises(ValueError, match="only listing rows"):
        publish_record(FakeProducer(), context)
    with pytest.raises(TypeError, match="SourceRecord"):
        publish_record(FakeProducer(), {"id": 1})


def test_a_record_publish_waits_for_the_broker_and_surfaces_its_failure():
    _, row = _records()

    with pytest.raises(TimeoutError):
        publish_record(FakeProducer(error=TimeoutError("broker")), row)
    with pytest.raises(ValueError, match="positive"):
        publish_record(FakeProducer(), row, ack_timeout_seconds=0)
