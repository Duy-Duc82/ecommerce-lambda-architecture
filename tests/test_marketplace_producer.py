"""Acknowledged producer tests: exact topic, key and ack timeout."""
import json
from decimal import Decimal

import pytest

from config.marketplace_wire import canonical_json
from config.topics import MARKETPLACE_CHANGES, MARKETPLACE_OBSERVATIONS
from data_ingestion.marketplace_change_producer import publish_change
from data_ingestion.marketplace_producer import publish_observation
from speed_layer.marketplace_change_rules import detect_observation_changes
from tests.test_marketplace_change_rules import CONFIG, _event
from tests.test_marketplace_schema import make_event


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


def test_observation_publish_uses_topic_partition_key_and_ack_timeout():
    producer = FakeProducer()
    event = make_event()

    publish_observation(producer, event, ack_timeout_seconds=7)

    sent = producer.sent[0]
    assert sent["topic"] == MARKETPLACE_OBSERVATIONS.name
    assert sent["key"] == event.partition_key
    assert sent["key"] == f"tiki:{event.payload.offer.platform_listing_id}"
    assert sent["future"].timeouts == [7]


def test_observation_publish_sends_canonical_wire_values():
    producer = FakeProducer()
    event = make_event()

    publish_observation(producer, event)

    assert producer.sent[0]["value"] == json.loads(canonical_json(event))
    assert producer.sent[0]["value"]["payload"]["observation"]["current_price"] == "100.00"


def test_observation_publish_rejects_a_foreign_event_type():
    with pytest.raises(TypeError, match="MarketplaceObservationV1"):
        publish_observation(FakeProducer(), _new_offer_change())


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


@pytest.mark.parametrize("publish", [publish_observation, publish_change])
def test_publish_rejects_non_positive_ack_timeout(publish):
    event = make_event() if publish is publish_observation else _new_offer_change()

    with pytest.raises(ValueError, match="ack_timeout_seconds"):
        publish(FakeProducer(), event, ack_timeout_seconds=0)


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
    forged = type(price_change)(
        **{**price_change.__dict__, "rule_version": "speed-rules.v2"}
    )

    with pytest.raises(Exception):
        publish_change(producer, forged)
