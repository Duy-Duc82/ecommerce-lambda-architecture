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


# ----------------------------------------------------------------------------
# The factories must build a producer the installed client accepts. Every
# test above uses a fake, which is how enable_idempotence — an option
# kafka-python-ng does not have — survived until the first real start
# (Phase 8 WP1, 2026-10-01). Checked against the client's own config table,
# so no broker is needed.
# ----------------------------------------------------------------------------
from data_ingestion import marketplace_change_producer, marketplace_producer


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


@pytest.mark.parametrize("factory", [marketplace_producer.create_marketplace_producer, marketplace_change_producer.create_change_producer])
def test_the_producer_factory_passes_only_options_the_client_knows(monkeypatch, factory):
    captured = _captured_config(monkeypatch, factory)

    unknown = set(captured) - _known_options()

    assert unknown == set(), f"KafkaProducer would refuse {sorted(unknown)}"


@pytest.mark.parametrize("factory", [marketplace_producer.create_marketplace_producer, marketplace_change_producer.create_change_producer])
def test_the_producer_keeps_per_key_order_under_retries(monkeypatch, factory):
    # Without idempotence, a retried batch can overtake the next one when more
    # than one request is in flight. One in flight is what keeps an offer's
    # observations, and its changes, in the order they were sent.
    captured = _captured_config(monkeypatch, factory)

    assert captured["acks"] == "all"
    assert captured["retries"] > 0
    assert captured["max_in_flight_requests_per_connection"] == 1


class OrderedProducer(FakeProducer):
    """Logs sends, flushes and ack waits in the order they happen."""

    def __init__(self, error_at=None):
        super().__init__()
        self.log, self.error_at = [], error_at

    def send(self, topic, key=None, value=None):
        index = len(self.sent)
        future = super().send(topic, key=key, value=value)
        self.log.append(("send", index))
        error = RuntimeError(f"ack {index} lost") if index == self.error_at else None
        log = self.log

        class Logged(FakeFuture):
            def get(self, timeout=None):
                log.append(("get", index))
                return super().get(timeout)

        logged = Logged(future.record, error)
        self.sent[-1]["future"] = logged
        return logged

    def flush(self):
        super().flush()
        self.log.append(("flush",))


def test_a_batch_of_changes_is_sent_before_any_ack_is_awaited():
    from data_ingestion.marketplace_change_producer import publish_changes

    producer = OrderedProducer()
    changes = [_new_offer_change()] * 3

    acks = publish_changes(producer, changes, ack_timeout_seconds=9)

    assert producer.log == [("send", 0), ("send", 1), ("send", 2), ("flush",), ("get", 0), ("get", 1), ("get", 2)]
    assert [a["offset"] for a in acks] == [0, 1, 2]
    assert all(row["future"].timeouts == [9] for row in producer.sent)
    assert [row["key"] for row in producer.sent] == [c.offer_id for c in changes]


def test_any_lost_ack_fails_the_whole_batch():
    from data_ingestion.marketplace_change_producer import publish_changes

    with pytest.raises(RuntimeError, match="ack 1 lost"):
        publish_changes(OrderedProducer(error_at=1), [_new_offer_change()] * 3)


def test_no_change_means_no_send_and_no_flush():
    from data_ingestion.marketplace_change_producer import publish_changes

    producer = OrderedProducer()

    assert publish_changes(producer, []) == []
    assert producer.log == []


def test_a_batch_publish_refuses_a_non_positive_timeout():
    from data_ingestion.marketplace_change_producer import publish_changes

    with pytest.raises(ValueError):
        publish_changes(OrderedProducer(), [_new_offer_change()], ack_timeout_seconds=0)
