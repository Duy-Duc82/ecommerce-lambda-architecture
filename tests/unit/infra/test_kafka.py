"""The one Kafka producer factory and acknowledged sends."""
from __future__ import annotations

import json

import pytest

from ecomdata.infra import kafka


def test_the_factory_builds_an_acks_all_single_in_flight_producer(monkeypatch):
    import kafka as kafka_python

    captured = {}

    class FakeProducer:
        def __init__(self, **options):
            captured.update(options)

    monkeypatch.setattr(kafka_python, "KafkaProducer", FakeProducer)

    kafka.create_producer("broker:9092", client_id="crawler")

    assert captured["bootstrap_servers"] == "broker:9092"
    assert captured["acks"] == "all"
    assert captured["max_in_flight_requests_per_connection"] == 1
    assert captured["client_id"] == "crawler"
    assert captured["key_serializer"]("k") == b"k" and captured["key_serializer"](None) is None
    assert json.loads(captured["value_serializer"]({"b": 1, "a": 2})) == {"a": 2, "b": 1}


def test_a_send_waits_for_the_acknowledgement():
    calls = []

    class Future:
        def get(self, timeout):
            calls.append(("get", timeout))
            return "metadata"

    class Producer:
        def send(self, topic, key, value):
            calls.append(("send", topic, key, value))
            return Future()

    assert kafka.send_acknowledged(Producer(), "t", key="k", value={"x": 1}, ack_timeout_seconds=5) == "metadata"
    assert calls == [("send", "t", "k", {"x": 1}), ("get", 5)]


def test_a_non_positive_ack_timeout_is_refused():
    with pytest.raises(ValueError, match="ack_timeout_seconds"):
        kafka.send_acknowledged(object(), "t", key=None, value=1, ack_timeout_seconds=0)
