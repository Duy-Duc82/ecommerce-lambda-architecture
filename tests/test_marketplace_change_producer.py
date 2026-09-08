"""Phase 5 SPD-04: marketplace change producer."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from common.serialization import serialize_for_wire
from config.marketplace_schema import MarketplaceChangeType, create_change_event
from data_ingestion.marketplace_change_producer import (
    ChangePublishError,
    change_key,
    change_topic,
    encode_change,
    publish_change,
    publish_changes,
)

T0 = datetime(2026, 9, 1, 4, 0, tzinfo=timezone.utc)
RULE = "marketplace-change-rules.v1"


def _change(index: int = 1, *, offer_id: str | None = None):
    return create_change_event(
        marketplace_code="tiki",
        offer_id=offer_id or f"offer_{index}",
        change_type=MarketplaceChangeType.PRICE_CHANGED,
        current_observation_id=f"obs_{index}",
        detected_at=T0,
        rule_version=RULE,
        previous_observation_id=f"obs_prev_{index}",
        field_name="current_price",
        previous_value=Decimal("2000000"),
        current_value=Decimal("1000000"),
    )


class FakeFuture:
    def __init__(self, error: Exception | None = None):
        self._error = error
        self.timeouts: list[float] = []

    def get(self, timeout=None):
        self.timeouts.append(timeout)
        if self._error is not None:
            raise self._error
        return object()


class FakeProducer:
    def __init__(self, fail_on: int | None = None):
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.futures: list[FakeFuture] = []
        self._fail_on = fail_on

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))
        error = None
        if self._fail_on is not None and len(self.sent) == self._fail_on:
            error = RuntimeError("broker unavailable")
        future = FakeFuture(error)
        self.futures.append(future)
        return future


def test_topic_resolves_to_the_registry_name():
    assert change_topic() == "marketplace.changes.v1"


def test_key_is_the_offer_id():
    change = _change(offer_id="offer_abc")
    assert change_key(change) == b"offer_abc"


def test_canonical_bytes_match_the_shared_serializer():
    change = _change()
    expected = json.dumps(
        serialize_for_wire(change),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert encode_change(change) == expected


def test_canonical_bytes_keep_money_as_strings():
    payload = json.loads(encode_change(_change()).decode("utf-8"))
    assert payload["previous_value"] == "2000000"
    assert payload["current_value"] == "1000000"
    assert isinstance(payload["previous_value"], str)


def test_publish_sends_to_the_exact_topic_with_the_offer_key():
    producer = FakeProducer()
    change = _change(offer_id="offer_xyz")
    publish_change(producer, change)
    topic, key, value = producer.sent[0]
    assert topic == "marketplace.changes.v1"
    assert key == b"offer_xyz"
    assert value == encode_change(change)


def test_publish_waits_for_ack_with_the_configured_timeout():
    producer = FakeProducer()
    publish_change(producer, _change(), ack_timeout_seconds=12.5)
    assert producer.futures[0].timeouts == [12.5]


def test_wrong_type_is_rejected_before_any_send():
    producer = FakeProducer()
    with pytest.raises(TypeError):
        publish_change(producer, {"event_id": "not-a-change"})
    assert producer.sent == []


def test_publish_changes_rejects_a_bad_item_before_sending_any():
    producer = FakeProducer()
    with pytest.raises(TypeError):
        publish_changes(producer, [_change(1), "nope"])
    assert producer.sent == []


def test_publish_changes_returns_the_acked_count():
    producer = FakeProducer()
    assert publish_changes(producer, [_change(1), _change(2), _change(3)]) == 3


def test_ack_failure_propagates_with_partial_progress():
    producer = FakeProducer(fail_on=2)
    with pytest.raises(ChangePublishError) as excinfo:
        publish_changes(producer, [_change(1), _change(2), _change(3)])
    assert excinfo.value.acked == 1
    assert excinfo.value.attempted == 3


def test_publishing_nothing_sends_nothing():
    producer = FakeProducer()
    assert publish_changes(producer, []) == 0
    assert producer.sent == []


def test_module_imports_without_kafka_installed():
    import importlib

    module = importlib.import_module("data_ingestion.marketplace_change_producer")
    assert hasattr(module, "publish_changes")
