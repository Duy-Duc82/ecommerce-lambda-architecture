"""Strict observation-wire tests for the Phase 4 contract the speed layer reads."""
import json
from datetime import timedelta, timezone

import pytest

from common.serialization import serialize_for_wire
from config.marketplace_wire import (
    WireContractError,
    canonical_json,
    marketplace_observation_from_wire,
)
from tests.test_marketplace_schema import make_event


def _wire():
    return json.loads(canonical_json(make_event()))


def test_observation_wire_round_trip():
    event = make_event()

    assert marketplace_observation_from_wire(serialize_for_wire(event)) == event
    assert marketplace_observation_from_wire(_wire()) == event


def test_observation_wire_rejects_non_object():
    with pytest.raises(WireContractError, match="must be an object"):
        marketplace_observation_from_wire(["not", "an", "object"])


@pytest.mark.parametrize(
    "path, message",
    [
        ([], "event"),
        (["payload"], "event.payload"),
        (["payload", "offer"], "event.payload.offer"),
        (["payload", "observation"], "event.payload.observation"),
    ],
)
def test_observation_wire_rejects_unknown_fields_at_every_level(path, message):
    wire = _wire()
    target = wire
    for key in path:
        target = target[key]
    target["surprise"] = 1

    with pytest.raises(WireContractError, match=message):
        marketplace_observation_from_wire(wire)


@pytest.mark.parametrize(
    "path, field",
    [
        ([], "crawl_run_id"),
        (["payload", "offer"], "currency"),
        (["payload", "observation"], "raw_sha256"),
    ],
)
def test_observation_wire_rejects_missing_fields(path, field):
    wire = _wire()
    target = wire
    for key in path:
        target = target[key]
    del target[field]

    with pytest.raises(WireContractError, match="missing"):
        marketplace_observation_from_wire(wire)


def test_observation_wire_rejects_wrong_schema_version():
    wire = _wire()
    wire["schema_version"] = "marketplace-observation.v2"

    with pytest.raises(WireContractError, match="schema_version"):
        marketplace_observation_from_wire(wire)


def test_observation_wire_rejects_wrong_event_type():
    wire = _wire()
    wire["event_type"] = "OFFER_CHANGED"

    with pytest.raises(WireContractError, match="event_type"):
        marketplace_observation_from_wire(wire)


def test_observation_wire_rejects_offer_id_disagreeing_with_its_observation():
    wire = _wire()
    wire["payload"]["offer"]["offer_id"] = "offer_forged"

    with pytest.raises(WireContractError, match="must equal offer.offer_id"):
        marketplace_observation_from_wire(wire)


def test_observation_wire_rejects_forged_offer_identity():
    wire = _wire()
    wire["payload"]["offer"]["offer_id"] = "offer_forged"
    wire["payload"]["observation"]["offer_id"] = "offer_forged"

    with pytest.raises(WireContractError, match="offer_id does not match"):
        marketplace_observation_from_wire(wire)


def test_observation_wire_rejects_forged_observation_identity():
    wire = _wire()
    wire["payload"]["observation"]["raw_sha256"] = "b" * 64

    with pytest.raises(WireContractError, match="observation_id does not match"):
        marketplace_observation_from_wire(wire)


def test_observation_wire_rejects_non_canonical_partition_key():
    wire = _wire()
    wire["partition_key"] = "tiki/p1"

    with pytest.raises(WireContractError, match="partition_key"):
        marketplace_observation_from_wire(wire)


def test_observation_wire_rejects_uppercase_marketplace():
    wire = _wire()
    wire["marketplace"] = "TIKI"

    with pytest.raises(WireContractError, match="offer_id does not match"):
        marketplace_observation_from_wire(wire)


def test_observation_wire_rejects_float_prices():
    wire = _wire()
    wire["payload"]["observation"]["current_price"] = 100.0

    with pytest.raises(WireContractError, match="decimal string"):
        marketplace_observation_from_wire(wire)


def test_observation_wire_rejects_bool_counts():
    wire = _wire()
    wire["payload"]["observation"]["review_count"] = True

    with pytest.raises(WireContractError, match="integer"):
        marketplace_observation_from_wire(wire)


def test_observation_wire_rejects_naive_timestamps():
    wire = _wire()
    wire["payload"]["observation"]["observed_at"] = "2026-09-04T08:30:01"

    with pytest.raises(WireContractError, match="timezone"):
        marketplace_observation_from_wire(wire)


def test_observation_wire_normalizes_utc():
    event = make_event()
    wire = _wire()
    tehran = timezone(timedelta(hours=3, minutes=30))
    observed_at = event.payload.observation.observed_at
    wire["payload"]["observation"]["observed_at"] = observed_at.astimezone(
        tehran
    ).isoformat()

    decoded = marketplace_observation_from_wire(wire)

    assert decoded.payload.observation.observed_at.tzinfo == timezone.utc
    assert decoded.payload.observation.observed_at == observed_at
    assert decoded == event


def test_observation_wire_keeps_optional_nulls():
    decoded = marketplace_observation_from_wire(_wire())

    assert decoded.payload.offer.seller_id is None
    assert decoded.payload.offer.brand is None
    assert decoded.payload.observation.list_price is None
    assert decoded.payload.observation.rating_value is None
