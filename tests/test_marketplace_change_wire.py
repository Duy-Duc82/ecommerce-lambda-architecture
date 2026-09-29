"""Strict change-wire tests, items 18-19 of the Phase 5 plan section 13."""
import json
from datetime import timedelta, timezone
from decimal import Decimal

import pytest

from config.marketplace_schema import MarketplaceChangeType
from config.marketplace_wire import (
    WireContractError,
    canonical_json,
    marketplace_change_from_wire,
)
from speed_layer.marketplace_change_rules import (
    ChangeRuleConfig,
    detect_observation_changes,
    detect_stale_change,
)
from tests.test_marketplace_change_rules import CONFIG, _event, _later


def _new_offer_change():
    return detect_observation_changes(None, _event(), CONFIG).changes[0]


def _price_change():
    first = detect_observation_changes(None, _event(), CONFIG)
    second = detect_observation_changes(
        first.next_state, _later(current_price=Decimal("150.00")), CONFIG
    )
    return second.changes[0]


def _stale_change():
    state = detect_observation_changes(None, _event(), CONFIG).next_state
    return detect_stale_change(state, CONFIG)[1]


def _wire(change):
    return json.loads(canonical_json(change))


# 18
def test_strict_change_decoder_rejects_floats():
    wire = _wire(_price_change())
    wire["current_value"] = 1.0

    with pytest.raises(WireContractError, match="forbidden JSON float"):
        marketplace_change_from_wire(wire)


def test_strict_change_decoder_rejects_nested_floats():
    wire = _wire(_new_offer_change())
    wire["current_value"]["current_price"] = 100.5

    with pytest.raises(WireContractError, match="forbidden JSON float"):
        marketplace_change_from_wire(wire)


def test_strict_change_decoder_rejects_unknown_fields():
    wire = _wire(_price_change())
    wire["extra_field"] = "x"

    with pytest.raises(WireContractError, match="unknown"):
        marketplace_change_from_wire(wire)


def test_strict_change_decoder_rejects_missing_fields():
    wire = _wire(_price_change())
    del wire["rule_version"]

    with pytest.raises(WireContractError, match="missing"):
        marketplace_change_from_wire(wire)


def test_strict_change_decoder_rejects_forged_ids():
    wire = _wire(_price_change())
    wire["event_id"] = "change_forged"

    with pytest.raises(WireContractError, match="deterministic identity"):
        marketplace_change_from_wire(wire)


def test_strict_change_decoder_rejects_rule_version_drift():
    wire = _wire(_price_change())
    wire["rule_version"] = "speed-rules.v2"

    with pytest.raises(WireContractError, match="deterministic identity"):
        marketplace_change_from_wire(wire)


def test_strict_change_decoder_rejects_wrong_schema_version():
    wire = _wire(_price_change())
    wire["schema_version"] = "marketplace-change.v2"

    with pytest.raises(WireContractError, match="schema_version"):
        marketplace_change_from_wire(wire)


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda w: w.update(previous_observation_id="obs-x"), "previous_observation_id"),
        (lambda w: w.update(field_name="current_price"), "field_name"),
    ],
)
def test_strict_change_decoder_enforces_new_offer_shape(mutate, message):
    wire = _wire(_new_offer_change())
    mutate(wire)

    with pytest.raises(WireContractError, match=message):
        marketplace_change_from_wire(wire)


def test_strict_change_decoder_requires_field_name_for_valued_changes():
    wire = _wire(_price_change())
    wire["field_name"] = None

    with pytest.raises(WireContractError, match="field_name is required"):
        marketplace_change_from_wire(wire)


def test_strict_change_decoder_requires_previous_observation_for_stale():
    wire = _wire(_stale_change())
    wire["previous_observation_id"] = None

    with pytest.raises(WireContractError, match="previous_observation_id is required"):
        marketplace_change_from_wire(wire)


def test_strict_change_decoder_rejects_naive_detected_at():
    wire = _wire(_price_change())
    wire["detected_at"] = "2026-09-04T08:30:01"

    with pytest.raises(WireContractError, match="timezone"):
        marketplace_change_from_wire(wire)


# 19
@pytest.mark.parametrize(
    "factory", [_new_offer_change, _price_change, _stale_change]
)
def test_strict_change_decoder_accepts_canonical_values(factory):
    change = factory()

    assert marketplace_change_from_wire(_wire(change)) == change


def test_strict_change_decoder_normalizes_utc():
    change = _price_change()
    wire = _wire(change)
    tehran = timezone(timedelta(hours=3, minutes=30))
    wire["detected_at"] = change.detected_at.astimezone(tehran).isoformat()

    decoded = marketplace_change_from_wire(wire)

    assert decoded.detected_at.tzinfo == timezone.utc
    assert decoded.detected_at == change.detected_at


def test_strict_change_decoder_accepts_zulu_suffix():
    change = _price_change()
    wire = _wire(change)
    wire["detected_at"] = (
        change.detected_at.isoformat().replace("+00:00", "Z")
    )

    assert marketplace_change_from_wire(wire).detected_at == change.detected_at


def test_canonical_json_is_stable_and_sorted():
    change = _price_change()

    first = canonical_json(change)

    assert first == canonical_json(change)
    assert list(json.loads(first)) == sorted(json.loads(first))
    assert change.change_type is MarketplaceChangeType.PRICE_CHANGED


def test_change_rule_config_rejects_invalid_bounds():
    with pytest.raises(ValueError, match="rule_version"):
        ChangeRuleConfig(" ", Decimal("1"), Decimal("0.1"), 1)
    with pytest.raises(ValueError, match="large_drop_absolute"):
        ChangeRuleConfig("v1", Decimal("-1"), Decimal("0.1"), 1)
    with pytest.raises(ValueError, match="large_drop_relative"):
        ChangeRuleConfig("v1", Decimal("1"), Decimal("1.5"), 1)
    with pytest.raises(ValueError, match="stale_after_seconds"):
        ChangeRuleConfig("v1", Decimal("1"), Decimal("0.1"), 0)
