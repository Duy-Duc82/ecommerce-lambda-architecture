import json
import pytest
from common.serialization import serialize_for_wire
from config.marketplace_wire import WireContractError, canonical_json, marketplace_change_from_wire, marketplace_observation_from_wire
from speed_layer.marketplace_change_rules import ChangeRuleConfig, detect_observation_changes
from tests.test_marketplace_schema import make_event
from decimal import Decimal


def test_observation_wire_round_trip():
    event = make_event()
    assert marketplace_observation_from_wire(serialize_for_wire(event)) == event


def test_change_wire_rejects_float_and_forged_id():
    change = detect_observation_changes(None, make_event(), ChangeRuleConfig("v1", Decimal(0), Decimal(0), 1)).changes[0]
    wire = json.loads(canonical_json(change))
    wire["current_value"] = {"price": 1.0}
    with pytest.raises(WireContractError): marketplace_change_from_wire(wire)
    wire = json.loads(canonical_json(change)); wire["event_id"] = "forged"
    with pytest.raises(WireContractError): marketplace_change_from_wire(wire)
