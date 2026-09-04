import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum

import pytest

from common.serialization import serialize_for_wire


class State(str, Enum):
    READY = "READY"


@dataclass
class NestedValue:
    happened_at: datetime
    amount: Decimal
    state: State
    values: tuple[int, ...]
    missing: None = None


def test_serialize_for_wire_handles_nested_supported_values():
    value = {
        "nested": NestedValue(
            happened_at=datetime(2026, 9, 4, 10, 30, tzinfo=timezone(timedelta(hours=2))),
            amount=Decimal("10.50"),
            state=State.READY,
            values=(1, 2),
        ),
        "items": ("a", None),
    }

    assert serialize_for_wire(value) == {
        "nested": {
            "happened_at": "2026-09-04T08:30:00+00:00",
            "amount": "10.50",
            "state": "READY",
            "values": [1, 2],
            "missing": None,
        },
        "items": ["a", None],
    }


def test_serialize_for_wire_preserves_decimal_scale():
    assert serialize_for_wire(Decimal("10.50")) == "10.50"


def test_serialize_for_wire_converts_aware_datetime_to_utc():
    value = datetime(2026, 9, 4, 10, 30, tzinfo=timezone(timedelta(hours=2)))
    assert serialize_for_wire(value) == "2026-09-04T08:30:00+00:00"


def test_serialize_for_wire_keeps_legacy_naive_datetime_serializable():
    value = datetime(2026, 9, 4, 8, 30)
    assert serialize_for_wire(value) == "2026-09-04T08:30:00"


def test_serialize_for_wire_rejects_unknown_type():
    with pytest.raises(TypeError, match="UnsupportedValue"):
        serialize_for_wire(UnsupportedValue())


def test_serialize_for_wire_rejects_non_string_mapping_key():
    with pytest.raises(TypeError, match="string keys"):
        serialize_for_wire({1: "not allowed"})


def test_serialize_for_wire_output_is_accepted_by_json_dumps():
    assert json.dumps(serialize_for_wire({"amount": Decimal("10.50")})) == '{"amount": "10.50"}'


class UnsupportedValue:
    pass
