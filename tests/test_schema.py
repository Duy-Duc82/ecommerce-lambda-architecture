from datetime import datetime

import pytest

from config.schema import CANONICAL_FIELDS, normalize_event, to_wire, validate_event


def test_normalize_event_maps_kaggle_row_to_canonical():
    event = normalize_event({
        "event_time": "2019-10-01 00:00:00 UTC",
        "event_type": "purchase",
        "product_id": "1001",
        "category_id": "10",
        "category_code": "electronics.smartphone",
        "brand": "samsung",
        "price": "500.00",
        "user_id": "501",
        "user_session": "session-1",
        "payment_method": "card",  # non-source field must be dropped
    })
    assert set(event.keys()) == set(CANONICAL_FIELDS)
    assert event["event_type"] == "purchase"
    assert event["user_session"] == "session-1"
    assert event["price"] == 500.0
    assert "payment_method" not in event
    assert "order_id" not in event


def test_event_type_aliases_normalize():
    assert normalize_event({"event_type": "page_view", "user_id": "u", "product_id": "p",
                            "event_time": "2019-10-01 00:00:00"})["event_type"] == "view"
    assert normalize_event({"event_type": "add_to_cart", "user_id": "u", "product_id": "p",
                            "event_time": "2019-10-01 00:00:00"})["event_type"] == "cart"


def test_validate_rejects_unsupported_and_missing():
    with pytest.raises(ValueError):
        validate_event({"event_type": "search", "event_time": datetime.now(),
                        "user_id": "u", "product_id": "p"})
    with pytest.raises(ValueError):
        normalize_event({"event_type": "view", "product_id": "p", "event_time": "2019-10-01 00:00:00"})


def test_to_wire_serializes_event_time():
    event = normalize_event({"event_type": "view", "user_id": "u", "product_id": "p",
                             "event_time": "2019-10-01 08:30:00"})
    wire = to_wire(event)
    assert isinstance(wire["event_time"], str)
    assert wire["event_time"].startswith("2019-10-01T08:30:00")
