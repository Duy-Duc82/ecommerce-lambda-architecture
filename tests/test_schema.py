import json
from datetime import datetime

import pytest

from config.schema import (
    CANONICAL_FIELDS,
    PRICE_SNAPSHOT_FIELDS,
    normalize_event,
    normalize_price_snapshot,
    to_wire,
    validate_event,
    validate_price_snapshot,
)


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


def test_to_wire_serializes_price_snapshot_time_to_json():
    snapshot = normalize_price_snapshot({
        "site": "tiki",
        "product_id": "p1",
        "price": 100,
    })
    payload = json.dumps(to_wire(snapshot))
    assert "snapshot_time" in payload


def test_normalize_price_snapshot_maps_crawler_output_to_canonical():
    snapshot = normalize_price_snapshot({
        "site": "Tiki",
        "product_id": "279212151",
        "product_name": "MacBook Neo A18 Pro",
        "category_path": "1846",
        "brand": "Apple",
        "price": 16990000,
        "list_price": 18990000,
        "rating": 5,
        "review_count": 4,
        "seller_name": "seller:1",
        "in_stock": True,
        "url": "https://tiki.vn/macbook-neo-a18-pro-p279212151.html",
    })
    assert set(snapshot.keys()) == set(PRICE_SNAPSHOT_FIELDS)
    assert snapshot["site"] == "tiki"
    assert snapshot["price"] == 16990000.0
    assert snapshot["currency"] == "VND"


def test_validate_price_snapshot_rejects_missing_id_and_negative_price():
    with pytest.raises(ValueError):
        validate_price_snapshot({
            "snapshot_time": datetime.now(), "site": "tiki", "product_id": "",
            "price": 10.0, "list_price": None,
        })
    with pytest.raises(ValueError):
        normalize_price_snapshot({"site": "tiki", "product_id": "p1", "price": -5})
