"""Canonical behavioral-event schema shared by ingestion, speed and batch code.

The whole platform speaks ONE event contract, derived from the Kaggle
Multi-Category Store dataset. There are no orders, payments, reviews or
geography here because the source does not contain those facts.

Row-level helpers (`normalize_event`, `validate_event`) are used by the Kafka
producer and the speed layer. The Spark batch job (`warehouse_job.py`)
implements the same contract column-wise for scale.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from common.serialization import serialize_for_wire

# ---- Canonical contract ----------------------------------------------------

EVENT_TYPES = ("view", "cart", "purchase")

# Raw event types accepted from sources, mapped to the canonical three.
_EVENT_TYPE_ALIASES = {
    "view": "view",
    "page_view": "view",
    "cart": "cart",
    "add_to_cart": "cart",
    "purchase": "purchase",
}

CANONICAL_FIELDS = [
    "event_time",      # ISO-8601 string on the wire, datetime in memory
    "event_type",      # view | cart | purchase
    "user_id",
    "user_session",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
]


def _parse_event_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if value is None or str(value).strip() == "":
        raise ValueError("event_time is required")
    text = str(value).strip().replace(" UTC", "").replace("Z", "+00:00")
    return datetime.fromisoformat(text)


def normalize_event(raw: dict[str, Any]) -> dict[str, Any]:
    """Return one canonical behavioral event from a raw source/Kafka record.

    Accepts both Kaggle CSV column names and already-canonical keys.
    """
    raw_type = str(raw.get("event_type") or "").strip().lower()
    event_type = _EVENT_TYPE_ALIASES.get(raw_type, raw_type)

    event_time = _parse_event_time(
        raw.get("event_time") or raw.get("timestamp") or raw.get("order_date")
    )

    price_value = raw.get("price")
    price = float(price_value) if price_value not in (None, "") else None

    event = {
        "event_time": event_time,
        "event_type": event_type,
        "user_id": str(raw.get("user_id") or raw.get("customer_id") or "").strip(),
        "user_session": str(raw.get("user_session") or raw.get("session_id") or "").strip(),
        "product_id": str(raw.get("product_id") or "").strip(),
        "category_id": str(raw.get("category_id") or "").strip(),
        "category_code": str(raw.get("category_code") or raw.get("category") or "unknown").strip(),
        "brand": str(raw.get("brand") or "").strip(),
        "price": price,
    }
    validate_event(event)
    return event


def validate_event(event: dict[str, Any]) -> bool:
    """Raise ValueError if the event violates the canonical contract."""
    if event.get("event_type") not in EVENT_TYPES:
        raise ValueError(f"unsupported event_type: {event.get('event_type')!r}")
    if not isinstance(event.get("event_time"), datetime):
        raise ValueError("event_time must be a datetime")
    for field in ("user_id", "product_id"):
        if not event.get(field):
            raise ValueError(f"missing required field: {field}")
    price = event.get("price")
    if price is not None and float(price) < 0:
        raise ValueError("price must be non-negative")
    return True


def to_wire(event: dict[str, Any]) -> dict[str, Any]:
    """Serialize a supported event mapping for JSON transport."""
    wire = serialize_for_wire(event)
    if not isinstance(wire, dict):
        raise TypeError("event must serialize to a dictionary")
    return wire


# ---- Price-snapshot contract (crawler layer) --------------------------------
#
# A crawler cannot observe real user behavior (view/cart/purchase) — that data
# only exists inside the platform being crawled. What a crawler *can* see is
# the public catalog/price state at a point in time, so this is a deliberately
# separate contract, not a bolt-on to CANONICAL_FIELDS/EVENT_TYPES above.

PRICE_SNAPSHOT_FIELDS = [
    "snapshot_time",
    "site",
    "product_id",       # site-scoped id; not comparable across sites without a
                         # separate product-matching step (out of scope for now)
    "product_name",
    "category_path",
    "brand",
    "price",
    "list_price",
    "currency",
    "rating",
    "review_count",
    "seller_name",
    "in_stock",
    "url",
]


def normalize_price_snapshot(raw: dict[str, Any]) -> dict[str, Any]:
    """Return one canonical price snapshot from a site-adapter's parsed output."""
    snapshot_time = _parse_event_time(raw.get("snapshot_time") or datetime.now(timezone.utc))

    def _num(value: Any) -> float | None:
        return float(value) if value not in (None, "") else None

    snapshot = {
        "snapshot_time": snapshot_time,
        "site": str(raw.get("site") or "").strip().lower(),
        "product_id": str(raw.get("product_id") or "").strip(),
        "product_name": str(raw.get("product_name") or "").strip(),
        "category_path": str(raw.get("category_path") or "").strip(),
        "brand": str(raw.get("brand") or "").strip(),
        "price": _num(raw.get("price")),
        "list_price": _num(raw.get("list_price")),
        "currency": str(raw.get("currency") or "VND").strip(),
        "rating": _num(raw.get("rating")),
        "review_count": int(raw["review_count"]) if raw.get("review_count") not in (None, "") else None,
        "seller_name": str(raw.get("seller_name") or "").strip(),
        "in_stock": bool(raw.get("in_stock", True)),
        "url": str(raw.get("url") or "").strip(),
    }
    validate_price_snapshot(snapshot)
    return snapshot


def validate_price_snapshot(snapshot: dict[str, Any]) -> bool:
    """Raise ValueError if the price snapshot violates the contract."""
    if not isinstance(snapshot.get("snapshot_time"), datetime):
        raise ValueError("snapshot_time must be a datetime")
    for field in ("site", "product_id"):
        if not snapshot.get(field):
            raise ValueError(f"missing required field: {field}")
    price = snapshot.get("price")
    if price is None:
        raise ValueError("price is required")
    if price < 0:
        raise ValueError("price must be non-negative")
    list_price = snapshot.get("list_price")
    if list_price is not None and list_price < 0:
        raise ValueError("list_price must be non-negative")
    return True
