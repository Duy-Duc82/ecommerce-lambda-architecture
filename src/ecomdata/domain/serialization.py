"""Convert supported Python values into JSON-compatible wire values."""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping


def serialize_for_wire(value: Any) -> Any:
    """Return a JSON-compatible representation of value.

    Raise TypeError for unsupported values or mappings with non-string keys.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, datetime):
        if value.tzinfo is not None and value.utcoffset() is not None:
            value = value.astimezone(timezone.utc)
        return value.isoformat()

    if isinstance(value, Decimal):
        return format(value, "f")

    if isinstance(value, Enum):
        return serialize_for_wire(value.value)

    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: serialize_for_wire(getattr(value, field.name))
            for field in fields(value)
        }

    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("wire mappings must have string keys")
        return {
            key: serialize_for_wire(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [serialize_for_wire(item) for item in value]

    raise TypeError(f"unsupported type for wire serialization: {type(value).__name__}")
