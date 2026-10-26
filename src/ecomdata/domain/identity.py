"""Deterministic identities for marketplace records and events."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from ecomdata.domain import validation as v

_SHA256_PATTERN = re.compile(r"[0-9a-fA-F]{64}\Z")
_PREFIX_PATTERN = re.compile(r"[a-z0-9_]+\Z")


def _normalize_identity_part(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, Enum):
        return _normalize_identity_part(value.value)

    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("identity datetime must be timezone-aware")
        return value.astimezone(UTC).isoformat(timespec="microseconds")

    if isinstance(value, Decimal):
        if value == 0:
            return "0"
        return format(value.normalize(), "f")

    if isinstance(value, str):
        return value.strip()

    if isinstance(value, (int, bool)):
        return str(value)

    raise TypeError(f"unsupported identity part type: {type(value).__name__}")


def _require_identity_value(value: Any, field_name: str) -> Any:
    normalized = _normalize_identity_part(value)
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return value


def deterministic_id(prefix: str, *parts: Any) -> str:
    """Return a stable, prefixed SHA-256 identity for normalized parts."""
    if not isinstance(prefix, str):
        raise TypeError("prefix must be a string")
    normalized_prefix = prefix.strip().lower()
    if not normalized_prefix or not _PREFIX_PATTERN.fullmatch(normalized_prefix):
        raise ValueError("prefix must contain only letters, digits, and underscores")
    if not parts:
        raise ValueError("at least one identity part is required")

    normalized = [normalized_prefix, *(_normalize_identity_part(part) for part in parts)]
    payload = "\x1f".join(normalized).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return f"{normalized_prefix}_{digest}"


def make_raw_artifact_id(
    marketplace_code: str,
    request_url: str,
    fetched_at: datetime,
    body_sha256: str,
) -> str:
    """Build the identity of one fetched raw response."""
    return deterministic_id(
        "raw",
        v.require_text(marketplace_code, "marketplace_code", error=TypeError),
        v.require_text(request_url, "request_url", error=TypeError),
        fetched_at,
        v.sha256_hex(body_sha256, "body_sha256", error=TypeError),
    )


def make_offer_id(
    marketplace_code: str,
    platform_listing_id: str,
) -> str:
    """Build the stable identity of a marketplace listing."""
    return deterministic_id(
        "offer",
        v.require_text(marketplace_code, "marketplace_code", error=TypeError),
        v.require_text(platform_listing_id, "platform_listing_id", error=TypeError),
    )


def make_seller_id(
    marketplace_code: str,
    platform_seller_id: str,
) -> str:
    """Build the stable identity of a marketplace seller."""
    return deterministic_id(
        "seller",
        v.require_text(marketplace_code, "marketplace_code", error=TypeError),
        v.require_text(platform_seller_id, "platform_seller_id", error=TypeError),
    )


def make_observation_id(
    marketplace_code: str,
    platform_listing_id: str,
    observed_at: datetime,
    raw_sha256: str,
) -> str:
    """Build the identity of one observation of a listing."""
    return deterministic_id(
        "obs",
        v.require_text(marketplace_code, "marketplace_code", error=TypeError),
        v.require_text(platform_listing_id, "platform_listing_id", error=TypeError),
        observed_at,
        v.sha256_hex(raw_sha256, "raw_sha256", error=TypeError),
    )


def make_change_id(
    offer_id: str,
    current_observation_id: str,
    change_type: str,
    rule_version: str,
) -> str:
    """Build the identity of one derived marketplace change."""
    return deterministic_id(
        "change",
        v.require_text(offer_id, "offer_id", error=TypeError),
        v.require_text(current_observation_id, "current_observation_id", error=TypeError),
        _require_identity_value(change_type, "change_type"),
        v.require_text(rule_version, "rule_version", error=TypeError),
    )
