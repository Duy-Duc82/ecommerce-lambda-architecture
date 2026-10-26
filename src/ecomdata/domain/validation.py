"""The one set of value checks every domain contract uses.

Each helper validates one value and returns it normalised (stripped text,
UTC datetime, lower-case checksum). Messages always start with the field
name, so a caller can match on it.

Before this module the same checks were written out about nine times, in the
contracts, the scheduling rules, the audit rows, the identities and both Tiki
adapters, each with a slightly different message and exception type.
"""
from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from urllib.parse import urlparse

SHA256_PATTERN = re.compile(r"[0-9a-fA-F]{64}\Z")
CURRENCY_PATTERN = re.compile(r"[A-Za-z]{3}\Z")
RAW_URI_SCHEMES = frozenset({"s3a", "s3", "file"})
ERROR_MESSAGE_LIMIT = 2000


def require_text(value: Any, field_name: str, *, error: type[Exception] = ValueError) -> str:
    """Non-blank text, stripped. ``error`` is the type raised for a non-string."""
    if not isinstance(value, str):
        raise error(f"{field_name} must be text")
    value = value.strip()
    if not value:
        raise ValueError(f"{field_name} is required")
    return value


def optional_text(value: Any, field_name: str) -> str | None:
    """Text stripped to ``None`` when blank; ``None`` stays ``None``."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be text")
    return value.strip() or None


def aware_utc(value: Any, field_name: str, *, error: type[Exception] = ValueError) -> datetime:
    """A timezone-aware datetime, converted to UTC."""
    if not isinstance(value, datetime):
        raise error(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def non_bool_int(value: Any, field_name: str, *, minimum: int | None = 0) -> int:
    """An int that is not a bool, at least ``minimum`` (``None``: no bound)."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer, not bool")
    if minimum is not None and value < minimum:
        raise ValueError(f"{field_name} must be non-negative" if minimum == 0 else f"{field_name} must be at least {minimum}")
    return value


def optional_non_negative_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    return non_bool_int(value, field_name, minimum=0)


def non_negative_decimal(value: Any, field_name: str) -> Decimal | None:
    """A finite Decimal at least zero; ``None`` passes through."""
    if value is None:
        return None
    if not isinstance(value, Decimal):
        raise ValueError(f"{field_name} must be a Decimal")
    if not value.is_finite() or value < 0:
        raise ValueError(f"{field_name} must be non-negative and finite")
    return value


def sha256_hex(value: Any, field_name: str, *, error: type[Exception] = ValueError) -> str:
    value = require_text(value, field_name, error=error)
    if not SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be exactly 64 hexadecimal characters")
    return value.lower()


def http_url(value: Any, field_name: str) -> str:
    value = require_text(value, field_name)
    try:
        parsed = urlparse(value)
        hostname = parsed.hostname
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid HTTP(S) URL") from exc
    if parsed.scheme.lower() not in {"http", "https"} or not hostname:
        raise ValueError(f"{field_name} must be a valid HTTP(S) URL")
    return value


def currency_code(value: Any, field_name: str = "currency") -> str:
    value = require_text(value, field_name)
    if not CURRENCY_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be exactly three ASCII letters")
    return value.upper()


def raw_uri(value: Any, field_name: str = "raw_uri") -> str:
    """A storage URI: ``s3a``/``s3`` with a bucket, or ``file`` with a path."""
    value = require_text(value, field_name)
    parsed = urlparse(value)
    scheme = parsed.scheme.lower()
    if scheme not in RAW_URI_SCHEMES:
        raise ValueError(f"{field_name} must use s3a, s3, or file scheme")
    if scheme in {"s3", "s3a"} and not parsed.hostname:
        raise ValueError(f"{field_name} must include a bucket hostname")
    if scheme == "file" and not parsed.path:
        raise ValueError(f"{field_name} must include a file path")
    return value


def enum_member[E: Enum](value: Any, enum_type: type[E], field_name: str) -> E:
    if not isinstance(value, enum_type):
        raise ValueError(f"{field_name} must be a {enum_type.__name__}")
    return value


def http_status(value: Any, field_name: str = "http_status") -> int:
    status = non_bool_int(value, field_name, minimum=100)
    if status > 599:
        raise ValueError(f"{field_name} must be between 100 and 599")
    return status


def exception(value: Any, field_name: str = "error") -> Exception:
    if not isinstance(value, Exception):
        raise ValueError(f"{field_name} must be an Exception")
    return value


def truncated(message: Any, limit: int = ERROR_MESSAGE_LIMIT) -> str:
    return str(message)[:limit]
