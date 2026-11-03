"""The shared value checks of ecomdata.domain.validation."""
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum

import pytest

from ecomdata.domain import validation as v


def test_require_text_strips_and_rejects_blank_or_non_text():
    assert v.require_text("  a ", "f") == "a"
    with pytest.raises(ValueError, match="f is required"):
        v.require_text("   ", "f")
    with pytest.raises(ValueError, match="f must be text"):
        v.require_text(3, "f")
    with pytest.raises(TypeError, match="f must be text"):
        v.require_text(3, "f", error=TypeError)


def test_optional_text_turns_blank_into_none():
    assert v.optional_text(None, "f") is None
    assert v.optional_text("  ", "f") is None
    assert v.optional_text(" x ", "f") == "x"


def test_aware_utc_converts_and_rejects_naive():
    local = datetime(2026, 10, 24, 7, tzinfo=timezone(timedelta(hours=7)))
    assert v.aware_utc(local, "t") == datetime(2026, 10, 24, tzinfo=UTC)
    with pytest.raises(ValueError, match="timezone-aware"):
        v.aware_utc(datetime(2026, 1, 1), "t")
    with pytest.raises(TypeError, match="t must be a datetime"):
        v.aware_utc("2026", "t", error=TypeError)


@pytest.mark.parametrize("value", [True, 1.0, "1"])
def test_non_bool_int_refuses_bool_float_and_text(value):
    with pytest.raises(ValueError, match="bool"):
        v.non_bool_int(value, "n")


def test_non_bool_int_bounds():
    assert v.non_bool_int(0, "n") == 0
    with pytest.raises(ValueError, match="n must be non-negative"):
        v.non_bool_int(-1, "n")
    with pytest.raises(ValueError, match="n must be at least 1"):
        v.non_bool_int(0, "n", minimum=1)
    assert v.non_bool_int(-5, "n", minimum=None) == -5
    assert v.optional_non_negative_int(None, "n") is None


def test_decimal_sha_url_currency_and_raw_uri():
    assert v.non_negative_decimal(Decimal("1.5"), "d") == Decimal("1.5")
    for bad in (Decimal("-1"), Decimal("NaN"), 1.5):
        with pytest.raises(ValueError):
            v.non_negative_decimal(bad, "d")
    assert v.sha256_hex("A" * 64, "s") == "a" * 64
    with pytest.raises(ValueError, match="64 hexadecimal"):
        v.sha256_hex("abc", "s")
    assert v.http_url("https://tiki.vn/x", "u") == "https://tiki.vn/x"
    with pytest.raises(ValueError, match="HTTP"):
        v.http_url("ftp://x", "u")
    assert v.currency_code("vnd") == "VND"
    with pytest.raises(ValueError):
        v.currency_code("VN")
    assert v.raw_uri("s3a://bucket/key") == "s3a://bucket/key"
    for bad in ("http://x/y", "s3a:///key"):
        with pytest.raises(ValueError):
            v.raw_uri(bad)


def test_enum_status_exception_and_truncation():
    class Colour(str, Enum):
        RED = "RED"

    assert v.enum_member(Colour.RED, Colour, "c") is Colour.RED
    with pytest.raises(ValueError, match="c must be a Colour"):
        v.enum_member("RED", Colour, "c")
    assert v.http_status(200) == 200
    with pytest.raises(ValueError):
        v.http_status(600)
    with pytest.raises(ValueError, match="must be an Exception"):
        v.exception("boom")
    assert len(v.truncated("x" * 5000)) == v.ERROR_MESSAGE_LIMIT
