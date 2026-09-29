from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from common.identity import (
    deterministic_id,
    make_change_id,
    make_observation_id,
    make_offer_id,
    make_raw_artifact_id,
    make_seller_id,
)


MARKETPLACE = "tiki"
LISTING = "p1"
SELLER = "seller-1"
FETCHED_AT = datetime(2026, 9, 4, 8, 30, tzinfo=timezone.utc)
RAW_SHA256 = "a" * 64


def test_same_inputs_produce_the_same_id():
    assert deterministic_id("test", "a", 1) == deterministic_id("test", "a", 1)


def test_any_changed_input_produces_a_different_id():
    first = deterministic_id("test", "a", 1)
    assert first != deterministic_id("test", "b", 1)
    assert first != deterministic_id("test", "a", 2)


def test_equivalent_timestamps_with_different_utc_offsets_match():
    utc = datetime(2026, 9, 4, 8, 30, tzinfo=timezone.utc)
    offset = datetime(2026, 9, 4, 10, 30, tzinfo=timezone(timedelta(hours=2)))
    assert deterministic_id("time", utc) == deterministic_id("time", offset)


def test_naive_datetime_raises_value_error():
    with pytest.raises(ValueError, match="timezone-aware"):
        deterministic_id("time", datetime(2026, 9, 4, 8, 30))


def test_invalid_prefix_raises_value_error():
    with pytest.raises(ValueError):
        deterministic_id("bad-prefix", "value")
    with pytest.raises(ValueError):
        deterministic_id("", "value")


def test_generic_helper_requires_at_least_one_part():
    with pytest.raises(ValueError, match="at least one"):
        deterministic_id("test")


@pytest.mark.parametrize(
    ("factory", "args"),
    [
        (make_raw_artifact_id, ("", "https://example.test", FETCHED_AT, RAW_SHA256)),
        (make_offer_id, (MARKETPLACE, "")),
        (make_seller_id, (MARKETPLACE, "")),
        (make_observation_id, (MARKETPLACE, "", FETCHED_AT, RAW_SHA256)),
        (make_change_id, ("offer-1", "obs-1", "PRICE_CHANGED", "rule-v1")),
    ],
)
def test_convenience_helpers_reject_missing_required_fields(factory, args):
    if factory is make_change_id:
        args = ("", args[1], args[2], args[3])
    with pytest.raises(ValueError):
        factory(*args)


def test_invalid_sha256_text_raises_value_error():
    with pytest.raises(ValueError):
        make_raw_artifact_id(MARKETPLACE, "https://example.test", FETCHED_AT, "not-a-sha")
    with pytest.raises(ValueError):
        make_observation_id(MARKETPLACE, LISTING, FETCHED_AT, "g" * 64)


@pytest.mark.parametrize(
    ("factory", "args", "prefix"),
    [
        (make_raw_artifact_id, (MARKETPLACE, "https://example.test", FETCHED_AT, RAW_SHA256), "raw_"),
        (make_seller_id, (MARKETPLACE, SELLER), "seller_"),
        (make_offer_id, (MARKETPLACE, LISTING), "offer_"),
        (make_observation_id, (MARKETPLACE, LISTING, FETCHED_AT, RAW_SHA256), "obs_"),
        (make_change_id, ("offer-1", "obs-1", "PRICE_CHANGED", "rule-v1"), "change_"),
    ],
)
def test_convenience_ids_have_full_lowercase_sha256_suffix(factory, args, prefix):
    result = factory(*args)
    suffix = result.removeprefix(prefix)
    assert result.startswith(prefix)
    assert len(suffix) == 64
    assert suffix == suffix.lower()
    assert all(character in "0123456789abcdef" for character in suffix)


def test_decimal_scale_normalizes_identically_in_generic_helper():
    assert deterministic_id("amount", Decimal("10.0")) == deterministic_id("amount", Decimal("10.00"))


def test_offer_id_does_not_depend_on_changing_offer_attributes():
    assert make_offer_id(MARKETPLACE, LISTING) == make_offer_id(MARKETPLACE, LISTING)
