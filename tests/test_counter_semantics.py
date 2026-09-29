"""Counter registry tests, items 24, 26 and 27 of the Phase 6 plan section 15.

The delta arithmetic itself lives in the Spark marts and is covered in
tests/test_marketplace_marts.py.
"""
import pytest

from config.counter_semantics import (
    CounterSemantic,
    counter_semantic,
    counter_semantics_for,
)

REGISTERED_FIELDS = {"rating_count", "review_count", "sold_count"}


# 24
def test_registry_is_explicit_and_unknown_is_untrusted():
    semantics = counter_semantics_for("tiki")

    assert {item.field_name for item in semantics} == REGISTERED_FIELDS
    assert counter_semantic("unknown", "sold_count") is None
    assert len({(item.marketplace, item.field_name) for item in semantics}) == 3


def test_registry_has_no_duplicate_entries_for_any_marketplace():
    for marketplace in ("tiki", "yame", "emwear", "allbirds"):
        semantics = counter_semantics_for(marketplace)
        keys = [(item.marketplace, item.field_name) for item in semantics]
        assert len(keys) == len(set(keys)), marketplace


@pytest.mark.parametrize("field", ["sold", "views", "likes", "", "sold_count "])
def test_registry_rejects_unknown_counter_fields(field):
    assert counter_semantic("tiki", field) is None


def test_registry_normalizes_the_marketplace_code():
    assert counter_semantics_for(" TIKI ") == counter_semantics_for("tiki")
    assert counter_semantic(" TiKi ", "sold_count") is not None


# 26
@pytest.mark.parametrize("marketplace", ["unknown", "shopee", "", "   "])
def test_unregistered_semantics_cannot_enter_a_valid_delta(marketplace):
    assert counter_semantics_for(marketplace) == ()
    for field in REGISTERED_FIELDS:
        assert counter_semantic(marketplace, field) is None


# 27
def test_every_entry_carries_the_version_a_delta_must_be_pinned_to():
    for item in counter_semantics_for("tiki"):
        assert item.semantic_version
        assert item.enabled is True
        assert item.monotonic_expected is True


def test_a_semantic_version_change_makes_a_different_entry():
    current = counter_semantic("tiki", "sold_count")
    bumped = CounterSemantic(
        current.marketplace,
        current.field_name,
        current.enabled,
        current.monotonic_expected,
        "v2",
    )

    assert bumped != current
    assert bumped.semantic_version != current.semantic_version


def test_counter_semantic_is_frozen():
    item = counter_semantic("tiki", "sold_count")

    with pytest.raises(Exception):
        item.enabled = False
