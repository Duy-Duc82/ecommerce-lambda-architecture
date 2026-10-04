"""Stub listing IDs at load-generation scale — Phase 9 plan section 7.

The Phase 8 formula ``fixture_id * 10000 + crc32(category) % 100 * 100 + page``
was distinct for the smoke's three categories and two pages only. Two
categories with the same ``crc32 % 100`` -- 9004 and 9012 -- or a page past
99 made different offers share one ID, so a load run would have measured
the dedup path instead of new offers.
"""
from __future__ import annotations

import json

from ops import stub_source as stub


def _ids(state, category, page):
    _, _, body = stub.listing_page(state, category, page)
    return [row["id"] for row in json.loads(body)["data"] if row.get("id") is not None]


def test_two_categories_sharing_a_crc_bucket_get_distinct_ids():
    state = stub.load_state(last_page=1)

    assert set(_ids(state, "9004", 1)).isdisjoint(_ids(state, "9012", 1))


def test_a_page_past_99_does_not_reuse_another_page_s_ids():
    state = stub.load_state(last_page=200)
    first = {listing for page in range(1, 101) for listing in _ids(state, "9001", page)}
    later = {listing for page in range(101, 201) for listing in _ids(state, "9001", page)}

    assert first.isdisjoint(later)


def test_ids_are_unique_across_200_pages_and_50_categories():
    state = stub.load_state(last_page=200)
    ids = [listing for category in range(9001, 9051) for page in range(1, 201)
           for listing in _ids(state, str(category), page)]

    assert len(ids) == len(set(ids))


def test_the_default_page_is_the_fixture_once_with_unchanged_ids():
    """Copy 0 keeps its ID, so a wider stub does not renumber the smoke's offers."""
    state = stub.load_state(last_page=1)
    _, _, body = stub.listing_page(state, "9001", 1)
    rows = json.loads(body)["data"]

    assert len(rows) == len(state.rows)
    assert [row.get("id") for row in rows] == [
        stub.listing_id(r["id"], "9001", 1) if isinstance(r.get("id"), int) else r.get("id") for r in state.rows]


def test_a_wide_page_repeats_the_fixture_with_new_ids_and_keeps_its_invalid_rows():
    state = stub.load_state(last_page=1, rows_per_page=40)
    _, _, body = stub.listing_page(state, "9001", 1)
    payload = json.loads(body)
    rows = payload["data"]
    valid = [row["id"] for row in rows if isinstance(row.get("id"), int)]
    invalid_in_fixture = sum(not isinstance(r.get("id"), int) for r in state.rows)

    assert len(rows) == 40 and payload["paging"]["per_page"] == 40
    assert len(valid) == len(set(valid))
    assert len(rows) - len(valid) == sum(1 for i in range(40) if not isinstance(state.rows[i % len(state.rows)].get("id"), int))
    assert invalid_in_fixture >= 1


def test_rows_per_page_must_be_positive():
    import pytest

    with pytest.raises(ValueError):
        stub.load_state(rows_per_page=0)
