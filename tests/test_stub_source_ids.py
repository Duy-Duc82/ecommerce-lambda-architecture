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
