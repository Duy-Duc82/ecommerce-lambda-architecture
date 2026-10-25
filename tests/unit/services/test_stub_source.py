"""The offline category-page stub of the smoke, the drills, the bench and the demo."""
from __future__ import annotations

import json
import re
import threading
import urllib.error
import urllib.request
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from ecomdata.domain.acquisition import FetchBlockedError, FetchBundle, FetchPart, ListingPageParseError
from ecomdata.ingestion.sources.tiki.extractor import TikiExtractor
from services.stub_source import server as stub

CATEGORY = "stub-category/c9001"


def state(**overrides):
    return stub.load_state(**overrides)


def page_data(body):
    match = re.search(rb'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', body, re.S)
    return json.loads(match.group(1))["props"]["initialState"]["catalog"]


def served(state_, category=CATEGORY):
    status, headers, body = stub.listing_page(state_, category)
    return status, headers, body


def rows_of(state_, category=CATEGORY):
    return page_data(served(state_, category)[2])["data"]


def extract(body):
    bundle = FetchBundle(
        "https://stub/x/c1", datetime(2026, 10, 25, tzinfo=UTC), (FetchPart("https://stub/x/c1", 200, "text/html", body),)
    )
    return TikiExtractor().extract(bundle)


def test_the_stub_reads_its_own_fixture_not_the_tests():
    assert stub.FIXTURE.name == "fixture.json" and stub.FIXTURE.parent.name == "stub_source"
    assert "tests" not in stub.FIXTURE.parts


def test_prices_follow_the_serve_counter_by_a_fixed_table():
    fixture = state().rows
    a, b = state(), state()

    prices_a = [[row.get("price") for row in rows_of(a)] for _ in range(5)]
    prices_b = [[row.get("price") for row in rows_of(b)] for _ in range(5)]

    assert prices_a == prices_b
    base = [row.get("price") for row in fixture]

    def scaled(percent):
        return [p * percent // 100 if isinstance(p, int) and p > 0 else p for p in base]

    assert prices_a[0] == base
    assert prices_a[1] == scaled(95)
    assert prices_a[2] == scaled(60)
    assert prices_a[3] == base
    assert prices_a[4] == prices_a[0]


def test_the_large_step_is_a_large_drop_under_the_configured_rule():
    from ecomdata.settings import load_settings

    relative_cut = load_settings({}).speed.large_drop_relative

    # The rule is absolute OR relative; the relative cut alone qualifies.
    for row in state().rows:
        if not isinstance(row.get("price"), int) or row["price"] <= 0:
            continue
        base, cut = row["price"], row["price"] * stub.PRICE_STEPS[2] // 100
        assert Decimal(base - cut) / Decimal(base) >= relative_cut


def test_the_bench_load_uses_the_stubs_price_table_and_fixture():
    from ecomdata.ops import bench_load

    assert bench_load.PRICE_STEPS == stub.PRICE_STEPS
    assert bench_load.FIXTURE == stub.FIXTURE
    assert bench_load._titles()


def test_a_page_is_a_tiki_category_page_the_extractor_reads_whole():
    s = state()

    page = extract(served(s)[2])

    assert len(page.rows) == len(s.rows) and page.last_page == 1
    assert page.page_context["categoryData"]["id"] == s.page_context["categoryData"]["id"]
    assert sum(1 for row in page.rows if row.get("advertisement")) == 1
    # One row is invalid on purpose: ELT keeps it, Silver quarantines it.
    assert sum(1 for row in page.rows if "id" not in row) == 1


def test_categories_hold_distinct_stable_listing_ids():
    s = state()
    ids = {c: [row["id"] for row in rows_of(s, c) if "id" in row] for c in ("a/c1", "b/c2", "c/c3")}
    flat = [listing for listings in ids.values() for listing in listings]

    assert len(flat) == len(set(flat))
    assert ids == {c: [row["id"] for row in rows_of(state(), c) if "id" in row] for c in ids}


def test_each_category_counts_its_own_serves():
    s = state()
    served(s, "a/c1")
    served(s, "a/c1")
    served(s, "b/c2")

    assert s.snapshot()["served"] == {"a/c1/1": 2, "b/c2/1": 1}


@pytest.mark.parametrize("mode, status", [("429", 429), ("500", 500)])
def test_error_modes_answer_with_their_status_and_count_no_serve(mode, status):
    s = state()
    s.set_mode(mode)

    got, headers, _ = served(s)

    assert got == status
    if mode == "429":
        assert headers["Retry-After"] == str(stub.RETRY_AFTER_SECONDS)
    assert s.snapshot()["served"] == {}


def test_block_mode_is_http_200_without_next_data_which_the_extractor_calls_a_block():
    s = state()
    s.set_mode("block")

    status, _, body = served(s)

    assert status == 200 and b"__NEXT_DATA__" not in body
    with pytest.raises(FetchBlockedError):
        extract(body)
    assert s.snapshot()["served"] == {}


def test_drift_is_a_shape_the_extractor_rejects():
    s = state()
    s.set_mode("drift")

    with pytest.raises(ListingPageParseError, match="must be a list"):
        extract(served(s)[2])


def test_the_timeout_mode_sleeps_its_configured_time(monkeypatch):
    slept = []
    monkeypatch.setattr(stub.time, "sleep", slept.append)
    s = state(timeout_seconds=12)
    s.set_mode("timeout")

    served(s)

    assert slept == [12]


def test_an_unknown_mode_is_refused():
    with pytest.raises(ValueError, match="unknown stub mode"):
        state().set_mode("slow")


def test_the_server_answers_robots_pages_and_mode_switches():
    server = stub.serve(state(), host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        robots = urllib.request.urlopen(f"{base}/robots.txt", timeout=5).read()
        page = urllib.request.urlopen(f"{base}/{CATEGORY}", timeout=5).read()
        switched = urllib.request.urlopen(urllib.request.Request(f"{base}/_stub/mode", data=b"500"), timeout=5).read()
        with pytest.raises(urllib.error.HTTPError) as refused:
            urllib.request.urlopen(f"{base}/{CATEGORY}", timeout=5)
        state_now = json.loads(urllib.request.urlopen(f"{base}/_stub/state", timeout=5).read())
    finally:
        server.shutdown()
        server.server_close()

    assert b"Allow: /" in robots
    assert len(page_data(page)["data"]) == 6
    assert json.loads(switched)["mode"] == "500"
    assert refused.value.code == 500
    assert state_now == {"mode": "500", "served": {f"{CATEGORY}/1": 1}}


# --- listing ids at load-generation scale ------------------------------------------
def ids(state_, category):
    return [row["id"] for row in rows_of(state_, category) if row.get("id") is not None]


def test_two_categories_sharing_a_crc_bucket_get_distinct_ids():
    s = state()

    assert set(ids(s, "x/c9004")).isdisjoint(ids(s, "x/c9012"))


def test_ids_are_unique_across_50_categories():
    s = state()
    every = [listing for category in range(9001, 9051) for listing in ids(s, f"stub/c{category}")]

    assert len(every) == len(set(every))


def test_the_default_page_is_the_fixture_once_with_stable_ids():
    """Copy 0 keeps its ID, so a wider stub does not renumber the smoke's offers."""
    s = state()
    rows = rows_of(s, "stub/c9001")

    assert len(rows) == len(s.rows)
    assert [row.get("id") for row in rows] == [
        stub.listing_id(r["id"], "stub/c9001") if isinstance(r.get("id"), int) else r.get("id") for r in s.rows
    ]


def test_a_wide_page_repeats_the_fixture_with_new_ids_and_keeps_its_invalid_rows():
    s = state(rows_per_page=40)
    rows = rows_of(s, "stub/c9001")
    valid = [row["id"] for row in rows if isinstance(row.get("id"), int)]

    assert len(rows) == 40
    assert len(valid) == len(set(valid))
    assert len(rows) - len(valid) == sum(
        1 for i in range(40) if not isinstance(s.rows[i % len(s.rows)].get("id"), int)
    )


def test_rows_per_page_must_be_positive():
    with pytest.raises(ValueError):
        stub.load_state(rows_per_page=0)
