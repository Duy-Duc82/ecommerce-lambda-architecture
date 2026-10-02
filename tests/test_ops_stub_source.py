"""Phase 8 plan section 8: the offline listing stub (test 31)."""
from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request

import pytest

from ops import stub_source as stub


def state(**overrides):
    return stub.load_state(**overrides)


def served(state_, category="1846", page=1):
    status, headers, body = stub.listing_page(state_, category, page)
    return status, headers, json.loads(body)


def test_prices_follow_the_serve_counter_by_a_fixed_table():
    fixture = state().rows
    a, b = state(), state()

    prices_a = [[row["price"] for row in served(a)[2]["data"]] for _ in range(5)]
    prices_b = [[row["price"] for row in served(b)[2]["data"]] for _ in range(5)]

    assert prices_a == prices_b
    base = [row["price"] for row in fixture]
    scaled = lambda percent: [p * percent // 100 if isinstance(p, int) and p > 0 else p for p in base]  # noqa: E731
    assert prices_a[0] == base
    assert prices_a[1] == scaled(95)
    assert prices_a[2] == scaled(60)
    assert prices_a[3] == base
    assert prices_a[4] == prices_a[0]


def test_the_large_step_is_a_large_drop_under_the_configured_rule():
    from decimal import Decimal

    from config.settings import MARKETPLACE_LARGE_DROP_RELATIVE

    # The rule is absolute OR relative; the relative cut alone qualifies.
    for row in state().rows:
        if not isinstance(row.get("price"), int) or row["price"] <= 0:
            continue
        base, cut = row["price"], row["price"] * 60 // 100
        assert Decimal(base - cut) / Decimal(base) >= MARKETPLACE_LARGE_DROP_RELATIVE


def test_pages_and_categories_hold_distinct_stable_listing_ids():
    s = state(last_page=2)
    ids = {
        (category, page): [row["id"] for row in served(s, category, page)[2]["data"]]
        for category in ("1846", "1789") for page in (1, 2)
    }
    flat = [listing for listings in ids.values() for listing in listings]

    valid = sum(isinstance(row.get("id"), int) for row in s.rows)
    flat = [listing for listing in flat if listing is not None]
    assert len(flat) == len(set(flat)) == 4 * valid
    assert ids == {key: [row["id"] for row in served(state(last_page=2), *key)[2]["data"]] for key in ids}


def test_each_page_counts_its_own_serves():
    s = state()
    served(s, "1846", 1)
    served(s, "1846", 1)
    served(s, "1846", 2)

    assert s.snapshot()["served"] == {"1846/1": 2, "1846/2": 1}


def test_a_page_past_the_last_is_empty():
    status, _, body = served(state(last_page=2), page=3)

    assert status == 200
    assert body["data"] == []
    assert body["paging"]["last_page"] == 2


@pytest.mark.parametrize("mode, status", [("429", 429), ("500", 500)])
def test_error_modes_answer_with_their_status_and_count_no_serve(mode, status):
    s = state()
    s.set_mode(mode)

    got, headers, _ = stub.listing_page(s, "1846", 1)

    assert got == status
    if mode == "429":
        assert headers["Retry-After"] == str(stub.RETRY_AFTER_SECONDS)
    assert s.snapshot()["served"] == {}


def _parse(body):
    from datetime import datetime, timezone

    from crawler.contracts import ListingPageRequest
    from tests.test_tiki_contract import _adapter, _artifact, _fetch

    adapter = _adapter()
    fetch = _fetch(adapter, body)
    return adapter.parse_listing_page(
        request=ListingPageRequest("tiki", "marketplace-tiki", "1846", 1), fetch_result=fetch,
        raw_artifact=_artifact(fetch), crawl_run_id="run-1", produced_at=datetime(2026, 10, 2, tzinfo=timezone.utc),
    )


def test_an_ok_page_parses_and_each_serve_yields_new_observations():
    s = state()
    first = _parse(stub.listing_page(s, "1846", 1)[2])
    second = _parse(stub.listing_page(s, "1846", 1)[2])

    # The frozen fixture holds one row the adapter rejects on purpose.
    assert (len(first.observations), len(first.rejections)) == (2, 1)
    ids = lambda parsed: {event.payload.observation.observation_id for event in parsed.observations}  # noqa: E731
    assert ids(first).isdisjoint(ids(second))


def test_drift_is_a_shape_the_tiki_adapter_rejects():
    from crawler.contracts import ListingPageParseError

    s = state()
    s.set_mode("drift")

    with pytest.raises(ListingPageParseError, match="data must be a list"):
        _parse(stub.listing_page(s, "1846", 1)[2])


def test_the_timeout_mode_sleeps_its_configured_time(monkeypatch):
    slept = []
    monkeypatch.setattr(stub.time, "sleep", slept.append)
    s = state(timeout_seconds=12)
    s.set_mode("timeout")

    stub.listing_page(s, "1846", 1)

    assert slept == [12]


def test_an_unknown_mode_is_refused():
    with pytest.raises(ValueError, match="unknown stub mode"):
        state().set_mode("slow")


def test_the_server_answers_robots_listing_and_mode_switches():
    server = stub.serve(state(), host="127.0.0.1", port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        robots = urllib.request.urlopen(f"{base}/robots.txt", timeout=5).read()
        listing = json.loads(urllib.request.urlopen(
            f"{base}/api/personalish/v1/blocks/listings?category=1846&page=1&limit=40", timeout=5).read())
        switched = urllib.request.urlopen(urllib.request.Request(f"{base}/_stub/mode", data=b"500"), timeout=5).read()
        with pytest.raises(urllib.error.HTTPError) as refused:
            urllib.request.urlopen(f"{base}/api/personalish/v1/blocks/listings?category=1846&page=1", timeout=5)
        state_now = json.loads(urllib.request.urlopen(f"{base}/_stub/state", timeout=5).read())
    finally:
        server.shutdown()
        server.server_close()

    assert b"Allow: /" in robots
    assert len(listing["data"]) == 3
    assert json.loads(switched)["mode"] == "500"
    assert refused.value.code == 500
    assert state_now == {"mode": "500", "served": {"1846/1": 1}}
