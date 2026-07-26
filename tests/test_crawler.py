import json
from pathlib import Path

import pytest

from common.dlq import publish_to_dlq
from crawler.base import SiteCrawler
from crawler.sites.tiki import TikiCrawler

FIXTURES = Path(__file__).parent / "fixtures"


class _FakeCrawler(SiteCrawler):
    """Minimal SiteCrawler for exercising the Template Method in isolation."""

    site_name = "fake"
    base_url = "https://example.invalid"

    def __init__(self, listings, allow_robots=True):
        self._listings = listings
        self._allow_robots_override = allow_robots
        super().__init__(categories=["cat-1"], request_delay_seconds=0, jitter_seconds=0)

    def _load_robots(self):
        return None  # never touched: _allowed() is overridden below

    def _allowed(self):
        return self._allow_robots_override

    def fetch_listing(self, category):
        if category not in self._listings:
            raise RuntimeError(f"no fixture listing for {category}")
        return self._listings[category]

    def parse_product(self, raw, category):
        return {
            "product_id": raw["id"],
            "product_name": raw["name"],
            "category_path": category,
            "price": raw["price"],
        }


def test_crawl_yields_snapshot_for_valid_product():
    crawler = _FakeCrawler({"cat-1": [{"id": "p1", "name": "Widget", "price": 10}]})
    results = list(crawler.crawl())
    assert len(results) == 1
    snapshot, raw, error = results[0]
    assert error is None
    assert snapshot["site"] == "fake"
    assert snapshot["product_id"] == "p1"
    assert snapshot["price"] == 10.0
    assert raw == {"id": "p1", "name": "Widget", "price": 10}


def test_crawl_quarantines_invalid_product_without_stopping():
    crawler = _FakeCrawler(
        {"cat-1": [
            {"id": "p1", "name": "Widget", "price": 10},
            {"id": "", "name": "Missing id", "price": 5},  # rejected: no product_id
        ]}
    )
    results = list(crawler.crawl())
    assert len(results) == 2
    ok = [r for r in results if r[2] is None]
    failed = [r for r in results if r[2] is not None]
    assert len(ok) == 1
    assert len(failed) == 1
    assert isinstance(failed[0][2], ValueError)


def test_crawl_category_fetch_failure_is_quarantined_not_raised():
    crawler = _FakeCrawler({})  # fetch_listing raises for any category
    results = list(crawler.crawl())
    assert len(results) == 1
    snapshot, raw, error = results[0]
    assert snapshot is None
    assert isinstance(error, RuntimeError)


def test_crawl_skips_everything_when_robots_disallow():
    crawler = _FakeCrawler({"cat-1": [{"id": "p1", "name": "Widget", "price": 10}]}, allow_robots=False)
    assert list(crawler.crawl()) == []


def test_tiki_parse_product_maps_verified_listing_fields():
    payload = json.loads((FIXTURES / "tiki_listing_sample.json").read_text(encoding="utf-8"))
    crawler = TikiCrawler.__new__(TikiCrawler)  # skip __init__ (no network/robots needed here)
    macbook = payload["data"][0]

    parsed = crawler.parse_product(macbook, category="1846")

    assert parsed["product_id"] == "279212151"
    assert parsed["product_name"] == "MacBook Neo A18 Pro"
    assert parsed["brand"] == "Apple"
    assert parsed["price"] == 16990000
    # list_price is 0 in the raw payload (Tiki quirk) -> falls back to original_price
    assert parsed["list_price"] == 18990000
    assert parsed["seller_name"] == "seller:1"
    assert parsed["url"] == "https://tiki.vn/macbook-neo-a18-pro-p279212151.html"
    assert parsed["category_path"] == "1846"


def test_tiki_parse_product_missing_id_yields_empty_product_id():
    payload = json.loads((FIXTURES / "tiki_listing_sample.json").read_text(encoding="utf-8"))
    crawler = TikiCrawler.__new__(TikiCrawler)
    broken = payload["data"][2]

    parsed = crawler.parse_product(broken, category="1846")

    assert parsed["product_id"] == ""  # normalize_price_snapshot rejects this downstream


class _FakeProducer:
    def __init__(self, raise_on_send=False):
        self.sent = []
        self.raise_on_send = raise_on_send

    def send(self, topic, value=None, key=None):
        if self.raise_on_send:
            raise ConnectionError("kafka down")
        self.sent.append((topic, value))


def test_publish_to_dlq_sends_to_dot_dlq_topic():
    producer = _FakeProducer()
    publish_to_dlq(producer, "ecommerce_price_snapshots", {"bad": "row"}, ValueError("boom"), source="crawler.tiki")

    assert len(producer.sent) == 1
    topic, record = producer.sent[0]
    assert topic == "ecommerce_price_snapshots.dlq"
    assert record["raw_payload"] == {"bad": "row"}
    assert record["error"] == "boom"
    assert record["source"] == "crawler.tiki"


def test_publish_to_dlq_never_raises_on_producer_failure():
    producer = _FakeProducer(raise_on_send=True)
    publish_to_dlq(producer, "topic", {"bad": "row"}, ValueError("boom"), source="crawler.tiki")  # must not raise
