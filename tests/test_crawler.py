import io
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

import crawler.runner as phase2_runner
from common.dlq import publish_to_dlq
from config.marketplace_schema import create_marketplace_offer, create_observation_event, create_offer_observation
from crawler.base import SiteCrawler
from crawler.contracts import FetchResult, ListingPageRequest, ParsedListingPage, encode_listing_page_task_target
from crawler.sites.tiki import TikiCrawler

FIXTURES = Path(__file__).parent / "fixtures"


class _FakeCrawler(SiteCrawler):
    """Minimal SiteCrawler for exercising the Template Method in isolation.

    ``listings`` maps category -> list of pages (each page a list of raw rows);
    a bare list of rows is accepted as shorthand for a single page.
    """

    site_name = "fake"
    base_url = "https://example.invalid"

    def __init__(self, listings, allow_robots=True, categories=("cat-1",), max_pages=10):
        self._listings = {
            category: pages if pages and isinstance(pages[0], list) else [pages]
            for category, pages in listings.items()
        }
        self._allow_robots_override = allow_robots
        self.fetched_pages = []
        self.checked_urls = []
        super().__init__(
            categories=list(categories),
            request_delay_seconds=0,
            jitter_seconds=0,
            max_pages=max_pages,
        )

    def _load_robots(self):
        return None  # never touched: _allowed() is overridden below

    def _allowed(self, url):
        self.checked_urls.append(url)
        return self._allow_robots_override

    def fetch_listing(self, category, page=1):
        self.fetched_pages.append((category, page))
        if category not in self._listings:
            raise RuntimeError(f"no fixture listing for {category}")
        pages = self._listings[category]
        return pages[page - 1] if page <= len(pages) else []

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


def _row(pid, price=10):
    return {"id": pid, "name": f"Widget {pid}", "price": price}


def test_crawl_walks_pages_until_empty_page():
    crawler = _FakeCrawler({"cat-1": [[_row("p1"), _row("p2")], [_row("p3")], []]})

    snapshots = [s for s, _, err in crawler.crawl() if err is None]

    assert [s["product_id"] for s in snapshots] == ["p1", "p2", "p3"]
    # Page 4 is never requested: page 3 came back empty.
    assert crawler.fetched_pages == [("cat-1", 1), ("cat-1", 2), ("cat-1", 3)]


def test_crawl_stops_paging_at_max_pages():
    pages = [[_row(f"p{i}")] for i in range(1, 11)]
    crawler = _FakeCrawler({"cat-1": pages}, max_pages=3)

    snapshots = [s for s, _, err in crawler.crawl() if err is None]

    assert [s["product_id"] for s in snapshots] == ["p1", "p2", "p3"]
    assert crawler.fetched_pages == [("cat-1", 1), ("cat-1", 2), ("cat-1", 3)]


def test_crawl_page_failure_ends_only_that_category():
    crawler = _FakeCrawler(
        {"cat-1": [[_row("p1")]], "cat-2": [[_row("p2")], [_row("p3")]]},
        categories=["missing-cat", "cat-2"],
    )

    results = list(crawler.crawl())
    failed = [r for r in results if r[2] is not None]
    ok = [r for r in results if r[2] is None]

    assert len(failed) == 1
    assert failed[0][1] == {"category": "missing-cat", "page": 1}
    # The bad category is abandoned after its first page, cat-2 still fully crawled.
    assert ("missing-cat", 2) not in crawler.fetched_pages
    assert [s["product_id"] for s, _, _ in ok] == ["p2", "p3"]


def test_crawl_does_not_yield_the_same_product_from_two_categories():
    """Snapshot grain is (site, product, day) — a cross-listed product is one row."""
    crawler = _FakeCrawler(
        {"cat-1": [[_row("shared"), _row("only-1")]], "cat-2": [[_row("shared"), _row("only-2")]]},
        categories=["cat-1", "cat-2"],
    )

    snapshots = [s for s, _, err in crawler.crawl() if err is None]

    assert [s["product_id"] for s in snapshots] == ["shared", "only-1", "only-2"]


def test_crawl_checks_robots_against_the_real_request_url():
    """A crawler must not be cleared by base_url when it fetches an API path."""

    class _ApiCrawler(_FakeCrawler):
        def request_url(self, category, page=1):
            return f"https://example.invalid/api/listings?c={category}&p={page}"

    crawler = _ApiCrawler({"cat-1": [[_row("p1")]]})
    list(crawler.crawl())

    assert crawler.checked_urls
    assert all("/api/listings" in url for url in crawler.checked_urls)


class _RealRobotsCrawler(_FakeCrawler):
    """Same fake, but exercising SiteCrawler's real robots.txt handling."""

    _load_robots = SiteCrawler._load_robots
    _allowed = SiteCrawler._allowed


def _mock_robots(monkeypatch, status_code=200, text=""):
    class _Resp:
        pass

    _Resp.status_code = status_code
    _Resp.text = text
    monkeypatch.setattr(
        "crawler.base.requests.get",
        lambda url, headers=None, timeout=None: _Resp(),
    )


def test_robots_parsing_keeps_rules_after_a_blank_line(monkeypatch):
    """RobotFileParser.read() drops everything after a blank line; we must not."""
    _mock_robots(monkeypatch, text="User-agent: *\nDisallow: /customer/\n\nDisallow: /api/v2/me/\n")
    crawler = _RealRobotsCrawler({"cat-1": [[_row("p1")]]})

    assert crawler._allowed("https://example.invalid/products") is True
    assert crawler._allowed("https://example.invalid/customer/orders") is False
    # The rule after the blank line is the one stock robotparser would lose.
    assert crawler._allowed("https://example.invalid/api/v2/me/") is False


def test_robots_blank_lines_do_not_merge_other_agents_rules(monkeypatch):
    """Dropping blank lines must not hand another bot's Disallow to us."""
    _mock_robots(
        monkeypatch,
        text="User-agent: *\nAllow: /\n\nUser-agent: EvilBot\nDisallow: /\n",
    )
    crawler = _RealRobotsCrawler({"cat-1": [[_row("p1")]]})

    assert crawler._allowed("https://example.invalid/products") is True


def test_robots_missing_file_allows_crawling(monkeypatch):
    _mock_robots(monkeypatch, status_code=404)
    crawler = _RealRobotsCrawler({"cat-1": [[_row("p1")]]})

    assert crawler._allowed("https://example.invalid/anything") is True


def test_robots_forbidden_disallows_crawling(monkeypatch):
    _mock_robots(monkeypatch, status_code=403)
    crawler = _RealRobotsCrawler({"cat-1": [[_row("p1")]]})

    assert crawler._allowed("https://example.invalid/anything") is False


def test_robots_fetch_failure_disallows_crawling(monkeypatch):
    def boom(url, headers=None, timeout=None):
        raise ConnectionError("no network")

    monkeypatch.setattr("crawler.base.requests.get", boom)
    crawler = _RealRobotsCrawler({"cat-1": [[_row("p1")]]})

    assert crawler._allowed("https://example.invalid/anything") is False


def test_tiki_checks_robots_against_the_listing_api_url():
    crawler = TikiCrawler.__new__(TikiCrawler)
    assert crawler.request_url("1846", page=2).startswith(
        "https://tiki.vn/api/personalish/v1/blocks/listings"
    )


def test_crawl_stops_when_a_page_repeats_the_previous_one():
    """Defends against an endpoint that ignores `page` and replays page 1."""
    same_page = [_row("p1"), _row("p2")]
    crawler = _FakeCrawler({"cat-1": [same_page, list(same_page), list(same_page)]}, max_pages=10)

    snapshots = [s for s, _, err in crawler.crawl() if err is None]

    assert [s["product_id"] for s in snapshots] == ["p1", "p2"]
    assert crawler.fetched_pages == [("cat-1", 1), ("cat-1", 2)]


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
    # url_key already carries the "-p<id>" suffix; the id must not be appended twice
    assert parsed["url"] == "https://tiki.vn/macbook-neo-a18-pro-p279212151.html"
    assert parsed["category_path"] == "1/2/1846/8095/2458"


def test_tiki_parse_product_falls_back_to_queried_category():
    """Older/partial listing rows omit primary_category_path."""
    payload = json.loads((FIXTURES / "tiki_listing_sample.json").read_text(encoding="utf-8"))
    crawler = TikiCrawler.__new__(TikiCrawler)
    headphones = payload["data"][1]

    parsed = crawler.parse_product(headphones, category="1846")

    assert parsed["category_path"] == "1846"
    assert parsed["url"] == "https://tiki.vn/tai-nghe-khong-day-p279212999.html"


def test_tiki_parse_product_missing_id_yields_empty_product_id():
    payload = json.loads((FIXTURES / "tiki_listing_sample.json").read_text(encoding="utf-8"))
    crawler = TikiCrawler.__new__(TikiCrawler)
    broken = payload["data"][2]

    parsed = crawler.parse_product(broken, category="1846")

    assert parsed["product_id"] == ""  # normalize_price_snapshot rejects this downstream


class _FakeResponse:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self._body


def _tiki_for_fetch():
    """A TikiCrawler with just the state fetch_listing needs (no robots.txt call)."""
    crawler = TikiCrawler.__new__(TikiCrawler)
    crawler.user_agent = "test-agent"
    crawler._last_page = {}
    return crawler


def test_tiki_fetch_listing_requests_the_asked_page_and_learns_last_page(monkeypatch):
    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append(params)
        return _FakeResponse({"data": [{"id": 1}], "paging": {"last_page": 2, "total": 80}})

    monkeypatch.setattr("crawler.sites.tiki.requests.get", fake_get)
    crawler = _tiki_for_fetch()

    assert crawler.fetch_listing("1846", page=3) == [{"id": 1}]
    assert calls[0]["page"] == 3
    assert calls[0]["category"] == "1846"
    assert crawler._last_page["1846"] == 2


def test_tiki_fetch_listing_returns_empty_past_last_page_without_a_request(monkeypatch):
    calls = []

    def fake_get(url, params=None, headers=None, timeout=None):
        calls.append(params)
        return _FakeResponse({"data": [{"id": 1}], "paging": {"last_page": 1}})

    monkeypatch.setattr("crawler.sites.tiki.requests.get", fake_get)
    crawler = _tiki_for_fetch()

    assert crawler.fetch_listing("1846", page=1) == [{"id": 1}]
    assert crawler.fetch_listing("1846", page=2) == []
    assert len(calls) == 1  # page 2 costs no request


def test_tiki_fetch_listing_survives_missing_paging_block(monkeypatch):
    monkeypatch.setattr(
        "crawler.sites.tiki.requests.get",
        lambda url, params=None, headers=None, timeout=None: _FakeResponse({"data": [{"id": 1}]}),
    )
    crawler = _tiki_for_fetch()

    assert crawler.fetch_listing("1846", page=1) == [{"id": 1}]
    assert crawler._last_page == {}  # nothing learned, page loop falls back to max_pages


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


UTC = timezone.utc
PHASE2_START = datetime(2026, 9, 4, 8, 30, tzinfo=UTC)


class _Phase2RunnerAdapter:
    site_name = "fake"
    marketplace_id = "marketplace-fake"
    adapter_version = "fake-v1"

    def __init__(self, categories, max_pages):
        self.categories = categories
        self.max_pages = max_pages

    def request_url(self, target, page):
        return f"https://example.test/listings?target={target}&page={page}"

    def allowed(self, url):
        return True

    def throttle(self):
        pass

    def fetch_listing_page(self, request):
        # Under the frozen clock these tests inject, a real adapter's
        # fetched_at *is* PHASE2_START: it reads the same clock the
        # orchestrator does. Fabricating a later instant would put the fetch
        # outside the acquisition window the report claims, which the
        # Phase2AcquisitionReport contract now rejects.
        return FetchResult(
            self.request_url(request.target, request.page),
            PHASE2_START,
            200,
            "application/json",
            b"page",
        )

    def parse_listing_page(self, **kwargs):
        artifact = kwargs["raw_artifact"]
        request = kwargs["request"]
        offer = create_marketplace_offer(
            marketplace_code="fake",
            marketplace_id="marketplace-fake",
            platform_listing_id=f"{request.target}-{request.page}",
            seller_id=None,
            product_title="Product",
            brand=None,
            category_path=request.target,
            source_url="https://example.test/product.html",
            currency="VND",
            first_seen_at=artifact.fetched_at,
            last_seen_at=artifact.fetched_at,
        )
        observation = create_offer_observation(
            marketplace_code="fake",
            platform_listing_id=offer.platform_listing_id,
            offer_id=offer.offer_id,
            observed_at=artifact.fetched_at,
            fetched_at=artifact.fetched_at,
            current_price=Decimal("10.00"),
            raw_uri=artifact.raw_uri,
            raw_sha256=artifact.body_sha256,
            adapter_version=artifact.adapter_version,
            crawl_run_id=kwargs["crawl_run_id"],
        )
        event = create_observation_event(
            marketplace_code="fake",
            offer=offer,
            observation=observation,
            platform_listing_id=offer.platform_listing_id,
            produced_at=kwargs["produced_at"],
        )
        return ParsedListingPage((event,), (), 1, 0, 1)


def _phase2_writer(calls):
    def writer(zone, path, data):
        calls.append((zone, path, data))
        return f"file:///lake/{path}"

    return writer


def test_phase2_execute_listing_page_is_one_page_and_has_no_kafka_path(monkeypatch):
    monkeypatch.setitem(phase2_runner.SITE_CRAWLERS, "fake", _Phase2RunnerAdapter)
    calls = []
    report = phase2_runner.execute_listing_page(
        site="fake",
        task_target=encode_listing_page_task_target("cat-1", 1),
        crawl_run_id="run-1",
        writer=_phase2_writer(calls),
        clock=lambda: PHASE2_START,
    )
    assert report.is_task_success is True
    assert report.target == "cat-1"
    assert report.page == 1
    assert len(calls) == 2
    assert not hasattr(phase2_runner, "create_producer")
    assert not hasattr(phase2_runner, "KAFKA_BOOTSTRAP_SERVERS")


def test_phase2_execute_rejects_bad_target_or_site_before_adapter(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("adapter must not be constructed")

    monkeypatch.setitem(phase2_runner.SITE_CRAWLERS, "fake", fail)
    with pytest.raises(ValueError):
        phase2_runner.execute_listing_page(
            site="fake",
            task_target="not-json",
            crawl_run_id="run-1",
            writer=lambda *args: "file:///tmp/x",
            clock=lambda: PHASE2_START,
        )
    with pytest.raises(ValueError, match="unknown crawler site"):
        phase2_runner.execute_listing_page(
            site="missing",
            task_target=encode_listing_page_task_target("cat-1", 1),
            crawl_run_id="run-1",
            writer=lambda *args: "file:///tmp/x",
            clock=lambda: PHASE2_START,
        )


def test_phase2_run_site_stops_at_reported_last_page(monkeypatch):
    monkeypatch.setitem(phase2_runner.SITE_CRAWLERS, "fake", _Phase2RunnerAdapter)
    monkeypatch.setitem(phase2_runner.SITE_CATEGORIES, "fake", ["cat-1"])
    result = phase2_runner.run_site(
        "fake",
        categories=["cat-1"],
        max_pages=5,
        crawl_run_id="run-1",
        writer=_phase2_writer([]),
        clock=lambda: PHASE2_START,
    )
    assert result["pages"] == 1
    assert result["observations"] == 1
    assert result["failed_pages"] == 0


def test_phase2_run_site_emits_only_canonical_events_to_optional_jsonl(monkeypatch):
    monkeypatch.setitem(phase2_runner.SITE_CRAWLERS, "fake", _Phase2RunnerAdapter)
    monkeypatch.setitem(phase2_runner.SITE_CATEGORIES, "fake", ["cat-1"])
    output = io.StringIO()
    result = phase2_runner.run_site(
        "fake",
        categories=["cat-1"],
        max_pages=1,
        crawl_run_id="run-1",
        writer=_phase2_writer([]),
        clock=lambda: PHASE2_START,
        output=output,
    )
    line = json.loads(output.getvalue())
    assert result["raw_bytes"] == len(b"page")
    assert line["event_id"].startswith("obs_")
    assert line["payload"]["observation"]["raw_uri"].startswith("file:")
