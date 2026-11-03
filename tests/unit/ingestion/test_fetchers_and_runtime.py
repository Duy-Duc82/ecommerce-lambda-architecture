"""HttpFetcher, and the per-worker runtime that shares robots and the request clock."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ecomdata.domain.acquisition import ListingPageRequest, encode_listing_page_task_target
from ecomdata.domain.observations import ResourceType
from ecomdata.domain.sources import LAZADA, TIKI, SourceRegistry, SourceSpec
from ecomdata.infra.http import HttpClient, RobotsPolicy, Throttle
from ecomdata.ingestion.runtime import SourceRuntime, listing_page_executor
from ecomdata.ingestion.scheduling import CrawlTask, CrawlTaskStatus, CrawlTier
from ecomdata.ingestion.sources.tiki.extractor import TikiExtractor

NOW = datetime(2026, 10, 25, 8, 30, tzinfo=UTC)
ROBOTS = b"User-agent: *\nDisallow: /private/\n"
PAGE = b'<script id="__NEXT_DATA__">{"props":{"initialState":{"catalog":{"data":[{"id":1,"price":5}]}}}}</script>'


class Response:
    def __init__(self, url, status, body):
        self.url, self.status_code, self.content = url, status, body
        self.headers = {"Content-Type": "text/html", "Retry-After": "7"}


class Session:
    """A requests.Session stand-in: serves robots.txt and category pages, and counts every call."""

    def __init__(self):
        self.calls, self.timeouts, self.closed = [], [], 0

    def get(self, url, headers, timeout):
        self.calls.append(url)
        self.timeouts.append(timeout)
        if url.endswith("/robots.txt"):
            return Response(url, 200, ROBOTS)
        return Response(url, 200, PAGE)

    def close(self):
        self.closed += 1


def runtime_for(session, registry=None, **kwargs):
    client = HttpClient(user_agent="bot", timeout_seconds=5, session=session)
    robots = RobotsPolicy(client, user_agent="bot", ttl_seconds=3600)
    slept, now = [], [0.0]

    def sleep(seconds):
        slept.append(seconds)
        now[0] += seconds

    throttle = Throttle(min_interval_seconds=2, sleep=sleep, clock=lambda: now[0])
    runtime = SourceRuntime(
        registry or SourceRegistry([TIKI]), http_client=client, robots=robots, throttle=throttle, clock=lambda: NOW, **kwargs
    )
    return runtime, slept


def request(target="laptop/c8095"):
    return ListingPageRequest("tiki", "marketplace-tiki", target, 1)


def test_the_http_fetcher_returns_one_document_part_with_the_response_metadata():
    runtime, _ = runtime_for(Session())
    fetcher = runtime.fetcher("tiki")

    url = fetcher.request_url(request())
    bundle = fetcher.fetch(request(), url)

    assert url == "https://tiki.vn/laptop/c8095"
    assert bundle.fetched_at == NOW and bundle.http_status == 200 and bundle.body == PAGE
    assert bundle.content_type == "text/html" and bundle.retry_after == "7"
    assert [part.role for part in bundle.parts] == ["document"]


def test_a_base_url_override_redirects_the_fetch_to_the_stub_source():
    runtime, _ = runtime_for(Session(), base_urls={"tiki": "http://stub:8080"})

    assert runtime.fetcher("tiki").request_url(request()) == "http://stub:8080/laptop/c8095"


def test_robots_are_downloaded_once_for_every_page_a_worker_acquires():
    # Regression, bug (b): an adapter was built per task and each one fetched
    # robots.txt with a hard-coded timeout.
    session = Session()
    runtime, _ = runtime_for(session)
    fetcher = runtime.fetcher("tiki")

    for target in ("laptop/c8095", "xe-dap/c8431", "dien-thoai/c1789"):
        assert fetcher.allowed(fetcher.request_url(request(target)))

    assert session.calls == ["https://tiki.vn/robots.txt"]
    assert runtime.fetcher("tiki") is fetcher
    assert runtime.extractor("tiki") is runtime.extractor("tiki")


def test_robots_use_the_configured_timeout_of_the_shared_client():
    session = Session()
    runtime, _ = runtime_for(session)

    runtime.fetcher("tiki").allowed("https://tiki.vn/laptop/c8095")

    assert session.timeouts == [5]


def test_the_throttle_spaces_two_requests_to_one_host_by_the_source_floor():
    runtime, slept = runtime_for(Session())
    fetcher = runtime.fetcher("tiki")

    fetcher.throttle("https://tiki.vn/a/c1")
    fetcher.throttle("https://tiki.vn/b/c2")

    assert slept == [pytest.approx(2.0)]


def test_a_source_floor_above_the_configured_delay_wins():
    slow = SourceSpec(
        code="slow",
        marketplace_id="marketplace-slow",
        name="Slow",
        base_url="https://slow.test",
        fetcher="http",
        extractor=TikiExtractor,
        normalizer="x",
        currency="VND",
        min_delay_seconds=5.0,
    )
    runtime, slept = runtime_for(Session(), SourceRegistry([slow]))
    fetcher = runtime.fetcher("slow")

    fetcher.throttle("https://slow.test/a")
    fetcher.throttle("https://slow.test/b")

    assert slept == [pytest.approx(5.0)]


def test_a_browser_source_without_a_browser_is_a_clear_error():
    runtime, _ = runtime_for(Session(), SourceRegistry([LAZADA]))

    with pytest.raises(RuntimeError, match="needs a browser"):
        runtime.fetcher("lazada")


def test_the_executor_turns_a_frontier_task_into_an_acquired_page():
    session = Session()
    runtime, _ = runtime_for(session)
    writes = []

    def writer(zone, path, data):
        writes.append(path)
        return f"file:///lake/{path}"

    task = CrawlTask(
        task_id="t1",
        marketplace_code="tiki",
        marketplace_id="marketplace-tiki",
        target=encode_listing_page_task_target("laptop/c8095", 1),
        resource_type=ResourceType.LISTING_PAGE,
        tier=CrawlTier.NORMAL,
        priority=0,
        scheduled_for=NOW,
        status=CrawlTaskStatus.LEASED,
        lease_owner="w1",
        lease_expires_at=NOW,
        attempts=1,
        max_attempts=5,
    )

    report = listing_page_executor(runtime, writer=writer)(task=task, crawl_run_id="run-1")

    assert report.is_task_success and report.row_count == 1
    assert session.calls == ["https://tiki.vn/robots.txt", "https://tiki.vn/laptop/c8095"]
    assert any(path.endswith(".jsonl.gz") for path in writes)


def test_closing_the_runtime_closes_the_client_and_forgets_the_fetchers():
    session = Session()
    runtime, _ = runtime_for(session)
    first = runtime.fetcher("tiki")

    runtime.close()

    assert session.closed >= 1
    assert runtime.fetcher("tiki") is not first
