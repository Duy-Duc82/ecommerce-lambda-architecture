"""BrowserFetcher over a fake page (no network, no browser): capture, wait, block, close."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ecomdata.domain.acquisition import (
    ROLE_DOCUMENT,
    ROLE_XHR,
    AcquisitionStage,
    FetchBlockedError,
    FetchTransportError,
    ListingPageRequest,
)
from ecomdata.domain.sources import LAZADA, SourceRegistry
from ecomdata.infra.browser import BrowserSession
from ecomdata.infra.http import HttpClient, RobotsPolicy, Throttle
from ecomdata.ingestion.fetchers.browser import BrowserFetcher, browser_fetcher_factory
from ecomdata.ingestion.pipeline import acquire_listing_page
from ecomdata.ingestion.runtime import SourceRuntime
from ecomdata.ingestion.sources.lazada.extractor import LazadaExtractor
from tests.paths import FIXTURES

NOW = datetime(2026, 10, 25, 9, 0, tzinfo=UTC)
URL = "https://www.lazada.vn/dien-thoai-di-dong/?page=1"
AJAX = URL + "&ajax=true"
LISTING = (FIXTURES / "sources" / "lazada" / "listing_xhr.json").read_bytes()
SHELL = (FIXTURES / "sources" / "lazada" / "category_page.html").read_text(encoding="utf-8")


class FakeResponse:
    def __init__(self, url, status=200, body=b"", content_type="application/json", headers=None, unreadable=False):
        self.url, self.status, self._body, self.unreadable = url, status, body, unreadable
        self.headers = {"content-type": content_type, **(headers or {})}

    def body(self):
        if self.unreadable:
            raise RuntimeError("response body unavailable")
        return self._body


class FakePage:
    """Plays a script of network events against the callbacks the fetcher registers."""

    def __init__(self, *, document=SHELL, status=200, final_url=URL, responses=(), late=(), headers=None, navigation=True):
        self.document, self.status, self.url = document, status, final_url
        self.responses, self.late = list(responses), list(late)
        self.headers = headers or {"content-type": "text/html; charset=utf-8"}
        self.navigation, self.waited, self.selectors, self.handlers = navigation, [], [], []
        self.clock = 0.0

    def on(self, event, callback):
        assert event == "response"
        self.handlers.append(callback)

    def emit(self, responses):
        for response in responses:
            for handler in self.handlers:
                handler(response)

    def goto(self, url, timeout, wait_until):
        self.goto_args = (url, timeout, wait_until)
        if not self.navigation:
            return None
        self.emit(self.responses)
        return FakeResponse(url, self.status, b"", "text/html", self.headers)

    def wait_for_selector(self, selector, timeout):
        self.selectors.append((selector, timeout))
        if selector == "never":
            raise TimeoutError("selector never appeared")

    def wait_for_timeout(self, milliseconds):
        self.waited.append(milliseconds)
        self.clock += milliseconds / 1000
        self.emit(self.late)
        self.late = []

    def content(self):
        return self.document


class FakeSession:
    def __init__(self, page):
        self._page, self.closed, self.opened = page, 0, 0

    def page(self):
        from contextlib import contextmanager

        @contextmanager
        def open_page():
            self.opened += 1
            yield self._page

        return open_page()

    def close(self):
        self.closed += 1


class OpenRobots:
    """A robots.txt session that allows everything, as lazada.vn's does for category pages."""

    def get(self, url, headers, timeout):
        body = b"User-agent: *\nDisallow: /undefined/\n"
        return type("R", (), {"url": url, "status_code": 200, "content": body, "headers": {}})()

    def close(self):
        pass


def fetcher_for(page, *, spec=LAZADA, timeout=2.0, settle=0.5):
    session = FakeSession(page)
    client = HttpClient(user_agent="bot", timeout_seconds=5, session=OpenRobots())
    robots = RobotsPolicy(client, user_agent="bot")
    slept = []
    fetcher = BrowserFetcher(
        spec, LazadaExtractor(), session=session, timeout_seconds=timeout, settle_seconds=settle,
        monotonic=lambda: page.clock, robots=robots, throttle=Throttle(min_interval_seconds=0, sleep=slept.append),
        clock=lambda: NOW,
    )
    return fetcher, session


def request(page=1):
    return ListingPageRequest("lazada", "marketplace-lazada", "dien-thoai-di-dong", page)


def test_the_bundle_is_the_rendered_document_plus_the_captured_responses():
    page = FakePage(responses=[FakeResponse(AJAX, body=LISTING), FakeResponse("https://cdn.test/x.js", body=b"var a")])
    fetcher, session = fetcher_for(page)

    bundle = fetcher.fetch(request(), URL)

    assert [part.role for part in bundle.parts] == [ROLE_DOCUMENT, ROLE_XHR]
    assert bundle.document.body == SHELL.encode("utf-8") and bundle.document.http_status == 200
    assert bundle.captured[0].url == AJAX and bundle.captured[0].body == LISTING
    assert bundle.captured[0].content_type == "application/json"
    assert bundle.fetched_at == NOW and bundle.request_url == URL
    assert page.goto_args == (URL, 2000, "domcontentloaded")
    assert session.opened == 1


def test_only_responses_matching_the_capture_patterns_are_kept():
    page = FakePage(responses=[
        FakeResponse("https://www.lazada.vn/other?x=1", body=b"{}"),
        FakeResponse(AJAX, body=LISTING),
        FakeResponse("https://www.lazada.vn/ajax=true-not-a-param", body=b"{}"),
    ])
    fetcher, _ = fetcher_for(page)

    assert [part.url for part in fetcher.fetch(request(), URL).captured] == [AJAX]


def test_a_response_that_cannot_be_read_is_not_data():
    page = FakePage(responses=[FakeResponse(AJAX, unreadable=True), FakeResponse(AJAX + "&b=1", body=LISTING)])
    fetcher, _ = fetcher_for(page)

    assert [part.url for part in fetcher.fetch(request(), URL).captured] == [AJAX + "&b=1"]


def test_it_waits_for_the_listing_response_and_then_for_late_ones():
    page = FakePage(late=[FakeResponse(AJAX, body=LISTING)])
    fetcher, _ = fetcher_for(page, timeout=5, settle=1)

    bundle = fetcher.fetch(request(), URL)

    assert len(bundle.captured) == 1
    assert page.waited[0] == 250 and page.waited[-1] == 1000  # polled, then settled


def test_without_a_listing_response_it_stops_at_the_timeout_and_returns_the_document():
    page = FakePage()
    fetcher, _ = fetcher_for(page, timeout=1, settle=0)

    bundle = fetcher.fetch(request(), URL)

    assert bundle.captured == () and page.clock == pytest.approx(1.0)


def test_the_spec_wait_for_selector_is_awaited_with_the_timeout():
    spec = LAZADA.__class__(**{**LAZADA.__dict__, "wait_for": '[data-qa-locator="general-products"]'})
    page = FakePage(responses=[FakeResponse(AJAX, body=LISTING)])
    fetcher, _ = fetcher_for(page, spec=spec, timeout=7)

    fetcher.fetch(request(), URL)

    assert page.selectors == [('[data-qa-locator="general-products"]', 7000)]


def test_a_selector_that_never_shows_is_a_transport_failure():
    spec = LAZADA.__class__(**{**LAZADA.__dict__, "wait_for": "never"})
    fetcher, _ = fetcher_for(FakePage(), spec=spec)

    with pytest.raises(FetchTransportError, match="never showed"):
        fetcher.fetch(request(), URL)


def test_a_navigation_without_a_response_is_a_transport_failure():
    fetcher, _ = fetcher_for(FakePage(navigation=False))

    with pytest.raises(FetchTransportError, match="no response"):
        fetcher.fetch(request(), URL)


@pytest.mark.parametrize(
    "document",
    [
        "<html><body>Please slide to verify you are human</body></html>",
        "<html><iframe src='/_____tmd_____/punish?x5secdata=abc'></iframe></html>",
        "<html><body>baxia-punish</body></html>",
    ],
)
def test_a_challenge_page_is_a_block_with_its_evidence(document):
    fetcher, _ = fetcher_for(FakePage(document=document, responses=[FakeResponse(AJAX, body=LISTING)]))

    with pytest.raises(FetchBlockedError, match="block or challenge") as blocked:
        fetcher.fetch(request(), URL)

    assert blocked.value.evidence and "near:" in blocked.value.evidence


def test_a_redirect_to_a_challenge_url_is_a_block_before_any_waiting():
    page = FakePage(final_url="https://www.lazada.vn/_____tmd_____/punish?x5secdata=1")
    fetcher, _ = fetcher_for(page)

    with pytest.raises(FetchBlockedError):
        fetcher.fetch(request(), URL)

    assert page.waited == []


def test_an_error_status_is_returned_for_the_http_stage_not_waited_on():
    page = FakePage(status=429, headers={"content-type": "text/html", "retry-after": "30"})
    fetcher, _ = fetcher_for(page)

    bundle = fetcher.fetch(request(), URL)

    assert bundle.http_status == 429 and bundle.retry_after == "30" and page.waited == []


def test_closing_the_fetcher_closes_the_session_once_per_call():
    fetcher, session = fetcher_for(FakePage())

    fetcher.close()

    assert session.closed == 1


def test_timeouts_must_make_sense():
    page = FakePage()
    with pytest.raises(ValueError, match="timeout_seconds"):
        fetcher_for(page, timeout=0)


def test_the_acquisition_pipeline_stores_the_document_and_the_xhr_as_two_parts():
    page = FakePage(responses=[FakeResponse(AJAX, body=LISTING)])
    fetcher, _ = fetcher_for(page)
    stored = {}

    def writer(zone, path, data):
        stored[path] = data
        return f"file:///lake/{path}"

    report = acquire_listing_page(
        LAZADA, fetcher, LazadaExtractor(), request(), crawl_run_id="run-1", writer=writer, clock=lambda: NOW
    )

    assert report.is_task_success and report.row_count == 5 and report.last_page == 60
    assert sorted(p.split("/")[-1] for p in stored if "raw_artifact_id" in p and "part" in p) == ["part-0.bin.gz", "part-1.bin.gz"]
    assert [part.role for part in report.raw_artifact.parts] == ["document", "xhr"]
    assert report.page_exhausted is False


def test_a_block_during_acquisition_is_a_failed_fetch_that_stores_nothing():
    page = FakePage(document="<html>slide to verify</html>")
    fetcher, _ = fetcher_for(page)
    stored = []

    report = acquire_listing_page(
        LAZADA, fetcher, LazadaExtractor(), request(), crawl_run_id="run-1",
        writer=lambda z, p, d: stored.append(p) or "file:///x", clock=lambda: NOW,
    )

    assert report.failure.stage is AcquisitionStage.FETCH and isinstance(report.failure.error, FetchBlockedError)
    assert stored == []


# --- the worker's runtime and the session's life --------------------------------------


def test_the_runtime_builds_one_browser_fetcher_and_closes_its_session_on_close():
    from types import SimpleNamespace

    crawl = SimpleNamespace(user_agent="bot/1", browser_timeout_seconds=9.0, browser_settle_seconds=1.0)
    client = HttpClient(user_agent="bot", timeout_seconds=5, session=type("S", (), {"close": lambda self: None})())
    runtime = SourceRuntime(
        SourceRegistry([LAZADA]), http_client=client, robots=RobotsPolicy(client, user_agent="bot"),
        throttle=Throttle(min_interval_seconds=0), browser_factory=browser_fetcher_factory(crawl),
    )

    fetcher = runtime.fetcher("lazada")

    assert runtime.fetcher("lazada") is fetcher
    assert isinstance(fetcher.session, BrowserSession) and fetcher.session.product_token == "bot/1"
    assert (fetcher.timeout_seconds, fetcher.settle_seconds) == (9.0, 1.0)
    assert fetcher.session.closed is False  # nothing is launched until a page is asked for
    runtime.close()
    assert fetcher.session.closed is True
