"""One real Lazada category page through the real browser (marker ``integration``).

Needs Playwright with Chromium, and the network: run it inside the crawler
image (``docker run --rm -v ... ecommerce/crawler-browser:1 pytest -m integration
tests/integration``) or on a host that has ``playwright install chromium``.
It makes exactly one page request, after robots.txt, at the source's politeness
floor, and gives up (skips) if Lazada answers with a challenge page: a block is
recorded, never worked around.
"""
from __future__ import annotations

import os
from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.integration

playwright = pytest.importorskip("playwright.sync_api")


def test_one_live_category_page_yields_at_least_thirty_products():
    from ecomdata.domain.acquisition import FetchBlockedError, ListingPageRequest
    from ecomdata.domain.sources import LAZADA
    from ecomdata.infra.browser import BrowserSession
    from ecomdata.infra.http import HttpClient, RobotsPolicy, Throttle
    from ecomdata.ingestion.fetchers.browser import BrowserFetcher
    from ecomdata.ingestion.pipeline import acquire_listing_page
    from ecomdata.ingestion.sources.lazada.extractor import LazadaExtractor

    user_agent = os.environ.get("CRAWL_USER_AGENT", "EcommercePriceResearchBot/0.1 (+thesis project; rate-limited)")
    client = HttpClient(user_agent=user_agent, timeout_seconds=15)
    session = BrowserSession(product_token=user_agent)
    fetcher = BrowserFetcher(
        LAZADA,
        LazadaExtractor(),
        session=session,
        robots=RobotsPolicy(client, user_agent=user_agent),
        throttle=Throttle(min_interval_seconds=LAZADA.min_delay_seconds),
        timeout_seconds=60,
        settle_seconds=3,
    )
    stored: dict[str, bytes] = {}

    def writer(zone: str, path: str, data: bytes) -> str:
        stored[path] = data
        return f"file:///integration/{path}"

    try:
        report = acquire_listing_page(
            LAZADA,
            fetcher,
            LazadaExtractor(),
            ListingPageRequest("lazada", LAZADA.marketplace_id, "dien-thoai-di-dong", 1),
            crawl_run_id="integration",
            writer=writer,
            clock=lambda: datetime.now(UTC),
        )
    finally:
        fetcher.close()
        client.close()

    if report.failure is not None and isinstance(report.failure.error, FetchBlockedError):
        pytest.skip(f"Lazada answered with a challenge page: {report.failure.error.evidence}")
    assert report.is_task_success, report.failure
    assert report.row_count >= 30
    assert report.last_page and report.last_page > 1
    assert [part.role for part in report.raw_artifact.parts][:2] == ["document", "xhr"]
    assert all(record.payload.get("itemId") for record in report.records if record.record_kind.value == "listing_row")
