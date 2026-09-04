"""Template Method for per-site product/price crawlers.

Mirrors the Strategy + Template Method shape already used by
``analytics/base.py::Forecaster``: each site adapter implements only the
site-specific fetch/parse steps; ``SiteCrawler`` owns the parts every site
must get right the same way — robots.txt compliance, rate limiting, and
normalize/validate — so quarantine behavior is consistent across sites
without every adapter re-implementing it.
"""

from __future__ import annotations

import logging
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Iterator
from urllib import robotparser
from urllib.parse import urlparse

import requests

from config.schema import normalize_price_snapshot
from config.settings import (
    CRAWL_JITTER_SECONDS,
    CRAWL_MAX_PAGES,
    CRAWL_REQUEST_DELAY_SECONDS,
    CRAWL_USER_AGENT,
)
from config.marketplace_schema import RawArtifact
from crawler.contracts import FetchResult, ListingPageRequest, ParsedListingPage

logger = logging.getLogger(__name__)

# One (snapshot, raw_payload, error) triple per product seen.
# snapshot is None and error is not None when the record was rejected.
CrawlResult = tuple[dict[str, Any] | None, dict[str, Any], Exception | None]


class SiteCrawler(ABC):
    site_name: str
    marketplace_id: str
    adapter_version: str
    base_url: str

    def __init__(
        self,
        categories: list[str],
        request_delay_seconds: float = CRAWL_REQUEST_DELAY_SECONDS,
        jitter_seconds: float = CRAWL_JITTER_SECONDS,
        user_agent: str = CRAWL_USER_AGENT,
        max_pages: int = CRAWL_MAX_PAGES,
        sleeper: Any | None = None,
        jitter_source: Any | None = None,
        clock: Any | None = None,
    ):
        self.categories = categories
        self.request_delay_seconds = request_delay_seconds
        self.jitter_seconds = jitter_seconds
        self.user_agent = user_agent
        self.max_pages = max(1, max_pages)
        self._sleeper = sleeper or time.sleep
        self._jitter_source = jitter_source or random.uniform
        self._clock = clock
        self._robots = self._load_robots()

    def _load_robots(self) -> robotparser.RobotFileParser:
        """Fetch and parse robots.txt, ignoring blank lines inside the file.

        ``RobotFileParser.read()`` follows the 1994 draft where a blank line
        ends a record, so every rule after the first blank line is dropped.
        Real files rely on the current spec (RFC 9309), where only a
        ``User-agent`` line starts a new group — Tiki's robots.txt has a blank
        line at line 108 and loses ~half its rules when read that way
        (``can_fetch("/api/v2/me/")`` flips from True to False once the blank
        line is removed). Stripping blank lines before ``parse()`` gives RFC
        9309 grouping, because the parser already starts a new group on a
        ``User-agent`` line that follows rules.
        """
        parser = robotparser.RobotFileParser()
        parsed = urlparse(self.base_url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        parser.set_url(robots_url)
        try:
            response = requests.get(
                robots_url, headers={"User-Agent": self.user_agent}, timeout=10
            )
            if response.status_code in (401, 403):
                logger.warning("%s: robots.txt returned %d; treating as disallowed",
                               self.site_name, response.status_code)
                parser.disallow_all = True
            elif response.status_code >= 400:
                # No robots.txt (404 and friends) means no restrictions.
                parser.allow_all = True
            else:
                parser.parse([line for line in response.text.splitlines() if line.strip()])
        except Exception as exc:
            logger.warning("%s: could not read robots.txt (%s); treating as disallowed",
                           self.site_name, exc)
            parser.disallow_all = True
        return parser

    def request_url(self, target: str, page: int = 1) -> str:
        """URL ``fetch_listing`` will actually request — what robots.txt gates.

        Override in adapters that fetch anything other than the site root; the
        default is only correct for a crawler that requests ``base_url`` itself.
        """
        return self.base_url

    def allowed(self, request_url: str) -> bool:
        """Return whether robots.txt permits the exact request URL."""
        return self._allowed(request_url)

    def _allowed(self, url: str) -> bool:
        """Whether robots.txt permits fetching ``url``.

        Takes the real request URL: checking ``base_url`` instead would clear a
        crawler to hit an API path the site disallows, since a site's home page
        is almost always allowed.
        """
        try:
            return self._robots.can_fetch(self.user_agent, url)
        except Exception:
            return False

    def _throttle(self) -> None:
        self._sleeper(self.request_delay_seconds + self._jitter_source(0, self.jitter_seconds))

    def throttle(self) -> None:
        """Apply the configured delay through injectable dependencies."""
        self._throttle()

    def fetch_listing_page(self, request: ListingPageRequest) -> FetchResult:
        """Fetch one raw page; source adapters implement this boundary."""
        raise NotImplementedError(
            f"{type(self).__name__} must implement fetch_listing_page()"
        )

    def parse_listing_page(
        self,
        *,
        request: ListingPageRequest,
        fetch_result: FetchResult,
        raw_artifact: RawArtifact,
        crawl_run_id: str,
        produced_at: datetime,
    ) -> ParsedListingPage:
        """Parse already-fetched bytes; source adapters implement this boundary."""
        raise NotImplementedError(
            f"{type(self).__name__} must implement parse_listing_page()"
        )

    @abstractmethod
    def fetch_listing(self, category: str, page: int = 1) -> list[dict[str, Any]]:
        """Return raw per-product payloads for one page of one category/query.

        Return an empty list to tell ``crawl()`` this category is exhausted;
        an adapter that knows the site's last page should do that instead of
        letting the page loop spend requests up to ``max_pages``.
        """

    @abstractmethod
    def parse_product(self, raw: dict[str, Any], category: str) -> dict[str, Any]:
        """Map one site-specific raw payload to price-snapshot-shaped fields.

        Must set at least product_id and price; `site` is filled in by crawl().
        See config.schema.PRICE_SNAPSHOT_FIELDS for the target shape.
        """

    def crawl(self) -> Iterator[CrawlResult]:
        """Yield one (snapshot, raw, error) triple per product across all categories.

        Walks pages 1..max_pages per category, stopping early on an empty page.
        A category-level fetch failure or a single product's parse/validation
        failure never stops the run — both are yielded as quarantined records
        for the caller to route to a dead-letter queue; a page-level failure
        ends that category only.

        A product seen in an earlier category is not yielded twice: the
        price-snapshot grain is (site, product, day), so a product listed under
        two categories must not become two rows.

        Every page is checked against robots.txt using the URL that page will
        actually request, so a disallowed listing path stops the crawl even
        when the site's home page is allowed.
        """
        seen_ids: set[str] = set()
        pages_fetched = 0
        duplicates = 0

        for category in self.categories:
            category_ids: set[str] = set()

            for page in range(1, self.max_pages + 1):
                url = self.request_url(category, page)
                if not self._allowed(url):
                    logger.warning(
                        "%s: robots.txt disallows %s; skipping category %s",
                        self.site_name, url, category,
                    )
                    break

                self._throttle()
                try:
                    raw_products = self.fetch_listing(category, page=page)
                except Exception as exc:
                    logger.error(
                        "%s: fetch_listing(%s, page=%d) failed: %s",
                        self.site_name, category, page, exc,
                    )
                    yield None, {"category": category, "page": page}, exc
                    break

                pages_fetched += 1
                if not raw_products:
                    break

                page_ids: set[str] = set()
                for raw in raw_products:
                    try:
                        parsed = self.parse_product(raw, category)
                        parsed.setdefault("site", self.site_name)
                        snapshot = normalize_price_snapshot(parsed)
                    except Exception as exc:
                        yield None, raw, exc
                        continue

                    product_id = snapshot["product_id"]
                    page_ids.add(product_id)
                    if product_id in seen_ids:
                        duplicates += 1
                        continue
                    seen_ids.add(product_id)
                    yield snapshot, raw, None

                # Guard against an endpoint that ignores `page` and keeps
                # replaying the same rows: without this the loop would burn
                # max_pages requests per category for nothing.
                if page_ids and page_ids <= category_ids:
                    logger.warning(
                        "%s: %s page %d repeated page %d's products; stopping this category",
                        self.site_name, category, page, page - 1,
                    )
                    break
                category_ids |= page_ids

        logger.info(
            "%s: fetched %d page(s) across %d categor(ies); %d unique product(s), %d cross-category duplicate(s)",
            self.site_name, pages_fetched, len(self.categories), len(seen_ids), duplicates,
        )
