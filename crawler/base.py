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
from typing import Any, Iterator
from urllib import robotparser
from urllib.parse import urlparse

from config.schema import normalize_price_snapshot
from config.settings import CRAWL_JITTER_SECONDS, CRAWL_REQUEST_DELAY_SECONDS, CRAWL_USER_AGENT

logger = logging.getLogger(__name__)

# One (snapshot, raw_payload, error) triple per product seen.
# snapshot is None and error is not None when the record was rejected.
CrawlResult = tuple[dict[str, Any] | None, dict[str, Any], Exception | None]


class SiteCrawler(ABC):
    site_name: str
    base_url: str

    def __init__(
        self,
        categories: list[str],
        request_delay_seconds: float = CRAWL_REQUEST_DELAY_SECONDS,
        jitter_seconds: float = CRAWL_JITTER_SECONDS,
        user_agent: str = CRAWL_USER_AGENT,
    ):
        self.categories = categories
        self.request_delay_seconds = request_delay_seconds
        self.jitter_seconds = jitter_seconds
        self.user_agent = user_agent
        self._robots = self._load_robots()

    def _load_robots(self) -> robotparser.RobotFileParser:
        parser = robotparser.RobotFileParser()
        parsed = urlparse(self.base_url)
        parser.set_url(f"{parsed.scheme}://{parsed.netloc}/robots.txt")
        try:
            parser.read()
        except Exception:
            logger.warning("%s: could not read robots.txt; treating as disallowed", self.site_name)
        return parser

    def _allowed(self) -> bool:
        try:
            return self._robots.can_fetch(self.user_agent, self.base_url)
        except Exception:
            return False

    def _throttle(self) -> None:
        time.sleep(self.request_delay_seconds + random.uniform(0, self.jitter_seconds))

    @abstractmethod
    def fetch_listing(self, category: str) -> list[dict[str, Any]]:
        """Return raw per-product payloads for one category/query."""

    @abstractmethod
    def parse_product(self, raw: dict[str, Any], category: str) -> dict[str, Any]:
        """Map one site-specific raw payload to price-snapshot-shaped fields.

        Must set at least product_id and price; `site` is filled in by crawl().
        See config.schema.PRICE_SNAPSHOT_FIELDS for the target shape.
        """

    def crawl(self) -> Iterator[CrawlResult]:
        """Yield one (snapshot, raw, error) triple per product across all categories.

        A category-level fetch failure or a single product's parse/validation
        failure never stops the run — both are yielded as quarantined records
        for the caller to route to a dead-letter queue.
        """
        if not self._allowed():
            logger.warning("%s: disallowed by robots.txt, skipping all categories", self.site_name)
            return

        for category in self.categories:
            self._throttle()
            try:
                raw_products = self.fetch_listing(category)
            except Exception as exc:
                logger.error("%s: fetch_listing(%s) failed: %s", self.site_name, category, exc)
                yield None, {"category": category}, exc
                continue

            for raw in raw_products:
                try:
                    parsed = self.parse_product(raw, category)
                    parsed.setdefault("site", self.site_name)
                    snapshot = normalize_price_snapshot(parsed)
                    yield snapshot, raw, None
                except Exception as exc:
                    yield None, raw, exc
