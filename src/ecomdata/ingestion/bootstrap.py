"""Composition root of the ingestion tools: settings and the lake, read once per process."""
from __future__ import annotations

from collections.abc import Iterable
from functools import lru_cache
from typing import Any

from ecomdata.settings import Settings, load_settings


@lru_cache(maxsize=1)
def settings() -> Settings:
    return load_settings(dotenv=True)


def object_store() -> Any:
    from ecomdata.infra.object_store import object_store_for

    return object_store_for(settings().storage)


def http_stack(config: Settings):
    """The HTTP client, the robots policy and the throttle a worker shares across its sources."""
    from ecomdata.infra.http import HttpClient, RobotsPolicy, Throttle

    crawl = config.crawl
    client = HttpClient(user_agent=crawl.user_agent, timeout_seconds=crawl.http_timeout_seconds)
    robots = RobotsPolicy(client, user_agent=crawl.user_agent, ttl_seconds=crawl.robots_ttl_seconds)
    throttle = Throttle(min_interval_seconds=crawl.request_delay_seconds, jitter_seconds=crawl.jitter_seconds)
    return client, robots, throttle


def build_runtime(config: Settings, codes: Iterable[str]):
    """The runtime of the sources a worker crawls; a browser is only built for a source that needs one."""
    from ecomdata.domain.sources import BROWSER, SOURCES
    from ecomdata.ingestion.runtime import SourceRuntime

    client, robots, throttle = http_stack(config)
    browser_factory = None
    if any(SOURCES.get(code).fetcher == BROWSER for code in codes):
        from ecomdata.ingestion.fetchers.browser import browser_fetcher_factory

        browser_factory = browser_fetcher_factory(config.crawl)
    return SourceRuntime(
        SOURCES,
        http_client=client,
        robots=robots,
        throttle=throttle,
        base_urls=config.crawl.base_url_overrides,
        browser_factory=browser_factory,
    )
