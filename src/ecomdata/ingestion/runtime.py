"""The per-worker runtime of the sources it crawls.

One :class:`SourceRuntime` is built per worker process. It owns the HTTP
client, the robots policy (cached per host, with a TTL) and the throttle (a
request clock per host), and hands out one fetcher and one extractor per source,
created on first use. A fetcher made per task, as before, re-downloaded
robots.txt for every page and shared no request clock.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from ecomdata.domain.acquisition import ListingPageRequest, decode_listing_page_task_target
from ecomdata.domain.sources import API, BROWSER, HTTP, SourceRegistry, SourceSpec
from ecomdata.infra.http import HttpClient, RobotsPolicy, Throttle
from ecomdata.ingestion.fetchers.http import HttpFetcher
from ecomdata.ingestion.pipeline import acquire_listing_page


def _instance(candidate: Any) -> Any:
    return candidate() if isinstance(candidate, type) else candidate


class SourceRuntime:
    def __init__(
        self,
        registry: SourceRegistry,
        *,
        http_client: HttpClient,
        robots: RobotsPolicy,
        throttle: Throttle,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        base_urls: Mapping[str, str] | None = None,
        browser_factory: Callable[[SourceSpec], Any] | None = None,
    ) -> None:
        self.registry = registry
        self.http_client = http_client
        self.robots = robots
        self.throttle = throttle
        self.clock = clock
        self.base_urls = dict(base_urls or {})
        self.browser_factory = browser_factory
        self._extractors: dict[str, Any] = {}
        self._fetchers: dict[str, Any] = {}

    def spec(self, code: str) -> SourceSpec:
        return self.registry.get(code)

    def extractor(self, code: str) -> Any:
        spec = self.spec(code)
        if spec.code not in self._extractors:
            self._extractors[spec.code] = _instance(self.registry.extractor(spec.code))
        return self._extractors[spec.code]

    def fetcher(self, code: str) -> Any:
        spec = self.spec(code)
        if spec.code not in self._fetchers:
            self._fetchers[spec.code] = self._build_fetcher(spec)
        return self._fetchers[spec.code]

    def _build_fetcher(self, spec: SourceSpec) -> Any:
        common = {
            "robots": self.robots,
            "throttle": self.throttle,
            "base_url": self.base_urls.get(spec.code),
            "clock": self.clock,
        }
        if spec.fetcher == HTTP:
            return HttpFetcher(spec, self.extractor(spec.code), client=self.http_client, **common)
        if spec.fetcher == BROWSER:
            if self.browser_factory is None:
                raise RuntimeError(f"{spec.code} needs a browser, and this worker has none")
            return self.browser_factory(spec, self.extractor(spec.code), **common)
        if spec.fetcher == API:
            raise NotImplementedError("the API fetcher is not implemented (ADR-6: no Shopee code)")
        raise ValueError(f"unknown fetcher kind: {spec.fetcher}")

    def acquire(self, code: str, request: ListingPageRequest, *, crawl_run_id: str, writer: Callable[..., str]):
        spec = self.spec(code)
        return acquire_listing_page(
            spec,
            self.fetcher(code),
            self.extractor(code),
            request,
            crawl_run_id=crawl_run_id,
            writer=writer,
            clock=self.clock,
        )

    def close(self) -> None:
        for fetcher in self._fetchers.values():
            fetcher.close()
        self._fetchers.clear()
        self.http_client.close()


def listing_page_executor(runtime: SourceRuntime, *, writer: Callable[..., str]) -> Callable[..., Any]:
    """The executor the worker calls: one frontier task becomes one acquired page."""

    def execute(*, task: Any, crawl_run_id: str):
        target, page = decode_listing_page_task_target(task.target)
        spec = runtime.spec(task.marketplace_code)
        request = ListingPageRequest(spec.code, spec.marketplace_id, target, page)
        return runtime.acquire(spec.code, request, crawl_run_id=crawl_run_id, writer=writer)

    return execute
