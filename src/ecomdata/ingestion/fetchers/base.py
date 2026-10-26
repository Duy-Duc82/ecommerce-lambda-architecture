"""What every fetch strategy shares: the URL, robots, and the throttle."""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from ecomdata.domain.acquisition import FetchBundle, ListingPageRequest
from ecomdata.infra.http import RobotsPolicy, Throttle


class SourceFetcher(ABC):
    """One source's fetch strategy over the shared robots policy and throttle.

    ``robots`` and ``throttle`` are owned by whoever builds the fetcher (the
    worker builds them once), so a fetcher made per task would still share the
    cached robots.txt and the per-host request clock.
    """

    def __init__(
        self,
        spec: Any,
        extractor: Any,
        *,
        robots: RobotsPolicy,
        throttle: Throttle,
        base_url: str | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.spec = spec
        self.extractor = extractor
        self.robots = robots
        self._throttle = throttle
        self.base_url = base_url or spec.base_url
        self.clock = clock

    def request_url(self, request: ListingPageRequest) -> str:
        return self.extractor.request_url(self.base_url, request.target, request.page)

    def allowed(self, url: str) -> bool:
        return self.robots.allowed(url)

    def throttle(self, url: str) -> None:
        """Wait out the source's politeness floor (the configured delay may be longer)."""
        self._throttle.wait(url, min_interval_seconds=self.spec.min_delay_seconds)

    @abstractmethod
    def fetch(self, request: ListingPageRequest, url: str) -> FetchBundle: ...

    def close(self) -> None:
        return None
