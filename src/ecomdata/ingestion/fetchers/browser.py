"""A page loaded in a browser: the rendered document and the responses it fetched.

For a source whose listing arrives as an XHR the page makes itself (Lazada's
``?ajax=true`` responses). The fetcher opens one page in the worker's
:class:`~ecomdata.infra.browser.BrowserSession`, records every response whose
URL matches the spec's ``capture_patterns``, waits for the page to show what
``wait_for`` names (a selector, optional) and for at least one captured
response, then returns the document plus those responses as one bundle.

It never hides that it is a browser and never tries to pass a check: a page or
URL that matches one of the spec's ``blocked_patterns`` is a
:class:`~ecomdata.domain.acquisition.FetchBlockedError`, which backs the crawl
off and opens the source circuit.
"""
from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ecomdata.domain.acquisition import (
    ROLE_DOCUMENT,
    ROLE_XHR,
    FetchBlockedError,
    FetchBundle,
    FetchPart,
    FetchTransportError,
    ListingPageRequest,
)
from ecomdata.infra.browser import BrowserSession
from ecomdata.ingestion.fetchers.base import SourceFetcher

POLL_MILLISECONDS = 250
EVIDENCE_CONTEXT = 120


@dataclass(frozen=True)
class CapturedResponse:
    url: str
    status: int
    content_type: str | None
    body: bytes


class BrowserFetcher(SourceFetcher):
    def __init__(
        self,
        *args: Any,
        session: BrowserSession,
        timeout_seconds: float = 45.0,
        settle_seconds: float = 3.0,
        monotonic: Callable[[], float] = time.monotonic,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        if timeout_seconds <= 0 or settle_seconds < 0:
            raise ValueError("timeout_seconds must be positive and settle_seconds not negative")
        self.session = session
        self.timeout_seconds = timeout_seconds
        self.settle_seconds = settle_seconds
        self._monotonic = monotonic
        self._capture = [re.compile(pattern) for pattern in self.spec.capture_patterns]
        self._blocked = [re.compile(pattern, re.IGNORECASE) for pattern in self.spec.blocked_patterns]

    def _recorder(self, captured: list[CapturedResponse]) -> Callable[[Any], None]:
        def on_response(response: Any) -> None:
            if not any(pattern.search(response.url) for pattern in self._capture):
                return
            try:
                body = response.body()
            except Exception:
                # A response that cannot be read (redirect, aborted) is not data.
                return
            headers = getattr(response, "headers", None) or {}
            captured.append(
                CapturedResponse(response.url, int(response.status), headers.get("content-type"), bytes(body))
            )

        return on_response

    def _blocked_evidence(self, text: str) -> str | None:
        for pattern in self._blocked:
            match = pattern.search(text)
            if match:
                start = max(0, match.start() - EVIDENCE_CONTEXT)
                return f"/{pattern.pattern}/ near: {text[start : match.end() + EVIDENCE_CONTEXT]!r}"
        return None

    def _wait_for_data(self, page: Any, captured: list[CapturedResponse]) -> None:
        timeout_ms = int(self.timeout_seconds * 1000)
        if self.spec.wait_for:
            try:
                page.wait_for_selector(self.spec.wait_for, timeout=timeout_ms)
            except Exception as exc:
                raise FetchTransportError(
                    f"the page never showed {self.spec.wait_for!r} within {self.timeout_seconds:g}s: {exc}"
                ) from exc
        deadline = self._monotonic() + self.timeout_seconds
        while not captured and self._monotonic() < deadline and self._capture:
            page.wait_for_timeout(POLL_MILLISECONDS)
        # Late responses of the same page (a second listing request) still count.
        if self.settle_seconds:
            page.wait_for_timeout(int(self.settle_seconds * 1000))

    def fetch(self, request: ListingPageRequest, url: str) -> FetchBundle:
        captured: list[CapturedResponse] = []
        started = self._monotonic()
        with self.session.page() as page:
            page.on("response", self._recorder(captured))
            response = page.goto(url, timeout=int(self.timeout_seconds * 1000), wait_until="domcontentloaded")
            if response is None:
                raise FetchTransportError(f"navigation to {url} produced no response")
            status = int(response.status)
            headers = getattr(response, "headers", None) or {}
            final_url = page.url
            evidence = self._blocked_evidence(final_url)
            if evidence is None and status < 400:
                self._wait_for_data(page, captured)
            html = page.content()
        evidence = evidence or self._blocked_evidence(html)
        if evidence is not None:
            raise FetchBlockedError(f"{self.spec.code} answered with a block or challenge page", evidence=evidence)

        elapsed_ms = max(0, int((self._monotonic() - started) * 1000))
        document = FetchPart(
            url=final_url,
            http_status=status,
            content_type=headers.get("content-type", "text/html"),
            body=html.encode("utf-8"),
            role=ROLE_DOCUMENT,
            elapsed_ms=elapsed_ms,
        )
        parts = [
            FetchPart(item.url, item.status, item.content_type, item.body, role=ROLE_XHR) for item in captured
        ]
        return FetchBundle(
            request_url=url,
            fetched_at=self.clock(),
            parts=(document, *parts),
            retry_after=headers.get("retry-after"),
        )

    def close(self) -> None:
        self.session.close()


def browser_fetcher_factory(crawl: Any) -> Callable[..., BrowserFetcher]:
    """Builds the worker's browser fetchers from the crawl settings; the session starts on first use."""

    def build(spec: Any, extractor: Any, **common: Any) -> BrowserFetcher:
        return BrowserFetcher(
            spec,
            extractor,
            session=BrowserSession(product_token=crawl.user_agent),
            timeout_seconds=crawl.browser_timeout_seconds,
            settle_seconds=crawl.browser_settle_seconds,
            **common,
        )

    return build
