"""A Chromium session for sources whose data a page loads by script.

One :class:`BrowserSession` lives as long as one worker: the browser is started
on first use and closed once, when the worker stops. Each acquisition opens its
own context and page and closes them afterwards, so one page's cookies and
storage never reach the next.

The browser says what it is. It carries Playwright's own Chromium user agent
with the project's product token appended, and nothing hides that it is
automated: no stealth plugin, no fingerprint changes, no captcha handling. A
block or challenge page is reported, never worked around.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_LOCALE = "vi-VN"
DEFAULT_VIEWPORT = {"width": 1366, "height": 900}


class BrowserSession:
    def __init__(
        self,
        *,
        product_token: str,
        locale: str = DEFAULT_LOCALE,
        headless: bool = True,
        launcher: Callable[[], tuple[Any, Any]] | None = None,
    ) -> None:
        """``launcher`` returns ``(playwright, browser)``; a test passes a fake, production leaves it unset."""
        self.product_token = product_token.strip()
        self.locale = locale
        self.headless = headless
        self._launcher = launcher or self._launch_chromium
        self._playwright: Any = None
        self._browser: Any = None
        self._user_agent: str | None = None
        self.closed = False

    def _launch_chromium(self) -> tuple[Any, Any]:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:  # pragma: no cover - environment, not logic
            raise ImportError("playwright is required for browser sources: pip install 'ecomdata[browser]'") from exc
        playwright = sync_playwright().start()
        return playwright, playwright.chromium.launch(headless=self.headless)

    def _ensure_started(self) -> Any:
        if self.closed:
            raise RuntimeError("the browser session is closed")
        if self._browser is None:
            self._playwright, self._browser = self._launcher()
            logger.info("browser started (headless=%s)", self.headless)
        return self._browser

    @property
    def user_agent(self) -> str:
        """Chromium's own user agent, then the project's token (read once, from a blank page)."""
        if self._user_agent is None:
            browser = self._ensure_started()
            context = browser.new_context()
            try:
                page = context.new_page()
                base = page.evaluate("() => navigator.userAgent")
            finally:
                context.close()
            self._user_agent = f"{base} {self.product_token}".strip()
        return self._user_agent

    @contextmanager
    def page(self) -> Iterator[Any]:
        """A fresh context and page, both closed on exit, one tab only."""
        browser = self._ensure_started()
        context = browser.new_context(
            user_agent=self.user_agent, locale=self.locale, viewport=dict(DEFAULT_VIEWPORT)
        )
        try:
            yield context.new_page()
        finally:
            context.close()

    def close(self) -> None:
        """Close the browser and stop Playwright; safe to call twice."""
        if self.closed:
            return
        self.closed = True
        try:
            if self._browser is not None:
                self._browser.close()
        finally:
            if self._playwright is not None:
                self._playwright.stop()
            self._browser = self._playwright = None
