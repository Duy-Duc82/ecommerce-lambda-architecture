"""HTTP for the crawler: one client, robots.txt per host, a throttle per host.

* :class:`HttpClient` wraps one ``requests.Session``: fixed User-Agent, the
  configured timeout on every request, no exception on an HTTP status.
* :class:`RobotsPolicy` fetches ``/robots.txt`` lazily, the first time a host
  is asked about, and keeps the parsed rules per host for ``ttl_seconds``.
  An unreadable file (network error, 401/403) denies; a missing one (404)
  allows. Blank lines are dropped before parsing so groups follow RFC 9309,
  not the 1994 draft ``RobotFileParser`` implements.
* :class:`Throttle` keeps a minimum interval, plus jitter, between two
  requests to the same host. Clock and sleep are injected.
"""
from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib import robotparser
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HttpResponse:
    url: str
    status_code: int
    headers: Mapping[str, str]
    content: bytes
    elapsed_ms: int

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", "replace")

    def header(self, name: str) -> str | None:
        lowered = name.lower()
        return next((value for key, value in self.headers.items() if key.lower() == lowered), None)


class HttpClient:
    def __init__(
        self,
        *,
        user_agent: str,
        timeout_seconds: float,
        session: Any = None,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self._session = session
        self._monotonic = monotonic

    @property
    def session(self) -> Any:
        if self._session is None:
            import requests

            self._session = requests.Session()
        return self._session

    def get(self, url: str, *, headers: Mapping[str, str] | None = None) -> HttpResponse:
        started = self._monotonic()
        response = self.session.get(
            url, headers={"User-Agent": self.user_agent, **(headers or {})}, timeout=self.timeout_seconds
        )
        elapsed = max(0, int(round((self._monotonic() - started) * 1000)))
        return HttpResponse(
            url=getattr(response, "url", None) or url,
            status_code=int(response.status_code),
            headers=dict(getattr(response, "headers", {}) or {}),
            content=response.content,
            elapsed_ms=elapsed,
        )

    def close(self) -> None:
        if self._session is not None:
            self._session.close()


def robots_url_for(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}/robots.txt"


def parse_robots(text: str, url: str = "") -> robotparser.RobotFileParser:
    """RFC 9309 grouping: only a ``User-agent`` line after rules starts a group.

    ``RobotFileParser`` ends a record at a blank line, so every rule after the
    first blank line would be dropped (Tiki's file has one at line 108).
    """
    parser = robotparser.RobotFileParser()
    if url:
        parser.set_url(url)
    parser.parse([line for line in text.splitlines() if line.strip()])
    return parser


class RobotsPolicy:
    """Whether a URL may be fetched, by its host's robots.txt (cached, with TTL)."""

    def __init__(
        self,
        client: HttpClient | None,
        *,
        user_agent: str,
        ttl_seconds: float = 3600,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.client = client
        self.user_agent = user_agent
        self.ttl_seconds = ttl_seconds
        self._clock = clock
        self._rules: dict[str, tuple[float, robotparser.RobotFileParser]] = {}
        self.fetches = 0

    @classmethod
    def deny_all(cls, user_agent: str = "*") -> RobotsPolicy:
        """A policy that fetches nothing and allows nothing (parse-only tools)."""
        return cls(None, user_agent=user_agent)

    def _load(self, robots_url: str) -> robotparser.RobotFileParser:
        parser = robotparser.RobotFileParser()
        parser.set_url(robots_url)
        if self.client is None:
            parser.disallow_all = True
            return parser
        self.fetches += 1
        try:
            response = self.client.get(robots_url)
        except Exception as exc:
            logger.warning("could not read %s (%s); treating the host as disallowed", robots_url, exc)
            parser.disallow_all = True
            return parser
        if response.status_code in (401, 403):
            parser.disallow_all = True
        elif response.status_code >= 400:
            # No robots.txt means no restriction.
            parser.allow_all = True
        else:
            parser = parse_robots(response.text, robots_url)
        return parser

    def rules_for(self, url: str) -> robotparser.RobotFileParser:
        robots_url = robots_url_for(url)
        now = self._clock()
        cached = self._rules.get(robots_url)
        if cached is None or now - cached[0] >= self.ttl_seconds:
            cached = (now, self._load(robots_url))
            self._rules[robots_url] = cached
        return cached[1]

    def allowed(self, url: str) -> bool:
        try:
            return bool(self.rules_for(url).can_fetch(self.user_agent, url))
        except Exception:
            return False


class Throttle:
    """At least ``min_interval`` (+ jitter) seconds between two requests to one host."""

    def __init__(
        self,
        *,
        min_interval_seconds: float,
        jitter_seconds: float = 0.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        jitter_source: Callable[[float, float], float] = random.uniform,
    ):
        if min_interval_seconds < 0 or jitter_seconds < 0:
            raise ValueError("intervals must not be negative")
        self.min_interval_seconds = min_interval_seconds
        self.jitter_seconds = jitter_seconds
        self._sleep = sleep
        self._clock = clock
        self._jitter = jitter_source
        self._last: dict[str, float] = {}

    def wait(self, url: str, *, min_interval_seconds: float | None = None) -> float:
        """Sleep until this host may be requested again; return the seconds slept."""
        host = urlparse(url).netloc
        interval = max(self.min_interval_seconds, min_interval_seconds or 0.0)
        interval += self._jitter(0, self.jitter_seconds) if self.jitter_seconds else 0.0
        last = self._last.get(host)
        slept = 0.0
        if last is not None:
            remaining = interval - (self._clock() - last)
            if remaining > 0:
                self._sleep(remaining)
                slept = remaining
        self._last[host] = self._clock()
        return slept
