"""An offline stand-in for the Tiki listing API — Phase 8 plan section 8.

The smoke and every drill point ``TIKI_LISTING_URL`` at this server, so no
Phase 8 automation ever contacts the live marketplace. It is built on the
standard library only.

What it serves:

- ``/robots.txt``, allowing everything;
- any other GET as a listing page, built from the frozen Tiki fixture
  ``tests/fixtures/tiki_listing_sample.json``;
- ``POST /_stub/mode`` with a body of ``ok``, ``429``, ``500``, ``timeout`` or
  ``drift``, which switches every later listing response;
- ``GET /_stub/state``: the mode and the serve counters, for drills.

Determinism. Each ``(category, page)`` gets its own listing IDs, a hash of
the fixture ID, the category and the page, so every page holds distinct
offers at any scale. Each successful serve of a page advances that page's counter, and the
served price is the fixture price scaled by ``PRICE_STEPS[counter % 4]``.
The second serve is a small cut (``PRICE_CHANGED``), the third a cut deep
enough for ``LARGE_PRICE_DROP``, the fourth a return to the fixture price.
Nothing is random, and the bytes differ between serves, so each serve yields
new observations.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

FIXTURE = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "tiki_listing_sample.json"
MODES = ("ok", "429", "500", "timeout", "drift")
# Percent of the fixture price, by serve counter modulo 4. 95 is a plain
# PRICE_CHANGED; 60 is a 40 % cut, beyond MARKETPLACE_LARGE_DROP_RELATIVE
# (0.20), which on its own makes it a LARGE_PRICE_DROP.
PRICE_STEPS = (100, 95, 60, 100)
RETRY_AFTER_SECONDS = 1


def listing_id(fixture_id: int, category: str, page: int, copy: int = 0) -> int:
    """Distinct per (fixture row, category, page, copy), and stable across runs.

    ``copy`` numbers the repetitions of a fixture row on a page wider than the
    fixture (``rows_per_page``). Copy 0 keeps the ID it always had.

    A 60-bit slice of a SHA-256. The Phase 8 arithmetic packed the category
    into ``crc32 % 100``, so two categories in one bucket, or a page past 99,
    shared IDs once a load run went beyond the smoke's universe.
    """
    key = f"{fixture_id}:{category}:{page}" + (f":{copy}" if copy else "")
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(digest[:15], 16)


@dataclass
class StubState:
    rows: list[dict[str, Any]]
    last_page: int = 2
    # Rows per listing page; None serves the fixture once, as Phase 8 did.
    # Wider pages repeat the fixture, the invalid row included, with new IDs.
    rows_per_page: int | None = None
    timeout_seconds: float = 0.0
    mode: str = "ok"
    served: dict[str, int] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def set_mode(self, mode: str) -> None:
        if mode not in MODES:
            raise ValueError(f"unknown stub mode {mode!r}; expected one of {', '.join(MODES)}")
        with self.lock:
            self.mode = mode

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {"mode": self.mode, "served": dict(sorted(self.served.items()))}


def load_state(path: Path = FIXTURE, *, last_page: int = 2, timeout_seconds: float = 0.0,
               rows_per_page: int | None = None) -> StubState:
    rows = json.loads(path.read_text(encoding="utf-8"))["data"]
    if not rows:
        raise ValueError(f"{path} holds no listing rows")
    if rows_per_page is not None and rows_per_page < 1:
        raise ValueError("rows_per_page must be at least 1")
    return StubState(rows=rows, last_page=last_page, timeout_seconds=timeout_seconds, rows_per_page=rows_per_page)


def _scaled(value: Any, percent: int) -> Any:
    return value * percent // 100 if isinstance(value, int) and not isinstance(value, bool) and value > 0 else value


def listing_page(state: StubState, category: str, page: int) -> tuple[int, dict[str, str], bytes]:
    """The response to one listing request, as (status, headers, body).

    Pure apart from the state it advances, which is what tests drive directly.
    """
    with state.lock:
        mode = state.mode
    if mode == "429":
        return 429, {"Retry-After": str(RETRY_AFTER_SECONDS), "Content-Type": "application/json"}, b'{"error":"rate limited"}'
    if mode == "500":
        return 500, {"Content-Type": "application/json"}, b'{"error":"internal"}'
    if mode == "timeout":
        # Past the client's timeout; whatever follows is never read.
        time.sleep(state.timeout_seconds)
    if mode == "drift":
        # A shape the adapter must reject: data is no longer a list.
        body = {"data": {"items": state.rows}, "paging": {"last_page": state.last_page}}
        return 200, {"Content-Type": "application/json"}, json.dumps(body).encode("utf-8")

    key = f"{category}/{page}"
    with state.lock:
        counter = state.served.get(key, 0)
        state.served[key] = counter + 1
    percent = PRICE_STEPS[counter % len(PRICE_STEPS)]
    rows = []
    width = state.rows_per_page or len(state.rows)
    if 1 <= page <= state.last_page:
        for index in range(width):
            row = state.rows[index % len(state.rows)]
            served = dict(row)
            # The fixture's deliberately invalid row has no id; it stays invalid.
            if isinstance(row.get("id"), int):
                served["id"] = listing_id(row["id"], category, page, index // len(state.rows))
            served["price"] = _scaled(row.get("price"), percent)
            rows.append(served)
    body = {"data": rows, "paging": {"current_page": page, "last_page": state.last_page, "per_page": width}}
    return 200, {"Content-Type": "application/json"}, json.dumps(body, ensure_ascii=False, sort_keys=True).encode("utf-8")


def make_handler(state: StubState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def _send(self, status: int, headers: dict[str, str], body: bytes) -> None:
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - the http.server API
            parsed = urlparse(self.path)
            if parsed.path == "/robots.txt":
                self._send(200, {"Content-Type": "text/plain"}, b"User-agent: *\nAllow: /\n")
            elif parsed.path == "/_stub/state":
                self._send(200, {"Content-Type": "application/json"}, json.dumps(state.snapshot()).encode("utf-8"))
            else:
                query = parse_qs(parsed.query)
                category = query.get("category", [""])[0]
                try:
                    page = int(query.get("page", ["1"])[0])
                except ValueError:
                    page = 0
                self._send(*listing_page(state, category, page))

        def do_POST(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/_stub/mode":
                self._send(404, {"Content-Type": "text/plain"}, b"not found")
                return
            raw = self.rfile.read(int(self.headers.get("Content-Length") or 0)).decode("utf-8").strip()
            mode = json.loads(raw).get("mode", "") if raw.startswith("{") else raw
            try:
                state.set_mode(mode)
            except ValueError as error:
                self._send(400, {"Content-Type": "text/plain"}, str(error).encode("utf-8"))
                return
            self._send(200, {"Content-Type": "application/json"}, json.dumps(state.snapshot()).encode("utf-8"))

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            print(json.dumps({"event": "stub_request", "line": format % args}), flush=True)

    return Handler


def serve(state: StubState, *, host: str = "0.0.0.0", port: int = 8000) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), make_handler(state))


def main() -> None:
    from config.settings import CRAWL_HTTP_TIMEOUT_SECONDS

    parser = argparse.ArgumentParser(description="Offline Tiki listing stub for the smoke and the drills")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--last-page", type=int, default=2)
    parser.add_argument("--rows-per-page", type=int, default=None,
                        help="repeat the fixture to this many rows per page (default: the fixture once)")
    args = parser.parse_args()
    state = load_state(last_page=args.last_page, timeout_seconds=CRAWL_HTTP_TIMEOUT_SECONDS + 2,
                       rows_per_page=args.rows_per_page)
    server = serve(state, port=args.port)
    print(json.dumps({"event": "stub_listening", "port": args.port}), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
