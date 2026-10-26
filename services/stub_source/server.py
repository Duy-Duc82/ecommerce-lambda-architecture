"""An offline stand-in for the Tiki category pages.

The smoke, the drills, the bench and the demo point ``TIKI_BASE_URL`` at this
server, so none of that automation contacts the live marketplace. It is built on
the standard library only and reads nothing from ``tests/``: its fixture is
``fixture.json`` beside this file.

What it serves:

- ``/robots.txt``, allowing everything;
- ``/<slug>/c<id>`` as a Tiki category page: HTML holding the
  ``__NEXT_DATA__`` script with ``props.initialState.catalog``, built from the
  fixture rows and page context;
- ``POST /_stub/mode`` with a body of ``ok``, ``429``, ``500``, ``timeout``,
  ``drift`` or ``block``, which switches every later page response;
- ``GET /_stub/state``: the mode and the serve counters, for drills.

Determinism. Each category gets its own listing IDs, a hash of the fixture ID
and the category, so every category holds distinct offers at any scale. Each
successful serve of a category advances its counter, and the served price is the
fixture price scaled by ``PRICE_STEPS[counter % 4]``: the second serve is a small
cut, the third a cut deep enough for ``LARGE_PRICE_DROP``, the fourth a return to
the fixture price. Nothing is random, and the bytes differ between serves, so
each serve yields new observations.
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
from urllib.parse import urlparse

FIXTURE = Path(__file__).resolve().parent / "fixture.json"
MODES = ("ok", "429", "500", "timeout", "drift", "block")
# Percent of the fixture price, by serve counter modulo 4. 95 is a plain price
# change; 60 is a 40 % cut, beyond the large-drop relative threshold (0.20).
PRICE_STEPS = (100, 95, 60, 100)
RETRY_AFTER_SECONDS = 1
BLOCK_PAGE = b"<html><head><title>Checking</title></head><body><script>var _0x1f2e=['challenge'];</script></body></html>"


def listing_id(fixture_id: int, category: str, page: int = 1, copy: int = 0) -> int:
    """Distinct per (fixture row, category, copy), and stable across runs.

    ``copy`` numbers the repetitions of a fixture row on a page wider than the
    fixture (``rows_per_page``). Copy 0 leaves ``copy`` out of the hash key, so
    widening a page does not change the IDs of the rows already on it.

    A 60-bit slice of a SHA-256: above 2**53, so only this stub's JSON carries
    the ID as a number; Python reads it exactly and Silver stores it as text.
    """
    key = f"{fixture_id}:{category}:{page}" + (f":{copy}" if copy else "")
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(digest[:15], 16)


@dataclass
class StubState:
    rows: list[dict[str, Any]]
    page_context: dict[str, Any] = field(default_factory=dict)
    # Rows per category page; None serves the fixture once. Wider pages repeat
    # the fixture, the invalid row included, with new IDs.
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


def load_state(path: Path = FIXTURE, *, timeout_seconds: float = 0.0, rows_per_page: int | None = None) -> StubState:
    fixture = json.loads(path.read_text(encoding="utf-8"))
    rows = fixture["rows"]
    if not rows:
        raise ValueError(f"{path} holds no listing rows")
    if rows_per_page is not None and rows_per_page < 1:
        raise ValueError("rows_per_page must be at least 1")
    return StubState(
        rows=rows,
        page_context=fixture.get("page_context", {}),
        timeout_seconds=timeout_seconds,
        rows_per_page=rows_per_page,
    )


def _scaled(value: Any, percent: int) -> Any:
    return value * percent // 100 if isinstance(value, int) and not isinstance(value, bool) and value > 0 else value


def next_data_page(catalog: dict[str, Any]) -> bytes:
    document = {"props": {"initialState": {"catalog": catalog}}, "page": "/category", "query": {}}
    html = (
        '<!DOCTYPE html><html lang="vi"><head><meta charset="utf-8"><title>Stub category</title></head><body>'
        '<div id="__next"></div><script id="__NEXT_DATA__" type="application/json">'
        + json.dumps(document, ensure_ascii=False, sort_keys=True)
        + "</script></body></html>"
    )
    return html.encode("utf-8")


def listing_page(state: StubState, category: str, page: int = 1) -> tuple[int, dict[str, str], bytes]:
    """The response to one category request, as (status, headers, body).

    Pure apart from the state it advances, which is what tests drive directly.
    """
    with state.lock:
        mode = state.mode
    if mode == "429":
        return (
            429,
            {"Retry-After": str(RETRY_AFTER_SECONDS), "Content-Type": "application/json"},
            b'{"error":"rate limited"}',
        )
    if mode == "500":
        return 500, {"Content-Type": "application/json"}, b'{"error":"internal"}'
    if mode == "timeout":
        # Past the client's timeout; whatever follows is never read.
        time.sleep(state.timeout_seconds)
    if mode == "block":
        # HTTP 200 and no __NEXT_DATA__: how Tiki answers a rate-limited client.
        return 200, {"Content-Type": "text/html; charset=utf-8"}, BLOCK_PAGE
    if mode == "drift":
        # A shape the extractor must reject: data is no longer a list.
        body = next_data_page({**state.page_context, "data": {"items": state.rows}})
        return 200, {"Content-Type": "text/html; charset=utf-8"}, body

    with state.lock:
        counter = state.served.get(f"{category}/1", 0)
        state.served[f"{category}/1"] = counter + 1
    percent = PRICE_STEPS[counter % len(PRICE_STEPS)]
    width = state.rows_per_page or len(state.rows)
    rows = []
    for index in range(width):
        row = state.rows[index % len(state.rows)]
        served = dict(row)
        # The fixture's deliberately invalid row has no id; it stays invalid.
        if isinstance(row.get("id"), int):
            served["id"] = listing_id(row["id"], category, 1, index // len(state.rows))
        if "price" in row:
            served["price"] = _scaled(row["price"], percent)
        rows.append(served)
    return 200, {"Content-Type": "text/html; charset=utf-8"}, next_data_page({**state.page_context, "data": rows})


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
            path = urlparse(self.path).path
            if path == "/robots.txt":
                self._send(200, {"Content-Type": "text/plain"}, b"User-agent: *\nAllow: /\n")
            elif path == "/_stub/state":
                self._send(200, {"Content-Type": "application/json"}, json.dumps(state.snapshot()).encode("utf-8"))
            else:
                self._send(*listing_page(state, path.strip("/")))

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


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Offline Tiki category-page stub for the smoke and the drills")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--rows-per-page",
        type=int,
        default=None,
        help="repeat the fixture to this many rows per category page (default: the fixture once)",
    )
    parser.add_argument(
        "--timeout-seconds", type=float, default=12.0, help="how long the timeout mode sleeps (past the client's timeout)"
    )
    args = parser.parse_args(argv)
    state = load_state(timeout_seconds=args.timeout_seconds, rows_per_page=args.rows_per_page)
    server = serve(state, port=args.port)
    print(json.dumps({"event": "stub_listening", "port": args.port}), flush=True)
    server.serve_forever()
