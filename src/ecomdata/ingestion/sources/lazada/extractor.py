"""Lazada: the listing the category page asks for by itself.

``https://www.lazada.vn/<category>/?page=N`` renders in a browser and, while it
does, requests the same URL with ``ajax=true`` (matched by the spec's
``capture_patterns``). That response is JSON: ``mods.listItems`` holds the
product rows, every field as Lazada sent it, sponsored rows included (marked at
Silver); ``mods.filter``, ``mods.breadcrumb``, ``mods.sortBar``, ``mainInfo``
and ``seoInfo`` are the page context. ``page=N`` carries different rows.

``mainInfo`` also holds the visitor's anonymous identifier; the spec lists it
as personal data, so it is dropped (and named) when the records are built.
"""
from __future__ import annotations

import json
import math
import re
from typing import Any

from ecomdata.domain.acquisition import ExtractedPage, FetchBundle, ListingPageParseError

_CATEGORY_PATH = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*(?:/[a-z0-9]+(?:-[a-z0-9]+)*)*$")
# Lazada serves at most this many pages of one listing.
MAX_PAGES = 102
MODS_CONTEXT = ("filter", "breadcrumb", "sortBar")
TOP_CONTEXT = ("mainInfo", "seoInfo")


class LazadaExtractor:
    def request_url(self, base_url: str, target: str, page: int = 1) -> str:
        if not _CATEGORY_PATH.match(target):
            raise ValueError(f"Lazada category target must be a path like 'dien-thoai-di-dong', got {target!r}")
        if not 1 <= page <= MAX_PAGES:
            raise ValueError(f"Lazada serves pages 1 to {MAX_PAGES}, got {page}")
        return f"{base_url.rstrip('/')}/{target}/?page={page}"

    @staticmethod
    def _listing_response(bundle: FetchBundle) -> dict[str, Any]:
        for part in bundle.captured:
            try:
                decoded = json.loads(part.body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(decoded, dict) and isinstance(decoded.get("mods"), dict) and "listItems" in decoded["mods"]:
                return decoded
        raise ListingPageParseError(
            f"none of the {len(bundle.captured)} captured responses holds mods.listItems (layout change or no listing)"
        )

    @staticmethod
    def _last_page(main_info: Any) -> int | None:
        if not isinstance(main_info, dict):
            return None
        try:
            total, size = int(main_info["totalResults"]), int(main_info["pageSize"])
        except (KeyError, TypeError, ValueError):
            return None
        if size <= 0:
            return None
        return max(1, min(MAX_PAGES, math.ceil(total / size)))

    def extract(self, bundle: FetchBundle) -> ExtractedPage:
        decoded = self._listing_response(bundle)
        rows = decoded["mods"]["listItems"]
        if not isinstance(rows, list):
            raise ListingPageParseError("Lazada mods.listItems must be a list")
        if not all(isinstance(row, dict) for row in rows):
            raise ListingPageParseError("Lazada mods.listItems holds a row that is not an object")
        mods = decoded["mods"]
        context: dict[str, Any] = {"mods": {key: mods[key] for key in MODS_CONTEXT if key in mods}}
        context.update({key: decoded[key] for key in TOP_CONTEXT if key in decoded})
        return ExtractedPage(rows=tuple(rows), page_context=context, last_page=self._last_page(decoded.get("mainInfo")))
