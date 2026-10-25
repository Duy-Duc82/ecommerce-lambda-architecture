"""Tiki: the product list Next.js embeds in the public category page.

``https://tiki.vn/<slug>/c<id>`` carries ``<script id="__NEXT_DATA__">`` whose
``props.initialState.catalog.data`` is the product list, every field as Tiki
sent it, advertisements included (they are marked at Silver). Next to it sit the
category, filters, sellers, paging and sort options: the page context.

Measured 2026-10-11/12: only page 1 exists (``?page=2`` serves page 1 again), so
coverage comes from more categories. A rate-limited request gets HTTP 200 and an
~18 KB obfuscated script with no ``__NEXT_DATA__``; that is a block, which backs
the crawl off instead of failing the page for good.
"""
from __future__ import annotations

import json
import re
from typing import Any

from ecomdata.domain.acquisition import ExtractedPage, FetchBlockedError, FetchBundle, ListingPageParseError

_NEXT_DATA = re.compile(rb'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)
_CATEGORY_PATH = re.compile(r"^[a-z0-9-]+/c\d+$")

# catalog keys that describe the page rather than a product.
PAGE_CONTEXT_KEYS = (
    "categoryData",
    "filters",
    "sellers",
    "paging",
    "sort_options",
    "amplitude",
    "metaTitle",
    "h1",
)


class TikiExtractor:
    def request_url(self, base_url: str, target: str, page: int = 1) -> str:
        if not _CATEGORY_PATH.match(target):
            raise ValueError(f"Tiki category target must look like '<slug>/c<id>', got {target!r}")
        if page != 1:
            raise ValueError("Tiki category pages are not paginated by URL; only page 1 exists")
        return f"{base_url.rstrip('/')}/{target}"

    @staticmethod
    def _catalog(body: bytes) -> dict[str, Any]:
        match = _NEXT_DATA.search(body)
        if match is None:
            raise FetchBlockedError(
                "Tiki category page has no __NEXT_DATA__ (rate-limit challenge page or layout change)",
                evidence=body[:200].decode("utf-8", "replace"),
            )
        try:
            decoded = json.loads(match.group(1).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ListingPageParseError("Tiki __NEXT_DATA__ is not valid UTF-8 JSON") from exc
        try:
            catalog = decoded["props"]["initialState"]["catalog"]
        except (KeyError, TypeError) as exc:
            raise ListingPageParseError("Tiki __NEXT_DATA__ has no props.initialState.catalog") from exc
        if not isinstance(catalog, dict):
            raise ListingPageParseError("Tiki catalog must be an object")
        return catalog

    def extract(self, bundle: FetchBundle) -> ExtractedPage:
        catalog = self._catalog(bundle.body)
        rows = catalog.get("data")
        if not isinstance(rows, list):
            raise ListingPageParseError("Tiki catalog.data must be a list")
        if not rows:
            raise ListingPageParseError("Tiki category page has no products")
        if not all(isinstance(row, dict) for row in rows):
            raise ListingPageParseError("Tiki catalog.data holds a row that is not an object")
        context = {key: catalog[key] for key in PAGE_CONTEXT_KEYS if key in catalog}
        return ExtractedPage(rows=tuple(rows), page_context=context, last_page=1)
