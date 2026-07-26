"""Tiki adapter — public listing JSON API, no JS rendering required.

Field mapping was checked against a live response from
``https://tiki.vn/api/personalish/v1/blocks/listings`` (2026-07-26); Tiki does
not publish a stable API contract, so re-verify field names against a fresh
response before relying on this in production — a shape change here fails
loudly via ``normalize_price_snapshot`` (missing product_id/price), it does
not silently corrupt data.

Known gap: the listing endpoint exposes ``seller_id`` but not a seller display
name; a real seller_name requires an extra per-product detail call, which is
deliberately out of scope for the first cut (one request per product would
blow the rate-limit budget for a listing-sized crawl).
"""

from __future__ import annotations

from typing import Any

import requests

from crawler.base import SiteCrawler

LISTING_URL = "https://tiki.vn/api/personalish/v1/blocks/listings"
PAGE_SIZE = 40


class TikiCrawler(SiteCrawler):
    site_name = "tiki"
    base_url = "https://tiki.vn"

    def fetch_listing(self, category: str) -> list[dict[str, Any]]:
        response = requests.get(
            LISTING_URL,
            params={"category": category, "page": 1, "limit": PAGE_SIZE},
            headers={"User-Agent": self.user_agent},
            timeout=10,
        )
        response.raise_for_status()
        return response.json().get("data", [])

    def parse_product(self, raw: dict[str, Any], category: str) -> dict[str, Any]:
        price = raw.get("price")
        list_price = raw.get("list_price") or raw.get("original_price") or price
        product_id = raw.get("id")
        url_key = raw.get("url_key")
        seller_id = raw.get("seller_id")

        return {
            "product_id": str(product_id) if product_id is not None else "",
            "product_name": raw.get("name") or "",
            "category_path": category,
            "brand": raw.get("brand_name") or "",
            "price": price,
            "list_price": list_price,
            "currency": "VND",
            "rating": raw.get("rating_average"),
            "review_count": raw.get("review_count"),
            "seller_name": f"seller:{seller_id}" if seller_id is not None else "",
            "in_stock": bool(raw.get("availability", 1)) and raw.get("shippable", True) is not False,
            "url": f"https://tiki.vn/{url_key}-p{product_id}.html" if url_key and product_id else "",
        }
