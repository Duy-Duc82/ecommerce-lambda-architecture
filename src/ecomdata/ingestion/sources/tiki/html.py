"""Tiki adapter that reads the public category page, not the listing API.

The listing API (``/api/personalish/v1/blocks/listings``) is an internal
endpoint; the source rule for this project is to crawl pages, so this adapter
fetches ``https://tiki.vn/<slug>/c<id>`` and parses the product list Next.js
embeds in ``<script id="__NEXT_DATA__">`` at ``props.initialState.catalog.data``.

Checked live on 2026-10-12: the rows carry the same field names as the API
(``id``, ``name``, ``url_key``, ``price``, ``original_price``, ``discount_rate``,
``quantity_sold``, ``rating_average``, ``review_count``, ``seller_id``,
``primary_category_path``), so the row-to-event mapping is inherited from
``TikiCrawler`` unchanged and offer/observation ids stay continuous with it.

Differences from the API adapter:
- A frontier target is the category path ``<slug>/c<id>``; ``/c<id>`` alone
  returns 404.
- Only page 1 exists: ``?page=2`` serves page 1 again (measured 2026-10-11),
  so coverage comes from more categories, not more pages.
- Sponsored rows (non-empty ``advertisement``) rotate between loads and are
  dropped, leaving the organic list (~40 rows).
- A rate-limited request gets HTTP 200 with a ~18 KB obfuscated JS page and no
  ``__NEXT_DATA__``; that fails the whole page so the worker retries later.
  Category pages took 1 request/s without that (2026-10-12); product pages
  did not, and this adapter never requests them.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from ecomdata.domain.acquisition import (
    AcquisitionStage,
    CanonicalRecordError,
    FetchResult,
    ListingPageParseError,
    ListingPageRequest,
    ParsedListingPage,
    RecordRejection,
)
from ecomdata.domain.observations import RawArtifact
from ecomdata.ingestion.sources.tiki.api import TikiCrawler

_NEXT_DATA = re.compile(rb'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)
_CATEGORY_PATH = re.compile(r"^[a-z0-9-]+/c\d+$")


class TikiCategoryHtmlCrawler(TikiCrawler):
    adapter_version = "tiki-category-html-v1"
    listing_url = "https://tiki.vn"

    def request_url(self, category: str, page: int = 1) -> str:
        if not _CATEGORY_PATH.match(category):
            raise ValueError(f"Tiki category target must look like '<slug>/c<id>', got {category!r}")
        if page != 1:
            raise ValueError("Tiki category pages are not paginated by URL; only page 1 exists")
        return f"{self.listing_url}/{category}"

    @staticmethod
    def _catalog_rows(body: bytes) -> list[Any]:
        match = _NEXT_DATA.search(body)
        if match is None:
            raise ListingPageParseError("Tiki category page has no __NEXT_DATA__ (challenge page or layout change)")
        try:
            decoded = json.loads(match.group(1).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ListingPageParseError("Tiki __NEXT_DATA__ is not valid UTF-8 JSON") from exc
        try:
            rows = decoded["props"]["initialState"]["catalog"]["data"]
        except (KeyError, TypeError) as exc:
            raise ListingPageParseError("Tiki __NEXT_DATA__ has no props.initialState.catalog.data") from exc
        if not isinstance(rows, list):
            raise ListingPageParseError("Tiki catalog.data must be a list")
        return rows

    def parse_listing_page(
        self,
        *,
        request: ListingPageRequest,
        fetch_result: FetchResult,
        raw_artifact: RawArtifact,
        crawl_run_id: str,
        produced_at: datetime,
    ) -> ParsedListingPage:
        """Parse only already-saved bytes into canonical observations."""
        rows = [
            row for row in self._catalog_rows(fetch_result.body)
            if not (isinstance(row, dict) and row.get("advertisement"))
        ]
        if not rows:
            raise ListingPageParseError("Tiki category page has no organic products")

        observations = []
        rejections = []
        seen_listing_ids: set[str] = set()
        duplicate_count = 0
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                rejections.append(
                    RecordRejection(index, AcquisitionStage.PARSE, ListingPageParseError("listing row must be an object"))
                )
                continue
            try:
                raw_id = self._text(row.get("id"), "id", required=True)
            except Exception:
                raw_id = None
            try:
                event = self._event_from_row(
                    row,
                    request=request,
                    raw_artifact=raw_artifact,
                    crawl_run_id=crawl_run_id,
                    produced_at=produced_at,
                    row_index=index,
                )
            except Exception as exc:
                wrapped = CanonicalRecordError(f"Tiki row {index} rejected: {exc}")
                wrapped.__cause__ = exc
                rejections.append(RecordRejection(index, AcquisitionStage.VALIDATION, wrapped, raw_id))
                continue
            listing_id = event.payload.offer.platform_listing_id
            if listing_id in seen_listing_ids:
                duplicate_count += 1
                continue
            seen_listing_ids.add(listing_id)
            observations.append(event)

        return ParsedListingPage(
            observations=tuple(observations),
            rejections=tuple(rejections),
            source_record_count=len(rows),
            duplicate_count=duplicate_count,
            last_page=1,
        )
