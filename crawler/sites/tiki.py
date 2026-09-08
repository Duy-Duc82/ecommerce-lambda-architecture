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

import json
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlencode

import requests

from common.identity import make_seller_id
from config.marketplace_schema import (
    Availability,
    MarketplaceOffer,
    RawArtifact,
    ResourceType,
    create_marketplace_offer,
    create_observation_event,
    create_offer_observation,
)
from config.settings import CRAWL_HTTP_TIMEOUT_SECONDS
from crawler.base import SiteCrawler
from crawler.contracts import (
    AcquisitionStage,
    CanonicalRecordError,
    FetchResult,
    ListingPageParseError,
    ListingPageRequest,
    ParsedListingPage,
    RecordRejection,
)

logger = logging.getLogger(__name__)

LISTING_URL = "https://tiki.vn/api/personalish/v1/blocks/listings"
# 40 is the page size the site itself uses; asking for more is not honoured.
PAGE_SIZE = 40


class TikiCrawler(SiteCrawler):
    site_name = "tiki"
    marketplace_id = "marketplace-tiki"
    adapter_version = "tiki-listing-v1"
    base_url = "https://tiki.vn"
    default_currency = "VND"
    rating_scale = Decimal("5")

    def __init__(self, *args: Any, **kwargs: Any):
        # `clock` is deliberately NOT popped: SiteCrawler.__init__ accepts it
        # and assigns self._clock itself. Popping it here would leave the base
        # constructor with clock=None, which then overwrites the injected clock
        # and silently falls back to wall time — so fetched_at would ignore the
        # caller's clock and the Bronze hour= partition would follow whatever
        # clock the machine happened to have.
        self._http_get = kwargs.pop("http_get", None)
        self._monotonic = kwargs.pop("monotonic", time.monotonic)
        super().__init__(*args, **kwargs)
        # category id -> last page the endpoint reported, learned from page 1.
        self._last_page: dict[str, int] = {}

    def request_url(self, category: str, page: int = 1) -> str:
        """The listing API path, not tiki.vn/ — that is what robots.txt gates.

        Tiki's robots.txt currently allows this path (it disallows private
        paths like /api/v2/me/ and /v1/private/), but the check has to be made
        against this URL for that to mean anything.
        """
        return f"{LISTING_URL}?{urlencode({'category': category, 'page': page, 'limit': PAGE_SIZE})}"

    def _now(self) -> datetime:
        """The observation instant. Callers inject a clock to make runs replayable."""
        clock = getattr(self, "_clock", None) or (lambda: datetime.now(timezone.utc))
        return clock()

    def fetch_listing_page(self, request: ListingPageRequest) -> FetchResult:
        """Fetch one page as opaque bytes without decoding or raising on HTTP."""
        if request.marketplace_code != self.site_name:
            raise ValueError("request marketplace_code must be tiki")
        if request.resource_type is not ResourceType.LISTING_PAGE:
            raise ValueError("Tiki listing adapter accepts LISTING_PAGE only")

        request_url = self.request_url(request.target, request.page)
        get = getattr(self, "_http_get", None) or requests.get
        monotonic = getattr(self, "_monotonic", time.monotonic)
        started = monotonic()
        try:
            response = get(
                request_url,
                headers={"User-Agent": self.user_agent},
                timeout=CRAWL_HTTP_TIMEOUT_SECONDS,
            )
        except Exception:
            raise

        body = response.content
        fetched_at = self._now()
        elapsed = max(0, int(round((monotonic() - started) * 1000)))
        headers = getattr(response, "headers", {}) or {}
        response_url = getattr(response, "url", None) or request_url
        return FetchResult(
            request_url=response_url,
            fetched_at=fetched_at,
            http_status=response.status_code,
            content_type=headers.get("Content-Type") or headers.get("content-type"),
            body=body,
            retry_after=headers.get("Retry-After") or headers.get("retry-after"),
            elapsed_ms=elapsed,
        )

    @staticmethod
    def _decimal(value: Any, field_name: str, *, required: bool = False) -> Decimal | None:
        if value is None or value == "":
            if required:
                raise ValueError(f"{field_name} is required")
            return None
        if isinstance(value, bool) or isinstance(value, (dict, list, tuple)):
            raise ValueError(f"{field_name} must be numeric")
        try:
            parsed = Decimal(str(value).strip())
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"{field_name} must be a decimal") from exc
        if not parsed.is_finite() or parsed < 0:
            raise ValueError(f"{field_name} must be non-negative and finite")
        return parsed

    @staticmethod
    def _integer(value: Any, field_name: str) -> int | None:
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            raise ValueError(f"{field_name} must be an integer, not bool")
        if isinstance(value, int):
            if value < 0:
                raise ValueError(f"{field_name} must be non-negative")
            return value
        if isinstance(value, str) and value.strip().isdigit():
            return int(value.strip())
        raise ValueError(f"{field_name} must be an integer")

    @staticmethod
    def _text(value: Any, field_name: str, *, required: bool = False) -> str | None:
        if value is None:
            if required:
                raise ValueError(f"{field_name} is required")
            return None
        if not isinstance(value, str):
            if required and isinstance(value, (int, float)) and not isinstance(value, bool):
                value = str(value)
            else:
                raise ValueError(f"{field_name} must be text")
        value = value.strip()
        if not value and required:
            raise ValueError(f"{field_name} is required")
        return value or None

    @staticmethod
    def _availability(row: dict[str, Any]) -> Availability:
        availability = row.get("availability")
        shippable = row.get("shippable")
        if shippable is False or shippable == 0:
            return Availability.OUT_OF_STOCK
        if availability is False or availability == 0:
            return Availability.OUT_OF_STOCK
        if availability is True or availability == 1:
            return Availability.IN_STOCK
        return Availability.UNKNOWN

    def _event_from_row(
        self,
        row: dict[str, Any],
        *,
        request: ListingPageRequest,
        raw_artifact: RawArtifact,
        crawl_run_id: str,
        produced_at: datetime,
        row_index: int,
    ):
        listing_id = self._text(row.get("id"), "id", required=True)
        title = self._text(row.get("name"), "name", required=True)
        url_key = self._text(row.get("url_key"), "url_key", required=True)
        current_price = self._decimal(row.get("price"), "price", required=True)
        list_price = self._decimal(row.get("list_price"), "list_price")
        original_price = self._decimal(row.get("original_price"), "original_price")
        if list_price in (None, Decimal("0")):
            list_price = original_price if original_price not in (None, Decimal("0")) else None

        seller_source = row.get("seller_id")
        seller_platform_id = (
            self._text(seller_source, "seller_id", required=True)
            if seller_source is not None
            else None
        )
        seller_id = (
            make_seller_id(self.site_name, seller_platform_id)
            if seller_platform_id is not None
            else None
        )
        observed_at = raw_artifact.fetched_at
        offer = create_marketplace_offer(
            marketplace_code=self.site_name,
            marketplace_id=self.marketplace_id,
            platform_listing_id=listing_id,
            seller_id=seller_id,
            product_title=title,
            brand=self._text(row.get("brand_name"), "brand_name"),
            category_path=self._text(row.get("primary_category_path"), "primary_category_path") or request.target,
            source_url=f"{self.base_url}/{url_key.lstrip('/')}.html",
            currency=self.default_currency,
            first_seen_at=observed_at,
            last_seen_at=observed_at,
        )
        observation = create_offer_observation(
            marketplace_code=self.site_name,
            platform_listing_id=listing_id,
            offer_id=offer.offer_id,
            observed_at=observed_at,
            fetched_at=raw_artifact.fetched_at,
            current_price=current_price,
            raw_uri=raw_artifact.raw_uri,
            raw_sha256=raw_artifact.body_sha256,
            adapter_version=raw_artifact.adapter_version,
            crawl_run_id=crawl_run_id,
            list_price=list_price,
            discount_amount=self._decimal(row.get("discount"), "discount"),
            discount_percent=self._decimal(row.get("discount_rate"), "discount_rate"),
            rating_value=self._decimal(row.get("rating_average"), "rating_average"),
            rating_scale=self.rating_scale if row.get("rating_average") not in (None, "") else None,
            review_count=self._integer(row.get("review_count"), "review_count"),
            sold_count=self._sold_count(row),
            availability=self._availability(row),
            ranking_position=(request.page - 1) * PAGE_SIZE + row_index + 1,
        )
        return create_observation_event(
            marketplace_code=self.site_name,
            offer=offer,
            observation=observation,
            platform_listing_id=listing_id,
            produced_at=produced_at,
        )

    @classmethod
    def _sold_count(cls, row: dict[str, Any]) -> int | None:
        value = row.get("quantity_sold")
        if value is None:
            return None
        if not isinstance(value, dict):
            raise ValueError("quantity_sold must be an object")
        return cls._integer(value.get("value"), "quantity_sold.value")

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
        try:
            decoded = json.loads(fetch_result.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
            raise ListingPageParseError("Tiki response is not valid UTF-8 JSON") from exc
        if not isinstance(decoded, dict):
            raise ListingPageParseError("Tiki response top level must be an object")
        rows = decoded.get("data")
        if not isinstance(rows, list):
            raise ListingPageParseError("Tiki response data must be a list")
        paging = decoded.get("paging")
        if paging is not None and not isinstance(paging, dict):
            raise ListingPageParseError("Tiki response paging must be an object")
        last_page = None
        if paging is not None and "last_page" in paging:
            value = paging["last_page"]
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ListingPageParseError("Tiki paging.last_page must be a positive integer")
            last_page = value

        observations = []
        rejections = []
        seen_listing_ids: set[str] = set()
        duplicate_count = 0
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                rejections.append(
                    RecordRejection(
                        index,
                        AcquisitionStage.PARSE,
                        ListingPageParseError("listing row must be an object"),
                    )
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
                rejections.append(
                    RecordRejection(
                        index,
                        AcquisitionStage.VALIDATION,
                        wrapped,
                        raw_id,
                    )
                )
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
            last_page=last_page,
        )

    def fetch_listing(self, category: str, page: int = 1) -> list[dict[str, Any]]:
        """Fetch one listing page. Empty list once past the reported last page.

        Pagination was measured live (2026-08-16): pages 1/2/3/50 of category
        1846 shared zero products, and ``paging.last_page`` caps most categories
        at 50 (total=2000). Returning [] past that cap keeps ``crawl()`` from
        spending requests on pages the site will not serve.
        """
        last_page = self._last_page.get(category)
        if last_page is not None and page > last_page:
            return []

        response = requests.get(
            LISTING_URL,
            params={"category": category, "page": page, "limit": PAGE_SIZE},
            headers={"User-Agent": self.user_agent},
            timeout=10,
        )
        response.raise_for_status()
        body = response.json()

        reported = (body.get("paging") or {}).get("last_page")
        if isinstance(reported, int) and reported > 0:
            self._last_page[category] = reported
        elif reported is not None:
            logger.debug("tiki: unusable paging.last_page=%r for category %s", reported, category)

        return body.get("data", [])

    def parse_product(self, raw: dict[str, Any], category: str) -> dict[str, Any]:
        price = raw.get("price")
        list_price = raw.get("list_price") or raw.get("original_price") or price
        product_id = raw.get("id")
        url_key = raw.get("url_key")
        seller_id = raw.get("seller_id")

        return {
            "product_id": str(product_id) if product_id is not None else "",
            "product_name": raw.get("name") or "",
            # The listing echoes the full ancestry ("1/2/1846/8095/2458"); the
            # queried category is only its root, so prefer the payload's path.
            "category_path": raw.get("primary_category_path") or category,
            "brand": raw.get("brand_name") or "",
            "price": price,
            "list_price": list_price,
            "currency": "VND",
            "rating": raw.get("rating_average"),
            "review_count": raw.get("review_count"),
            "seller_name": f"seller:{seller_id}" if seller_id is not None else "",
            "in_stock": bool(raw.get("availability", 1)) and raw.get("shippable", True) is not False,
            # url_key already ends in "-p<id>", so only ".html" is appended.
            # Re-adding the id gave "-p<id>-p<id>.html": Tiki still resolves it
            # off the trailing id, but it is not the canonical URL, so stored
            # snapshots would not join against links captured anywhere else.
            "url": f"https://tiki.vn/{url_key}.html" if url_key else "",
        }
