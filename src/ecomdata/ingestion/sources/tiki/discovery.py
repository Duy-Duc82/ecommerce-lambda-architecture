"""Tiki category discovery from the sitemap named in robots.txt."""
from __future__ import annotations

import re
from collections.abc import Callable
from urllib.parse import urlparse

_LOC = re.compile(r"<loc>\s*(.*?)\s*</loc>", re.S)
_CATEGORY_URL = re.compile(r"^https://tiki\.vn/([a-z0-9-]+/c(\d+))$")
SITEMAP_INDEX = "https://tiki.vn/clover/sitemap_index.xml"


def sitemap_locations(xml: str) -> list[str]:
    return _LOC.findall(xml)


class TikiSitemapDiscovery:
    """Walks sitemap index -> categories index -> category sitemaps.

    ``fetch_text`` fetches one URL as text and is where robots.txt and the
    politeness delay are applied, so this class only reads sitemaps.
    """

    def __init__(self, fetch_text: Callable[[str], str], *, sitemap_index: str = SITEMAP_INDEX):
        self.fetch_text = fetch_text
        self.sitemap_index = sitemap_index

    def discover(self, *, limit: int | None = None) -> list[str]:
        """Category targets ``<slug>/c<id>``, ascending by id (the broad top-level ones first)."""
        categories_index = next(
            (loc for loc in sitemap_locations(self.fetch_text(self.sitemap_index)) if "sitemap_categories" in loc), None
        )
        if categories_index is None:
            raise ValueError("the sitemap index lists no categories sitemap")
        found: dict[int, str] = {}
        for sitemap in sitemap_locations(self.fetch_text(categories_index)):
            if urlparse(sitemap).hostname != urlparse(self.sitemap_index).hostname:
                continue
            for loc in sitemap_locations(self.fetch_text(sitemap)):
                match = _CATEGORY_URL.match(loc)
                if match:
                    found.setdefault(int(match.group(2)), match.group(1))
        targets = [found[category_id] for category_id in sorted(found)]
        return targets[:limit] if limit is not None else targets
