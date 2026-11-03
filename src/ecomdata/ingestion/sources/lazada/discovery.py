"""Lazada categories from a configured list.

Lazada publishes no category sitemap this project may rely on, so discovery is a
hand-kept JSON list (``categories.json`` next to this module, shipped with the package): the large
categories worth sampling, each a path like ``dien-thoai-di-dong``.
"""
from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

DEFAULT_PATH = Path(__file__).resolve().parent / "categories.json"


class LazadaConfiguredDiscovery:
    def __init__(self, fetch_text: Callable[[str], str] | None = None, *, path: Path | None = None):
        # ``fetch_text`` is the discovery interface every source shares; a configured list reads a file.
        self.path = Path(path) if path else DEFAULT_PATH

    def discover(self, *, limit: int | None = None) -> list[str]:
        document = json.loads(self.path.read_text(encoding="utf-8"))
        targets = [entry["target"] for entry in document["categories"]]
        if len(set(targets)) != len(targets):
            raise ValueError(f"{self.path} lists a category twice")
        return targets[:limit] if limit is not None else targets
