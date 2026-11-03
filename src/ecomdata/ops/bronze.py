"""Reading Bronze/records from an operator's side: the listing rows of stored pages."""
from __future__ import annotations

import gzip
import json
from collections.abc import Iterator
from typing import Any


def listing_rows(src: Any, root: str, *, since_day: str | None = None) -> Iterator[dict[str, Any]]:
    """Every ``listing_row`` record under ``root`` (the Bronze/records URI; ``observed_date`` partitions before ``since_day`` skipped)."""
    for uri in src.list_objects(root):
        day = next((part.split("=", 1)[1] for part in uri.split("/") if part.startswith("observed_date=")), None)
        if since_day is not None and day is not None and day < since_day:
            continue
        payload = src.read_object(uri)
        if payload is None:
            continue
        for line in gzip.decompress(payload).decode("utf-8").splitlines():
            record = json.loads(line)
            if record.get("record_kind") == "listing_row":
                yield record
