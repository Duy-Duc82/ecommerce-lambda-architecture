"""Bronze/raw retention: delete the raw artifacts older than the retention window.

Only ``bronze/marketplace/raw`` is pruned. Bronze/records, Silver and Gold are
never touched: the records keep every field of every row, so a raw body older
than the window is needed only to audit the extractor, not to rebuild data.

An artifact's age is the ``observed_date=`` partition of its path, so a listing
and one comparison decide it; nothing inside the window is deleted, and
``dry_run`` lists what would go.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from ecomdata.ingestion.raw_store import BRONZE_ZONE, RAW_ROOT


@dataclass(frozen=True)
class PruneResult:
    cutoff: date
    examined: int
    deleted: int
    kept: int
    dry_run: bool


def observed_date_of(key: str) -> date | None:
    """The ``observed_date=YYYY-MM-DD`` partition of an object key, or None if it has none."""
    for segment in key.split("/"):
        if segment.startswith("observed_date="):
            try:
                return date.fromisoformat(segment.split("=", 1)[1])
            except ValueError:
                return None
    return None


def prune_raw(
    store: Any, *, today: date, older_than_days: int, dry_run: bool = False, log: Callable[[str], None] = print
) -> PruneResult:
    """Delete raw objects whose date is more than ``older_than_days`` before ``today``.

    An object whose key carries no readable date is kept: age unknown is not age exceeded.
    """
    if older_than_days < 1:
        raise ValueError("older_than_days must be at least 1")
    cutoff = today - timedelta(days=older_than_days)
    examined = deleted = 0
    for key in store.list(BRONZE_ZONE, RAW_ROOT):
        examined += 1
        day = observed_date_of(key)
        if day is None or day >= cutoff:
            continue
        if not dry_run:
            store.delete(BRONZE_ZONE, key)
        deleted += 1
    log(
        f'{{"event": "prune_raw", "cutoff": "{cutoff.isoformat()}", "examined": {examined}, '
        f'"deleted": {deleted}, "dry_run": {str(dry_run).lower()}}}'
    )
    return PruneResult(cutoff, examined, deleted, examined - deleted, dry_run)
