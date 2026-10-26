"""Bronze/raw retention: only what is older than the window goes."""
from __future__ import annotations

from datetime import date

import pytest

from ecomdata.ops import retention
from ecomdata.processing.batch.memory import MemoryStore

TODAY = date(2026, 10, 25)


def raw_key(day, name="a"):
    return f"marketplace/raw/marketplace=tiki/observed_date={day}/hour=08/crawl_run_id=r/raw_artifact_id={name}/metadata.json"


class ListingStore(MemoryStore):
    def list(self, zone, prefix=""):
        return sorted(key for (z, key) in self.objects if z == zone and key.startswith(prefix))

    def delete(self, zone, key):
        self.objects.pop((zone, key), None)


def store_with(*days):
    store = ListingStore()
    for index, day in enumerate(days):
        store.put("bronze", raw_key(day, f"a{index}"), b"x")
    store.put("bronze", "marketplace/records/source=tiki/observed_date=2020-01-01/hour=00/x.jsonl.gz", b"x")
    store.put("silver", "marketplace/offer_observations_v2/observed_date=2020-01-01/p.parquet", b"x")
    return store


def test_only_raw_older_than_the_window_is_deleted():
    store = store_with("2026-09-01", "2026-09-24", "2026-09-26", "2026-10-25")

    result = retention.prune_raw(store, today=TODAY, older_than_days=30, log=lambda line: None)

    assert (result.examined, result.deleted, result.kept) == (4, 2, 2)
    kept = {key.split("observed_date=")[1][:10] for z, key in store.objects if "/raw/" in key}
    assert kept == {"2026-09-26", "2026-10-25"}


def test_records_and_silver_are_never_touched_whatever_their_age():
    store = store_with("2026-01-01")

    retention.prune_raw(store, today=TODAY, older_than_days=1, log=lambda line: None)

    assert any("/records/" in key for _, key in store.objects) and any(z == "silver" for z, _ in store.objects)


def test_nothing_inside_the_window_is_deleted():
    store = store_with("2026-10-24", "2026-10-25")

    result = retention.prune_raw(store, today=TODAY, older_than_days=30, log=lambda line: None)

    assert result.deleted == 0


def test_a_dry_run_deletes_nothing_and_reports_what_would_go():
    store = store_with("2026-01-01")
    before = dict(store.objects)

    result = retention.prune_raw(store, today=TODAY, older_than_days=30, dry_run=True, log=lambda line: None)

    assert result.deleted == 1 and store.objects == before


def test_a_key_without_a_readable_date_is_kept():
    store = ListingStore()
    store.put("bronze", "marketplace/raw/marketplace=tiki/observed_date=garbage/x", b"x")

    assert retention.prune_raw(store, today=TODAY, older_than_days=1, log=lambda line: None).deleted == 0


def test_a_window_below_one_day_is_refused():
    with pytest.raises(ValueError, match="at least 1"):
        retention.prune_raw(ListingStore(), today=TODAY, older_than_days=0)
