"""Delta housekeeping: ``OPTIMIZE`` then ``VACUUM`` on every Silver and star table.

    python -m ecomdata.processing.batch compact [--retain-hours N] [--dry-run]

Every hourly run rewrites a few Silver day partitions and merges into the star,
so tables gather small files and old versions that nothing reads. ``OPTIMIZE``
bin-packs the small files of each table; ``VACUUM`` then deletes files no
version inside the retention window references. Both run under the batch
advisory lock, so they never race an hourly run's writes. A path that is not a
Delta table yet (a source with no data, a Parquet lake) is skipped.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager, nullcontext
from typing import Any

# Delta refuses a VACUUM shorter than 7 days unless its safety check is turned off;
# a week also keeps every version time travel and a reprocessing run may need.
DEFAULT_RETAIN_HOURS = 168


def table_uris(silver_root: str, gold_root: str) -> list[str]:
    """Every Delta table of the batch: the Silver datasets and the star under ``<gold>/dw``."""
    from ecomdata.domain.datasets import STAR_NAMES
    from ecomdata.domain.sources import SOURCES
    from ecomdata.processing.batch.stages.silver import OBSERVATIONS, PAGE_CONTEXT, QUARANTINE, wide_name

    silver = [OBSERVATIONS, QUARANTINE, PAGE_CONTEXT, *(wide_name(spec.code) for spec in SOURCES)]
    return [f"{silver_root.rstrip('/')}/{name}" for name in silver] + [
        f"{gold_root.rstrip('/')}/dw/{name}" for name in STAR_NAMES
    ]


def compact_table(spark: Any, uri: str, *, retain_hours: int = DEFAULT_RETAIN_HOURS) -> dict[str, Any]:
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, uri):
        return {"uri": uri, "status": "SKIPPED"}
    table = DeltaTable.forPath(spark, uri)
    metrics = table.optimize().executeCompaction().first()["metrics"]
    table.vacuum(retain_hours)
    return {
        "uri": uri,
        "status": "COMPACTED",
        "files_removed": int(metrics["numFilesRemoved"]),
        "files_added": int(metrics["numFilesAdded"]),
    }


def compact_all(
    spark: Any,
    uris: Iterable[str],
    *,
    retain_hours: int = DEFAULT_RETAIN_HOURS,
    lock: Callable[[], AbstractContextManager[Any]] = nullcontext,
    compact: Callable[..., dict[str, Any]] = compact_table,
) -> list[dict[str, Any]]:
    """Compact each table in turn under ``lock``; one failing table does not stop the others."""
    if retain_hours < DEFAULT_RETAIN_HOURS:
        raise ValueError(f"retain_hours must be at least {DEFAULT_RETAIN_HOURS} (Delta's VACUUM safety window)")
    results = []
    with lock():
        for uri in uris:
            try:
                results.append(compact(spark, uri, retain_hours=retain_hours))
            except Exception as error:  # noqa: BLE001 - reported per table, the command exits non-zero
                results.append({"uri": uri, "status": "FAILED", "error": f"{type(error).__name__}: {error}"})
    return results


__all__ = ["DEFAULT_RETAIN_HOURS", "compact_all", "compact_table", "table_uris"]
