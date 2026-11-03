"""Print what each star table holds: its rows, its Delta version and the last operation on it.

    python3 -m ecomdata.processing.batch.dw_inspect

One JSON object on stdout, ``{"tables": {<name>: {...}}}``. A table not written
yet has ``rows`` null. Read-only: it never writes to the lake. The H7 end-to-end
script (``scripts/h7.ps1``) runs it in the ``batch-once`` container between
batch runs and compares the counts (a run must not lose star rows).
"""
from __future__ import annotations

import json
from typing import Any


def describe_table(spark: Any, uri: str) -> dict[str, Any]:
    from pyspark.errors import AnalysisException

    try:
        history = spark.sql(f"DESCRIBE HISTORY delta.`{uri}` LIMIT 1").first()
    except AnalysisException:
        return {"rows": None, "version": None, "operation": None, "uri": uri}
    rows = spark.read.format("delta").load(uri).count()
    return {
        "rows": int(rows),
        "version": int(history["version"]),
        "operation": history["operation"],
        "uri": uri,
    }


def lost_rows(before: dict[str, Any], after: dict[str, Any]) -> dict[str, tuple[int, int]]:
    """Tables whose row count fell from ``before`` to ``after``: name -> (before, after)."""
    lost: dict[str, tuple[int, int]] = {}
    for name, old in before.get("tables", {}).items():
        new = after.get("tables", {}).get(name, {})
        if old.get("rows") is not None and (new.get("rows") or 0) < old["rows"]:
            lost[name] = (old["rows"], new.get("rows") or 0)
    return lost


def main() -> None:
    from ecomdata.processing.batch.bootstrap import star_snapshot

    print(json.dumps(star_snapshot(), sort_keys=True))

if __name__ == "__main__":
    main()


__all__ = ["describe_table", "lost_rows", "main"]
