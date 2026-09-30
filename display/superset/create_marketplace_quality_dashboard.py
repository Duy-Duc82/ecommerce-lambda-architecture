"""Describe the marketplace quality and audit dashboard for Superset provisioning.

Phase 7 plan section 15. This dashboard reads two tables in ``audit`` as well
as one in ``cache``, which is a deliberate exception to the convention that
Superset reads only ``cache``: when the gate blocks publication nothing reaches
``cache``, so a quality dashboard fed only from there would go blank exactly
when it matters most.
"""
from __future__ import annotations
import json

CACHE_DATASETS = ["marketplace_price_anomaly_daily"]
# Read-only. History of what was judged and what was served, including runs
# that never reached the cache.
AUDIT_DATASETS = ["marketplace_quality_result", "marketplace_batch_run", "marketplace_cache_version"]

CHART_TITLES = [
    "Mandatory quality check history (pass/fail per check over time)",
    "Mandatory failures in the latest run, with expectation and observed value",
    "Batch run timeline by status, including publication blocked",
    "Published cache version versus latest run",
    "Statistical price outliers per source and day, split by method",
    "Price outlier detail versus the offer's own baseline",
    "Share of offer-days with enough baseline history to evaluate",
]

# Wording is contract, not style. A verdict here is a statistical outlier
# against one offer's own past, never a claim that a price is dishonest or
# wrong, and never a comparison across offers or marketplaces.
NOTES = [
    "A price outlier is measured against this offer's own recent observed price history.",
    "An outlier is not a claim that a price is wrong, dishonest or a bargain.",
    "No panel compares one offer, seller or marketplace against another.",
    "Mandatory quality check failure means publication blocked, not data deleted.",
    "A blocked run keeps its Gold output; the last good published version is unchanged.",
    "INSUFFICIENT_HISTORY and INSUFFICIENT_DISPERSION mean no verdict, not normal.",
]


def dashboard_spec() -> dict[str, object]:
    return {
        "title": "Marketplace data quality and publication audit",
        "datasets": {"cache": list(CACHE_DATASETS), "audit": list(AUDIT_DATASETS)},
        "charts": list(CHART_TITLES),
        "notes": list(NOTES),
    }


def main() -> None:
    print(json.dumps(dashboard_spec(), indent=2, sort_keys=True))


if __name__ == "__main__": main()
