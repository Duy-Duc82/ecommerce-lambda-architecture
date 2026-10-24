"""Describe the marketplace batch dashboard for Superset provisioning."""
from __future__ import annotations
import json
from pathlib import Path

DATASETS = [
    "marketplace_offer_current", "marketplace_seller_current", "marketplace_offer_price_history_daily",
    "marketplace_offer_change_daily", "marketplace_offer_freshness", "marketplace_category_price_daily",
    "marketplace_source_coverage_daily", "marketplace_crawl_reliability_daily", "marketplace_counter_delta_daily",
]
CHART_TITLES = [
    "Observed offer coverage by source/day", "Fresh versus stale observed offers", "Daily first/last/min/max price (currency-aware)",
    "Category/source price distribution (currency-aware)", "Price-change count and magnitude", "Crawl success, p95 latency and rejected rows", "Invalid public-counter transition rate", "Seller and offer current detail",
]


def dashboard_spec() -> dict[str, object]:
    return {"title": "Marketplace temporal warehouse — observed history", "datasets": DATASETS, "charts": CHART_TITLES, "notes": ["Observed offer is not marketplace inventory.", "Missing means missing from scheduled observation coverage.", "Public counter delta/velocity proxy is not sales or demand.", "Price change is not anomaly."]}


def main() -> None:
    print(json.dumps(dashboard_spec(), indent=2, sort_keys=True))


if __name__ == "__main__": main()
