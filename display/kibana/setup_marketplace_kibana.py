"""Provision the marketplace realtime views in Kibana.

Separate from ``setup_kibana.py``, which provisions the legacy behavioral
dashboards and is not modified here.

Every object is created with a fixed ID, so re-running this script updates the
same saved objects instead of accumulating near-duplicates — the same
determinism rule the change events themselves follow.

Panel wording matters as much as the query: ``sold_count`` is labelled a public
counter everywhere.  The marketplace does not document what it counts or when
it resets, so calling it "units sold" on a dashboard would put a claim in the
report that the data cannot support.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config.settings import (
    ES_INDEX_MARKETPLACE_CHANGES,
    ES_INDEX_MARKETPLACE_OBSERVATIONS,
    SPEED_FRESHNESS_THRESHOLD_MINUTES,
    SPEED_STALE_THRESHOLD_MINUTES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KIBANA_HOST = os.getenv("KIBANA_HOST", "http://localhost:5601")
HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}

OBSERVATIONS_VIEW_ID = "marketplace-observations-v1-view"
CHANGES_VIEW_ID = "marketplace-changes-v1-view"

DATA_VIEWS = (
    {
        "id": OBSERVATIONS_VIEW_ID,
        "title": ES_INDEX_MARKETPLACE_OBSERVATIONS,
        "name": "Marketplace observations (v1)",
        "timeFieldName": "observed_at",
    },
    {
        "id": CHANGES_VIEW_ID,
        "title": ES_INDEX_MARKETPLACE_CHANGES,
        "name": "Marketplace changes (v1)",
        "timeFieldName": "detected_at",
    },
)

# (id, title, data view, query, description)
SAVED_SEARCHES = (
    (
        "marketplace-recent-price-changes",
        "Recent price changes",
        CHANGES_VIEW_ID,
        "change_type: PRICE_CHANGED",
        "Every detected price movement, newest first.",
    ),
    (
        "marketplace-large-price-drops",
        "Large price drops",
        CHANGES_VIEW_ID,
        "change_type: LARGE_PRICE_DROP",
        "Drops past the configured absolute or relative threshold. "
        "Always accompanied by a PRICE_CHANGED event for the same observation.",
    ),
    (
        "marketplace-new-offers",
        "New offers",
        CHANGES_VIEW_ID,
        "change_type: NEW_OFFER",
        "Offers observed for the first time.",
    ),
    (
        "marketplace-stale-offers",
        "Stale offers",
        CHANGES_VIEW_ID,
        "change_type: OFFER_STALE",
        f"No successful observation for more than {SPEED_STALE_THRESHOLD_MINUTES} minutes. "
        "One event per last-seen observation, not one per sweep.",
    ),
    (
        "marketplace-availability-changes",
        "Availability changes",
        CHANGES_VIEW_ID,
        "change_type: AVAILABILITY_CHANGED",
        "Transitions between two known availability states. "
        "Transitions involving UNKNOWN are excluded: those are adapter coverage "
        "changing, not stock changing.",
    ),
    (
        "marketplace-public-counter-changes",
        "Public counter changes (not sales)",
        CHANGES_VIEW_ID,
        "change_type: COUNTER_CHANGED",
        "Movement of the marketplace's public sold_count. The marketplace does "
        "not document what this counts or when it resets, so it is not a sale, "
        "an order or demand.",
    ),
    (
        "marketplace-invalid-counter-changes",
        "Counter resets and invalid deltas",
        CHANGES_VIEW_ID,
        "counter_reset_or_invalid: true",
        "Counter movements that went backwards or are otherwise not usable for "
        "aggregation. Surfaced rather than clamped to zero.",
    ),
    (
        "marketplace-observation-rate",
        "Observation rate",
        OBSERVATIONS_VIEW_ID,
        "*",
        "Observations landed over time, per marketplace.",
    ),
    (
        "marketplace-source-freshness",
        "Last observation per source",
        OBSERVATIONS_VIEW_ID,
        "*",
        f"Sort by observed_at descending per marketplace. Freshness threshold is "
        f"{SPEED_FRESHNESS_THRESHOLD_MINUTES} minutes; the harder stale threshold "
        f"is {SPEED_STALE_THRESHOLD_MINUTES} minutes.",
    ),
)


def _wait(url: str, label: str, retries: int = 30) -> None:
    for attempt in range(retries):
        try:
            if requests.get(url, timeout=15).status_code < 500:
                logger.info("%s is up", label)
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    raise RuntimeError(f"{label} did not become available at {url}")


def create_data_views() -> None:
    for view in DATA_VIEWS:
        response = requests.post(
            f"{KIBANA_HOST}/api/data_views/data_view",
            headers=HEADERS,
            json={"data_view": view, "override": True},
            timeout=30,
        )
        if response.status_code < 300:
            logger.info("data view ready: %s", view["name"])
        else:
            logger.warning(
                "data view %s failed (%s): %s", view["name"], response.status_code, response.text[:300]
            )


def create_saved_searches() -> None:
    for object_id, title, view_id, query, description in SAVED_SEARCHES:
        payload = {
            "attributes": {
                "title": title,
                "description": description,
                "columns": ["marketplace", "offer_id", "change_type", "field_name",
                            "previous_value", "current_value", "delta_percent_exact"],
                "sort": [["detected_at", "desc"]],
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": (
                        '{"query":{"query":"%s","language":"kuery"},'
                        '"indexRefName":"kibanaSavedObjectMeta.searchSourceJSON.index",'
                        '"filter":[]}' % query
                    )
                },
            },
            "references": [
                {
                    "id": view_id,
                    "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "type": "index-pattern",
                }
            ],
        }
        # Fixed object id + overwrite: re-running updates, never duplicates.
        response = requests.post(
            f"{KIBANA_HOST}/api/saved_objects/search/{object_id}?overwrite=true",
            headers=HEADERS,
            json=payload,
            timeout=30,
        )
        if response.status_code < 300:
            logger.info("saved search ready: %s", title)
        else:
            logger.warning(
                "saved search %s failed (%s): %s", title, response.status_code, response.text[:300]
            )


def main() -> None:
    _wait(f"{KIBANA_HOST}/api/status", "Kibana")
    create_data_views()
    create_saved_searches()
    logger.info(
        "marketplace Kibana objects provisioned (%d data views, %d saved searches)",
        len(DATA_VIEWS),
        len(SAVED_SEARCHES),
    )


if __name__ == "__main__":
    main()
