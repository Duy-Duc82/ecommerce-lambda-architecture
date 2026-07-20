"""Kibana provisioning for the speed layer.

Creates two Kibana data views over the Elasticsearch indices written by the
realtime path, and sets sensible dashboard defaults:

  * ecommerce-events   (raw behavioral events, from data_ingestion/es_indexer.py)
  * ecommerce-metrics  (per-minute aggregates, from speed_layer/speed_layer.py)

Kibana charts/dashboards are then built interactively on top of these views.

Run standalone:  LOCAL=true python display/kibana/setup_kibana.py
In Docker:       the kibana-setup service runs this automatically.
"""

from __future__ import annotations

import json
import os
import sys
import time

import requests

ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
KIBANA_HOST = os.getenv("KIBANA_HOST", "http://kibana:5601")
if os.getenv("LOCAL", "false").lower() == "true":
    ES_HOST = "http://localhost:9200"
    KIBANA_HOST = "http://localhost:5601"

HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}

DATA_VIEWS = [
    {"id": "dv-ecommerce-events", "title": "ecommerce-events*",
     "name": "Ecommerce Events (raw)", "timeFieldName": "@timestamp"},
    {"id": "dv-ecommerce-metrics", "title": "ecommerce-metrics*",
     "name": "Ecommerce Metrics (realtime)", "timeFieldName": "@timestamp"},
]


def _wait(url: str, label: str, retries: int = 30) -> None:
    for i in range(retries):
        try:
            if requests.get(url, timeout=15).status_code < 500:
                print(f"[{label}] ready")
                return
        except Exception:
            pass
        print(f"[{label}] not ready ({i + 1}/{retries}), retry in 5s...")
        time.sleep(5)
    sys.exit(f"[{label}] unavailable after retries")


def create_data_views() -> None:
    print("\n[Kibana] Creating data views...")
    for dv in DATA_VIEWS:
        body = {
            "data_view": {
                "id": dv["id"],
                "title": dv["title"],
                "name": dv["name"],
                "timeFieldName": dv["timeFieldName"],
                "allowNoIndex": True,
            },
            "override": True,
        }
        r = requests.post(f"{KIBANA_HOST}/api/data_views/data_view", headers=HEADERS,
                          data=json.dumps(body), timeout=60)
        status = "OK" if r.status_code in (200, 201) else f"ERR {r.status_code}: {r.text[:100]}"
        print(f"  [{status}] {dv['name']} -> {dv['title']}")


def configure_defaults() -> None:
    print("\n[Kibana] Configuring defaults...")
    settings = {
        "timepicker:refreshIntervalDefaults": json.dumps({"pause": False, "value": 10000}),
        "timepicker:timeDefaults": json.dumps({"from": "now-30m", "to": "now"}),
    }
    for key, value in settings.items():
        r = requests.post(f"{KIBANA_HOST}/api/kibana/settings", headers=HEADERS,
                          data=json.dumps({"changes": {key: value}}), timeout=30)
        print(f"  [{'OK' if r.status_code in (200, 201) else '~'}] {key}")


def main() -> None:
    print("=" * 60)
    print("  Kibana Setup — speed layer data views")
    print("=" * 60)
    _wait(f"{ES_HOST}/_cluster/health", "ES")
    _wait(f"{KIBANA_HOST}/api/status", "Kibana")
    create_data_views()
    configure_defaults()
    print("\n[OK] Kibana ready:", KIBANA_HOST)


if __name__ == "__main__":
    main()
