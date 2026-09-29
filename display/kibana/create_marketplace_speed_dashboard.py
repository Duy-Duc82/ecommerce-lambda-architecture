"""Provision the marketplace realtime data view and a small saved dashboard."""
from __future__ import annotations
import json
import os
import requests

KIBANA_HOST = os.getenv("KIBANA_HOST", "http://localhost:5601")
INDEX = os.getenv("ES_INDEX_MARKETPLACE_CHANGES", "marketplace-changes-v1")
HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}


def create_marketplace_speed_dashboard() -> None:
    view = {"data_view": {"id": "dv-marketplace-speed", "title": f"{INDEX}*", "name": "Marketplace changes (public observations)", "timeFieldName": "detected_at"}, "override": True}
    requests.post(f"{KIBANA_HOST}/api/data_views/data_view", headers=HEADERS, data=json.dumps(view), timeout=30).raise_for_status()
    # Saved-object import is intentionally small and uses accurate observational
    # language; teams can add visual details in Kibana without changing contracts.
    dashboard = {"attributes": {"title": "Marketplace speed — public observation changes", "description": "Recent price, public counter change, new offer and stale offer events. Public counter change is not a sale or demand event."}, "references": [{"type": "index-pattern", "id": "dv-marketplace-speed", "name": "kibanaSavedObjectMeta.searchSourceJSON.index"}]}
    requests.post(f"{KIBANA_HOST}/api/saved_objects/dashboard/marketplace-speed-v1", headers=HEADERS, data=json.dumps(dashboard), timeout=30).raise_for_status()


if __name__ == "__main__": create_marketplace_speed_dashboard()
