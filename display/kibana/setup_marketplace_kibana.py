"""Install the marketplace index templates and import the dashboards (plan section 11.3).

Run by the ``kibana-marketplace-setup`` init container under the ``serve``
profile, and on the host with ``LOCAL=true``. Every step is idempotent:

1. wait for Elasticsearch and Kibana;
2. ``PUT`` each index template — a template applies only to indices created
   after it, so this must happen before anything writes (``ops/es_projector.py``
   installs them too, for the same reason);
3. create the four projector indices if absent, so a panel over an index
   nothing has written to yet is an empty chart and not an error;
4. delete the two Phase 5 shell objects the new dashboards replace, so a stack
   upgraded in place keeps no dashboard pointing at a data view that is gone;
5. import the committed ``.ndjson`` with ``overwrite=true``.

Importing is by file upload, which is the only saved-object API that takes a
whole export and resolves the references between its objects in one call.
"""
from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

import requests

from display.kibana.marketplace_dashboards import NDJSON_PATH, SUPERSEDED_OBJECTS
from display.kibana.marketplace_index_templates import INDEX_TEMPLATES, projector_indices

ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
KIBANA_HOST = os.getenv("KIBANA_HOST", "http://kibana:5601")
if os.getenv("LOCAL", "false").lower() == "true":
    ES_HOST = os.getenv("ES_HOST_LOCAL", "http://localhost:9200")
    KIBANA_HOST = os.getenv("KIBANA_HOST_LOCAL", "http://localhost:5601")

HEADERS = {"kbn-xsrf": "true"}
JSON_HEADERS = {**HEADERS, "Content-Type": "application/json"}


def _wait(url: str, label: str, *, retries: int = 60, delay: float = 5.0, sleep=time.sleep) -> None:
    for attempt in range(retries):
        try:
            if requests.get(url, timeout=15).status_code < 500:
                print(f"[{label}] ready", flush=True)
                return
        except requests.RequestException:
            pass
        print(f"[{label}] not ready ({attempt + 1}/{retries})", flush=True)
        sleep(delay)
    raise SystemExit(f"[{label}] unavailable after {retries} attempts")


def install_templates(session: Any = requests) -> list[str]:
    """``PUT _index_template/<name>`` over HTTP, so this needs no ES client."""
    installed = []
    for name, body in INDEX_TEMPLATES.items():
        response = session.put(f"{ES_HOST}/_index_template/{name}", headers={"Content-Type": "application/json"},
                               data=json.dumps(body), timeout=60)
        response.raise_for_status()
        installed.append(name)
        print(f"  [OK] index template {name}", flush=True)
    return installed


def ensure_indices(session: Any = requests) -> list[str]:
    """Create the four projector indices if they are absent.

    An index exists only once something writes to it, and the DLQ index may
    legitimately never get a document — a stack where nothing was quarantined
    is a healthy stack. A Lens panel over a missing index is an error rather
    than an empty chart, so the index is created here and the template, just
    installed, gives it its mapping.
    """
    created = []
    for index in projector_indices():
        if session.head(f"{ES_HOST}/{index}", timeout=30).status_code == 404:
            session.put(f"{ES_HOST}/{index}", timeout=60).raise_for_status()
            created.append(index)
            print(f"  [OK] created empty index {index}", flush=True)
    return created


def delete_superseded(session: Any = requests) -> list[str]:
    """Remove the Phase 5 shell. A 404 means it was never there, which is fine."""
    removed = []
    for object_type, object_id in SUPERSEDED_OBJECTS:
        response = session.delete(f"{KIBANA_HOST}/api/saved_objects/{object_type}/{object_id}",
                                  headers=HEADERS, timeout=30)
        if response.status_code == 404:
            continue
        response.raise_for_status()
        removed.append(f"{object_type}/{object_id}")
        print(f"  [OK] removed superseded {object_type}/{object_id}", flush=True)
    return removed


def import_saved_objects(session: Any = requests) -> dict:
    """Import the committed export. ``overwrite=true`` makes a re-run a no-op."""
    payload = NDJSON_PATH.read_bytes()
    response = session.post(
        f"{KIBANA_HOST}/api/saved_objects/_import",
        params={"overwrite": "true"},
        headers=HEADERS,
        files={"file": (NDJSON_PATH.name, payload, "application/ndjson")},
        timeout=120,
    )
    response.raise_for_status()
    result = response.json()
    if not result.get("success", False):
        # Never "imported with errors": a half-imported dashboard looks fine
        # in the list and is broken when opened.
        raise SystemExit(f"saved-object import failed: {json.dumps(result.get('errors', result))[:2000]}")
    print(f"  [OK] imported {result.get('successCount')} saved objects", flush=True)
    return result


def setup_marketplace_kibana() -> dict:
    _wait(f"{ES_HOST}/_cluster/health", "Elasticsearch")
    _wait(f"{KIBANA_HOST}/api/status", "Kibana")
    templates = install_templates()
    created = ensure_indices()
    removed = delete_superseded()
    result = import_saved_objects()
    return {"templates": templates, "indices_created": created, "superseded_removed": removed,
            "imported": result.get("successCount"), "kibana": KIBANA_HOST}


def main() -> int:
    summary = setup_marketplace_kibana()
    print(json.dumps({"event": "marketplace_kibana_ready", **summary}, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
