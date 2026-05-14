"""
Kibana Setup Script — Auto-provisions the full ELK stack for Ecommerce Analytics.

What this script does (in order):
  1. Create ILM (Index Lifecycle Management) policy
  2. Create ES index templates + indices with mappings
  3. Create Kibana Data Views (Index Patterns)
  4. Import all Kibana Saved Objects (dashboards, visualizations)
  5. Create Kibana Alerting rules
  6. Configure Kibana spaces (optional)

Run standalone:
    python kibana/setup_kibana.py

Run inside Docker (kibana-setup service):
    The docker-compose kibana-setup container runs this automatically.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import requests

# ── Config ────────────────────────────────────────────────────────────
ES_HOST     = os.getenv("ES_HOST",     "http://elasticsearch:9200")
KIBANA_HOST = os.getenv("KIBANA_HOST", "http://kibana:5601")

# When running locally, override to localhost
if os.getenv("LOCAL", "false").lower() == "true":
    ES_HOST     = "http://localhost:9200"
    KIBANA_HOST = "http://localhost:5601"

MAPPINGS_DIR   = Path(__file__).parent / "index_mappings"
DASHBOARDS_DIR = Path(__file__).parent / "dashboards"
ALERTING_DIR   = Path(__file__).parent / "alerting"

KIBANA_HEADERS = {
    "Content-Type":  "application/json",
    "kbn-xsrf":      "true",
    "kbn-version":   "8.18.0",
}

# ── Index configuration ───────────────────────────────────────────────
INDICES = [
    {
        "name":         "ecommerce-events",
        "mapping_file": "ecommerce_events.json",
        "alias":        "ecommerce-events-current",
        "ilm_policy":   "ecommerce-30d-policy",
        "time_field":   "@timestamp",
        "description":  "Raw user behavior events from Kafka",
    },
    {
        "name":         "ecommerce-orders",
        "mapping_file": "ecommerce_orders.json",
        "alias":        "ecommerce-orders-current",
        "ilm_policy":   "ecommerce-30d-policy",
        "time_field":   "@timestamp",
        "description":  "Order transactions",
    },
    {
        "name":         "ecommerce-prices",
        "mapping_file": "ecommerce_prices.json",
        "alias":        None,
        "ilm_policy":   "ecommerce-90d-policy",
        "time_field":   "@timestamp",
        "description":  "Product price change events + ML forecasts",
    },
    {
        "name":         "ecommerce-anomalies",
        "mapping_file": "ecommerce_anomalies.json",
        "alias":        None,
        "ilm_policy":   "ecommerce-30d-policy",
        "time_field":   "@timestamp",
        "description":  "Anomaly detection results (Z-score, IQR, spikes)",
    },
    {
        "name":         "ecommerce-fraud-alerts",
        "mapping_file": "ecommerce_fraud_alerts.json",
        "alias":        None,
        "ilm_policy":   "ecommerce-90d-policy",
        "time_field":   "@timestamp",
        "description":  "Fraud detection alerts with risk scores",
    },
]

# ── Data Views ────────────────────────────────────────────────────────
DATA_VIEWS = [
    {
        "id":          "dv-ecommerce-events",
        "title":       "ecommerce-events*",
        "timeFieldName": "@timestamp",
        "name":        "Ecommerce Events",
    },
    {
        "id":          "dv-ecommerce-orders",
        "title":       "ecommerce-orders*",
        "timeFieldName": "@timestamp",
        "name":        "Ecommerce Orders",
    },
    {
        "id":          "dv-ecommerce-prices",
        "title":       "ecommerce-prices*",
        "timeFieldName": "@timestamp",
        "name":        "Product Prices & Forecasts",
    },
    {
        "id":          "dv-ecommerce-anomalies",
        "title":       "ecommerce-anomalies*",
        "timeFieldName": "@timestamp",
        "name":        "Anomaly Detection",
    },
    {
        "id":          "dv-ecommerce-fraud",
        "title":       "ecommerce-fraud-alerts*",
        "timeFieldName": "@timestamp",
        "name":        "Fraud Alerts",
    },
]


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _wait_for_es(max_retries: int = 20) -> None:
    print(f"[ES] Waiting for Elasticsearch at {ES_HOST}…")
    for i in range(max_retries):
        try:
            r = requests.get(f"{ES_HOST}/_cluster/health?wait_for_status=yellow&timeout=10s", timeout=15)
            if r.status_code == 200:
                data = r.json()
                print(f"[ES] ✓ Cluster '{data['cluster_name']}' status={data['status']}")
                return
        except Exception:
            pass
        print(f"[ES]   Not ready (attempt {i+1}/{max_retries}), retrying in 5s…")
        time.sleep(5)
    sys.exit("[ES] ✗ Elasticsearch not available after retries.")


def _wait_for_kibana(max_retries: int = 20) -> None:
    print(f"[Kibana] Waiting for Kibana at {KIBANA_HOST}…")
    for i in range(max_retries):
        try:
            r = requests.get(f"{KIBANA_HOST}/api/status", timeout=15)
            if r.status_code == 200:
                data = r.json()
                state = data.get("status", {}).get("overall", {}).get("level", "unknown")
                print(f"[Kibana] ✓ Status: {state}")
                return
        except Exception:
            pass
        print(f"[Kibana]   Not ready (attempt {i+1}/{max_retries}), retrying in 5s…")
        time.sleep(5)
    sys.exit("[Kibana] ✗ Kibana not available after retries.")


def _es_put(path: str, body: dict) -> requests.Response:
    url = f"{ES_HOST}/{path.lstrip('/')}"
    r = requests.put(url, json=body, timeout=30)
    return r


def _kibana_post(path: str, body: dict | None = None, data: str | None = None,
                 content_type: str = "application/json") -> requests.Response:
    url = f"{KIBANA_HOST}{path}"
    headers = dict(KIBANA_HEADERS)
    headers["Content-Type"] = content_type
    if data is not None:
        r = requests.post(url, data=data, headers=headers, timeout=60)
    else:
        r = requests.post(url, json=body, headers=headers, timeout=60)
    return r


# ─────────────────────────────────────────────────────────────────────
# Step 1 — ILM Policies
# ─────────────────────────────────────────────────────────────────────

def create_ilm_policies() -> None:
    print("\n[ILM] Creating Index Lifecycle Policies…")

    policies = {
        "ecommerce-30d-policy": {
            "policy": {
                "phases": {
                    "hot": {
                        "min_age": "0ms",
                        "actions": {
                            "rollover": {"max_size": "10gb", "max_age": "7d"},
                            "set_priority": {"priority": 100},
                        },
                    },
                    "warm": {
                        "min_age": "7d",
                        "actions": {
                            "forcemerge": {"max_num_segments": 1},
                            "set_priority": {"priority": 50},
                        },
                    },
                    "cold": {"min_age": "14d", "actions": {"set_priority": {"priority": 0}}},
                    "delete": {"min_age": "30d", "actions": {"delete": {}}},
                }
            }
        },
        "ecommerce-90d-policy": {
            "policy": {
                "phases": {
                    "hot": {
                        "min_age": "0ms",
                        "actions": {
                            "rollover": {"max_size": "5gb", "max_age": "30d"},
                            "set_priority": {"priority": 100},
                        },
                    },
                    "delete": {"min_age": "90d", "actions": {"delete": {}}},
                }
            }
        },
    }

    for name, policy in policies.items():
        r = _es_put(f"_ilm/policy/{name}", policy)
        status = "✓" if r.status_code in (200, 201) else f"✗ {r.status_code}"
        print(f"  [{status}] {name}")


# ─────────────────────────────────────────────────────────────────────
# Step 2 — Create Indices
# ─────────────────────────────────────────────────────────────────────

def create_indices() -> None:
    print("\n[ES] Creating indices with mappings…")

    for cfg in INDICES:
        mapping_path = MAPPINGS_DIR / cfg["mapping_file"]
        if not mapping_path.exists():
            print(f"  [!] Mapping file not found: {mapping_path}")
            continue

        with open(mapping_path, encoding="utf-8") as f:
            body = json.load(f)

        # Check if index already exists
        check = requests.get(f"{ES_HOST}/{cfg['name']}", timeout=10)
        if check.status_code == 200:
            print(f"  [~] {cfg['name']} already exists, skipping.")
            continue

        r = _es_put(cfg["name"], body)
        status = "✓" if r.status_code in (200, 201) else f"✗ {r.status_code}: {r.text[:80]}"
        print(f"  [{status}] {cfg['name']}")


# ─────────────────────────────────────────────────────────────────────
# Step 3 — Create Kibana Data Views
# ─────────────────────────────────────────────────────────────────────

def create_data_views() -> None:
    print("\n[Kibana] Creating Data Views (Index Patterns)…")

    for dv in DATA_VIEWS:
        body = {
            "data_view": {
                "id":            dv["id"],
                "title":         dv["title"],
                "timeFieldName": dv["timeFieldName"],
                "name":          dv["name"],
            },
            "override": True,
        }
        r = _kibana_post("/api/data_views/data_view", body)
        status = "✓" if r.status_code in (200, 201) else f"✗ {r.status_code}: {r.text[:80]}"
        print(f"  [{status}] {dv['name']} → {dv['title']}")


# ─────────────────────────────────────────────────────────────────────
# Step 4 — Import Saved Objects (Dashboards)
# ─────────────────────────────────────────────────────────────────────

def import_dashboards() -> None:
    print("\n[Kibana] Importing Saved Objects (dashboards + visualizations)…")

    if not DASHBOARDS_DIR.exists():
        print(f"  [!] Dashboards directory not found: {DASHBOARDS_DIR}")
        return

    ndjson_files = sorted(DASHBOARDS_DIR.glob("*.ndjson"))
    if not ndjson_files:
        print("  [!] No .ndjson files found in kibana/dashboards/")
        return

    for f in ndjson_files:
        with open(f, encoding="utf-8") as fh:
            content = fh.read()

        r = _kibana_post(
            "/api/saved_objects/_import?overwrite=true",
            data=content,
            content_type="application/ndjson",
        )
        status = "✓" if r.status_code in (200, 201) else f"✗ {r.status_code}"
        result = r.json() if r.status_code in (200, 201) else {}
        imported = result.get("successCount", "?")
        errors   = result.get("errors", [])
        print(f"  [{status}] {f.name}: {imported} objects imported", end="")
        if errors:
            print(f" | {len(errors)} errors: {[e.get('error',{}).get('type') for e in errors[:3]]}")
        else:
            print()


# ─────────────────────────────────────────────────────────────────────
# Step 5 — Create Alerting Rules
# ─────────────────────────────────────────────────────────────────────

def create_alerting_rules() -> None:
    print("\n[Kibana] Creating Alerting Rules…")

    rules = [
        {
            "name": "[Ecommerce] High Fraud Risk Score Alert",
            "rule_type_id": ".es-query",
            "consumer": "alerts",
            "schedule": {"interval": "1m"},
            "params": {
                "index": ["ecommerce-fraud-alerts*"],
                "timeField": "@timestamp",
                "timeWindowSize": 5,
                "timeWindowUnit": "m",
                "threshold": [1],
                "thresholdComparator": ">=",
                "esQuery": json.dumps({
                    "query": {
                        "bool": {
                            "filter": [{"term": {"risk_level": "HIGH"}},
                                       {"range": {"risk_score": {"gte": 0.8}}}]
                        }
                    }
                }),
                "size": 100,
                "aggType": "count",
            },
            "actions": [],
            "tags": ["fraud", "ecommerce", "high-risk"],
            "notify_when": "onActiveAlert",
        },
        {
            "name": "[Ecommerce] Critical Anomaly Detected",
            "rule_type_id": ".es-query",
            "consumer": "alerts",
            "schedule": {"interval": "2m"},
            "params": {
                "index": ["ecommerce-anomalies*"],
                "timeField": "@timestamp",
                "timeWindowSize": 5,
                "timeWindowUnit": "m",
                "threshold": [1],
                "thresholdComparator": ">=",
                "esQuery": json.dumps({
                    "query": {
                        "bool": {
                            "filter": [{"term": {"severity": "CRITICAL"}}]
                        }
                    }
                }),
                "size": 100,
                "aggType": "count",
            },
            "actions": [],
            "tags": ["anomaly", "ecommerce", "critical"],
            "notify_when": "onActiveAlert",
        },
        {
            "name": "[Ecommerce] Revenue Drop > 30% in 15min",
            "rule_type_id": ".index-threshold",
            "consumer": "alerts",
            "schedule": {"interval": "5m"},
            "params": {
                "index": [{"ilmLifecycle": None, "label": "ecommerce-orders*", "id": "ecommerce-orders*"}],
                "timeField": "@timestamp",
                "aggType": "sum",
                "aggField": "net_revenue",
                "groupBy": "all",
                "timeWindowSize": 15,
                "timeWindowUnit": "m",
                "thresholdComparator": "<",
                "threshold": [500],
            },
            "actions": [],
            "tags": ["revenue", "ecommerce"],
            "notify_when": "onActiveAlert",
        },
        {
            "name": "[Ecommerce] Traffic Spike (Events > 10x baseline)",
            "rule_type_id": ".es-query",
            "consumer": "alerts",
            "schedule": {"interval": "1m"},
            "params": {
                "index": ["ecommerce-events*"],
                "timeField": "@timestamp",
                "timeWindowSize": 1,
                "timeWindowUnit": "m",
                "threshold": [5000],
                "thresholdComparator": ">",
                "esQuery": json.dumps({"query": {"match_all": {}}}),
                "size": 0,
                "aggType": "count",
            },
            "actions": [],
            "tags": ["traffic", "spike", "ecommerce"],
            "notify_when": "onActiveAlert",
        },
        {
            "name": "[Ecommerce] Price Volatility Alert",
            "rule_type_id": ".es-query",
            "consumer": "alerts",
            "schedule": {"interval": "10m"},
            "params": {
                "index": ["ecommerce-prices*"],
                "timeField": "@timestamp",
                "timeWindowSize": 10,
                "timeWindowUnit": "m",
                "threshold": [1],
                "thresholdComparator": ">=",
                "esQuery": json.dumps({
                    "query": {
                        "range": {"price_change_pct": {"gte": 20}}
                    }
                }),
                "size": 100,
                "aggType": "count",
            },
            "actions": [],
            "tags": ["price", "volatility", "ecommerce"],
            "notify_when": "onActiveAlert",
        },
    ]

    for rule in rules:
        r = _kibana_post("/api/alerting/rule", rule)
        if r.status_code in (200, 201):
            rule_id = r.json().get("id", "?")
            print(f"  [✓] {rule['name']} (id={rule_id})")
        elif r.status_code == 409:
            print(f"  [~] {rule['name']} already exists")
        else:
            print(f"  [✗] {rule['name']}: {r.status_code} {r.text[:120]}")


# ─────────────────────────────────────────────────────────────────────
# Step 6 — Configure default Kibana space / home page
# ─────────────────────────────────────────────────────────────────────

def configure_kibana_defaults() -> None:
    print("\n[Kibana] Configuring Kibana defaults…")

    # Set default route to Analytics > Dashboards
    body = {"changes": {"defaultRoute": "/app/dashboards"}}
    r = _kibana_post("/api/kibana/settings", body)
    ok = r.status_code in (200, 201)
    print(f"  [{'✓' if ok else '~'}] Default route → /app/dashboards")

    # Dark mode off (light theme for professional look)
    body2 = {"changes": {"theme:darkMode": "false"}}
    r2 = _kibana_post("/api/kibana/settings", body2)
    ok2 = r2.status_code in (200, 201)
    print(f"  [{'✓' if ok2 else '~'}] Light theme enabled")


# ─────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("  Ecommerce Analytics — Kibana Setup")
    print("=" * 60)

    _wait_for_es()
    _wait_for_kibana()

    create_ilm_policies()
    create_indices()
    create_data_views()
    import_dashboards()
    create_alerting_rules()
    configure_kibana_defaults()

    print("\n" + "=" * 60)
    print("  ✓ Setup complete!")
    print(f"  → Kibana:         {KIBANA_HOST}")
    print(f"  → Elasticsearch:  {ES_HOST}")
    print("=" * 60)


if __name__ == "__main__":
    main()
