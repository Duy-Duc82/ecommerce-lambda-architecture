"""Builds a real dashboard for the speed layer's two data views
(ecommerce-events, ecommerce-metrics) using classic Kibana aggregation-based
visualizations, and imports it via the Saved Objects API.

setup_kibana.py only creates the two data views ("built interactively on top
of these views" per its docstring) — no dashboard ships with them. This
script fills that gap for the current canonical-event architecture. Classic
visualizations (not Lens) are used deliberately: their saved-object schema
has been stable across Kibana versions, avoiding Lens's migration-sensitive
internal state format.

Run:
    LOCAL=true python display/kibana/create_speed_dashboard.py
"""

from __future__ import annotations

import json
import os
import sys

import requests

ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
KIBANA_HOST = os.getenv("KIBANA_HOST", "http://kibana:5601")
if os.getenv("LOCAL", "false").lower() == "true":
    ES_HOST = "http://localhost:9200"
    KIBANA_HOST = "http://localhost:5601"

HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}

EVENTS_DV = "dv-ecommerce-events"
METRICS_DV = "dv-ecommerce-metrics"
DASHBOARD_ID = "dash-speed-layer-live-ops"

# The Kaggle-sourced sample carries historical event_time values (Oct 2019),
# not wall-clock time, so Kibana's "last 30 minutes" default would show an
# empty dashboard. Hard-restore the range that actually covers the data
# produced through the speed layer instead of relying on the global picker.
DASHBOARD_TIME_FROM = "2019-10-01T00:00:00.000Z"
DASHBOARD_TIME_TO = "2019-11-01T00:00:00.000Z"

MARKDOWN_SPECS = [
    {
        "id": "viz-speed-header-events",
        "title": "",
        "markdown": "### 📡 Luồng Sự Kiện Thô — `ecommerce-events`\nMỗi lượt xem/thêm giỏ hàng/mua hàng ngay khi được ES indexer ghi nhận.",
    },
    {
        "id": "viz-speed-header-metrics",
        "title": "",
        "markdown": "### 📊 Số Liệu Tổng Hợp — `ecommerce-metrics` (theo cửa sổ 1 phút)\nSố liệu tổng hợp mỗi phút, được tính bởi Spark Structured Streaming.",
    },
]

VIS_SPECS = [
    {
        "id": "viz-speed-total-events",
        "dv": EVENTS_DV,
        "title": "Tổng Số Sự Kiện",
        "type": "table",
        "aggs": [{"id": "1", "enabled": True, "type": "count", "schema": "metric",
                  "params": {"customLabel": "Số lượng sự kiện"}}],
        "params": {"perPage": 10, "showPartialRows": False, "showMetricsAtAllLevels": False,
                   "showToolbar": False, "totalFunc": "sum"},
    },
    {
        "id": "viz-speed-active-users",
        "dv": EVENTS_DV,
        "title": "Người Dùng Hoạt Động",
        "type": "table",
        "aggs": [{"id": "1", "enabled": True, "type": "cardinality", "schema": "metric",
                  "params": {"field": "user_id.keyword", "customLabel": "Số người dùng"}}],
        "params": {"perPage": 10, "showPartialRows": False, "showMetricsAtAllLevels": False,
                   "showToolbar": False, "totalFunc": "sum"},
    },
    {
        "id": "viz-speed-events-over-time",
        "dv": EVENTS_DV,
        "title": "Lượng Sự Kiện Theo Thời Gian",
        "type": "line",
        "aggs": [
            {"id": "1", "enabled": True, "type": "count", "schema": "metric",
             "params": {"customLabel": "Số lượng sự kiện"}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "event_time", "interval": "auto", "customLabel": "Thời gian"}},
        ],
        "params": {"type": "line", "grid": {"categoryLines": False}, "legendPosition": "bottom"},
    },
    {
        "id": "viz-speed-event-type-breakdown",
        "dv": EVENTS_DV,
        "title": "Phân Loại Theo Loại Sự Kiện",
        "type": "table",
        "aggs": [
            {"id": "1", "enabled": True, "type": "count", "schema": "metric",
             "params": {"customLabel": "Số lượng sự kiện"}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "bucket",
             "params": {"field": "event_type.keyword", "size": 10, "order": "desc", "orderBy": "1",
                        "customLabel": "Loại sự kiện"}},
        ],
        "params": {"perPage": 10, "showPartialRows": False, "showMetricsAtAllLevels": False,
                   "showToolbar": False, "totalFunc": "sum"},
    },
    {
        "id": "viz-speed-top-brands",
        "dv": EVENTS_DV,
        "title": "Thương Hiệu Nhiều Sự Kiện Nhất",
        "type": "table",
        "aggs": [
            {"id": "1", "enabled": True, "type": "count", "schema": "metric",
             "params": {"customLabel": "Số lượng sự kiện"}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "bucket",
             "params": {"field": "brand.keyword", "size": 10, "order": "desc", "orderBy": "1",
                        "customLabel": "Thương hiệu"}},
        ],
        "params": {"perPage": 10, "showPartialRows": False, "showMetricsAtAllLevels": False,
                   "showToolbar": False, "totalFunc": "sum"},
    },
    {
        "id": "viz-speed-top-categories",
        "dv": EVENTS_DV,
        "title": "Danh Mục Nhiều Sự Kiện Nhất",
        "type": "table",
        "aggs": [
            {"id": "1", "enabled": True, "type": "count", "schema": "metric",
             "params": {"customLabel": "Số lượng sự kiện"}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "bucket",
             "params": {"field": "category_code.keyword", "size": 10, "order": "desc", "orderBy": "1",
                        "customLabel": "Danh mục"}},
        ],
        "params": {"perPage": 10, "showPartialRows": False, "showMetricsAtAllLevels": False,
                   "showToolbar": False, "totalFunc": "sum"},
    },
    {
        "id": "viz-speed-revenue-over-time",
        "dv": METRICS_DV,
        "title": "Doanh Thu Theo Thời Gian (theo cửa sổ)",
        "type": "line",
        "aggs": [
            {"id": "1", "enabled": True, "type": "sum", "schema": "metric",
             "params": {"field": "revenue", "customLabel": "Doanh thu"}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "window_start", "interval": "auto", "customLabel": "Thời gian"}},
        ],
        "params": {"type": "line", "grid": {"categoryLines": False}, "legendPosition": "bottom"},
    },
    {
        "id": "viz-speed-windowed-events-by-type",
        "dv": METRICS_DV,
        "title": "Sự Kiện Theo Loại (theo cửa sổ)",
        "type": "table",
        "aggs": [
            {"id": "1", "enabled": True, "type": "sum", "schema": "metric",
             "params": {"field": "event_count", "customLabel": "Số lượng sự kiện"}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "bucket",
             "params": {"field": "event_type.keyword", "size": 10, "order": "desc", "orderBy": "1",
                        "customLabel": "Loại sự kiện"}},
        ],
        "params": {"perPage": 10, "showPartialRows": False, "showMetricsAtAllLevels": False,
                   "showToolbar": False, "totalFunc": "sum"},
    },
]

# 48-column grid; (x, y, w, h) per panel id. Laid out as two clearly
# separated sections (raw events, then windowed metrics), each opening with
# a markdown header, KPIs up top, the primary trend chart full-width and
# prominent, and same-grain breakdowns lined up in one row for easy scanning.
LAYOUT = {
    "viz-speed-header-events": (0, 0, 48, 3),
    "viz-speed-total-events": (0, 3, 24, 7),
    "viz-speed-active-users": (24, 3, 24, 7),
    "viz-speed-events-over-time": (0, 10, 48, 15),
    "viz-speed-event-type-breakdown": (0, 25, 16, 13),
    "viz-speed-top-brands": (16, 25, 16, 13),
    "viz-speed-top-categories": (32, 25, 16, 13),

    "viz-speed-header-metrics": (0, 38, 48, 3),
    "viz-speed-revenue-over-time": (0, 41, 48, 15),
    "viz-speed-windowed-events-by-type": (0, 56, 48, 12),
}


def build_objects() -> list[dict]:
    objects = []
    for spec in MARKDOWN_SPECS:
        vis_state = {
            "title": spec["title"],
            "type": "markdown",
            "params": {"markdown": spec["markdown"], "openLinksInNewTab": False},
            "aggs": [],
        }
        objects.append({
            "type": "visualization",
            "id": spec["id"],
            "attributes": {
                "title": spec["title"],
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"language": "kuery", "query": ""}, "filter": []})},
            },
            "references": [],
        })

    for spec in VIS_SPECS:
        vis_state = {
            "title": spec["title"],
            "type": spec["type"],
            "params": spec["params"],
            "aggs": spec["aggs"],
        }
        objects.append({
            "type": "visualization",
            "id": spec["id"],
            "attributes": {
                "title": spec["title"],
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "query": {"language": "kuery", "query": ""},
                        "filter": [],
                        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    }),
                },
            },
            "references": [{
                "id": spec["dv"],
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "type": "index-pattern",
            }],
        })

    all_specs = MARKDOWN_SPECS + VIS_SPECS
    panels = []
    dash_refs = [
        {"id": EVENTS_DV, "name": "dataView-ref-events", "type": "index-pattern"},
        {"id": METRICS_DV, "name": "dataView-ref-metrics", "type": "index-pattern"},
    ]
    for idx, spec in enumerate(all_specs, start=1):
        ref_name = f"panel_{idx}"
        x, y, w, h = LAYOUT[spec["id"]]
        panels.append({
            "type": "visualization",
            "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(idx)},
            "panelIndex": str(idx),
            "panelRefName": ref_name,
            "title": spec.get("title", ""),
        })
        dash_refs.append({"id": spec["id"], "name": ref_name, "type": "visualization"})

    objects.append({
        "type": "dashboard",
        "id": DASHBOARD_ID,
        "attributes": {
            "title": "Tầng Tốc Độ — Vận Hành Thời Gian Thực",
            "description": "Giao diện thời gian thực của luồng sự kiện chuẩn (sự kiện thô + số liệu tổng hợp theo cửa sổ 1 phút).",
            "panelsJSON": json.dumps(panels),
            "timeRestore": True,
            "timeFrom": DASHBOARD_TIME_FROM,
            "timeTo": DASHBOARD_TIME_TO,
            "optionsJSON": json.dumps({
                "useMargins": True, "hidePanelTitles": False,
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({"query": {"language": "kuery", "query": ""}, "filter": []}),
            },
        },
        "references": dash_refs,
    })
    return objects


def main() -> None:
    # _bulk_create writes objects verbatim (no import-time reference
    # rewriting), so panelRefName in panelsJSON and references[].name stay
    # exactly as authored — unlike _import, which silently renames by
    # reference panel names ("panel_1" -> "1:panel_1") without touching
    # panelsJSON, breaking panel resolution ("Could not find reference").
    objects = build_objects()
    for obj in objects:
        delete_resp = requests.delete(
            f"{KIBANA_HOST}/api/saved_objects/{obj['type']}/{obj['id']}",
            headers={"kbn-xsrf": "true"}, timeout=30,
        )
        if delete_resp.status_code not in (200, 404):
            sys.exit(f"[ERROR] delete {obj['type']}/{obj['id']}: {delete_resp.status_code} {delete_resp.text[:300]}")

    resp = requests.post(
        f"{KIBANA_HOST}/api/saved_objects/_bulk_create",
        headers={"kbn-xsrf": "true", "Content-Type": "application/json"},
        data=json.dumps(objects),
        timeout=60,
    )
    if resp.status_code >= 300:
        sys.exit(f"[ERROR] {resp.status_code}: {resp.text[:1000]}")
    result = resp.json()
    errors = [o for o in result.get("saved_objects", []) if o.get("error")]
    if errors:
        sys.exit(f"[ERROR] {json.dumps(errors, indent=2)}")
    print(json.dumps(result, indent=2))
    print(f"\n[OK] Open: {KIBANA_HOST}/app/dashboards#/view/{DASHBOARD_ID}")


if __name__ == "__main__":
    main()
