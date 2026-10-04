"""The marketplace Kibana saved objects, built rather than hand-written (plan section 11.3).

Two dashboards:

* **Marketplace — realtime changes**, which fills the empty shell Phase 5 left;
* **Marketplace — source health and freshness**, the Brief section 17
  operational view, built on the four indices ``ops/es_projector.py`` writes.

A Lens panel is a few hundred lines of JSON, and thirteen of them hand-edited
would drift from each other on the first change. So the objects are built here
from small helpers and *generated* into
``display/kibana/saved_objects/marketplace_dashboards.ndjson``, which is the
committed artefact the setup script imports. A test re-runs the builder and
fails if the committed file no longer matches, so neither side can drift.

Honest labelling, required by the plan and repeated on the panels themselves:

* "Micro-batch duration" is ``completed_at - started_at`` for one Spark
  micro-batch. It is **not** observation-to-change latency: the change
  contract carries no processed time, and Phase 8 does not add one.
  End-to-end latency is a Phase 9 measurement (P2-01).
* These indices hold operational, recent state. Gold-level freshness and
  coverage stay in Superset (``cache.marketplace_offer_freshness``,
  ``cache.marketplace_source_coverage_daily``).
* A public counter change is a change in a number the marketplace displays.
  It is not a sale, and not a demand signal.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from display.kibana.marketplace_index_templates import index_patterns

SAVED_OBJECTS_DIR = Path(__file__).resolve().parent / "saved_objects"
NDJSON_PATH = SAVED_OBJECTS_DIR / "marketplace_dashboards.ndjson"

# The Phase 5 shell these objects replace. The importer deletes them by id, so
# a stack upgraded in place keeps no orphan pointing at a dropped data view.
SUPERSEDED_OBJECTS = (("dashboard", "marketplace-speed-v1"), ("index-pattern", "dv-marketplace-speed"))

# Kibana runs every saved-object migration newer than the version stamped on
# an imported object. Unstamped, a by-value Lens panel is dragged through the
# pre-8.2 migrations, which look for ``datasourceStates.indexpattern`` and
# fail on ``formBased`` ("Cannot read properties of undefined (reading
# 'layers')", a 500 from _import). These are the stamps Kibana 8.18.1 — the
# version docker-compose.yml pins — writes on an object it creates itself;
# they were read back from it, not guessed.
CORE_MIGRATION_VERSION = "8.8.0"
TYPE_MIGRATION_VERSIONS = {"index-pattern": "8.0.0", "search": "10.5.0", "dashboard": "10.2.0"}


def _stamped(obj: dict[str, Any]) -> dict[str, Any]:
    return {**obj, "coreMigrationVersion": CORE_MIGRATION_VERSION,
            "typeMigrationVersion": TYPE_MIGRATION_VERSIONS[obj["type"]]}

# ---------------------------------------------------------------------------
# Data views — one per index family, each on the field a dashboard filters by
# ---------------------------------------------------------------------------

DATA_VIEWS: tuple[dict[str, str], ...] = (
    {"id": "dv-marketplace-changes", "title": "marketplace-changes-*",
     "name": "Marketplace changes (public observations)", "timeFieldName": "detected_at"},
    {"id": "dv-marketplace-offers-current", "title": "marketplace-offers-current-*",
     "name": "Marketplace offers, current state", "timeFieldName": "observed_at"},
    {"id": "dv-marketplace-source-health", "title": "marketplace-source-health-*",
     # projected_at, not last_success_at: the row is rewritten every pass, so
     # a source that has not succeeded in days is still inside "last 24 hours".
     "name": "Marketplace source health", "timeFieldName": "projected_at"},
    {"id": "dv-marketplace-crawl-attempts", "title": "marketplace-crawl-attempts-*",
     "name": "Marketplace crawl attempts", "timeFieldName": "completed_at"},
    {"id": "dv-marketplace-speed-batches", "title": "marketplace-speed-batches-*",
     "name": "Marketplace speed micro-batches", "timeFieldName": "started_at"},
    {"id": "dv-marketplace-dlq", "title": "marketplace-dlq-*",
     "name": "Marketplace observation DLQ", "timeFieldName": "failed_at"},
)


def _data_view_object(view: dict[str, str]) -> dict[str, Any]:
    return {
        "id": view["id"],
        "type": "index-pattern",
        "managed": False,
        "attributes": {
            "title": view["title"],
            "name": view["name"],
            "timeFieldName": view["timeFieldName"],
            # The index may not exist yet on a fresh stack; the data view must
            # still import, or the dashboards import broken.
            "allowNoIndex": True,
            "fields": "[]",
            "runtimeFieldMap": "{}",
            "sourceFilters": "[]",
            "fieldAttrs": "{}",
            "fieldFormatMap": "{}",
        },
        "references": [],
    }


# ---------------------------------------------------------------------------
# Lens column helpers
# ---------------------------------------------------------------------------


def _count(label: str = "Count of records") -> dict[str, Any]:
    return {"label": label, "dataType": "number", "operationType": "count", "isBucketed": False,
            "scale": "ratio", "sourceField": "___records___", "params": {"emptyAsNull": True}}


def _date_histogram(field: str, *, interval: str = "auto", label: str | None = None) -> dict[str, Any]:
    return {"label": label or field, "dataType": "date", "operationType": "date_histogram",
            "sourceField": field, "isBucketed": True, "scale": "interval",
            "params": {"interval": interval, "includeEmptyRows": True, "dropPartials": False}}


def _terms(field: str, *, size: int = 10, order_by: str | None = None, label: str | None = None) -> dict[str, Any]:
    order = {"type": "column", "columnId": order_by} if order_by else {"type": "alphabetical", "fallback": True}
    return {"label": label or f"Top values of {field}", "dataType": "string", "operationType": "terms",
            "scale": "ordinal", "sourceField": field, "isBucketed": True,
            "params": {"size": size, "orderBy": order, "orderDirection": "desc", "otherBucket": False,
                       "missingBucket": False, "parentFormat": {"id": "terms"}, "include": [], "exclude": [],
                       "includeIsRegex": False, "excludeIsRegex": False}}


def _metric(operation: str, field: str, label: str, *, params: dict | None = None) -> dict[str, Any]:
    return {"label": label, "dataType": "number", "operationType": operation, "sourceField": field,
            "isBucketed": False, "scale": "ratio", "params": {"emptyAsNull": False, **(params or {})}}


def _last_value(field: str, *, sort_field: str, label: str, data_type: str = "number") -> dict[str, Any]:
    return {"label": label, "dataType": data_type, "operationType": "last_value", "sourceField": field,
            "isBucketed": False, "scale": "ratio",
            "params": {"sortField": sort_field, "showArrayValues": False, "emptyAsNull": False}}


def _layer(columns: dict[str, dict]) -> dict[str, Any]:
    # Insertion order is the column order; dicts preserve it, so the two can
    # never disagree.
    return {"columnOrder": list(columns), "columns": columns, "incompleteColumns": {}, "sampling": 1}


# ---------------------------------------------------------------------------
# Visualisation shapes
# ---------------------------------------------------------------------------

_AXES_ON = {"x": True, "yLeft": True, "yRight": True}


def _xy(layer_id: str, *, x: str, ys: list[str], split: str | None = None,
        series_type: str = "bar_stacked") -> dict[str, Any]:
    layer: dict[str, Any] = {"layerId": layer_id, "accessors": ys, "position": "top",
                             "seriesType": series_type, "showGridlines": False, "layerType": "data",
                             "xAccessor": x}
    if split:
        layer["splitAccessor"] = split
    return {"legend": {"isVisible": True, "position": "right"}, "valueLabels": "hide",
            "fittingFunction": "None", "axisTitlesVisibilitySettings": dict(_AXES_ON),
            "tickLabelsVisibilitySettings": dict(_AXES_ON), "labelsOrientation": {"x": 0, "yLeft": 0, "yRight": 0},
            "gridlinesVisibilitySettings": dict(_AXES_ON), "preferredSeriesType": series_type,
            "layers": [layer]}


def _pie(layer_id: str, *, group: str, metric: str) -> dict[str, Any]:
    return {"shape": "pie", "layers": [{"layerId": layer_id, "primaryGroups": [group], "metrics": [metric],
                                        "numberDisplay": "percent", "categoryDisplay": "default",
                                        "legendDisplay": "default", "nestedLegend": False,
                                        "layerType": "data"}]}


# ---------------------------------------------------------------------------
# Panels
# ---------------------------------------------------------------------------


def _grid(x: int, y: int, w: int, h: int, panel_index: str) -> dict[str, Any]:
    return {"x": x, "y": y, "w": w, "h": h, "i": panel_index}


class _Panels:
    """Collects panels and the dashboard-level references their panels need."""

    def __init__(self) -> None:
        self.panels: list[dict] = []
        self.references: list[dict] = []

    def add(self, built: tuple[dict, list[dict]]) -> None:
        panel, references = built
        self.panels.append(panel)
        self.references.extend(references)


def _lens_panel(panel_index: str, *, title: str, description: str, data_view: str,
                visualization_type: str, columns: dict[str, dict], visualization: dict,
                grid: dict, query: str = "") -> tuple[dict, list[dict]]:
    layer_id = f"layer-{panel_index}"
    attributes = {
        "title": title,
        "description": description,
        "visualizationType": visualization_type,
        "type": "lens",
        "references": [],
        "state": {
            "visualization": visualization,
            "query": {"query": query, "language": "kuery"},
            "filters": [],
            "datasourceStates": {"formBased": {"layers": {layer_id: _layer(columns)}}},
            "internalReferences": [],
            "adHocDataViews": {},
        },
    }
    panel = {
        "type": "lens",
        "panelIndex": panel_index,
        "gridData": {**grid, "i": panel_index},
        "panelConfig": {"attributes": attributes, "enhancements": {}, "title": title, "description": description},
        # Kibana 8.x reads embeddableConfig; panelConfig is the 9.x name. Both
        # are written so the file imports either way.
        "embeddableConfig": {"attributes": attributes, "enhancements": {}},
        "title": title,
        "version": "8.18.0",
    }
    reference = {"name": f"{panel_index}:indexpattern-datasource-layer-{layer_id}",
                 "type": "index-pattern", "id": data_view}
    return panel, [reference]


def _search_panel(panel_index: str, *, search_id: str, title: str, grid: dict) -> tuple[dict, list[dict]]:
    panel = {
        "type": "search",
        "panelIndex": panel_index,
        "gridData": {**grid, "i": panel_index},
        "panelRefName": f"panel_{panel_index}",
        "embeddableConfig": {"enhancements": {}},
        "panelConfig": {"enhancements": {}},
        "title": title,
        "version": "8.18.0",
    }
    return panel, [{"name": f"panel_{panel_index}", "type": "search", "id": search_id}]


def _saved_search(object_id: str, *, title: str, description: str, data_view: str,
                  columns: list[str], sort_field: str) -> dict[str, Any]:
    source = {"query": {"query": "", "language": "kuery"}, "filter": [],
              "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"}
    return {
        "id": object_id,
        "type": "search",
        "managed": False,
        "attributes": {
            "title": title,
            "description": description,
            "columns": columns,
            "sort": [[sort_field, "desc"]],
            "grid": {},
            "hideChart": False,
            "isTextBasedQuery": False,
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(source, sort_keys=True)},
        },
        "references": [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                        "type": "index-pattern", "id": data_view}],
    }


def _dashboard(object_id: str, *, title: str, description: str, panels: list[dict],
               references: list[dict], time_from: str) -> dict[str, Any]:
    options = {"hidePanelTitles": False, "useMargins": True, "syncColors": False,
               "syncCursor": True, "syncTooltips": False}
    source = {"query": {"query": "", "language": "kuery"}, "filter": []}
    return {
        "id": object_id,
        "type": "dashboard",
        "managed": False,
        "attributes": {
            "title": title,
            "description": description,
            "panelsJSON": json.dumps(panels, sort_keys=True),
            "optionsJSON": json.dumps(options, sort_keys=True),
            "timeRestore": True,
            "timeFrom": time_from,
            "timeTo": "now",
            "refreshInterval": {"pause": False, "value": 30000},
            "version": 3,
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(source, sort_keys=True)},
        },
        "references": references,
    }


# ---------------------------------------------------------------------------
# Dashboard 1 — realtime changes
# ---------------------------------------------------------------------------

RECENT_CHANGES_SEARCH = "marketplace-recent-changes"
SOURCE_HEALTH_SEARCH = "marketplace-source-health-table"


def _realtime_dashboard() -> dict[str, Any]:
    built = _Panels()

    built.add(_search_panel(
        "p1", search_id=RECENT_CHANGES_SEARCH, grid=_grid(0, 0, 48, 12, "p1"),
        title="Recent changes — newest first (a public counter change is not a sale)"))

    built.add(_lens_panel(
        "p2", title="Large price drops over time",
        description="Count of LARGE_PRICE_DROP change events, bucketed by detected_at.",
        data_view="dv-marketplace-changes", visualization_type="lnsXY",
        query='change_type: "LARGE_PRICE_DROP"',
        columns={"c_x": _date_histogram("detected_at", label="detected_at"),
                 "c_y": _count("Large price drops")},
        visualization=_xy("layer-p2", x="c_x", ys=["c_y"]), grid=_grid(0, 12, 24, 14, "p2")))

    built.add(_lens_panel(
        "p3", title="New and stale offers over time",
        description="NEW_OFFER and OFFER_STALE events by detected_at. OFFER_STALE means no "
                    "observation within MARKETPLACE_STALE_AFTER_SECONDS, not that the offer ended.",
        data_view="dv-marketplace-changes", visualization_type="lnsXY",
        query='change_type: ("NEW_OFFER" or "OFFER_STALE")',
        columns={"c_x": _date_histogram("detected_at", label="detected_at"),
                 "c_split": _terms("change_type", size=2, label="change_type"),
                 "c_y": _count("Events")},
        visualization=_xy("layer-p3", x="c_x", ys=["c_y"], split="c_split"), grid=_grid(24, 12, 24, 14, "p3")))

    built.add(_lens_panel(
        "p4", title="Changes by change_type",
        description="Share of each of the seven frozen change types over the dashboard's time range.",
        data_view="dv-marketplace-changes", visualization_type="lnsPie",
        columns={"c_split": _terms("change_type", size=7, order_by="c_metric", label="change_type"),
                 "c_metric": _count("Events")},
        visualization=_pie("layer-p4", group="c_split", metric="c_metric"), grid=_grid(0, 26, 24, 14, "p4")))

    built.add(_lens_panel(
        "p5", title="Offers by availability (current state)",
        description="Current offer state, one document per offer. Availability as the marketplace "
                    "reports it, at the offer's last observation.",
        data_view="dv-marketplace-offers-current", visualization_type="lnsPie",
        columns={"c_split": _terms("availability", size=10, order_by="c_metric", label="availability"),
                 "c_metric": _count("Offers")},
        visualization=_pie("layer-p5", group="c_split", metric="c_metric"), grid=_grid(24, 26, 24, 14, "p5")))

    return _dashboard(
        "marketplace-realtime-changes-v1",
        title="Marketplace — realtime changes",
        description="Price, public counter, availability, new-offer and stale-offer events from the speed "
                    "layer. Public counter changes are changes in a number the marketplace displays; they "
                    "are not sales and not a demand signal.",
        panels=built.panels, references=built.references, time_from="now-24h")


# ---------------------------------------------------------------------------
# Dashboard 2 — source health and freshness
# ---------------------------------------------------------------------------


def _source_health_dashboard() -> dict[str, Any]:
    built = _Panels()

    built.add(_search_panel(
        "p1", search_id=SOURCE_HEALTH_SEARCH, grid=_grid(0, 0, 48, 10, "p1"),
        title="Source last success, circuit state and consecutive failures"))

    built.add(_lens_panel(
        "p2", title="Freshness — seconds since the last observation, per source",
        description="now minus the newest observation the source produced, measured when the projector "
                    "last ran (every MARKETPLACE_OPS_PROJECT_INTERVAL_SECONDS).",
        data_view="dv-marketplace-source-health", visualization_type="lnsXY",
        columns={"c_x": _terms("marketplace_code", size=20, label="marketplace"),
                 "c_y": _last_value("freshness_seconds", sort_field="projected_at",
                                    label="Seconds since last observation")},
        visualization=_xy("layer-p2", x="c_x", ys=["c_y"], series_type="bar"),
        grid=_grid(0, 10, 24, 14, "p2")))

    built.add(_lens_panel(
        "p3", title="Crawl attempts by error_kind over time",
        description="Settled crawl attempts by completed_at, split by error_kind. A successful attempt "
                    "has no error_kind and is not shown.",
        data_view="dv-marketplace-crawl-attempts", visualization_type="lnsXY",
        query="error_kind: *",
        columns={"c_x": _date_histogram("completed_at", label="completed_at"),
                 "c_split": _terms("error_kind", size=10, label="error_kind"),
                 "c_y": _count("Attempts")},
        visualization=_xy("layer-p3", x="c_x", ys=["c_y"], split="c_split"), grid=_grid(24, 10, 24, 14, "p3")))

    built.add(_lens_panel(
        "p4", title="HTTP status distribution",
        description="Response status of settled crawl attempts. An attempt that never got a response "
                    "has no http_status and is not shown.",
        data_view="dv-marketplace-crawl-attempts", visualization_type="lnsPie",
        query="http_status: *",
        columns={"c_split": _terms("http_status", size=10, order_by="c_metric", label="http_status"),
                 "c_metric": _count("Attempts")},
        visualization=_pie("layer-p4", group="c_split", metric="c_metric"), grid=_grid(0, 24, 16, 14, "p4")))

    built.add(_lens_panel(
        "p5", title="Fetch latency per day — median and 95th percentile",
        description="latency_ms of one crawl request: the fetch alone. It is not pipeline latency and "
                    "not observation-to-change latency.",
        data_view="dv-marketplace-crawl-attempts", visualization_type="lnsXY",
        columns={"c_x": _date_histogram("completed_at", interval="1d", label="completed_at"),
                 "c_p50": _metric("median", "latency_ms", "Median fetch latency (ms)"),
                 "c_p95": _metric("percentile", "latency_ms", "95th percentile fetch latency (ms)",
                                  params={"percentile": 95})},
        visualization=_xy("layer-p5", x="c_x", ys=["c_p50", "c_p95"], series_type="line"),
        grid=_grid(16, 24, 32, 14, "p5")))

    built.add(_lens_panel(
        "p6", title="Parsed and rejected observations per day",
        description="Sum of parsed_count and rejected_count over settled crawl attempts. Rejected means "
                    "the parser refused the record, not that the fetch failed.",
        data_view="dv-marketplace-crawl-attempts", visualization_type="lnsXY",
        columns={"c_x": _date_histogram("completed_at", interval="1d", label="completed_at"),
                 "c_parsed": _metric("sum", "parsed_count", "Parsed"),
                 "c_rejected": _metric("sum", "rejected_count", "Rejected")},
        visualization=_xy("layer-p6", x="c_x", ys=["c_parsed", "c_rejected"]),
        grid=_grid(0, 38, 24, 14, "p6")))

    built.add(_lens_panel(
        "p7", title="Observation rate — parsed observations per hour",
        description="Sum of parsed_count per hour of completed_at. Against the stub source these are "
                    "replayed fixtures, not new marketplace observations.",
        data_view="dv-marketplace-crawl-attempts", visualization_type="lnsXY",
        columns={"c_x": _date_histogram("completed_at", interval="1h", label="completed_at"),
                 "c_y": _metric("sum", "parsed_count", "Parsed observations")},
        visualization=_xy("layer-p7", x="c_x", ys=["c_y"]), grid=_grid(24, 38, 24, 14, "p7")))

    built.add(_lens_panel(
        "p8", title="DLQ records by stage over time",
        description="Quarantined observation records by failed_at, split by the stage that refused them "
                    "(DECODE or CONTRACT_VALIDATION). A valid observation never reaches the DLQ.",
        data_view="dv-marketplace-dlq", visualization_type="lnsXY",
        columns={"c_x": _date_histogram("failed_at", label="failed_at"),
                 "c_split": _terms("stage", size=5, label="stage"),
                 "c_y": _count("DLQ records")},
        visualization=_xy("layer-p8", x="c_x", ys=["c_y"], split="c_split"), grid=_grid(0, 52, 24, 14, "p8")))

    built.add(_lens_panel(
        "p9", title="Speed micro-batch duration — median and 95th percentile (NOT end-to-end latency)",
        description="completed_at minus started_at for one Spark micro-batch. End-to-end "
                    "observation-to-change latency is a Phase 9 measurement; the change contract carries "
                    "no processed time.",
        data_view="dv-marketplace-speed-batches", visualization_type="lnsXY",
        columns={"c_x": _date_histogram("started_at", label="started_at"),
                 "c_p50": _metric("median", "duration_ms", "Median micro-batch duration (ms)"),
                 "c_p95": _metric("percentile", "duration_ms", "95th percentile micro-batch duration (ms)",
                                  params={"percentile": 95})},
        visualization=_xy("layer-p9", x="c_x", ys=["c_p50", "c_p95"], series_type="line"),
        grid=_grid(24, 52, 24, 14, "p9")))

    built.add(_lens_panel(
        "p10", title="Rows per speed micro-batch",
        description="Input rows and emitted change events per micro-batch. A batch that applied no "
                    "change still consumed observations.",
        data_view="dv-marketplace-speed-batches", visualization_type="lnsXY",
        columns={"c_x": _date_histogram("started_at", label="started_at"),
                 "c_input": _metric("sum", "input_rows", "Input rows"),
                 "c_change": _metric("sum", "change_rows", "Change events")},
        visualization=_xy("layer-p10", x="c_x", ys=["c_input", "c_change"]),
        grid=_grid(0, 66, 48, 14, "p10")))

    return _dashboard(
        "marketplace-source-health-v1",
        title="Marketplace — source health and freshness",
        description="Operational view of the crawl, the DLQ and the speed layer, from the four indices "
                    "the ops projector writes. Recent operational state only: Gold-level freshness and "
                    "coverage stay in Superset.",
        panels=built.panels, references=built.references, time_from="now-7d")


# ---------------------------------------------------------------------------
# The file
# ---------------------------------------------------------------------------


def saved_objects() -> list[dict[str, Any]]:
    """Every object, data views first so an import resolves references in order."""
    return [
        *(_data_view_object(view) for view in DATA_VIEWS),
        _saved_search(
            RECENT_CHANGES_SEARCH,
            title="Marketplace — recent changes",
            description="Change events newest first. A public counter change is a change in a number the "
                        "marketplace displays; it is not a sale.",
            data_view="dv-marketplace-changes", sort_field="detected_at",
            columns=["detected_at", "marketplace", "change_type", "offer_id", "field_name",
                     "previous_value", "current_value", "rule_version"]),
        _saved_search(
            SOURCE_HEALTH_SEARCH,
            title="Marketplace — source health",
            description="One row per source, rewritten by the projector every pass. circuit_open is "
                        "opened_until > now at the time of that pass.",
            data_view="dv-marketplace-source-health", sort_field="projected_at",
            columns=["marketplace_code", "circuit_open", "consecutive_failures", "last_success_at",
                     "last_failure_at", "last_observation_at", "freshness_seconds"]),
        _realtime_dashboard(),
        _source_health_dashboard(),
    ]


def ndjson() -> str:
    """The committed export: one object per line, then Kibana's summary line."""
    objects = [_stamped(obj) for obj in saved_objects()]
    lines = [json.dumps(obj, sort_keys=True, ensure_ascii=False) for obj in objects]
    lines.append(json.dumps({"excludedObjects": [], "excludedObjectsCount": 0,
                             "exportedCount": len(objects), "missingRefCount": 0,
                             "missingReferences": []}, sort_keys=True))
    return "\n".join(lines) + "\n"


def referenced_index_patterns() -> set[str]:
    """Every index pattern the data views claim, for the saved-object check."""
    return {obj["attributes"]["title"] for obj in saved_objects() if obj["type"] == "index-pattern"}


def write() -> Path:
    SAVED_OBJECTS_DIR.mkdir(parents=True, exist_ok=True)
    NDJSON_PATH.write_text(ndjson(), encoding="utf-8")
    return NDJSON_PATH


def main() -> int:
    unknown = referenced_index_patterns() - set(index_patterns())
    if unknown:
        raise SystemExit(f"data views reference patterns no index template installs: {sorted(unknown)}")
    path = write()
    print(json.dumps({"event": "saved_objects_written", "path": str(path),
                      "objects": len(saved_objects())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
