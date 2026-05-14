"""
Generate Kibana 8.18 NDJSON dashboard files for all 5 analytics pages.
Run: python kibana/generate_dashboards.py
Output: kibana/dashboards/*.ndjson  (importable via Kibana UI or setup_kibana.py)
"""
import json, uuid
from pathlib import Path

OUT = Path(__file__).parent / "dashboards"
OUT.mkdir(exist_ok=True)

# ── Shared Data View IDs ──────────────────────────────────────────────
DV = {
    "events":    "dv-ecommerce-events",
    "orders":    "dv-ecommerce-orders",
    "prices":    "dv-ecommerce-prices",
    "anomalies": "dv-ecommerce-anomalies",
    "fraud":     "dv-ecommerce-fraud",
}

TS = "2024-01-01T00:00:00.000Z"

def ref(dv_id, name="indexpattern-datasource-current-indexpattern"):
    return {"id": dv_id, "name": name, "type": "index-pattern"}

def layer_ref(dv_id, layer_id="layer1"):
    return {"id": dv_id, "name": f"indexpattern-datasource-layer-{layer_id}", "type": "index-pattern"}

# ── Lens: Area chart (date_histogram × metric) ────────────────────────
def lens_area(title, dv_id, metric_op, metric_field, label, color="#006BB4"):
    refs = [ref(dv_id), layer_ref(dv_id)]
    return {
        "title": title, "description": "", "visualizationType": "lnsXY", "type": "lens",
        "references": refs,
        "state": {
            "datasourceStates": {
                "formBased": {
                    "layers": {
                        "layer1": {
                            "columnOrder": ["col_date", "col_metric"],
                            "columns": {
                                "col_date": {
                                    "dataType": "date", "isBucketed": True,
                                    "label": "@timestamp", "operationType": "date_histogram",
                                    "params": {"interval": "auto", "includeEmptyRows": True},
                                    "scale": "interval", "sourceField": "@timestamp"
                                },
                                "col_metric": {
                                    "dataType": "number", "isBucketed": False,
                                    "label": label, "operationType": metric_op,
                                    "scale": "ratio", "sourceField": metric_field
                                }
                            }
                        }
                    }
                }
            },
            "filters": [], "query": {"language": "kuery", "query": ""},
            "visualization": {
                "preferredSeriesType": "area",
                "layers": [{"accessors": ["col_metric"], "layerId": "layer1",
                            "layerType": "data", "seriesType": "area",
                            "xAccessor": "col_date",
                            "yConfig": [{"color": color, "forAccessor": "col_metric"}]}],
                "legend": {"isVisible": True, "position": "bottom"},
                "valueLabels": "hide",
                "fittingFunction": "None",
                "axisTitlesVisibilitySettings": {"x": True, "yLeft": True, "yRight": False},
                "gridlinesVisibilitySettings": {"x": False, "yLeft": True, "yRight": False},
                "tickLabelsVisibilitySettings": {"x": True, "yLeft": True, "yRight": False},
            }
        }
    }

# ── Lens: Metric (KPI card) ───────────────────────────────────────────
def lens_metric(title, dv_id, metric_op, metric_field, label, prefix="", postfix=""):
    refs = [ref(dv_id), layer_ref(dv_id)]
    return {
        "title": title, "description": "", "visualizationType": "lnsMetric", "type": "lens",
        "references": refs,
        "state": {
            "datasourceStates": {
                "formBased": {
                    "layers": {
                        "layer1": {
                            "columnOrder": ["col_metric"],
                            "columns": {
                                "col_metric": {
                                    "dataType": "number", "isBucketed": False,
                                    "label": label, "operationType": metric_op,
                                    "scale": "ratio", "sourceField": metric_field,
                                    "params": {"format": {"id": "number", "params": {"decimals": 0}}}
                                }
                            }
                        }
                    }
                }
            },
            "filters": [], "query": {"language": "kuery", "query": ""},
            "visualization": {
                "layerId": "layer1", "layerType": "data",
                "metricAccessor": "col_metric",
                "color": "#006BB4",
                "titlePosition": "bottom"
            }
        }
    }

# ── Lens: Bar chart (terms × metric) ─────────────────────────────────
def lens_bar(title, dv_id, terms_field, metric_op, metric_field, label, limit=10):
    refs = [ref(dv_id), layer_ref(dv_id)]
    return {
        "title": title, "description": "", "visualizationType": "lnsXY", "type": "lens",
        "references": refs,
        "state": {
            "datasourceStates": {
                "formBased": {
                    "layers": {
                        "layer1": {
                            "columnOrder": ["col_terms", "col_metric"],
                            "columns": {
                                "col_terms": {
                                    "dataType": "string", "isBucketed": True,
                                    "label": f"Top {limit} {terms_field}",
                                    "operationType": "terms", "scale": "ordinal",
                                    "sourceField": terms_field,
                                    "params": {"size": limit, "orderBy": {"columnId": "col_metric", "type": "column"},
                                               "orderDirection": "desc", "otherBucket": False, "missingBucket": False}
                                },
                                "col_metric": {
                                    "dataType": "number", "isBucketed": False,
                                    "label": label, "operationType": metric_op,
                                    "scale": "ratio", "sourceField": metric_field
                                }
                            }
                        }
                    }
                }
            },
            "filters": [], "query": {"language": "kuery", "query": ""},
            "visualization": {
                "preferredSeriesType": "bar_horizontal",
                "layers": [{"accessors": ["col_metric"], "layerId": "layer1",
                            "layerType": "data", "seriesType": "bar_horizontal",
                            "xAccessor": "col_terms"}],
                "legend": {"isVisible": False, "position": "right"},
                "valueLabels": "inside",
            }
        }
    }

# ── Lens: Donut (terms × count) ───────────────────────────────────────
def lens_donut(title, dv_id, terms_field, limit=8):
    refs = [ref(dv_id), layer_ref(dv_id)]
    return {
        "title": title, "description": "", "visualizationType": "lnsPie", "type": "lens",
        "references": refs,
        "state": {
            "datasourceStates": {
                "formBased": {
                    "layers": {
                        "layer1": {
                            "columnOrder": ["col_terms", "col_count"],
                            "columns": {
                                "col_terms": {
                                    "dataType": "string", "isBucketed": True,
                                    "label": terms_field, "operationType": "terms",
                                    "scale": "ordinal", "sourceField": terms_field,
                                    "params": {"size": limit, "orderBy": {"columnId": "col_count", "type": "column"},
                                               "orderDirection": "desc", "otherBucket": True, "missingBucket": False}
                                },
                                "col_count": {
                                    "dataType": "number", "isBucketed": False,
                                    "label": "Count", "operationType": "count",
                                    "scale": "ratio", "sourceField": "___records___"
                                }
                            }
                        }
                    }
                }
            },
            "filters": [], "query": {"language": "kuery", "query": ""},
            "visualization": {
                "shape": "donut", "layerId": "layer1", "layerType": "data",
                "metric": "col_count", "groups": ["col_terms"],
                "legend": {"isVisible": True, "position": "right", "legendSize": "auto"},
            }
        }
    }

# ── Lens: Line (date_histogram × metric) ─────────────────────────────
def lens_line(title, dv_id, metric_op, metric_field, label, color="#D4351C"):
    attrs = lens_area(title, dv_id, metric_op, metric_field, label, color)
    attrs["state"]["visualization"]["preferredSeriesType"] = "line"
    attrs["state"]["visualization"]["layers"][0]["seriesType"] = "line"
    return attrs

# ── TSVB Markdown panel ───────────────────────────────────────────────
def tsvb_markdown(content):
    return {
        "title": "", "description": "", "type": "visualization",
        "visState": json.dumps({
            "title": "", "type": "markdown",
            "params": {"markdown": content, "openLinksInNewTab": False},
            "aggs": []
        }),
        "uiStateJSON": "{}", "description": "", "version": 1,
        "kibanaSavedObjectMeta": {"searchSourceJSON": '{"query":{"query":"","language":"kuery"},"filter":[]}'}
    }

# ── Build panel dict (embedded Lens) ─────────────────────────────────
def panel(idx, lens_attrs, x, y, w, h, title=""):
    pid = str(idx)
    return {
        "version": "8.18.0", "type": "lens",
        "gridData": {"x": x, "y": y, "w": w, "h": h, "i": pid},
        "panelIndex": pid,
        "embeddableConfig": {"attributes": lens_attrs, "enhancements": {}},
        "title": title
    }

def dashboard_obj(dash_id, title, description, panels_list, dv_ids, tags=None):
    refs = []
    for dv_id in dv_ids:
        refs.append({"id": dv_id, "name": f"dataView-ref-{dv_id}", "type": "index-pattern"})
    return {
        "type": "dashboard", "id": dash_id,
        "attributes": {
            "title": title, "description": description,
            "panelsJSON": json.dumps(panels_list),
            "timeRestore": False,
            "optionsJSON": json.dumps({
                "useMargins": True, "syncColors": True,
                "syncCursor": True, "syncTooltips": True,
                "hidePanelTitles": False
            }),
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"language": "kuery", "query": ""},
                    "filter": []
                })
            },
            "refreshInterval": {"pause": False, "value": 30000},
        },
        "references": refs, "managed": False,
        "coreMigrationVersion": "8.8.0",
        "created_at": TS, "updated_at": TS, "version": "WzEsMV0="
    }

def data_view_obj(dv_id, title, time_field="@timestamp"):
    return {
        "type": "index-pattern", "id": dv_id,
        "attributes": {
            "title": title, "timeFieldName": time_field,
            "name": title.replace("*","").strip(),
            "fieldAttrs": "{}", "fields": "[]",
            "runtimeFieldMap": "{}", "sourceFilters": "[]", "typeMeta": "{}"
        },
        "references": [], "managed": False,
        "coreMigrationVersion": "8.8.0",
        "created_at": TS, "updated_at": TS, "version": "WzEsMV0="
    }

def vis_obj(vis_id, attrs):
    return {
        "type": "lens", "id": vis_id,
        "attributes": attrs,
        "references": attrs.get("references", []),
        "managed": False,
        "coreMigrationVersion": "8.8.0",
        "created_at": TS, "updated_at": TS, "version": "WzEsMV0="
    }

def write_ndjson(filename, objects):
    path = OUT / filename
    with open(path, "w", encoding="utf-8") as f:
        for obj in objects:
            f.write(json.dumps(obj, separators=(",", ":")) + "\n")
    print(f"  [OK] {path.name}  ({len(objects)} objects)")

# ═════════════════════════════════════════════════════════════════════
# DASHBOARD 1 — Executive Overview
# ═════════════════════════════════════════════════════════════════════
def dash_executive():
    ev, or_ = DV["events"], DV["orders"]

    p = [
        # Row 0 — KPI row (y=0, h=6)
        panel(1,  lens_metric("Total Events",       ev, "count",    "___records___", "Events"),              0,  0, 8,  6, "Total Events"),
        panel(2,  lens_metric("Total Orders",        or_, "count",  "___records___", "Orders"),              8,  0, 8,  6, "Total Orders"),
        panel(3,  lens_metric("Total Revenue",       or_, "sum",    "net_revenue",   "Revenue ($)"),         16, 0, 8,  6, "Total Revenue ($)"),
        panel(4,  lens_metric("Avg Order Value",     or_, "average","net_revenue",   "AOV ($)"),             24, 0, 8,  6, "Avg Order Value ($)"),
        panel(5,  lens_metric("Unique Users",        ev, "unique_count","user_id",   "Users"),               32, 0, 8,  6, "Unique Users"),
        panel(6,  lens_metric("Unique Sessions",     ev, "unique_count","session_id","Sessions"),            40, 0, 8,  6, "Unique Sessions"),

        # Row 1 — Revenue trend (y=6, h=15)
        panel(7,  lens_area("Revenue Trend", or_, "sum","net_revenue","Revenue ($)"), 0, 6, 30, 15, "Revenue Over Time"),
        panel(8,  lens_donut("Orders by Category",  or_, "category"),                 30, 6, 18, 15, "Revenue by Category"),

        # Row 2 — Events breakdown (y=21, h=14)
        panel(9,  lens_bar("Top 10 Products by Views", ev,"product_name.keyword","count","___records___","Views"),       0, 21, 24, 14, "Top Viewed Products"),
        panel(10, lens_donut("Traffic by Device",   ev, "device_type"),                24, 21, 12, 14, "Device Type"),
        panel(11, lens_donut("Traffic by Platform", ev, "platform"),                   36, 21, 12, 14, "Platform"),
    ]

    dash = dashboard_obj(
        "dash-executive-overview",
        "📊 Executive Overview — Ecommerce KPIs",
        "High-level business KPIs: revenue, orders, sessions, top products, and traffic breakdown.",
        p, [ev, or_],
        tags=["ecommerce","overview"]
    )
    write_ndjson("01_executive_overview.ndjson", [
        data_view_obj(ev,  "ecommerce-events*"),
        data_view_obj(or_, "ecommerce-orders*"),
        dash
    ])

# ═════════════════════════════════════════════════════════════════════
# DASHBOARD 2 — Customer Behavior
# ═════════════════════════════════════════════════════════════════════
def dash_behavior():
    ev = DV["events"]

    p = [
        # Funnel-style: views, cart adds, purchases (area layers)
        panel(1, lens_area("Page Views Over Time",       ev,"count","___records___","Events","#006BB4"), 0,  0, 48, 12, "Event Volume Over Time"),
        panel(2, lens_bar("Top Categories by Views",     ev,"category","count","___records___","Views"),  0, 12, 24, 14, "Category Engagement"),
        panel(3, lens_bar("Top Searched Keywords",       ev,"search_keyword.keyword","count","___records___","Searches",8), 24,12,24,14,"Search Keywords"),
        panel(4, lens_donut("Event Type Distribution",   ev,"event_type"),                                0, 26, 16, 13, "Event Types"),
        panel(5, lens_bar("Top 10 Products by Purchase",ev,"product_name.keyword","count","___records___","Purchases"), 16,26,32,13,"Most Purchased Products"),
        panel(6, lens_area("New Users vs Sessions",      ev,"unique_count","user_id","Unique Users","#00BFB3"),          0, 39, 24, 12, "Unique Users Trend"),
        panel(7, lens_donut("Traffic by Region",         ev,"region"),                                  24, 39, 24, 12, "Regional Distribution"),
    ]

    dash = dashboard_obj(
        "dash-customer-behavior",
        "👤 Customer Behavior Analytics",
        "Analyze customer preferences, event funnels, search keywords, and regional traffic.",
        p, [ev]
    )
    write_ndjson("02_customer_behavior.ndjson", [
        data_view_obj(ev, "ecommerce-events*"), dash
    ])

# ═════════════════════════════════════════════════════════════════════
# DASHBOARD 3 — Anomaly Detection
# ═════════════════════════════════════════════════════════════════════
def dash_anomaly():
    an = DV["anomalies"]

    p = [
        # KPIs
        panel(1, lens_metric("Total Anomalies",     an,"count","___records___","Count"),             0,  0, 12, 6, "Total Anomalies"),
        panel(2, lens_metric("Avg Z-Score",          an,"average","z_score","Z-Score"),              12, 0, 12, 6, "Avg Z-Score"),
        panel(3, lens_metric("Avg Anomaly Score",    an,"average","anomaly_score","Score"),          24, 0, 12, 6, "Avg Anomaly Score"),
        panel(4, lens_metric("Avg Deviation %",      an,"average","deviation_pct","Deviation"),      36, 0, 12, 6, "Avg Deviation (%)"),

        # Timeline with anomaly score
        panel(5, lens_line("Anomaly Score Timeline",  an,"max","anomaly_score","Max Score","#D4351C"),  0, 6,  36, 14, "Anomaly Score Over Time"),
        panel(6, lens_donut("Severity Distribution",  an,"severity"),                                  36, 6, 12, 14, "Severity"),

        # Category breakdown
        panel(7, lens_bar("Anomalies by Category",    an,"category","count","___records___","Anomalies"), 0,20,24,14,"Anomalies by Category"),
        panel(8, lens_bar("Anomalies by Type",         an,"anomaly_type","count","___records___","Count"),24,20,24,14,"Anomaly Types"),

        # Z-score trend
        panel(9, lens_area("Z-Score Trend",            an,"max","z_score","Z-Score","#F86200"),        0, 34, 48, 12, "Max Z-Score Over Time"),
    ]

    dash = dashboard_obj(
        "dash-anomaly-detection",
        "⚠️ Anomaly Detection — Event Monitoring",
        "Monitor unusual traffic, revenue spikes, Z-score deviations, and severity distributions.",
        p, [an]
    )
    write_ndjson("03_anomaly_detection.ndjson", [
        data_view_obj(an, "ecommerce-anomalies*"), dash
    ])

# ═════════════════════════════════════════════════════════════════════
# DASHBOARD 4 — Price Forecasting
# ═════════════════════════════════════════════════════════════════════
def dash_price():
    pr = DV["prices"]

    p = [
        # KPI row
        panel(1, lens_metric("Avg Price Change %", pr,"average","price_change_pct","Change %"),      0,  0, 12, 6, "Avg Price Change (%)"),
        panel(2, lens_metric("Avg Volatility Score",pr,"average","volatility_score","Volatility"),   12, 0, 12, 6, "Avg Volatility Score"),
        panel(3, lens_metric("Avg Forecast 7d",     pr,"average","forecast_7d","Forecast $"),        24, 0, 12, 6, "Avg 7-Day Forecast ($)"),
        panel(4, lens_metric("Total Price Events",  pr,"count","___records___","Events"),            36, 0, 12, 6, "Price Change Events"),

        # Price trend
        panel(5, lens_line("Avg Price Over Time",    pr,"average","new_price","Avg Price ($)","#006BB4"), 0,  6, 36, 15, "Avg Price Trend"),
        panel(6, lens_donut("Price Direction",        pr,"price_direction"),                              36, 6, 12, 15, "Price Direction"),

        # SMA vs EMA
        panel(7, lens_area("SMA 7d Trend",           pr,"average","sma_7d","SMA 7d","#00BFB3"),          0, 21, 24, 13, "SMA 7-Day Moving Average"),
        panel(8, lens_area("EMA 14d Trend",           pr,"average","ema_14d","EMA 14d","#F86200"),        24,21, 24, 13, "EMA 14-Day Moving Average"),

        # Volatility & elasticity
        panel(9, lens_bar("Volatility by Category",  pr,"category","average","volatility_score","Volatility"), 0,34,24,13,"Price Volatility by Category"),
        panel(10,lens_bar("Elasticity by Category",  pr,"category","average","elasticity_index","Elasticity"), 24,34,24,13,"Price Elasticity by Category"),
    ]

    dash = dashboard_obj(
        "dash-price-forecasting",
        "📈 Price Forecasting — ML Price Intelligence",
        "Historical prices, SMA/EMA trends, volatility, elasticity, and 7/30-day ML forecasts.",
        p, [pr]
    )
    write_ndjson("04_price_forecasting.ndjson", [
        data_view_obj(pr, "ecommerce-prices*"), dash
    ])

# ═════════════════════════════════════════════════════════════════════
# DASHBOARD 5 — Fraud Detection
# ═════════════════════════════════════════════════════════════════════
def dash_fraud():
    fr = DV["fraud"]

    p = [
        # KPIs
        panel(1, lens_metric("Total Alerts",          fr,"count","___records___","Alerts"),              0,  0, 12, 6, "Total Fraud Alerts"),
        panel(2, lens_metric("Avg Risk Score",         fr,"average","risk_score","Risk Score"),          12, 0, 12, 6, "Avg Risk Score"),
        panel(3, lens_metric("Avg Isolation Score",    fr,"average","isolation_forest_score","IF Score"),24, 0, 12, 6, "Avg Isolation Forest Score"),
        panel(4, lens_metric("Total At-Risk Amount",   fr,"sum","total_amount","Amount ($)"),            36, 0, 12, 6, "Total At-Risk Amount ($)"),

        # Timeline
        panel(5, lens_line("Fraud Alerts Timeline",    fr,"count","___records___","Alerts","#D4351C"),    0,  6, 36, 14, "Fraud Alerts Over Time"),
        panel(6, lens_donut("Risk Level Distribution", fr,"risk_level"),                                 36, 6, 12, 14, "Risk Levels"),

        # Breakdowns
        panel(7, lens_bar("Fraud by Type",             fr,"fraud_type","count","___records___","Count"),  0, 20, 24, 13, "Fraud Type Breakdown"),
        panel(8, lens_bar("Fraud by Payment Method",   fr,"payment_method","count","___records___","Count"),24,20,24,13,"Payment Method Risk"),

        # Geo + model
        panel(9, lens_bar("Fraud by Region",           fr,"region","sum","total_amount","Amount ($)"),    0, 33, 24, 13, "At-Risk Amount by Region"),
        panel(10,lens_bar("Detection Model Breakdown", fr,"detection_model","count","___records___","Alerts"),24,33,24,13,"Detection Model"),
    ]

    dash = dashboard_obj(
        "dash-fraud-detection",
        "🛡️ Fraud Detection — Threat Intelligence",
        "Monitor fraud alerts, risk scores, Isolation Forest results, payment risk, and geo distribution.",
        p, [fr]
    )
    write_ndjson("05_fraud_detection.ndjson", [
        data_view_obj(fr, "ecommerce-fraud-alerts*"), dash
    ])

# ═════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("Generating Kibana 8.18 NDJSON dashboards…")
    dash_executive()
    dash_behavior()
    dash_anomaly()
    dash_price()
    dash_fraud()
    print(f"\nDone! Files in: {OUT}")
    print("Import via: python kibana/setup_kibana.py")
    print("   OR: Kibana → Stack Management → Saved Objects → Import")
