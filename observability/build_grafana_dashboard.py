"""Build the Grafana 'Lambda pipeline logs' dashboard: volume and errors per layer, then one log panel per layer."""
import json
from pathlib import Path

DS = {"type": "loki", "uid": "loki"}
SEL = 'project="mp-tikihtml"'
ERR = ""  # errors are selected by the level label Promtail sets
LAYERS = [("crawl", "Crawl (crawl-worker): tải trang, ghi bronze, publish Kafka"),
          ("ingest", "Ingest (kafka-init): tạo topic Kafka"),
          ("speed", "Speed (Spark Structured Streaming): Kafka → ES / Redis"),
          ("batch", "Batch (scheduler + Spark batch): Silver → Gold → quality → Postgres"),
          ("serving", "Serving (es-projector, Kibana, Superset)"),
          ("storage", "Storage (Kafka, MinIO, Postgres, Redis, Elasticsearch)")]
panels, pid, y = [], 1, 0


def add(p, h, w=24, x=0):
    global pid, y
    p.update(id=pid, gridPos={"h": h, "w": w, "x": x, "y": y}, datasource=DS)
    pid += 1
    panels.append(p)


def ts(title, expr, legend, w, x, unit="short", stack=True):
    return {"type": "timeseries", "title": title,
            "targets": [{"refId": "A", "expr": expr, "legendFormat": legend, "datasource": DS}],
            "fieldConfig": {"defaults": {"unit": unit, "custom": {"drawStyle": "bars", "fillOpacity": 60,
                                                                  "stacking": {"mode": "normal" if stack else "none"}}}, "overrides": []},
            "options": {"legend": {"displayMode": "table", "placement": "right", "calcs": ["sum"]}}}


add(ts("Số dòng log / phút theo layer", f'sum by (layer) (count_over_time({{{SEL}, layer=~"$layer"}}[1m]))', "{{layer}}", 12, 0), 8, 12, 0)
add(ts("Lỗi / 5 phút theo layer (level=error: ERROR, FATAL, Traceback, XxxError:)",
       f'sum by (layer) (count_over_time({{{SEL}, layer=~"$layer", level="error"}} [5m]))', "{{layer}}", 12, 12), 8, 12, 12)
y += 8
add({"type": "stat", "title": "Lỗi trong khoảng thời gian đang chọn",
     "targets": [{"refId": "A", "expr": f'sum by (layer) (count_over_time({{{SEL}, layer=~"$layer", level="error"}} [$__range]))',
                  "legendFormat": "{{layer}}", "instant": True, "datasource": DS}],
     "fieldConfig": {"defaults": {"thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 1}]}}, "overrides": []},
     "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "textMode": "value_and_name"}}, 5)
y += 5
add({"type": "logs", "title": "Chỉ dòng lỗi và cảnh báo (level error/warn), mới nhất trước",
     "targets": [{"refId": "A", "expr": f'{{{SEL}, layer=~"$layer", service=~"$service", level=~"error|warn"}}', "datasource": DS}],
     "options": {"showTime": True, "showLabels": True, "wrapLogMessage": True, "sortOrder": "Descending", "enableLogDetails": True}}, 10)
y += 10
for layer, title in LAYERS:
    panels.append({"type": "row", "title": title, "id": pid, "collapsed": False, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})
    pid += 1
    y += 1
    add({"type": "logs", "title": f"Log {layer}",
         "targets": [{"refId": "A", "expr": f'{{{SEL}, layer="{layer}", service=~"$service"}} |~ "$search"', "datasource": DS}],
         "options": {"showTime": True, "showLabels": True, "wrapLogMessage": True, "sortOrder": "Descending", "enableLogDetails": True}}, 9)
    y += 9

dash = {
    "uid": "lambda-logs", "title": "Lambda pipeline — log theo layer", "timezone": "Asia/Ho_Chi_Minh",
    "refresh": "10s", "time": {"from": "now-1h", "to": "now"}, "schemaVersion": 39, "tags": ["lambda", "logs"],
    "templating": {"list": [
        {"name": "layer", "type": "query", "datasource": DS, "query": {"label": "layer", "type": 1, "stream": "", "refId": "LokiVariableQueryEditor-VariableQuery"},
         "definition": "label_values(layer)", "multi": True, "includeAll": True, "current": {"text": "All", "value": "$__all"}, "refresh": 2},
        {"name": "service", "type": "query", "datasource": DS, "query": {"label": "service", "type": 1, "stream": f'{{{SEL}, layer=~"$layer"}}', "refId": "LokiVariableQueryEditor-VariableQuery"},
         "definition": "label_values(service)", "multi": True, "includeAll": True, "current": {"text": "All", "value": "$__all"}, "refresh": 2},
        {"name": "search", "type": "textbox", "label": "Tìm trong log (regex)", "query": "", "current": {"text": "", "value": ""}},
    ]},
    "panels": panels,
}
out = Path(__file__).parent / "grafana" / "dashboards" / "lambda-logs.json"
out.write_text(json.dumps(dash, ensure_ascii=False, indent=1), encoding="utf-8")
print(out, len(panels))
