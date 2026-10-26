"""Kibana dashboard for the advisor demo: data flowing through the speed layer minute by minute.

Built with the repository's own Lens helpers (src/ecomdata/serving/kibana/marketplace_dashboards.py) on the data views
the marketplace setup already imported. Imported with overwrite, so a re-run replaces it.

usage: python observability/build_kibana_demo_dashboard.py [http://localhost:5602]
"""
import json, sys
from pathlib import Path

import requests

from ecomdata.serving.kibana import marketplace_dashboards as kd  # noqa: E402

KIBANA = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:5602"
CRAWL, SPEED, CHANGES, OFFERS = ("dv-marketplace-crawl-attempts", "dv-marketplace-speed-batches",
                                 "dv-marketplace-changes", "dv-marketplace-offers-current")


def metric_vis(layer_id, accessor):
    return {"layerId": layer_id, "layerType": "data", "metricAccessor": accessor}


P, g = kd._Panels(), kd._grid
minute = lambda f: kd._date_histogram(f, interval="1m", label="phút")

P.add(kd._lens_panel("m1", title="Offer đang theo dõi", description="Số offer có trạng thái hiện tại trong Elasticsearch.",
                     data_view=OFFERS, visualization_type="lnsMetric", columns={"m": kd._count("Offer")},
                     visualization=metric_vis("layer-m1", "m"), grid=g(0, 0, 12, 7, "m1")))
P.add(kd._lens_panel("m2", title="Trang crawl trong khung thời gian", description="Số lượt crawl trang danh mục Tiki.",
                     data_view=CRAWL, visualization_type="lnsMetric", columns={"m": kd._count("Trang")},
                     visualization=metric_vis("layer-m2", "m"), grid=g(12, 0, 12, 7, "m2")))
P.add(kd._lens_panel("m3", title="Dòng speed layer đã xử lý", description="Tổng input_rows của các micro-batch.",
                     data_view=SPEED, visualization_type="lnsMetric", columns={"m": kd._metric("sum", "input_rows", "Dòng")},
                     visualization=metric_vis("layer-m3", "m"), grid=g(24, 0, 12, 7, "m3")))
P.add(kd._lens_panel("m4", title="Sự kiện thay đổi", description="Đổi giá, đổi bộ đếm, offer mới ... do speed layer phát hiện.",
                     data_view=CHANGES, visualization_type="lnsMetric", columns={"m": kd._count("Sự kiện")},
                     visualization=metric_vis("layer-m4", "m"), grid=g(36, 0, 12, 7, "m4")))

P.add(kd._lens_panel("p1", title="① Crawl: số trang tải mỗi phút",
                     description="Mỗi cột là một phút. Crawler tải một trang danh mục khoảng mỗi 5 giây (~10 trang/phút).",
                     data_view=CRAWL, visualization_type="lnsXY",
                     columns={"x": minute("completed_at"), "s": kd._terms("status", size=3, label="trạng thái"), "y": kd._count("Trang")},
                     visualization=kd._xy("layer-p1", x="x", ys=["y"], split="s"), grid=g(0, 7, 24, 14, "p1")))
P.add(kd._lens_panel("p2", title="② Speed layer: số dòng xử lý mỗi phút",
                     description="Tổng input_rows của các micro-batch Spark Structured Streaming trong mỗi phút (~40 sản phẩm mỗi trang).",
                     data_view=SPEED, visualization_type="lnsXY",
                     columns={"x": minute("started_at"), "y": kd._metric("sum", "input_rows", "Dòng xử lý")},
                     visualization=kd._xy("layer-p2", x="x", ys=["y"], series_type="area"), grid=g(24, 7, 24, 14, "p2")))
P.add(kd._lens_panel("p3", title="③ Micro-batch mỗi phút và thời gian xử lý",
                     description="Spark trigger mỗi 5 giây: khoảng 12 micro-batch mỗi phút. Đường là thời gian xử lý trung vị (ms).",
                     data_view=SPEED, visualization_type="lnsXY",
                     columns={"x": minute("started_at"), "c": kd._count("Micro-batch"),
                              "d": kd._metric("median", "duration_ms", "Thời gian xử lý trung vị (ms)")},
                     visualization=kd._xy("layer-p3", x="x", ys=["c", "d"], series_type="line"), grid=g(0, 21, 24, 14, "p3")))
P.add(kd._lens_panel("p4", title="④ Sự kiện thay đổi mỗi phút theo loại",
                     description="PRICE_CHANGED, COUNTER_CHANGED, LARGE_PRICE_DROP ... theo detected_at.",
                     data_view=CHANGES, visualization_type="lnsXY",
                     columns={"x": minute("detected_at"), "s": kd._terms("change_type", size=7, label="loại"), "y": kd._count("Sự kiện")},
                     visualization=kd._xy("layer-p4", x="x", ys=["y"], split="s"), grid=g(24, 21, 24, 14, "p4")))
P.add(kd._lens_panel("p5", title="⑤ Thời gian một lượt crawl (ms)",
                     description="Trung vị và p95 latency_ms mỗi phút: chờ giãn cách 3,5 s, tải trang, ghi Bronze, publish Kafka.",
                     data_view=CRAWL, visualization_type="lnsXY",
                     columns={"x": minute("completed_at"), "a": kd._metric("median", "latency_ms", "Trung vị (ms)"),
                              "b": kd._metric("percentile", "latency_ms", "p95 (ms)", params={"percentile": 95})},
                     visualization=kd._xy("layer-p5", x="x", ys=["a", "b"], series_type="line"), grid=g(0, 35, 48, 12, "p5")))

dash = kd._dashboard("marketplace-live-demo", title="Demo — dữ liệu chảy liên tục qua pipeline",
                     description="Crawl → Kafka → Spark Structured Streaming → Elasticsearch, từng phút. Tự làm mới 5 giây.",
                     panels=P.panels, references=P.references, time_from="now-1h")
dash["attributes"]["refreshInterval"] = {"pause": False, "value": 5000}
nd = json.dumps(kd._stamped(dash), ensure_ascii=False) + "\n"
r = requests.post(f"{KIBANA}/api/saved_objects/_import", params={"overwrite": "true"}, headers={"kbn-xsrf": "true"},
                  files={"file": ("demo.ndjson", nd.encode("utf-8"), "application/ndjson")}, timeout=60)
r.raise_for_status()
print(json.dumps(r.json(), ensure_ascii=False)[:600])
print(f"{KIBANA}/app/dashboards#/view/marketplace-live-demo")
