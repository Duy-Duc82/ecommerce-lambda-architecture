"""Superset dashboard for the batch layer's outputs (Gold marts in cache.*, batch audit in audit.*).

Charts read the datasets superset-init imported from display/superset/datasources.yaml; two virtual
datasets join change/counter marts to offer titles. Every chart gets a query_context, and the script
asks the chart-data API for each one and fails if any returns an error or no rows.

usage: python build_superset_batch_dashboard.py [http://localhost:8089]
"""
import json, sys

import requests

S = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8089"
SLUG = "marketplace-batch-gold"
TITLE = "Marketplace — batch layer (Gold, chất lượng, lịch sử)"

VIRTUAL = {
    "v_top_price_moves": """
        select c.observed_date, o.product_title, o.category_path, c.price_change_count, c.price_drop_count,
               c.price_increase_count, c.max_price_drop, c.max_price_increase, o.current_price
        from cache.marketplace_offer_change_daily c
        join cache.marketplace_offer_current o using (marketplace, offer_id)""",
    "v_counter_velocity": """
        select d.observed_date, o.product_title, d.counter_name, d.first_value, d.last_value, d.valid_delta_sum,
               round(d.velocity_proxy_per_hour::numeric, 3) as velocity_per_hour, d.invalid_transition_count
        from cache.marketplace_counter_delta_daily d
        join cache.marketplace_offer_current o using (marketplace, offer_id)""",
}


class Client:
    def __init__(self):
        self.s = requests.Session()
        t = self.s.post(f"{S}/api/v1/security/login", json={"username": "admin", "password": "admin", "provider": "db", "refresh": True}, timeout=30)
        t.raise_for_status()
        self.tok = t.json()["access_token"]
        self.csrf = self.s.get(f"{S}/api/v1/security/csrf_token/", headers=self.h(), timeout=30).json()["result"]

    def h(self, w=False):
        h = {"Authorization": f"Bearer {self.tok}"}
        if w:
            h.update({"X-CSRFToken": self.csrf, "Referer": S, "Content-Type": "application/json"})
        return h

    def get(self, p):
        r = self.s.get(f"{S}{p}", headers=self.h(), timeout=60)
        r.raise_for_status()
        return r.json()

    def send(self, method, p, body):
        r = self.s.request(method, f"{S}{p}", headers=self.h(True), json=body, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {p}: {r.status_code} {r.text[:600]}")
        return r.json()


def M(col, agg, label):
    return {"expressionType": "SIMPLE", "column": {"column_name": col}, "aggregate": agg, "label": label}


def MS(sql, label):
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


def raw(cols, order, n=200, where=None):
    return {"query_mode": "raw", "all_columns": cols, "order_by_cols": [json.dumps([order[0], order[1]])], "row_limit": n,
            "adhoc_filters": [{"expressionType": "SQL", "clause": "WHERE", "sqlExpression": where}] if where else []}


def qc(ds, p, viz):
    where = " AND ".join(f["sqlExpression"] for f in p.get("adhoc_filters", []))
    if p.get("query_mode") == "raw":
        q = {"columns": p["all_columns"], "metrics": [], "orderby": [json.loads(o) for o in p["order_by_cols"]], "row_limit": p["row_limit"]}
    elif viz == "big_number_total":
        q = {"columns": [], "metrics": [p["metric"]], "row_limit": 1}
    elif viz == "pie":
        q = {"columns": p["groupby"], "metrics": [p["metric"]], "row_limit": p.get("row_limit", 50)}
    else:
        q = {"columns": [p["x_axis"], *p.get("groupby", [])], "metrics": p["metrics"], "row_limit": p.get("row_limit", 1000)}
    q.update({"filters": [], "extras": {"where": where} if where else {}})
    return {"datasource": {"id": ds, "type": "table"}, "force": False, "queries": [q], "form_data": p, "result_format": "json", "result_type": "full"}


def main():
    c = Client()
    db = next(d["id"] for d in c.get("/api/v1/database/")["result"] if d["database_name"] == "ecommerce_bi")
    ds = {(d["schema"], d["table_name"]): d["id"] for d in c.get("/api/v1/dataset/?q=(page_size:500)")["result"]}
    for name, sql in VIRTUAL.items():
        if ("cache", name) not in ds:
            ds[("cache", name)] = c.send("POST", "/api/v1/dataset/", {"database": db, "schema": "cache", "table_name": name, "sql": sql})["id"]
        else:
            c.send("PUT", f"/api/v1/dataset/{ds[('cache', name)]}?override_columns=true", {"sql": sql})
    for (schema, t), i in ds.items():  # pick up columns of tables that were empty when superset-init ran
        if (schema in ("cache", "audit") and t.startswith("marketplace")) or t in VIRTUAL:
            c.s.put(f"{S}/api/v1/dataset/{i}/refresh", headers=c.h(True), timeout=60)

    D = lambda t, s="cache": ds[(s, t)]
    charts = [
        # row 1: batch health
        ("Batch: các lần chạy gần nhất", D("marketplace_batch_run", "audit"), "table",
         raw(["run_id", "status", "quality_status", "as_of", "started_at", "completed_at", "silver_rows", "gold_rows", "cache_published", "error_message"], ("started_at", False), 50), 8, 40),
        ("Batch: số offer trong Gold", D("marketplace_offer_current"), "big_number_total",
         {"metric": MS("count(*)", "offers"), "subheader": "cache.marketplace_offer_current"}, 4, 40),
        ("Chất lượng: kết quả kiểm tra của batch gần nhất", D("marketplace_quality_result", "audit"), "table",
         raw(["checked_at", "run_id", "check_name", "dataset_name", "severity", "status", "observed_value", "expectation"], ("checked_at", False), 100), 12, 45),
        # row 2: source coverage and freshness
        ("Nguồn: độ phủ theo ngày", D("marketplace_source_coverage_daily"), "table",
         raw(["observed_date", "marketplace", "observed_offer_count", "fresh_offer_count", "stale_offer_count", "coverage_rate", "rejection_rate"], ("observed_date", False), 30), 6, 40),
        ("Nguồn: độ tin cậy crawl theo ngày", D("marketplace_crawl_reliability_daily"), "table",
         raw(["request_date", "request_count", "succeeded_count", "failed_count", "success_rate", "avg_latency_ms", "p95_latency_ms", "rate_limited_count"], ("request_date", False), 30), 6, 40),
        ("Độ tươi của offer (as_of batch gần nhất)", D("marketplace_offer_freshness"), "pie",
         {"groupby": ["freshness_status"], "metric": MS("count(*)", "offers"), "row_limit": 10, "show_labels": True}, 4, 45),
        ("Trạng thái phát hiện giá bất thường", D("marketplace_price_anomaly_daily"), "pie",
         {"groupby": ["anomaly_status"], "metric": MS("count(*)", "offer-days"), "row_limit": 10, "show_labels": True}, 4, 45),
        ("Số lần đổi giá theo ngày", D("marketplace_offer_change_daily"), "echarts_timeseries_bar",
         {"x_axis": "observed_date", "metrics": [M("price_drop_count", "SUM", "giảm giá"), M("price_increase_count", "SUM", "tăng giá")],
          "groupby": [], "row_limit": 100, "stack": True}, 4, 45),
        # row 3: prices
        ("Giá theo danh mục (ngày gần nhất)", D("marketplace_category_price_daily"), "table",
         raw(["observed_date", "category_path", "observed_offer_count", "min_price", "p25_price", "median_price", "p75_price", "max_price"], ("observed_offer_count", False), 100), 12, 50),
        ("Sản phẩm đổi giá nhiều nhất", D("v_top_price_moves"), "table",
         raw(["observed_date", "product_title", "price_change_count", "price_drop_count", "price_increase_count", "max_price_drop", "max_price_increase", "current_price"], ("price_change_count", False), 100, "price_change_count > 0"), 12, 50),
        ("Lịch sử giá theo ngày (min / max / số lần quan sát)", D("marketplace_offer_price_history_daily"), "table",
         raw(["observed_date", "offer_id", "first_price", "last_price", "min_price", "max_price", "observation_count", "distinct_price_count"], ("distinct_price_count", False), 100), 6, 50),
        ("Bộ đếm công khai: tốc độ tăng (không phải doanh số)", D("v_counter_velocity"), "table",
         raw(["observed_date", "product_title", "counter_name", "first_value", "last_value", "valid_delta_sum", "velocity_per_hour", "invalid_transition_count"], ("valid_delta_sum", False), 100, "valid_delta_sum > 0 or invalid_transition_count > 0"), 6, 50),
        ("Người bán theo số offer", D("marketplace_seller_current"), "table",
         raw(["seller_id", "observed_offer_count", "first_seen_at", "last_seen_at"], ("observed_offer_count", False), 50), 12, 40),
    ]
    names = {x[0] for x in charts}
    for ch in c.get("/api/v1/chart/?q=(page_size:500)")["result"]:
        if ch["slice_name"] in names:
            c.s.delete(f"{S}/api/v1/chart/{ch['id']}", headers=c.h(True), timeout=60)
    made = []
    for name, dsid, viz, params, w, hgt in charts:
        p = dict(params, datasource=f"{dsid}__table", viz_type=viz)
        cid = c.send("POST", "/api/v1/chart/", {"slice_name": name, "viz_type": viz, "datasource_type": "table", "datasource_id": dsid,
                                                "params": json.dumps(p), "query_context": json.dumps(qc(dsid, p, viz))})["id"]
        made.append((cid, w, hgt, name))
    dash = next((d["id"] for d in c.get("/api/v1/dashboard/?q=(page_size:200)")["result"] if d.get("slug") == SLUG), None)
    if dash is None:
        dash = c.send("POST", "/api/v1/dashboard/", {"dashboard_title": TITLE, "slug": SLUG, "published": True})["id"]
    for cid, *_ in made:
        c.send("PUT", f"/api/v1/chart/{cid}", {"dashboards": [dash]})
    pos = {"DASHBOARD_VERSION_KEY": "v2", "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"], "meta": {}},
           "GRID_ID": {"id": "GRID_ID", "type": "GRID", "children": [], "parents": ["ROOT_ID"], "meta": {}}}
    row, used, n = None, 12, 0
    for cid, w, hgt, _ in made:
        if used + w > 12:
            n += 1
            row = f"ROW-{n}"
            pos[row] = {"id": row, "type": "ROW", "children": [], "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            pos["GRID_ID"]["children"].append(row)
            used = 0
        pos[row]["children"].append(f"CHART-{cid}")
        pos[f"CHART-{cid}"] = {"id": f"CHART-{cid}", "type": "CHART", "children": [], "parents": ["ROOT_ID", "GRID_ID", row],
                               "meta": {"chartId": cid, "width": w, "height": hgt}}
        used += w
    c.send("PUT", f"/api/v1/dashboard/{dash}", {"slug": SLUG, "published": True, "position_json": json.dumps(pos),
                                                "json_metadata": json.dumps({"refresh_frequency": 300, "timed_refresh_immune_slices": []})})
    problems = []
    for cid, _, _, name in made:
        r = c.s.get(f"{S}/api/v1/chart/{cid}/data/", headers=c.h(), timeout=120)
        rows = sum(len(q.get("data") or []) for q in r.json().get("result", [])) if r.ok else 0
        if not r.ok or rows == 0:
            problems.append((name, r.status_code, r.text[:200] if not r.ok else "no rows"))
    print(json.dumps({"url": f"{S}/superset/dashboard/{SLUG}/", "charts": len(made), "problems": problems}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
