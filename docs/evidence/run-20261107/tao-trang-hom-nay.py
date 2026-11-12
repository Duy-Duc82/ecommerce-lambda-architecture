"""Tạo trang HTML tóm tắt dữ liệu crawl hôm nay (giờ VN) từ Elasticsearch + Airflow.

    python docs/evidence/run-20261107/tao-trang-hom-nay.py

Ghi ra hom-nay.html cùng thư mục. Chạy lại bất cứ lúc nào để cập nhật số.
"""
import datetime as dt
import html
import json
import pathlib
import subprocess
import urllib.request

ES = "http://127.0.0.1:9203"
VN = dt.timezone(dt.timedelta(hours=7))
now = dt.datetime.now(VN)
start = now.replace(hour=0, minute=0, second=0, microsecond=0)
START = start.isoformat()
OUT = pathlib.Path(__file__).with_name("hom-nay.html")


def es(path, body):
    req = urllib.request.Request(ES + path, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def hourly(index, field, extra=None):
    q = {"range": {field: {"gte": START}}}
    if extra:
        q = {"bool": {"filter": [q, extra]}}
    r = es(f"/{index}/_search", {"size": 0, "query": q, "aggs": {"h": {"date_histogram": {
        "field": field, "calendar_interval": "hour", "time_zone": "+07:00",
        "extended_bounds": {"min": START, "max": now.isoformat()}, "min_doc_count": 0}}}})
    return [(dt.datetime.fromisoformat(b["key_as_string"]).hour if "key_as_string" in b
             else dt.datetime.fromtimestamp(b["key"] / 1000, VN).hour, b["doc_count"])
            for b in r["aggregations"]["h"]["buckets"]]


def count(index, query):
    return es(f"/{index}/_count", {"query": query})["count"]


since = lambda f: {"range": {f: {"gte": START}}}
pages_ok = count("marketplace-crawl-attempts-v1", {"bool": {"filter": [since("started_at"), {"term": {"status": "SUCCEEDED"}}]}})
pages_all = count("marketplace-crawl-attempts-v1", since("started_at"))
offers_today = count("marketplace-offers-current-v1", since("observed_at"))
offers_total = count("marketplace-offers-current-v1", {"match_all": {}})
price_changes = count("marketplace-changes-v1", {"bool": {"filter": [since("detected_at"), {"term": {"field_name": "current_price"}}]}})
new_offers = count("marketplace-changes-v1", {"bool": {"filter": [since("detected_at"), {"term": {"change_type": "NEW_OFFER"}}]}})

crawl_h = hourly("marketplace-crawl-attempts-v1", "started_at", {"term": {"status": "SUCCEEDED"}})
price_h = hourly("marketplace-changes-v1", "detected_at", {"term": {"field_name": "current_price"}})

# Top giảm giá mạnh nhất hôm nay
# Duyệt HẾT thay đổi giá hôm nay (scroll); bản cũ chỉ lấy 500 bản ghi đầu nên top 10 bị sai.
r = es("/marketplace-changes-v1/_search?scroll=2m", {"size": 5000, "query": {"bool": {"filter": [
    since("detected_at"), {"term": {"field_name": "current_price"}}]}}})
price_hits = []
while r["hits"]["hits"]:
    price_hits += r["hits"]["hits"]
    r = es("/_search/scroll", {"scroll": "2m", "scroll_id": r["_scroll_id"]})
drops = {}
for h in price_hits:
    s = h["_source"]
    try:
        old, new = float(s["previous_value"]), float(s["current_value"])
    except (TypeError, ValueError):
        continue
    if old > 0 and new < old:
        pct = (old - new) / old * 100
        if pct > drops.get(s["offer_id"], (0,))[0]:
            drops[s["offer_id"]] = (pct, old, new)
top = sorted(drops.items(), key=lambda kv: -kv[1][0])[:10]
info = {}
if top:
    m = es("/marketplace-offers-current-v1/_mget", {"ids": [k for k, _ in top]})
    info = {d["_id"]: d.get("_source", {}) for d in m["docs"] if d.get("found")}

# Airflow: các run batch hôm nay
sql = ("select run_id, state, to_char(start_date at time zone 'Asia/Ho_Chi_Minh','HH24:MI'), "
       "coalesce(round(extract(epoch from end_date-start_date)/60.0,1)::text,'') from dag_run "
       f"where dag_id='marketplace_batch' and start_date >= '{START}' order by start_date")
try:
    out = subprocess.run(["docker", "exec", "rf-postgres-dw", "psql", "-U", "admin", "-d", "airflow", "-AtF", "|", "-c", sql],
                         capture_output=True, text=True, timeout=30).stdout
    runs = [l.split("|") for l in out.strip().splitlines() if l]
except Exception:
    runs = []


def fmt(n):
    return f"{n:,.0f}".replace(",", ".")


def bars(data, label):
    w, h, pad_l, pad_b, pad_t = 640, 220, 40, 26, 14
    mx = max([v for _, v in data] + [1])
    n = max(len(data), 1)
    bw = (w - pad_l - 8) / n
    parts = []
    for i in range(5):
        y = pad_t + (h - pad_t - pad_b) * i / 4
        val = mx * (1 - i / 4)
        parts.append(f'<line x1="{pad_l}" x2="{w}" y1="{y:.1f}" y2="{y:.1f}" class="grid"/>'
                     f'<text x="{pad_l - 6}" y="{y + 4:.1f}" class="ax" text-anchor="end">{fmt(val)}</text>')
    for i, (hr, v) in enumerate(data):
        bh = (h - pad_t - pad_b) * v / mx
        x = pad_l + i * bw + 2
        y = h - pad_b - bh
        r = min(4, bh / 2, (bw - 4) / 2)
        parts.append(f'<g class="bar"><rect x="{x:.1f}" y="{pad_t}" width="{bw - 4:.1f}" height="{h - pad_t - pad_b}" fill="transparent"/>'
                     f'<path d="M{x:.1f},{h - pad_b} V{y + r:.1f} Q{x:.1f},{y:.1f} {x + r:.1f},{y:.1f} H{x + bw - 4 - r:.1f} '
                     f'Q{x + bw - 4:.1f},{y:.1f} {x + bw - 4:.1f},{y + r:.1f} V{h - pad_b}Z" class="mark"/>'
                     f'<title>{hr:02d}:00 – {fmt(v)} {label}</title></g>'
                     f'<text x="{x + (bw - 4) / 2:.1f}" y="{h - 8}" class="ax" text-anchor="middle">{hr}h</text>')
    return f'<svg viewBox="0 0 {w} {h}" role="img" aria-label="{label} theo giờ">{"".join(parts)}</svg>'


rows = "".join(
    f'<tr><td><a href="{html.escape(info.get(k, {}).get("source_url") or "#")}" target="_blank">'
    f'{html.escape((info.get(k, {}).get("product_title") or k)[:90])}</a></td>'
    f'<td class="n">{fmt(old)} đ</td><td class="n">{fmt(new)} đ</td><td class="n"><b>−{pct:.0f}%</b></td></tr>'
    for k, (pct, old, new) in top) or '<tr><td colspan="4" class="muted">Chưa có thay đổi giá nào hôm nay.</td></tr>'

st_label = {"success": "✔ Thành công", "failed": "✖ Lỗi", "running": "⟳ Đang chạy", "queued": "… Chờ"}
run_rows = "".join(
    f'<tr><td>{html.escape(r[2])}</td><td>{st_label.get(r[1], r[1])}</td><td class="n">{r[3] and r[3] + " phút"}</td>'
    f'<td class="muted">{html.escape(r[0])}</td></tr>' for r in runs if len(r) >= 4
) or '<tr><td colspan="4" class="muted">Chưa có run batch nào hôm nay.</td></tr>'

page = f"""<!doctype html><html lang="vi"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Dữ liệu hôm nay</title>
<style>
:root{{--bg:#f6f6f4;--card:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--muted:#8a8984;--grid:#e4e3df;--s1:#2a78d6;--s2:#eb6834}}
@media (prefers-color-scheme:dark){{:root{{--bg:#111110;--card:#1a1a19;--ink:#f0efec;--ink2:#c3c2b7;--muted:#8f8e86;--grid:#2c2c2a;--s1:#3987e5;--s2:#d95926}}}}
body{{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 system-ui,Segoe UI,sans-serif}}
main{{max-width:980px;margin:0 auto;padding:24px 16px 48px}}
h1{{font-size:26px;margin:0 0 4px}} h2{{font-size:17px;margin:0 0 4px}} .sub{{color:var(--ink2);margin:0 0 20px}}
.tiles{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin-bottom:16px}}
.card{{background:var(--card);border-radius:12px;padding:16px 18px;box-shadow:0 1px 2px #0001}}
.big{{font-size:32px;font-weight:650;letter-spacing:-.5px}} .lbl{{color:var(--ink2);font-size:14px}}
.grid2{{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:12px;margin-bottom:16px}}
svg{{width:100%;height:auto;display:block;margin-top:8px}} .grid{{stroke:var(--grid)}} .ax{{fill:var(--muted);font-size:11px}}
.c1 .mark{{fill:var(--s1)}} .c2 .mark{{fill:var(--s2)}} .bar:hover .mark{{opacity:.75}}
table{{width:100%;border-collapse:collapse;font-size:14px;margin-top:8px}} td,th{{padding:7px 6px;border-bottom:1px solid var(--grid);text-align:left}}
th{{color:var(--ink2);font-weight:500}} .n{{text-align:right;white-space:nowrap}} .muted{{color:var(--muted)}}
a{{color:var(--ink);text-decoration:none}} a:hover{{text-decoration:underline}} .note{{color:var(--ink2);font-size:13px;margin-top:6px}}
</style></head><body><main>
<h1>Dữ liệu Tiki thu thập hôm nay</h1>
<p class="sub">Ngày {now:%d/%m/%Y}, từ 00:00 đến {now:%H:%M} (giờ Việt Nam) · Luồng: crawl → Kafka → speed → Elasticsearch, batch Airflow mỗi giờ → Superset</p>
<div class="tiles">
 <div class="card"><div class="big">{fmt(pages_ok)}</div><div class="lbl">trang danh mục tải thành công<br><span class="muted">{fmt(pages_all - pages_ok)} lỗi / {fmt(pages_all)} lượt</span></div></div>
 <div class="card"><div class="big">{fmt(offers_today)}</div><div class="lbl">sản phẩm được cập nhật hôm nay<br><span class="muted">tổng đang theo dõi: {fmt(offers_total)}</span></div></div>
 <div class="card"><div class="big">{fmt(price_changes)}</div><div class="lbl">lần đổi giá phát hiện được<br><span class="muted">phát hiện gần thời gian thực</span></div></div>
 <div class="card"><div class="big">{fmt(new_offers)}</div><div class="lbl">sản phẩm mới xuất hiện</div></div>
</div>
<div class="grid2">
 <div class="card c1"><h2>Số trang crawl thành công mỗi giờ</h2><div class="lbl">Mỗi trang danh mục chứa khoảng 40 sản phẩm</div>{bars(crawl_h, "trang")}</div>
 <div class="card c2"><h2>Số lần đổi giá phát hiện mỗi giờ</h2><div class="lbl">So với lần quan sát trước của cùng sản phẩm</div>{bars(price_h, "lần đổi giá")}</div>
</div>
<div class="card" style="margin-bottom:16px"><h2>10 sản phẩm giảm giá mạnh nhất hôm nay</h2>
<table><tr><th>Sản phẩm</th><th class="n">Giá trước</th><th class="n">Giá mới</th><th class="n">Giảm</th></tr>{rows}</table>
<p class="note">Giảm giá được tính giữa hai lần crawl liên tiếp của cùng một sản phẩm.</p></div>
<div class="card"><h2>Batch Airflow hôm nay</h2><div class="lbl">Mỗi giờ: Bronze → Silver → Gold → kiểm tra chất lượng → đẩy lên Superset</div>
<table><tr><th>Bắt đầu</th><th>Trạng thái</th><th class="n">Thời gian</th><th>Run</th></tr>{run_rows}</table></div>
<p class="note">Tạo lúc {now:%H:%M:%S %d/%m/%Y}. Chạy lại script để cập nhật.</p>
</main></body></html>"""
OUT.write_text(page, encoding="utf-8")
print(OUT)
