"""Fill the continuity section of the report from rounds*.jsonl (re-runnable: content kept between paired markers)."""
import json, re, subprocess, sys
from datetime import datetime, timedelta

REPORT = r"C:\code\data\docs\evidence\source-hunt-20261011\bao-cao-nguon-du-lieu.html"
extra_note = sys.argv[1] if len(sys.argv) > 1 else ""


def run(path):
    subprocess.run([sys.executable, "analyze.py", path], check=True, capture_output=True)
    return json.load(open("continuity.json", encoding="utf-8"))


def span(path):
    ts = [json.loads(l)["ts"] for l in open(path, encoding="utf-8")]
    f = lambda s: (datetime.fromisoformat(s) + timedelta(hours=7)).strftime("%H:%M")
    return f(min(ts)), f(max(ts)), len(set(ts))


import os
with open("rounds23.jsonl", "w", encoding="utf-8") as f:
    for p in ("rounds2.jsonl", "rounds3.jsonl"):
        if os.path.exists(p):
            f.write(open(p, encoding="utf-8").read())
a, b = run("rounds.jsonl"), run("rounds23.jsonl")
json.dump(dict(core=a, extra=b), open("continuity_all.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
s1, s2 = span("rounds.jsonl"), span("rounds23.jsonl")
NAMES = dict(lazada="Lazada (Playwright)", tiki="Tiki (2 danh mục)", chotot="Chợ Tốt", tgdd="TGDD", dmx="ĐMX", phongvu="Phong Vũ", fptshop="FPT Shop",
             nguyenkim="Nguyễn Kim", websosanh="Websosanh", bachhoaxanh="Bách Hóa Xanh (Playwright)", meta="meta.vn",
             mediamart="MediaMart", pico="Pico", kingfoodmart="Kingfoodmart", hoanghamobile="Hoàng Hà", bonbanh="Bonbanh",
             tiki_product="Tiki trang SP (canary)")
def fmt(v, p=False):
    if v is None:
        return "—"
    if p:
        return f"{v * 100:.0f}%"
    return f"{round(v, 2):g}".replace(".", ",")
rows = []
for grp, d in (("Nhóm A + BHX", a), ("Nhóm B", b)):
    rows.append(f'<tr class="tier"><td colspan="8">{grp}</td></tr>')
    for site, m in d.items():
        rows.append("<tr>" + "".join([
            f'<td class="site">{NAMES.get(site, site)}</td>',
            f'<td class="r">{m["rounds"]}</td>',
            f'<td class="r">{fmt(m["success_rate"], True)}</td>',
            f'<td class="r">{fmt(m["items_per_round"])}</td>',
            f'<td class="r">{fmt(m["secs_per_page"])}s</td>',
            f'<td class="r">{fmt(m["id_jaccard"])}</td>',
            f'<td class="r">{fmt(m["price_changed_share"], True)}</td>',
            f'<td class="r">{fmt(m["new_ids_per_round"])}</td>']) + "</tr>")
table = ('<div class="tw"><table><thead><tr><th>Nguồn</th><th>Lần tải</th><th>Thành công</th><th>Item/vòng</th>'
         '<th>Giây/trang</th><th>Jaccard ID</th><th>Item đổi giá</th><th>ID mới/vòng</th></tr></thead><tbody>'
         + "".join(rows) + "</tbody></table></div>")
allm = list(a.values()) + [m for k, m in b.items() if k != "tiki_product"]
tot_req = sum(m["rounds"] for m in allm)
ok_req = sum(round(m["rounds"] * m["success_rate"]) for m in allm)
window = (f"Nhóm A: {s1[2]} vòng từ {s1[0]} đến {s1[1]}. Nhóm B + Lazada: {s2[2]} lượt từ {s2[0]} đến {s2[1]} (giờ Việt Nam, 11/10/2026).")
fails = [NAMES.get(k, k) for d in (a, b) for k, m in d.items() if k != "tiki_product" and m["success_rate"] < 1]
fail_txt = ("Không có 403/429/captcha. Lỗi duy nhất: " + ", ".join(fails) +
            " trả trang \"máy chủ quá tải\" với HTTP 200 ở 1 lần tải (mục 3.6). Parser không phải sửa lần nào. ") if fails else \
           "Không có 403/429/captcha ở trang danh mục. Parser không hỏng lần nào. "
summary = (f"{ok_req}/{tot_req} lần tải danh mục thành công ({ok_req / tot_req * 100:.0f}%) trên 16 nguồn trong khoảng "
           f"{s1[0]}–{max(s1[1], s2[1])}. {fail_txt}"
           f"Thời gian tải 0,1–1 giây/trang với HTTP, 9–16 giây với Playwright. {extra_note}")


def put(html, tag, content):
    pat = re.compile(r"<!--%s-->.*?<!--/%s-->" % (tag, tag), re.S)
    new = f"<!--{tag}-->{content}<!--/{tag}-->"
    if pat.search(html):
        return pat.sub(lambda _: new, html)
    return html.replace(f"<!--{tag}-->", new, 1)


subprocess.run([sys.executable, "snapshot_adapter.py", "rounds.jsonl", "rounds2.jsonl"] +
               (["rounds3.jsonl"] if __import__("os").path.exists("rounds3.jsonl") else []), check=True, capture_output=True)
q = json.load(open("snapshot_quality.json", encoding="utf-8"))
F = ["original_price", "sold_cum", "stock", "rating", "review_count", "offer_count", "region", "condition"]
FH = ["giá gốc", "đã bán", "tồn kho", "rating", "review", "số shop", "khu vực", "tình trạng"]
qrows = []
for site, t in q.items():
    rej = ", ".join(f"{k}: {v}" for k, v in t["rejects"].items()) or "—"
    cells = "".join(f'<td class="r">{fmt(t["coverage"].get(f), True) if t["coverage"].get(f) else ""}</td>' for f in F)
    qrows.append(f'<tr><td class="site">{NAMES.get(site, site)}</td><td class="r">{t["snapshots"]}</td><td class="r">{t["valid"]}</td>{cells}<td>{rej}</td></tr>')
qtable = ('<div class="tw" style="margin-top:10px"><table><thead><tr><th>Nguồn</th><th>Snapshot</th><th>Hợp lệ</th>'
          + "".join(f"<th>{x}</th>" for x in FH) + "<th>Bị loại</th></tr></thead><tbody>" + "".join(qrows) + "</tbody></table></div>"
          '<p class="sub" style="margin-top:8px">Ô số là tỷ lệ snapshot có trường đó (từ trang danh mục; trang SP của TGDD/ĐMX/Nguyễn Kim có thêm rating/review). '
          'Lỗi <code>sold_decreased</code> của Tiki là listing nhiều biến thể đổi biến thể đại diện.</p>')
tot = sum(t["snapshots"] for t in q.values())
val = sum(t["valid"] for t in q.values())
pct = f"{val / tot * 100:.1f}".replace(".", ",")
adapter_txt = f"Trên {tot:,} snapshot từ {len(q)} nguồn: {val:,} hợp lệ".replace(",", ".") + f" ({pct}%)."

h = open(REPORT, encoding="utf-8").read()
h = put(h, "ADAPTER", adapter_txt)
h = put(h, "ADAPTERTABLE", qtable)
h = put(h, "CONTINUITY", table)
h = put(h, "WINDOW", window)
h = put(h, "SUMMARY", summary)
open(REPORT, "w", encoding="utf-8").write(h)
print(window)
print(summary)
