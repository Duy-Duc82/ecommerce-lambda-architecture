"""Demo: crawl thẳng (không gọi API) cùng một từ khóa trên Tiki (HTML thuần) và Lazada (Playwright), xuất CSV."""
import re, json, csv, time, sys, gzip, urllib.request, urllib.parse
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright
Q = sys.argv[1] if len(sys.argv) > 1 else "samsung galaxy a56"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
now = datetime.now(timezone.utc).isoformat(timespec="seconds")
rows = []

def walk(o, acc):
    if isinstance(o, dict):
        if "price" in o and "name" in o and "id" in o: acc.append(o)
        for v in o.values(): walk(v, acc)
    elif isinstance(o, list):
        for v in o: walk(v, acc)

# Tiki: trang tìm kiếm HTML, đọc __NEXT_DATA__
u = "https://tiki.vn/search?q=" + urllib.parse.quote(Q)
req = urllib.request.Request(u, headers={"User-Agent": UA, "Accept-Language": "vi-VN", "Accept-Encoding": "gzip"})
with urllib.request.urlopen(req, timeout=30) as r:
    b = r.read(); h = (gzip.decompress(b) if b[:2] == b"\x1f\x8b" else b).decode("utf-8", "replace")
m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', h, re.S)
acc = []; walk(json.loads(m.group(1)), acc) if m else None
seen = set()
for x in acc:
    if x["id"] in seen or not isinstance(x.get("price"), (int, float)): continue
    seen.add(x["id"])
    rows.append({"site": "tiki", "method": "http+__NEXT_DATA__", "listing_id": x["id"], "title": x["name"], "price_vnd": int(x["price"]),
                 "original_price_vnd": x.get("original_price"), "seller": x.get("seller_name"), "url": "https://tiki.vn/" + str(x.get("url_path", "")).split("?")[0], "crawled_at": now})
print("tiki:", len(seen), "listings")

# Lazada: trang tìm kiếm render bằng Chromium headless
with sync_playwright() as p:
    br = p.chromium.launch(headless=True); pg = br.new_page(locale="vi-VN", viewport={"width": 1366, "height": 900})
    pg.goto("https://www.lazada.vn/catalog/?q=" + urllib.parse.quote(Q), wait_until="domcontentloaded", timeout=45000); pg.wait_for_timeout(8000)
    cards = pg.eval_on_selector_all('[data-qa-locator="product-item"]', """els=>els.map(e=>{const a=e.querySelector('a[title]')||e.querySelector('a');
        const m=e.innerText.match(/([\\d.]{4,})\s?₫/); return {id:e.getAttribute('data-item-id'),title:a?(a.title||a.innerText):'',href:a?a.href.split('?')[0]:'',price:m?m[1]:null}})""")
    br.close()
for c in cards:
    if c["price"]:
        rows.append({"site": "lazada", "method": "playwright+DOM", "listing_id": c["id"], "title": c["title"].strip(), "price_vnd": int(c["price"].replace(".", "")),
                     "original_price_vnd": None, "seller": None, "url": c["href"], "crawled_at": now})
print("lazada:", sum(1 for r in rows if r["site"] == "lazada"), "listings")

out = "demo_compare_" + re.sub(r"\W+", "_", Q) + ".csv"
with open(out, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
print("wrote", out)
# so sánh nhanh: chỉ các listing có đủ token của từ khóa trong tên
toks = Q.lower().split()
rel = [r for r in rows if all(t in r["title"].lower() for t in toks) and "điện thoại" in r["title"].lower() and not re.search(r"ốp|kính|cường lực|bao da|dán|sạc|cáp", r["title"].lower())]
for s in ("tiki", "lazada"):
    ps = sorted(r["price_vnd"] for r in rel if r["site"] == s)
    if ps: print(f"{s}: {len(ps)} listing khớp tên | min {ps[0]:,} | median {ps[len(ps)//2]:,} | max {ps[-1]:,}")
