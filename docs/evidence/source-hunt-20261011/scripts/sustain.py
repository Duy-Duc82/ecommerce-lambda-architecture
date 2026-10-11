"""Sustained polite fetch: N product pages from one sitemap at a fixed delay; log status/latency."""
import re, json, time, sys, random
from fetch import get
from prodpage import summarize

site, sm, n, delay = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4])
st, x = get(sm)
locs = re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
random.seed(7)
sample = random.sample(locs, min(n, len(locs)))
rows = []
t_start = time.time()
for u in sample:
    time.sleep(delay)
    t0 = time.time()
    try:
        s, h = get(u)
        sm_ = summarize(h)
        ld = sm_.get("ld") or {}
        rows.append(dict(status=s, secs=round(time.time() - t0, 2), price=ld.get("price"), offers=ld.get("offers"),
                         avail=ld.get("avail"), sold=(sm_.get("next") or {}).get("sold")))
    except Exception as e:
        rows.append(dict(status=str(e)[:40], secs=round(time.time() - t0, 2)))
codes = {}
for r in rows:
    codes[str(r["status"])] = codes.get(str(r["status"]), 0) + 1
instock = sum(1 for r in rows if r.get("avail") and "InStock" in r["avail"])
priced = sum(1 for r in rows if r.get("price") not in (None, 0, "0"))
out = dict(site=site, n=len(rows), delay=delay, wall_min=round((time.time() - t_start) / 60, 1), codes=codes,
           mean_secs=round(sum(r["secs"] for r in rows) / len(rows), 2), max_secs=max(r["secs"] for r in rows),
           in_stock_share=round(instock / len(rows), 2), priced_share=round(priced / len(rows), 2),
           sold_present=sum(1 for r in rows if r.get("sold") is not None))
print(json.dumps(out, ensure_ascii=False))
json.dump(dict(summary=out, rows=rows), open(f"sustain_{site}.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
