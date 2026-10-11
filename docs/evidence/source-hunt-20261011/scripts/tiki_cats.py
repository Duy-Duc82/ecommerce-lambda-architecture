"""Sustained Tiki category-page test: N random categories at a fixed delay."""
import re, json, time, random, sys
from fetch import get
import crawl_round as cr

n, delay = int(sys.argv[1]), float(sys.argv[2])
_, x = get("https://tiki.vn/clover/sitemap_categories_1.xml")
cats = re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
random.seed(3)
rows = []
for u in random.sample(cats, n):
    time.sleep(delay)
    t0 = time.time()
    try:
        st, h = get(u)
        items = cr.p_tiki(h) if "__NEXT_DATA__" in h else []
        rows.append(dict(url=u, status=st, kb=len(h) // 1024, n=len(items), secs=round(time.time() - t0, 2),
                         sold=sum(1 for i in items if i["extra"].get("sold") is not None)))
    except Exception as e:
        rows.append(dict(url=u, status=str(e)[:50], n=0, secs=round(time.time() - t0, 2)))
    print(json.dumps(rows[-1], ensure_ascii=False), flush=True)
ok = [r for r in rows if r.get("n")]
summ = dict(n=len(rows), with_items=len(ok), mean_items=round(sum(r["n"] for r in ok) / max(1, len(ok)), 1),
            total_items=sum(r["n"] for r in ok), challenged=sum(1 for r in rows if r.get("kb", 99) < 30))
print("SUMMARY", json.dumps(summ))
json.dump(dict(summary=summ, rows=rows), open("tiki_cats.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
