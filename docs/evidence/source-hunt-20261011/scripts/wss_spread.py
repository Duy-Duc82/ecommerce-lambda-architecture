"""Websosanh AggregateOffer spread: sample compare pages across categories, measure high/low ratio and offerCount."""
import re, json, time, random, statistics
from fetch import get
from prodpage import ld_products

SMS = ["product-dien-thoai-may-tinh-bang-sitemap.xml", "product-tin-hoc-sitemap.xml",
       "product-thiet-bi-gia-dung-sitemap.xml", "product-dien-lanh-sitemap.xml"]
PER = 25
rows = []
random.seed(11)
for sm in SMS:
    _, x = get("https://websosanh.vn/" + sm)
    locs = re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
    for u in random.sample(locs, PER):
        time.sleep(3)
        try:
            st, h = get(u)
            lp = ld_products(h)
            o = (lp[0].get("offers") or {}) if lp else {}
            rows.append(dict(cat=sm.split("-sitemap")[0][8:], url=u, name=(lp[0].get("name") if lp else None),
                             low=o.get("lowPrice"), high=o.get("highPrice"), offers=o.get("offerCount")))
        except Exception as e:
            rows.append(dict(cat=sm, url=u, err=str(e)[:60]))
    print(sm, "done", flush=True)

live = [r for r in rows if r.get("offers") and r.get("low")]
multi = [r for r in live if r["offers"] >= 2 and r["low"] > 0]
ratios = sorted(r["high"] / r["low"] for r in multi)
summ = dict(sampled=len(rows), live=len(live), dead=len(rows) - len(live), multi_offer=len(multi),
            offers_median=statistics.median([r["offers"] for r in live]) if live else None,
            offers_max=max((r["offers"] for r in live), default=None),
            ratio_median=round(statistics.median(ratios), 2) if ratios else None,
            share_ratio_gt2=round(sum(x > 2 for x in ratios) / len(ratios), 2) if ratios else None,
            share_ratio_gt3=round(sum(x > 3 for x in ratios) / len(ratios), 2) if ratios else None,
            share_ratio_gt10=round(sum(x > 10 for x in ratios) / len(ratios), 2) if ratios else None)
by_cat = {}
for r in live:
    c = by_cat.setdefault(r["cat"], dict(live=0, multi=0, gt3=0))
    c["live"] += 1
    if r["offers"] >= 2:
        c["multi"] += 1
        c["gt3"] += r["high"] / r["low"] > 3
summ["by_cat"] = by_cat
top = sorted(multi, key=lambda r: -r["high"] / r["low"])[:6]
summ["extreme_examples"] = [dict(name=(r["name"] or "")[:60], low=r["low"], high=r["high"], offers=r["offers"]) for r in top]
print(json.dumps(summ, ensure_ascii=False, indent=1))
json.dump(dict(summary=summ, rows=rows), open("wss_spread.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
