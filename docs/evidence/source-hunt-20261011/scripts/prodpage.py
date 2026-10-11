"""Product-page test: sample N product URLs from each sitemap, fetch politely, report
status, latency and which structured fields are present (JSON-LD Product / __NEXT_DATA__ / RSC)."""
import re, json, time, sys, random
from fetch import get
from rsc import rsc

SRC = {
    "tiki": ("https://tiki.vn/clover/sitemap_products_1.xml", 3),
    "tgdd": ("https://www.thegioididong.com/newsitemap/sitemap-product-2026-10?page=1", 6),
    "dmx": ("https://www.dienmayxanh.com/newsitemap/sitemap-product", 6),
    "fptshop": ("https://fptshop.com.vn/products/sitemap-dien-thoai.xml", 3),
    "cellphones": ("https://cellphones.com.vn/sitemap/product-sitemap.xml", 3),
    "websosanh": ("https://websosanh.vn/product-dien-thoai-may-tinh-bang-sitemap.xml", 3),
}


def ld_products(h):
    out = []
    for blk in re.findall(r'<script[^>]*ld\+json[^>]*>(.*?)</script>', h, re.S):
        try:
            j = json.loads(blk.strip())
        except Exception:
            continue
        st = [j]
        while st:
            x = st.pop()
            if isinstance(x, list):
                st += x
            elif isinstance(x, dict):
                if x.get("@type") == "Product":
                    out.append(x)
                st += [v for k, v in x.items() if k != "offers"]
    return out


def summarize(h):
    s = {}
    lp = ld_products(h)
    if lp:
        p = lp[0]
        o = p.get("offers") or {}
        o = o[0] if isinstance(o, list) and o else o
        ar = p.get("aggregateRating") or {}
        s["ld"] = dict(name=(p.get("name") or "")[:60], sku=p.get("sku"), price=o.get("price") or o.get("lowPrice"),
                       high=o.get("highPrice"), offers=o.get("offerCount"), avail=str(o.get("availability", ""))[-12:],
                       rating=ar.get("ratingValue"), reviews=ar.get("reviewCount") or ar.get("ratingCount"))
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', h, re.S)
    if m:
        t = m.group(1)
        s["next"] = {k: bool(re.search(r'"%s"' % k, t)) for k in ("price", "quantity_sold", "stock_item", "review_count", "rating_average")}
        q = re.search(r'"quantity_sold":\{"text":"[^"]*","value":(\d+)', t)
        s["next"]["sold"] = int(q.group(1)) if q else None
    if "self.__next_f" in h:
        r = rsc(h)
        s["rsc"] = {k: r.count('"%s"' % k) for k in ("price", "currentPrice", "realQuantity", "stock")}
    return s


if __name__ == "__main__":
    N = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    only = set(sys.argv[2:])
    res = {}
    for site, (sm, delay) in SRC.items():
        if only and site not in only:
            continue
        st, x = get(sm)
        locs = re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
        if "<sitemapindex" in x:  # dmx: newest child
            time.sleep(delay)
            st, x = get(locs[-1])
            locs = re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
        random.seed(1)
        sample = random.sample(locs, min(N, len(locs)))
        rows = []
        for u in sample:
            time.sleep(delay)
            t0 = time.time()
            try:
                stc, h = get(u)
                rows.append(dict(url=u, status=stc, secs=round(time.time() - t0, 2), kb=len(h) // 1024, **summarize(h)))
            except Exception as e:
                rows.append(dict(url=u, status="ERR " + str(e)[:60], secs=round(time.time() - t0, 2)))
        ok = sum(1 for r in rows if r["status"] == 200)
        withprice = sum(1 for r in rows if (r.get("ld") or {}).get("price") or (r.get("next") or {}).get("price") or (r.get("rsc") or {}).get("price"))
        res[site] = dict(sitemap=sm, n=len(rows), ok=ok, with_price=withprice,
                         mean_secs=round(sum(r["secs"] for r in rows) / max(1, len(rows)), 2), rows=rows)
        print(f"{site:11} ok={ok}/{len(rows)} price={withprice} mean={res[site]['mean_secs']}s", flush=True)
        for r in rows[:2]:
            print("   ", json.dumps({k: v for k, v in r.items() if k != "url"}, ensure_ascii=False)[:400], flush=True)
    json.dump(res, open("prodpage.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
