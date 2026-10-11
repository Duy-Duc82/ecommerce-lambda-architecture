"""Count product URLs in each candidate's sitemap (index -> children, capped)."""
import re, sys, time, json
from fetch import get

ROOTS = {
    "tiki": "https://tiki.vn/clover/sitemap_index.xml",
    "tgdd": "https://www.thegioididong.com/newsitemap/sitemap-product",
    "dmx": "https://www.dienmayxanh.com/newsitemap/sitemap-product",
    "cellphones": "https://cellphones.com.vn/sitemap/sitemap_index.xml?v=google",
    "chotot": "https://www.chotot.com/sitemap-index.xml",
    "websosanh": "https://websosanh.vn/product-sitemap.xml",
    "phongvu": "https://phongvu.vn/sitemap.xml",
    "fptshop": "https://fptshop.com.vn/sitemap.xml",
    "bachhoaxanh": "https://www.bachhoaxanh.com/sitemapnew",
}
MAX_CHILD = 6
out = {}
for site, url in ROOTS.items():
    if len(sys.argv) > 1 and site not in sys.argv[1:]:
        continue
    r = {"root": url}
    try:
        st, x = get(url)
        locs = re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
        is_index = "<sitemapindex" in x
        r.update(status=st, index=is_index, n_loc=len(locs))
        if is_index:
            r["children_sample"] = locs[:8]
            counts = []
            prod = [l for l in locs if re.search(r"product|san-pham|sp|mua-ban|item", l, re.I)] or locs
            r["n_product_children"] = len(prod)
            for c in prod[:MAX_CHILD]:
                time.sleep(2)
                try:
                    cs, cx = get(c)
                    counts.append(len(re.findall(r"<loc>", cx)))
                except Exception as e:
                    counts.append(str(e)[:40])
            r["child_counts"] = counts
        else:
            r["sample"] = locs[:3]
    except Exception as e:
        r["error"] = str(e)[:120]
    out[site] = r
    print(site, json.dumps(r, ensure_ascii=False)[:900], flush=True)
    time.sleep(2)
json.dump(out, open("sitemaps.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
