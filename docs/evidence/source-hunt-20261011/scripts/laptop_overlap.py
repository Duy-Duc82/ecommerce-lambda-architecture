"""Laptop cross-site matching by manufacturer part code (e.g. UX5406AA-SU415W) found in Phong Vu listing names,
searched in URL slugs of FPT, CellphoneS, Websosanh, meta.vn sitemaps and TGDD/DMX laptop sitemap pages."""
import re, json, time
from fetch import get
import crawl_round as cr

h = open("raw/phongvu.html", encoding="utf-8").read()
items = cr.p_phongvu(h)
CODE = re.compile(r"\b([A-Z0-9]{2,}[A-Z][A-Z0-9]*-[A-Z0-9]{3,}(?:-[A-Z0-9]+)?)\b")
pv = {}
for it in items:
    m = CODE.search(it["name"])
    if m:
        pv[m.group(1).lower()] = dict(name=it["name"][:70], price=it["price"])
print("phongvu laptops", len(items), "with part code", len(pv))


def slugs(urls):
    return {u.lower() for u in urls}


cat = {}
_, x = get("https://fptshop.com.vn/products/sitemap-may-tinh-xach-tay.xml")
cat["fptshop"] = slugs(re.findall(r"<loc>\s*(.*?)\s*</loc>", x))
time.sleep(2)
cps = set()
for i in ["", "2", "3", "4", "5", "6", "7", "8"]:
    _, x = get(f"https://cellphones.com.vn/sitemap/product-sitemap{i}.xml")
    cps |= slugs(re.findall(r"<loc>\s*(.*?)\s*</loc>", x))
    time.sleep(2)
cat["cellphones"] = cps
_, x = get("https://websosanh.vn/product-tin-hoc-sitemap.xml")
cat["websosanh"] = slugs(re.findall(r"<loc>\s*(.*?)\s*</loc>", x))
time.sleep(2)
res = {}
for site, urls in cat.items():
    blob = "\n".join(urls)
    hits = [c for c in pv if c in blob or c.replace("-", "") in blob]
    res[site] = dict(urls=len(urls), matched=len(hits), sample=hits[:8])
    print(site, "urls", len(urls), "matched", len(hits), "/", len(pv), hits[:6], flush=True)
allm = [c for c in pv if all(c in "\n".join(cat[s]) or c.replace("-", "") in "\n".join(cat[s]) for s in cat)]
print("on all:", len(allm), allm[:10])
json.dump(dict(phongvu_codes=len(pv), per_site=res, all=allm), open("laptop_overlap.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
