"""Catalog-level overlap of phone models using URL slugs:
TGDD /dtdd listing slugs (currently sold) vs FPT phone sitemap vs Websosanh phone sitemap vs CellphoneS sitemaps."""
import re, json, time
from fetch import get
from overlap import key

h = open("raw/tgdd.html", encoding="utf-8").read()
cat = {"tgdd": set(re.findall(r'href="/dtdd/([a-z0-9-]+)"', h))}
time.sleep(1)
_, x = get("https://fptshop.com.vn/products/sitemap-dien-thoai.xml")
cat["fptshop"] = {u.rstrip("/").split("/")[-1] for u in re.findall(r"<loc>\s*(.*?)\s*</loc>", x)}
time.sleep(2)
_, x = get("https://websosanh.vn/product-dien-thoai-may-tinh-bang-sitemap.xml")
cat["websosanh"] = {u.split("/")[3] for u in re.findall(r"<loc>\s*(.*?)\s*</loc>", x) if u.count("/") >= 5}
cps = set()
for i in ["", "2", "3", "4", "5", "6", "7", "8"]:
    time.sleep(2)
    _, x = get(f"https://cellphones.com.vn/sitemap/product-sitemap{i}.xml")
    cps |= {u.rstrip("/").split("/")[-1].removesuffix(".html") for u in re.findall(r"<loc>\s*(.*?)\s*</loc>", x)}
cat["cellphones"] = cps

keys = {}
for site, slugs in cat.items():
    keys[site] = {k.split("|")[0] for k in (key(s.replace("-", " ")) for s in slugs) if k}
    print(site, "slugs", len(slugs), "phone model keys", len(keys[site]))
base = keys["tgdd"]
res = {}
for site in ("fptshop", "websosanh", "cellphones"):
    inter = base & keys[site]
    res[site] = dict(n=len(inter), share=round(len(inter) / max(1, len(base)), 2), sample=sorted(inter)[:12])
    print(f"tgdd ∩ {site}: {len(inter)}/{len(base)}", sorted(inter)[:12])
allk = base & keys["fptshop"] & keys["websosanh"] & keys["cellphones"]
print("on all 4:", len(allk), sorted(allk))
json.dump(dict(per_site={s: len(k) for s, k in keys.items()}, vs_tgdd=res, all4=sorted(allk)),
          open("overlap2.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
