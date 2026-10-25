"""One-shot: laptop product URLs from FPT Shop, CellphoneS and Websosanh sitemaps (robots-allowed),
for the cross-site product matching benchmark. Writes raw/entity_sitemaps.json."""
import json, re, sys, time
from collect import fetch

out = {}
st, x, _ = fetch("https://fptshop.com.vn/products/sitemap-may-tinh-xach-tay.xml")
out["fptshop"] = re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
time.sleep(3)
cps = []
for i in ["", "2", "3", "4", "5", "6", "7", "8"]:
    st, x, _ = fetch(f"https://cellphones.com.vn/sitemap/product-sitemap{i}.xml")
    cps += re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
    time.sleep(3)
out["cellphones"] = cps
st, x, _ = fetch("https://websosanh.vn/product-tin-hoc-sitemap.xml")
out["websosanh"] = re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
json.dump(out, open(sys.argv[1], "w", encoding="utf-8"))
print({k: len(v) for k, v in out.items()})
