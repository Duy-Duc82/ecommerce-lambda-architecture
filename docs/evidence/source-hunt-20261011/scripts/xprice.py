"""Cross-retailer price table for models common to TGDD/FPT/CellphoneS: resolve each model's product URL from
sitemap slugs (model key + storage), fetch the product page, read JSON-LD Product name/price."""
import re, json, time
from fetch import get
from overlap import key
from prodpage import ld_products

MODELS = ["galaxy a37|128", "galaxy a57|256", "galaxy s26 ultra|256", "iphone 17|256", "iphone 17 pro|256",
          "iphone 17 pro max|256", "iphone 18 pro|256", "iphone 18 pro max|256", "reno16 f|128"]
DELAY = {"tgdd": 6, "fptshop": 4, "cellphones": 4}


def urls(site):
    if site == "tgdd":
        h = open("raw/tgdd.html", encoding="utf-8").read()
        return ["https://www.thegioididong.com/dtdd/" + s for s in set(re.findall(r'href="/dtdd/([a-z0-9-]+)"', h))]
    if site == "fptshop":
        _, x = get("https://fptshop.com.vn/products/sitemap-dien-thoai.xml")
        return re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
    out = []
    for i in ["", "2", "3", "4", "5", "6", "7", "8"]:
        _, x = get(f"https://cellphones.com.vn/sitemap/product-sitemap{i}.xml")
        out += re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
        time.sleep(1.5)
    return out


def slug_key(u):
    s = u.rstrip("/").split("/")[-1].removesuffix(".html").replace("-", " ")
    if re.search(r"\b(cu|like new|tray xuoc|op lung|cuong luc|dan|sac|cap|tai nghe)\b", s):
        return None
    return key(s)


table = {m: {} for m in MODELS}
for site in ("tgdd", "fptshop", "cellphones"):
    cand = {}
    for u in urls(site):
        k = slug_key(u)
        if not k:
            continue
        cand.setdefault(k, u)  # exact model|storage
        cand.setdefault(k.split("|")[0] + "|base", u)  # model without storage in slug (base variant page)
    time.sleep(2)
    for m in MODELS:
        u = cand.get(m) or cand.get(m.split("|")[0]) or cand.get(m.split("|")[0] + "|base")
        if not u:
            continue
        time.sleep(DELAY[site])
        try:
            st, h = get(u)
            lp = ld_products(h)
            o = (lp[0].get("offers") or {}) if lp else {}
            o = o[0] if isinstance(o, list) and o else o
            table[m][site] = dict(url=u, name=(lp[0].get("name") if lp else None), price=o.get("price") or o.get("lowPrice"),
                                  avail=str(o.get("availability", "")).split("/")[-1])
        except Exception as e:
            table[m][site] = dict(url=u, err=str(e)[:60])
        print(site, m, json.dumps(table[m][site], ensure_ascii=False)[:220], flush=True)
json.dump(table, open("xprice.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
