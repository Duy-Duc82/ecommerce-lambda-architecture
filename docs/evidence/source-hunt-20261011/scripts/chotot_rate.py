"""Estimate new-listing rate on Cho Tot: ages of listings on pages 1..5 of several categories."""
import re, json, time
from fetch import get
import crawl_round as cr

CATS = ["mua-ban-dien-thoai", "mua-ban-laptop", "mua-ban-do-dien-tu", "mua-ban"]


def age_sec(s):
    if not s:
        return None
    m = re.match(r"(\d+)\s*(giây|phút|giờ|ngày)", s)
    if not m:
        return None
    return int(m.group(1)) * {"giây": 1, "phút": 60, "giờ": 3600, "ngày": 86400}[m.group(2)]


out = {}
for c in CATS:
    ages, ids, prices = [], set(), []
    for p in range(1, 6):
        st, h = get(f"https://www.chotot.com/{c}" + (f"?page={p}" if p > 1 else ""))
        for it in cr.p_chotot(h):
            ids.add(it["id"])
            a = age_sec(it["extra"].get("age"))
            if a is not None:
                ages.append(a)
            if it["price"]:
                prices.append(it["price"])
        time.sleep(4)
    ages.sort()
    span = ages[-1] if ages else None
    out[c] = dict(listings=len(ids), oldest_age_sec=span,
                  per_hour=round(len(ids) / (span / 3600), 1) if span else None,
                  priced=len(prices), age_samples=ages[:3] + ages[-3:])
    print(c, json.dumps(out[c], ensure_ascii=False), flush=True)
json.dump(out, open("chotot_rate.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
