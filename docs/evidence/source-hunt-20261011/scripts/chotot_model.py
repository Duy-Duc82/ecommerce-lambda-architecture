"""Cho Tot price distribution per model (pages 1-5 of model-filter URL): IQR outliers per (model, capacity)."""
import re, json, time, statistics
from fetch import get
import crawl_round as cr

MODELS = {
    "iphone-15-pro-max": "https://www.chotot.com/mua-ban-dien-thoai-apple-iphone-15-pro-max-sdmd1ml2483",
    "iphone-13": "https://www.chotot.com/mua-ban-dien-thoai-apple-iphone-13-sdmd1ml2275",
    "iphone-16-pro-max": "https://www.chotot.com/mua-ban-dien-thoai-apple-iphone-16-pro-max-sdmd1ml2620",
}


def full_items(h):
    # keep only non-personal fields, including capacity code
    out = []
    for i in cr.longest_list(cr.next_data(h), "list_id"):
        out.append(dict(id=i["list_id"], subject=i.get("subject"), price=i.get("price"), cap=i.get("mobile_capacity"),
                        cond=i.get("elt_condition"), region=i.get("region_name"), company_ad=i.get("company_ad"),
                        age=i.get("date")))
    return out


if __name__ == "__main__":
    res = {}
    for name, url in MODELS.items():
        items = {}
        for p in range(1, 6):
            st, h = get(url + (f"?page={p}" if p > 1 else ""))
            for it in full_items(h):
                items[it["id"]] = it
            time.sleep(4)
        L = [i for i in items.values() if i["price"]]
        groups = {}
        for i in L:
            groups.setdefault(i["cap"], []).append(i)
        g_out = {}
        outliers = []
        for cap, g in groups.items():
            ps = sorted(i["price"] for i in g)
            if len(ps) < 8:
                continue
            q1, med, q3 = statistics.quantiles(ps, n=4)
            lo, hi = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
            o = [i for i in g if i["price"] < lo or i["price"] > hi]
            outliers += [dict(subject=(i["subject"] or "")[:50], price=i["price"], cap=cap, median=med) for i in o]
            g_out[str(cap)] = dict(n=len(ps), q1=q1, median=med, q3=q3, outliers=len(o))
        res[name] = dict(listings=len(L), dealers=sum(1 for i in L if i["company_ad"]), groups=g_out,
                         outlier_share=round(len(outliers) / max(1, sum(v["n"] for v in g_out.values())), 3),
                         outlier_examples=sorted(outliers, key=lambda o: o["price"] / o["median"])[:3] + sorted(outliers, key=lambda o: -o["price"] / o["median"])[:3])
        print(name, json.dumps(res[name], ensure_ascii=False)[:1200], flush=True)
    json.dump(res, open("chotot_model.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
