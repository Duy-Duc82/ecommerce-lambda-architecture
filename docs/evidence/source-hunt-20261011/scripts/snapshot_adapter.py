"""Prototype: map every source's parsed items onto one PRICE_SNAPSHOT record, validate, and report field coverage.

PRICE_SNAPSHOT fields:
  crawl_ts, site, product_id, name, price, original_price, sold_cum, stock, rating, review_count, offer_count, region, condition
Validation (reject reasons): missing id/name, price <= 0 or > 5e9, original_price < price*0.5 (suspicious),
sold_cum decreasing vs previous snapshot of the same (site, product_id).
"""
import json, re, sys
from collections import defaultdict

FIELD_MAP = {  # source extra key -> snapshot field
    "orig": "original_price", "retail": "original_price",
    "sold": "sold_cum", "orderQty": "sold_cum", "ordered": "sold_cum",
    "stock": "stock", "inStock": "stock", "avail": "stock",
    "rating": "rating", "reviews": "review_count", "merchants": "offer_count",
    "region": "region", "loc": "region", "cond": "condition",
}
FIELDS = ["original_price", "sold_cum", "stock", "rating", "review_count", "offer_count", "region", "condition"]


def to_snapshot(rec, it):
    ex = it.get("extra") or {}
    name = it.get("name")
    if isinstance(name, dict):  # pico: {"vi": ...}
        name = name.get("vi")
    if not name and rec["site"] == "bachhoaxanh":  # card text has no title line; the URL slug carries the name
        name = str(it.get("id", "")).replace("-", " ")
    s = dict(crawl_ts=rec["ts"], site=rec["site"], product_id=str(it.get("id")), name=name,
             price=float(it["price"]) if isinstance(it.get("price"), (int, float)) else None)
    for k, v in ex.items():
        f = FIELD_MAP.get(k)
        if f and s.get(f) is None and v not in (None, ""):
            s[f] = v
    for f in FIELDS:
        s.setdefault(f, None)
    if isinstance(s["sold_cum"], str):  # lazada text: "75 Đã bán", "1,2k Đã bán"
        m = re.search(r"([\d.,]+)\s*([kK])?", s["sold_cum"])
        s["sold_cum"] = int(float(m.group(1).replace(",", ".")) * (1000 if m.group(2) else 1)) if m else None
    for f in ("rating", "review_count"):  # JSON-LD gives strings like "5", "531"
        if isinstance(s[f], str):
            try:
                s[f] = float(s[f])
            except ValueError:
                s[f] = None
    return s


def validate(s, last_sold):
    if not s["product_id"] or not s["name"]:
        return "missing_id_or_name"
    ceiling = 1e11 if s["site"] == "bonbanh" else 5e9  # cars reach tens of billions VND
    if s["price"] is None or s["price"] <= 0 or s["price"] > ceiling:
        return "bad_price"
    op = s["original_price"]
    if isinstance(op, (int, float)) and op and op < s["price"] * 0.5:
        return "orig_lt_half_price"
    key = (s["site"], s["product_id"])
    sc = s["sold_cum"]
    if isinstance(sc, (int, float)):
        prev = last_sold.get(key)
        last_sold[key] = sc
        if prev is not None and sc < prev:
            return "sold_decreased"
    return None


if __name__ == "__main__":
    paths = sys.argv[1:] or ["rounds.jsonl", "rounds2.jsonl"]
    recs = []
    for p in paths:
        recs += [json.loads(l) for l in open(p, encoding="utf-8")]
    recs.sort(key=lambda r: r["ts"])
    last_sold, cov, rej, n = {}, defaultdict(lambda: defaultdict(int)), defaultdict(lambda: defaultdict(int)), defaultdict(int)
    out = open("snapshots.jsonl", "w", encoding="utf-8")
    for rec in recs:
        if rec["site"] == "tiki_product":
            continue
        for it in rec["items"]:
            s = to_snapshot(rec, it)
            n[s["site"]] += 1
            for f in FIELDS:
                if s[f] not in (None, ""):
                    cov[s["site"]][f] += 1
            r = validate(s, last_sold)
            if r:
                rej[s["site"]][r] += 1
            else:
                out.write(json.dumps(s, ensure_ascii=False) + "\n")
    out.close()
    table = {}
    for site in sorted(n):
        table[site] = dict(snapshots=n[site], valid=n[site] - sum(rej[site].values()), rejects=dict(rej[site]),
                           coverage={f: round(cov[site][f] / n[site], 2) for f in FIELDS if cov[site][f]})
        print(f"{site:13}", json.dumps(table[site], ensure_ascii=False))
    print("TOTAL", sum(n.values()), "valid", sum(t["valid"] for t in table.values()))
    json.dump(table, open("snapshot_quality.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
