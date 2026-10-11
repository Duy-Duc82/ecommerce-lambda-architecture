"""List price changes and sold-counter deltas between first and last round per site."""
import json, sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "rounds.jsonl"
by = defaultdict(list)
for line in open(path, encoding="utf-8"):
    r = json.loads(line)
    if r["n"]:
        by[(r["site"], r["url"])].append(r)
out = {}
for (site, url), rs in by.items():
    first, last = rs[0], rs[-1]
    a = {str(i["id"]): i for i in first["items"]}
    b = {str(i["id"]): i for i in last["items"]}
    common = set(a) & set(b)
    pch = [(str(a[k]["name"])[:45], a[k]["price"], b[k]["price"]) for k in common if a[k]["price"] != b[k]["price"]]
    sold = []
    for k in common:
        for f in ("sold", "orderQty", "ordered"):
            s0, s1 = (a[k].get("extra") or {}).get(f), (b[k].get("extra") or {}).get(f)
            if isinstance(s0, (int, float)) and isinstance(s1, (int, float)) and s1 != s0:
                sold.append((str(a[k]["name"])[:40], s0, s1))
    out[f"{site} {url.split('/')[-1]}"] = dict(span=f"{first['ts'][11:16]}->{last['ts'][11:16]} UTC", common=len(common),
                                              price_changes=pch[:8], n_price_changes=len(pch), sold_deltas=sold[:8], n_sold_changed=len(sold))
    print(site, url.split("/")[-1], json.dumps(out[f"{site} {url.split('/')[-1]}"], ensure_ascii=False)[:900])
json.dump(out, open(path.replace(".jsonl", "_changes.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=1)
