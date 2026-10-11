"""Continuity metrics per site from rounds.jsonl.

success = status 200 and n > 0; id_jaccard = mean Jaccard of item-id sets between
consecutive rounds (same URL); price_changed = share of ids present in consecutive
rounds whose price differs; new_ids = ids not seen in the previous round.
"""
import json, sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "rounds.jsonl"
by = defaultdict(list)
for line in open(path, encoding="utf-8"):
    r = json.loads(line)
    by[(r["site"], r["url"])].append(r)

agg = defaultdict(lambda: dict(rounds=0, ok=0, n=[], secs=[], jac=[], chg=[], new=[], fields=set()))
for (site, url), rs in by.items():
    a = agg[site]
    prev = None
    for r in rs:
        a["rounds"] += 1
        ok = r["status"] == 200 and r["n"] > 0
        a["ok"] += ok
        a["n"].append(r["n"])
        a["secs"].append(r["secs"])
        cur = {str(i["id"]): i["price"] for i in r["items"]}
        for i in r["items"][:3]:
            a["fields"].update(k for k, v in (i.get("extra") or {}).items() if v not in (None, "", 0))
        if prev is not None and cur and prev:
            inter = set(cur) & set(prev)
            a["jac"].append(len(inter) / len(set(cur) | set(prev)))
            a["chg"].append(sum(cur[k] != prev[k] for k in inter) / max(1, len(inter)))
            a["new"].append(len(set(cur) - set(prev)))
        prev = cur if ok else prev

mean = lambda xs: round(sum(xs) / len(xs), 3) if xs else None
out = {}
for site, a in agg.items():
    out[site] = dict(rounds=a["rounds"], success_rate=round(a["ok"] / a["rounds"], 3), items_per_round=mean(a["n"]),
                     secs_per_page=mean(a["secs"]), id_jaccard=mean(a["jac"]), price_changed_share=mean(a["chg"]),
                     new_ids_per_round=mean(a["new"]), extra_fields=sorted(a["fields"]))
    print(f"{site:12}", json.dumps(out[site], ensure_ascii=False))
json.dump(out, open("continuity.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
