"""Cho Tot listing lifecycle: snapshot model pages 1-5 (snap TAG), later diff A vs B:
listings gone from the first pages, price edits, new listings. Then check whether gone listings' detail pages still exist."""
import json, time, sys
from fetch import get
from chotot_model import MODELS, full_items

mode = sys.argv[1]
if mode == "snap":
    tag, snap = sys.argv[2], {}
    for name, url in MODELS.items():
        for p in range(1, 6):
            st, h = get(url + (f"?page={p}" if p > 1 else ""))
            for it in full_items(h):
                snap[str(it["id"])] = dict(model=name, price=it["price"], age=it["age"], subject=(it["subject"] or "")[:50])
            time.sleep(4)
    json.dump(dict(ts=time.strftime("%H:%M:%S"), items=snap), open(f"chotot_life_{tag}.json", "w", encoding="utf-8"), ensure_ascii=False)
    print(tag, len(snap))
else:
    A = json.load(open("chotot_life_A.json", encoding="utf-8"))
    B = json.load(open("chotot_life_B.json", encoding="utf-8"))
    a, b = A["items"], B["items"]
    common = set(a) & set(b)
    pc = [k for k in common if a[k]["price"] != b[k]["price"]]
    gone = sorted(set(a) - set(b))
    # does a gone listing still have a page? (pushed down vs removed). Check up to 8, 4 s apart.
    checks = []
    for k in gone[:8]:
        time.sleep(4)
        try:
            st, h = get(f"https://www.chotot.com/{k}.htm")
            alive = str(k) in h and ("__NEXT_DATA__" in h)
            removed = any(w in h for w in ("Tin đã ẩn", "không còn tồn tại", "đã được bán", "Tin rao này đã"))
            checks.append(dict(id=k, status=st, alive=alive, removed_marker=removed))
        except Exception as e:
            checks.append(dict(id=k, status=str(e)[:40]))
    out = dict(window=f"{A['ts']} -> {B['ts']}", A=len(a), B=len(b), common=len(common), gone=len(gone), new=len(set(b) - set(a)),
               price_edits=len(pc), price_edit_examples=[dict(subject=a[k]["subject"], old=a[k]["price"], new=b[k]["price"]) for k in pc[:6]],
               gone_checks=checks)
    print(json.dumps(out, ensure_ascii=False, indent=1))
    json.dump(out, open("chotot_life_diff.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
