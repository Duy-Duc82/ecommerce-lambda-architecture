"""Tiki demand-signal density: snapshot N fixed categories (mode=snap A|B), then compare (mode=diff).
Share of products whose quantity_sold rose between snapshots, and price changes."""
import re, json, time, random, sys
from fetch import get
import crawl_round as cr

mode = sys.argv[1]
if mode == "snap":
    tag = sys.argv[2]
    _, x = get("https://tiki.vn/clover/sitemap_categories_1.xml")
    cats = re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
    random.seed(42)
    sample = random.sample(cats, 30) + ["https://tiki.vn/dien-thoai-may-tinh-bang/c1789", "https://tiki.vn/nha-sach-tiki/c8322",
                                         "https://tiki.vn/bach-hoa-online/c4384", "https://tiki.vn/lam-dep-suc-khoe/c1520"]
    snap = {}
    for u in sample:
        time.sleep(5)
        try:
            st, h = get(u)
            for it in (cr.p_tiki(h) if "__NEXT_DATA__" in h else []):
                snap[str(it["id"])] = dict(cat=u.split("/")[-1], price=it["price"], sold=it["extra"].get("sold"), name=it["name"][:50])
        except Exception as e:
            print("err", u, str(e)[:60])
    json.dump(dict(ts=time.strftime("%H:%M:%S"), items=snap), open(f"tiki_demand_{tag}.json", "w", encoding="utf-8"), ensure_ascii=False)
    print(tag, "products", len(snap), "with sold", sum(1 for v in snap.values() if v["sold"] is not None))
else:
    A = json.load(open("tiki_demand_A.json", encoding="utf-8"))
    B = json.load(open("tiki_demand_B.json", encoding="utf-8"))
    a, b = A["items"], B["items"]
    common = set(a) & set(b)
    ws = [k for k in common if a[k]["sold"] is not None and b[k]["sold"] is not None]
    up = [k for k in ws if b[k]["sold"] > a[k]["sold"]]
    down = [k for k in ws if b[k]["sold"] < a[k]["sold"]]
    pc = [k for k in common if a[k]["price"] != b[k]["price"]]
    top = sorted(up, key=lambda k: -(b[k]["sold"] - a[k]["sold"]))[:8]
    out = dict(window=f"{A['ts']} -> {B['ts']}", products_A=len(a), products_B=len(b), common=len(common), with_sold=len(ws),
               sold_up=len(up), sold_up_share=round(len(up) / max(1, len(ws)), 3), units_sold_total=sum(b[k]["sold"] - a[k]["sold"] for k in up),
               sold_down=len(down), price_changed=len(pc), price_changed_share=round(len(pc) / max(1, len(common)), 3),
               top_movers=[dict(name=a[k]["name"], cat=a[k]["cat"], delta=b[k]["sold"] - a[k]["sold"], price=b[k]["price"]) for k in top],
               price_examples=[dict(name=a[k]["name"], old=a[k]["price"], new=b[k]["price"]) for k in pc[:6]])
    print(json.dumps(out, ensure_ascii=False, indent=1))
    json.dump(out, open("tiki_demand_diff.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
