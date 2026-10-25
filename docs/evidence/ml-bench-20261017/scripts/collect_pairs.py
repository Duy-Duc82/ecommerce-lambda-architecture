"""Prices for the laptop pairs FPT Shop <-> Websosanh that share a unique part number (silver labels from
entity_bench.py): FPT product page JSON-LD offers.price, Websosanh compare page JSON-LD AggregateOffer.
Two passes, 3 h apart, so a price move on either side can be seen. Writes raw/pairs.jsonl.

usage: python collect_pairs.py <raw_dir>
"""
import json, re, sys, time
from pathlib import Path

from collect import fetch, now
from entity_bench import catalog, labels


def ld_offer(h):
    for blk in re.findall(r'<script[^>]*ld\+json[^>]*>(.*?)</script>', h, re.S):
        try:
            j = json.loads(blk)
        except ValueError:
            continue
        for x in (j if isinstance(j, list) else [j]):
            if isinstance(x, dict) and x.get("@type") == "Product":
                o = x.get("offers") or {}
                o = o[0] if isinstance(o, list) and o else o
                return dict(name=x.get("name"), price=o.get("price"), low=o.get("lowPrice"), high=o.get("highPrice"),
                            count=o.get("offerCount"), avail=str(o.get("availability", "")).split("/")[-1])
    return None


def main():
    raw = Path(sys.argv[1])
    d = json.load(open(Path(sys.argv[2]) if len(sys.argv) > 2 else raw / "entity_sitemaps.json", encoding="utf-8"))
    fpt, wss = catalog(d["fptshop"], "fptshop"), catalog(d["websosanh"], "websosanh")
    pairs = [(fpt[i]["url"], wss[j]["url"]) for i, j in labels(fpt, wss).items() if j is not None]
    with open(raw / "pairs.jsonl", "a", encoding="utf-8") as f:
        for p in range(2):
            t0 = time.time()
            for fu, wu in pairs:
                rec = dict(ts=now(), pass_no=p, fpt_url=fu, wss_url=wu)
                for key, url, delay in (("fpt", fu, 3), ("wss", wu, 2)):
                    st, h, secs = fetch(url)
                    rec[key] = dict(status=st, secs=secs, offer=ld_offer(h) if st == 200 else None)
                    time.sleep(delay)
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                f.flush()
            if p == 0:
                time.sleep(max(0, int(__import__("os").environ.get("PAIRS_GAP", 3 * 3600)) - (time.time() - t0)))


if __name__ == "__main__":
    main()
