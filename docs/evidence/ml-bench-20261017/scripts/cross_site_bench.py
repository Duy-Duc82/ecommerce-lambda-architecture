"""Cross-site price comparison on matched laptop pairs (FPT Shop offer vs Websosanh AggregateOffer).

Speed candidate: when a retailer price arrives, compare it with the market band held for the matched product
(lowPrice / highPrice / offerCount from the latest batch). Measured here: how often a real FPT price falls
outside the band or far from lowPrice, and how many prices moved between the two passes.

usage: python cross_site_bench.py <raw_dir> <results_dir>
"""
import json, sys
from pathlib import Path

import numpy as np


def main():
    raw, res = Path(sys.argv[1]), Path(sys.argv[2])
    recs = [json.loads(l) for l in open(raw / "pairs.jsonl", encoding="utf-8")]
    out = {}
    by_pass = {}
    for r in recs:
        by_pass.setdefault(r["pass_no"], []).append(r)
    for p, rs in sorted(by_pass.items()):
        ok = [r for r in rs if (r["fpt"]["offer"] or {}).get("price") and (r["wss"]["offer"] or {}).get("low")]
        ratio = np.array([float(r["fpt"]["offer"]["price"]) / float(r["wss"]["offer"]["low"]) for r in ok])
        inside = [float(r["wss"]["offer"]["low"]) <= float(r["fpt"]["offer"]["price"]) <= float(r["wss"]["offer"]["high"]) for r in ok]
        spread = np.array([float(r["wss"]["offer"]["high"]) / float(r["wss"]["offer"]["low"]) for r in ok])
        out[f"pass_{p}"] = dict(
            pairs=len(rs), fpt_ok=sum(1 for r in rs if r["fpt"]["status"] == 200), wss_ok=sum(1 for r in rs if r["wss"]["status"] == 200),
            both_priced=len(ok),
            fpt_over_wss_low=dict(p10=round(float(np.percentile(ratio, 10)), 3), median=round(float(np.median(ratio)), 3),
                                  p90=round(float(np.percentile(ratio, 90)), 3)) if len(ok) else None,
            fpt_inside_band_share=round(float(np.mean(inside)), 3) if ok else None,
            fpt_more_than_10pct_above_low=round(float(np.mean(ratio > 1.10)), 3) if ok else None,
            fpt_below_low=round(float(np.mean(ratio < 1.0)), 3) if ok else None,
            wss_high_over_low_median=round(float(np.median(spread)), 3) if ok else None,
            wss_offer_count_median=float(np.median([r["wss"]["offer"]["count"] or 0 for r in ok])) if ok else None,
            fetch_secs_median=dict(fpt=float(np.median([r["fpt"]["secs"] for r in rs])), wss=float(np.median([r["wss"]["secs"] for r in rs]))))
    if 0 in by_pass and 1 in by_pass:
        a = {r["fpt_url"]: r for r in by_pass[0]}
        moved = dict(fpt=0, wss_low=0, wss_count=0, compared=0)
        for r in by_pass[1]:
            q = a.get(r["fpt_url"])
            if not q or not (q["fpt"]["offer"] and r["fpt"]["offer"] and q["wss"]["offer"] and r["wss"]["offer"]):
                continue
            moved["compared"] += 1
            moved["fpt"] += q["fpt"]["offer"]["price"] != r["fpt"]["offer"]["price"]
            moved["wss_low"] += q["wss"]["offer"]["low"] != r["wss"]["offer"]["low"]
            moved["wss_count"] += q["wss"]["offer"]["count"] != r["wss"]["offer"]["count"]
        out["moved_between_passes"] = moved
    json.dump(out, open(res / "cross_site_bench.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps(out, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
